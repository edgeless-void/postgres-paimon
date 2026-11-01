// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_bgworker.cpp — PostgreSQL background worker for paimon_heap.
 *
 * Runs inside PostgreSQL (no external process).  Reads the shared-memory
 * ring buffer and dispatches each framed message to a PaimonTableWriter,
 * which writes Parquet files to the staging directory.
 *
 * After each flush the bgworker promotes the table from the staging directory
 * to the warehouse directory (data files moved, metadata copied, LATEST
 * last) and then offloads the warehouse copy to S3 / HDFS.
 *
 * Lake location of a PostgreSQL table <schema>.<table>:
 *   local:   <warehouse>/<db>.db/<table>      db = "default" for schema public,
 *                                             otherwise the schema name
 *   S3/HDFS: <prefix>/<table>                 for db "default" (unchanged layout)
 *            <prefix>/<db>.db/<table>         otherwise
 *
 * GUCs consumed (all PGC_SIGHUP so pg_reload_conf() picks them up):
 *   paimon_heap.warehouse   — final warehouse root  (default /tmp/paimon-warehouse)
 *   paimon_heap.staging_dir — local staging root    (default same as warehouse)
 */

/*
 * Include C++ / Arrow / Parquet headers FIRST, before postgres.h.
 * This avoids the NIL macro clash: postgres defines NIL as ((List*)NULL)
 * which interferes with parquet's enum member names inside namespaces.
 */
#include <string>
#include <unordered_map>
#include <memory>
#include <filesystem>
#include <cstring>
#include <cerrno>
#include <cstdio>
#include <fstream>

#include "paimon_event.h"        /* pure-C header, safe first */
#include "paimon_table_writer.h" /* pulls in Arrow/Parquet */
#include "paimon_offload_s3.h"
#include "paimon_offload_hdfs.h"
#include "paimon_offload_common.h"
#include <unordered_set>

/* PostgreSQL headers after C++ headers. */
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "tcop/tcopprot.h"
}

#include "paimon_shmem.h"
#include "paimon_ring.h"
#include "paimon_wal.h"

namespace fs = std::filesystem;

/* ── GUC values (read by bgworker process) ──────────────────────── */
static char *guc_warehouse   = NULL;
static char *guc_staging_dir = NULL;

/* Flush trigger thresholds */
static int   guc_flush_txns  = 100;  /* flush after N transactions; 0 = every txn */
static int   guc_flush_secs  = 30;   /* flush after X seconds; 0 = disabled */
static int   guc_min_rows    = 0;    /* min rows per file; 0 = no minimum */

/* S3 offload (optional) */
static char *guc_s3_bucket   = NULL;
static char *guc_s3_prefix   = NULL;
static char *guc_s3_region   = NULL;
static char *guc_s3_endpoint = NULL;

/* HDFS offload (optional) */
static char *guc_hdfs_namenode      = NULL;
static bool  guc_hdfs_use_https     = false;
static char *guc_hdfs_path          = NULL;
static char *guc_hdfs_user          = NULL;
static char *guc_hdfs_conf_dir      = NULL;
static char *guc_hdfs_krb_principal = NULL;
static char *guc_hdfs_krb_keytab    = NULL;

/*
 * Configurable names for the two built-in Paimon system columns.
 * Non-static so paimon_parquet.cpp and paimon_schema.cpp can read them
 * (they run only in the bgworker process).
 */
char *g_paimon_seq_col_guc = NULL;   /* default: "_seq"      */
char *g_paimon_rk_col_guc  = NULL;   /* default: "_row_kind" */

/* ── State ──────────────────────────────────────────────────────── */
static volatile sig_atomic_t g_got_sigterm = 0;

/* Throttle state — updated by message handlers, used by flush logic */
static uint32_t g_pending_txns    = 0;
static uint64_t g_pending_rows    = 0;
static time_t   g_last_flush_time = 0;

/* One lake table per PostgreSQL relation OID. */
struct TableEntry {
    std::unique_ptr<paimon::PaimonTableWriter> writer;
    std::string db;      /* Paimon database ("default" = schema public) */
    std::string table;   /* table name */

    /* Path below the warehouse / staging root. */
    std::string local_rel() const { return db + ".db/" + table; }
    /* Path below the S3 prefix / HDFS base path. */
    std::string remote_rel() const {
        return db == "default" ? table : db + ".db/" + table;
    }
};

static std::unordered_map<uint32_t, TableEntry> g_tables;

/* OIDs we already warned about (DML for a table without a lake schema). */
static std::unordered_set<uint32_t> g_warned_no_schema;

static std::string effective_warehouse(void) {
    if (guc_warehouse && guc_warehouse[0])
        return guc_warehouse;
    return "/tmp/paimon-warehouse";
}

/* Paimon FileSystemCatalog stores tables at warehouse/<db>.db/table
   (Hive-style database directories with a .db suffix).  PostgreSQL schema
   "public" maps to Spark's paimon.default database. */
static const char *PAIMON_DEFAULT_DB = "default";

static std::string effective_staging(void) {
    if (guc_staging_dir && guc_staging_dir[0])
        return guc_staging_dir;
    return effective_warehouse();   /* same dir when no staging configured */
}

/* ── Signal handler ─────────────────────────────────────────────── */
extern "C" {
static void bgworker_sigterm(SIGNAL_ARGS) {
    int saved_errno = errno;
    g_got_sigterm = 1;
    SetLatch(MyLatch);
    errno = saved_errno;
}
} /* extern "C" */

/* ── Wire decode helpers ─────────────────────────────────────────── */
struct Cursor {
    const uint8_t *p;
    const uint8_t *end;
    bool ok()           const { return p <= end; }
    bool need(size_t n) const { return (size_t)(end - p) >= n; }
    uint8_t  u8()  { if (!need(1)){p=end+1;return 0;} return *p++; }
    uint16_t u16() { if (!need(2)){p=end+1;return 0;}
                     uint16_t v = (uint16_t)p[0]|((uint16_t)p[1]<<8); p+=2; return v; }
    uint32_t u32() { if (!need(4)){p=end+1;return 0;}
                     uint32_t v = (uint32_t)p[0]|((uint32_t)p[1]<<8)|
                                  ((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
                     p+=4; return v; }
    int32_t  i32() { return (int32_t)u32(); }
    uint64_t u64() { if (!need(8)){p=end+1;return 0;}
                     uint64_t v = (uint64_t)u32(); v |= ((uint64_t)u32()<<32); return v; }
    std::string str() { uint16_t l=u16(); if(!need(l)){p=end+1;return {};}
                        std::string s((const char*)p,l); p+=l; return s; }
};

/* ── Table accessor ──────────────────────────────────────────────── */

/* Wire identifier "<db>\0<table>"; a bare name (older messages) = default db. */
static void
split_ident(const std::string &ident, std::string &db, std::string &table)
{
    auto z = ident.find('\0');
    if (z == std::string::npos) {
        db    = PAIMON_DEFAULT_DB;
        table = ident;
    } else {
        db    = ident.substr(0, z);
        table = ident.substr(z + 1);
    }
    if (db.empty()) db = PAIMON_DEFAULT_DB;
}

static TableEntry *
find_table(uint32_t oid)
{
    auto it = g_tables.find(oid);
    return it == g_tables.end() ? nullptr : &it->second;
}

/*
 * Look up the writer for oid, opening it from disk if this bgworker has not
 * seen the table yet (e.g. after a restart): the writer loads the schema,
 * latest snapshot and manifest list from the staging directory.
 */
static TableEntry *
get_or_create(uint32_t oid, const std::string &ident)
{
    if (TableEntry *e = find_table(oid)) return e;
    if (ident.empty()) return nullptr;

    TableEntry e;
    split_ident(ident, e.db, e.table);
    if (e.table.empty()) return nullptr;

    std::string staging_root = effective_staging() + "/" + e.local_rel();
    e.writer = std::make_unique<paimon::PaimonTableWriter>(staging_root, e.table);
    auto res = g_tables.emplace(oid, std::move(e));
    return &res.first->second;
}

/* ── Offload helpers ─────────────────────────────────────────────── */

static bool
s3_configured(void)   { return guc_s3_bucket && guc_s3_bucket[0]; }
static bool
hdfs_configured(void) { return guc_hdfs_path && guc_hdfs_path[0]; }

static paimon::S3Config
s3_config(void)
{
    paimon::S3Config s3;
    s3.bucket   = guc_s3_bucket;
    if (guc_s3_prefix   && guc_s3_prefix[0])   s3.prefix   = guc_s3_prefix;
    if (guc_s3_region   && guc_s3_region[0])   s3.region   = guc_s3_region;
    if (guc_s3_endpoint && guc_s3_endpoint[0]) s3.endpoint = guc_s3_endpoint;
    return s3;
}

static paimon::HdfsConfig
hdfs_config(void)
{
    paimon::HdfsConfig hdfs;
    if (guc_hdfs_namenode      && guc_hdfs_namenode[0])
        hdfs.namenode       = guc_hdfs_namenode;
    hdfs.use_https          = guc_hdfs_use_https;
    hdfs.base_path          = guc_hdfs_path;
    if (guc_hdfs_user         && guc_hdfs_user[0])
        hdfs.user           = guc_hdfs_user;
    if (guc_hdfs_conf_dir     && guc_hdfs_conf_dir[0])
        hdfs.conf_dir       = guc_hdfs_conf_dir;
    if (guc_hdfs_krb_principal && guc_hdfs_krb_principal[0])
        hdfs.krb_principal  = guc_hdfs_krb_principal;
    if (guc_hdfs_krb_keytab   && guc_hdfs_krb_keytab[0])
        hdfs.krb_keytab     = guc_hdfs_krb_keytab;
    return hdfs;
}

/*
 * Sync the warehouse copy of a table to every configured remote target.
 * Local data files are removed only once *all* targets have them — removing
 * them after the first target (as each target used to do by itself) left
 * the second target without the data.
 */
static void
offload_table(const TableEntry &e)
{
    if (!s3_configured() && !hdfs_configured())
        return;

    std::string local_root = effective_warehouse() + "/" + e.local_rel();
    bool all_ok = true;

    if (s3_configured() &&
        !paimon::s3_upload_table(s3_config(), local_root, e.remote_rel()))
        all_ok = false;

    if (hdfs_configured() &&
        !paimon::hdfs_upload_table(hdfs_config(), local_root, e.remote_rel()))
        all_ok = false;

    if (!all_ok)
        return;

    std::error_code ec;
    for (const auto &f : paimon::offload_collect(local_root))
        if (f.phase == paimon::OFFLOAD_DATA &&
            f.local.extension() == ".parquet")
            fs::remove(f.local, ec);
}

static bool
same_file_content(const fs::path &a, const fs::path &b)
{
    std::error_code ec;
    if (!fs::exists(b, ec)) return false;
    if (fs::file_size(a, ec) != fs::file_size(b, ec)) return false;
    std::ifstream fa(a, std::ios::binary), fb(b, std::ios::binary);
    std::string ca((std::istreambuf_iterator<char>(fa)), std::istreambuf_iterator<char>());
    std::string cb((std::istreambuf_iterator<char>(fb)), std::istreambuf_iterator<char>());
    return ca == cb;
}

/* Copy src → dst atomically (tmp + rename). */
static bool
copy_atomic(const fs::path &src, const fs::path &dst)
{
    std::error_code ec;
    fs::create_directories(dst.parent_path(), ec);
    fs::path tmp = dst;
    tmp += ".tmp";
    fs::copy_file(src, tmp, fs::copy_options::overwrite_existing, ec);
    if (ec) return false;
    fs::rename(tmp, dst, ec);
    return !ec;
}

/* ── Promote staging → warehouse ────────────────────────────────── */
/*
 * When staging_dir differs from the warehouse, bring the warehouse copy of
 * the table up to date: data files are moved, metadata files (which the
 * writer keeps reading from staging) are copied, bottom-up with LATEST last
 * so a reader of the warehouse never sees a snapshot whose files are not
 * there yet.  Then offload the warehouse copy to S3 / HDFS.
 */
static void
promote_table(const TableEntry &e)
{
    std::string staging_dir = effective_staging();
    std::string ware_dir    = effective_warehouse();

    if (staging_dir != ware_dir) {
        fs::path src_root = fs::path(staging_dir) / e.local_rel();
        fs::path dst_root = fs::path(ware_dir)    / e.local_rel();
        bool ok = true;
        int  phase = -1;

        for (const auto &f : paimon::offload_collect(src_root)) {
            if (f.phase != phase) {
                if (!ok) break;
                phase = f.phase;
            }
            fs::path dst = dst_root / f.rel;
            std::error_code ec;

            if (f.phase == paimon::OFFLOAD_DATA) {
                fs::create_directories(dst.parent_path(), ec);
                fs::rename(f.local, dst, ec);
                if (ec) {
                    ec.clear();
                    if (copy_atomic(f.local, dst)) fs::remove(f.local, ec);
                    else ok = false;
                }
            } else if (f.phase == paimon::OFFLOAD_MANIFEST) {
                if (!fs::exists(dst, ec) && !copy_atomic(f.local, dst))
                    ok = false;
            } else {
                if (!same_file_content(f.local, dst) && !copy_atomic(f.local, dst))
                    ok = false;
            }
        }
        if (!ok) {
            ereport(WARNING,
                    errmsg("paimon_bgworker: promoting %s to the warehouse failed; "
                           "will retry on the next flush", e.local_rel().c_str()));
            return;
        }
    }

    offload_table(e);
}

/* ── Message handlers ────────────────────────────────────────────── */
static void
handle_ddl_create(Cursor &c)
{
    uint32_t oid    = c.u32();
    std::string tname = c.str();
    uint32_t nf     = c.u32();
    std::vector<paimon::FieldDef> fields;
    fields.reserve(nf);
    for (uint32_t i = 0; i < nf; ++i) {
        paimon::FieldDef f;
        f.field_id  = c.u16();
        f.type_code = (PaimonTypeCode)c.u8();
        f.typmod    = c.u32();
        f.not_null  = (c.u8() != 0);
        f.name      = c.str();
        fields.push_back(f);
    }
    uint32_t npk = c.u32();
    std::vector<uint16_t> pk_attnums;
    for (uint32_t i = 0; i < npk; ++i) pk_attnums.push_back(c.u16());
    if (!c.ok()) return;

    std::vector<std::string> pk_names;
    for (uint16_t an : pk_attnums)
        for (const auto &f : fields)
            if (f.field_id == an) { pk_names.push_back(f.name); break; }

    TableEntry *e = get_or_create(oid, tname);
    if (!e) return;
    try {
        bool    had    = e->writer->hasSchema();
        int64_t before = e->writer->schemaId();
        e->writer->initSchema(fields, pk_names);
        g_warned_no_schema.erase(oid);
        /* Publish a new / changed schema right away. */
        if (!had || e->writer->schemaId() != before)
            promote_table(*e);
    }
    catch (const std::exception &ex) {
        ereport(WARNING, errmsg("paimon_bgworker: initSchema(%s): %s",
                                e->local_rel().c_str(), ex.what()));
    }
}

/* ── Flush helpers ───────────────────────────────────────────────── */
static void
do_flush_all(void)
{
    for (auto &[oid, e] : g_tables) {
        try { e.writer->flush(); }
        catch (const std::exception &ex) {
            ereport(WARNING, errmsg("paimon_bgworker: flush: %s", ex.what()));
        }
    }
    for (auto &[oid, e] : g_tables) {
        try { promote_table(e); }
        catch (const std::exception &ex) {
            ereport(WARNING, errmsg("paimon_bgworker: promote %s: %s",
                                    e.local_rel().c_str(), ex.what()));
        }
    }
    g_pending_txns    = 0;
    g_pending_rows    = 0;
    g_last_flush_time = time(NULL);
}

static void
handle_dml_batch(Cursor &c)
{
    uint32_t    oid   = c.u32();
    std::string tname = c.str();
    c.u32();          /* schema_version */
    uint32_t nrows = c.u32();
    if (!c.ok()) return;

    /*
     * Open the table from disk if needed: after a bgworker restart the
     * backends do not re-send a schema they already delivered.
     */
    TableEntry *e = get_or_create(oid, tname);
    paimon::PaimonTableWriter *tw = e ? e->writer.get() : nullptr;
    if (tw && !tw->hasSchema()) {
        if (g_warned_no_schema.insert(oid).second)
            ereport(WARNING,
                    errmsg("paimon_bgworker: no lake schema for relation %u (%s); "
                           "discarding its rows until the schema arrives",
                           oid, e->local_rel().c_str()));
        tw = nullptr;
    }

    for (uint32_t r = 0; r < nrows; ++r) {
        PaimonRowKind kind  = (PaimonRowKind)c.u8();
        uint64_t      seq   = c.u64();
        uint16_t      ncols = c.u16();

        std::vector<uint16_t>        attnums(ncols);
        std::vector<PaimonTypeCode>  types(ncols);
        std::vector<int32_t>         lengths(ncols);
        std::vector<std::string>     values(ncols);

        for (uint16_t ci = 0; ci < ncols; ++ci) {
            attnums[ci] = c.u16();
            types[ci]   = (PaimonTypeCode)c.u8();
            int32_t len = c.i32();
            lengths[ci] = len;
            if (len > 0) {
                if (!c.need((size_t)len)) return;
                values[ci].assign((const char *)c.p, len);
                c.p += len;
            }
        }
        if (tw) tw->addRow(kind, seq, attnums, types, lengths, values);
    }
    g_pending_rows += nrows;
}

static void
handle_sync(Cursor &c)
{
    (void)c;
    g_pending_txns++;

    time_t now = time(NULL);

    /* Flush condition: a threshold is reached AND the row minimum is satisfied. */
    bool by_count = (guc_flush_txns == 0) ||
                    ((uint32_t)guc_flush_txns > 0 &&
                     g_pending_txns >= (uint32_t)guc_flush_txns);
    bool by_time  = (guc_flush_secs > 0) && (g_last_flush_time > 0) &&
                    ((now - g_last_flush_time) >= (time_t)guc_flush_secs);
    bool rows_ok  = (guc_min_rows <= 0) ||
                    (g_pending_rows >= (uint64_t)guc_min_rows);

    /*
     * Flush when (count or time threshold reached) AND row minimum satisfied.
     * Time-based flush ignores the row minimum to prevent indefinite deferral.
     */
    if ((by_count || by_time) && (rows_ok || by_time))
        do_flush_all();
}

static void
handle_ddl_add(Cursor &c)
{
    uint32_t oid = c.u32();
    paimon::FieldDef f;
    f.field_id  = c.u16();
    f.type_code = (PaimonTypeCode)c.u8();
    f.typmod    = c.u32();
    f.not_null  = (c.u8() != 0);
    f.name      = c.str();
    if (!c.ok()) return;
    /* Legacy message (pre-reconcile builds, recovery journal). */
    if (TableEntry *e = find_table(oid)) {
        e->writer->ddlAddField(f);
        promote_table(*e);
    }
}

static void
handle_ddl_drop(Cursor &c)
{
    uint32_t oid    = c.u32();
    uint16_t attnum = c.u16();
    if (!c.ok()) return;
    /* Legacy message (pre-reconcile builds, recovery journal). */
    if (TableEntry *e = find_table(oid)) {
        e->writer->ddlDropField(attnum);
        promote_table(*e);
    }
}

static void
handle_ddl_rename(Cursor &c)
{
    uint32_t    oid  = c.u32();
    uint16_t    an   = c.u16();
    std::string name = c.str();
    if (!c.ok()) return;
    /* Legacy message (pre-reconcile builds, recovery journal). */
    if (TableEntry *e = find_table(oid)) {
        e->writer->ddlRenameField(an, name);
        promote_table(*e);
    }
}

static void
handle_ddl_type(Cursor &c)
{
    uint32_t       oid    = c.u32();
    uint16_t       an     = c.u16();
    PaimonTypeCode tc     = (PaimonTypeCode)c.u8();
    uint32_t       typmod = c.u32();
    if (!c.ok()) return;
    /* Legacy message (pre-reconcile builds, recovery journal). */
    if (TableEntry *e = find_table(oid)) {
        e->writer->ddlChangeType(an, tc, typmod);
        promote_table(*e);
    }
}

static void
handle_ddl_truncate(Cursor &c)
{
    uint32_t    oid   = c.u32();
    std::string tname = c.str();
    if (!c.ok()) return;
    TableEntry *e = get_or_create(oid, tname);
    if (!e) return;
    e->writer->truncate();
    /* Publish the empty snapshot to the warehouse / S3 / HDFS. */
    promote_table(*e);
}

/*
 * DROP TABLE: the lake table disappears everywhere — local staging,
 * warehouse and S3/HDFS lose snapshot/, manifest/ and schema/, so readers no
 * longer see the table and a re-created table with the same name starts from
 * clean metadata.  Data files (bucket-N/) are left in place.  Rows still
 * buffered for the table are discarded.
 */
static void
handle_ddl_drop_table(Cursor &c)
{
    uint32_t    oid   = c.u32();
    std::string tname = c.str();
    if (!c.ok()) return;
    TableEntry *e = get_or_create(oid, tname);
    if (!e) return;

    e->writer->dropMetadataOnly();

    std::error_code ec;
    if (effective_staging() != effective_warehouse()) {
        fs::path ware_root = fs::path(effective_warehouse()) / e->local_rel();
        for (const auto &dir : paimon::offload_metadata_dirs())
            fs::remove_all(ware_root / dir, ec);
    }

    try {
        if (s3_configured() &&
            !paimon::s3_drop_table_metadata(s3_config(), e->remote_rel()))
            ereport(WARNING,
                    errmsg("paimon_bgworker: DROP TABLE %s: S3 metadata not removed",
                           e->local_rel().c_str()));
        if (hdfs_configured() &&
            !paimon::hdfs_drop_table_metadata(hdfs_config(), e->remote_rel()))
            ereport(WARNING,
                    errmsg("paimon_bgworker: DROP TABLE %s: HDFS metadata not removed",
                           e->local_rel().c_str()));
    }
    catch (const std::exception &ex) {
        ereport(WARNING, errmsg("paimon_bgworker: DROP TABLE %s: %s",
                                e->local_rel().c_str(), ex.what()));
    }

    g_tables.erase(oid);
    g_warned_no_schema.erase(oid);
}

/* ── Main dispatcher ─────────────────────────────────────────────── */
static void
dispatch_message(const char *msg, size_t len)
{
    /* First 4 bytes are total_len (already validated by ring reader). */
    const uint8_t *data = (const uint8_t *)msg + 4;
    size_t         plen = len - 4;
    if (plen < 1) return;

    uint8_t  msg_type = data[0];
    Cursor c{ data + 1, data + plen };

    /*
     * C++ exceptions (filesystem, Arrow, ...) must not escape: PG_TRY in the
     * main loop only catches ereport(ERROR), and an uncaught exception would
     * terminate the process and force a crash restart of the whole cluster.
     */
    try {
    switch ((PaimonMsgType)msg_type) {
        case PMSG_DDL_CREATE:    handle_ddl_create(c);   break;
        case PMSG_DML_BATCH:     handle_dml_batch(c);    break;
        case PMSG_SYNC:          handle_sync(c);         break;
        case PMSG_DDL_ADD_COL:   handle_ddl_add(c);      break;
        case PMSG_DDL_DROP_COL:  handle_ddl_drop(c);     break;
        case PMSG_DDL_RENAME:    handle_ddl_rename(c);   break;
        case PMSG_DDL_TYPE:      handle_ddl_type(c);     break;
        case PMSG_DDL_TRUNCATE:  handle_ddl_truncate(c); break;
        case PMSG_DDL_DROP_TBL:  handle_ddl_drop_table(c); break;
        default:
            ereport(DEBUG1, errmsg("paimon_bgworker: unknown msg_type %d",
                                   (int)msg_type));
            break;
    }
    }
    catch (const std::exception &ex) {
        ereport(WARNING, errmsg("paimon_bgworker: message type %d failed: %s",
                                (int)msg_type, ex.what()));
    }
    catch (...) {
        ereport(WARNING, errmsg("paimon_bgworker: message type %d failed",
                                (int)msg_type));
    }
}

/* ── Background worker entry point ──────────────────────────────── */
extern "C"
PGDLLEXPORT void
paimon_bgworker_main(Datum arg)
{
    /* Install signal handlers. */
    pqsignal(SIGTERM, bgworker_sigterm);
    pqsignal(SIGHUP,  SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();

    /* Initialize cloud-offload SDKs. */
    paimon::s3_sdk_init();

    /* Attach to shared memory. */
    paimon_shmem_init();

    PaimonRingHdr *hdr = paimon_ring_hdr();
    if (!hdr)
    {
        ereport(LOG, errmsg("paimon_bgworker: shared memory not available, exiting"));
        proc_exit(0);
    }

    ereport(LOG, errmsg("paimon_bgworker: started, warehouse=%s staging=%s",
                        effective_warehouse().c_str(),
                        effective_staging().c_str()));

    /* Replay any messages written to the recovery journal during crash recovery. */
    paimon_wal_replay_journal();

    /* Advertise our PID so ring writers can signal our latch. */
    pg_atomic_write_u32(&hdr->bgworker_pid, (uint32) MyProcPid);
    g_last_flush_time = time(NULL);

    while (!g_got_sigterm)
    {
        char   *msg      = NULL;
        size_t  mlen     = 0;

        /*
         * Compute how long to sleep: wake up at most flush_secs from the last
         * flush, or after 1 second minimum so we check g_got_sigterm regularly.
         */
        long timeout_ms = 1000L;
        if (guc_flush_secs > 0 && g_last_flush_time > 0)
        {
            time_t now  = time(NULL);
            time_t next = g_last_flush_time + (time_t)guc_flush_secs;
            long   rem  = (long)((next - now) * 1000L);
            if (rem < 100L)   rem = 100L;
            if (rem < timeout_ms) timeout_ms = rem;
        }

        PG_TRY();
        {
            mlen = paimon_ring_read_msg_timeout(&msg, timeout_ms);
        }
        PG_CATCH();
        {
            /* Interrupted (SIGTERM / query cancel).  Exit cleanly. */
            g_got_sigterm = 1;
            FlushErrorState();
        }
        PG_END_TRY();

        if (g_got_sigterm)
            break;

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (mlen > 0)
        {
            PG_TRY();
            {
                dispatch_message(msg, mlen);
            }
            PG_CATCH();
            {
                /* Don't crash the bgworker on a single bad message. */
                ereport(WARNING,
                        errmsg("paimon_bgworker: error processing message (ignored)"));
                FlushErrorState();
            }
            PG_END_TRY();

            pfree(msg);
        }
        else
        {
            /* Timeout — perform a time-based flush if data is pending. */
            if (guc_flush_secs > 0 && g_pending_rows > 0 && g_last_flush_time > 0)
            {
                time_t now = time(NULL);
                if ((now - g_last_flush_time) >= (time_t)guc_flush_secs)
                    do_flush_all();
            }
        }

        uint32_t dropped = pg_atomic_exchange_u32(&hdr->dropped_msgs, 0);
        if (dropped > 0)
            ereport(WARNING,
                    errmsg("paimon_bgworker: %u ring-buffer messages were dropped "
                           "(increase paimon_heap.ring_size_mb)", dropped));
    }

    /* Clear PID so producers stop trying to signal us. */
    pg_atomic_write_u32(&hdr->bgworker_pid, 0);

    /* Flush and publish everything before exiting. */
    do_flush_all();

    /* Shut down cloud-offload SDKs. */
    paimon::s3_sdk_shutdown();

    ereport(LOG, errmsg("paimon_bgworker: shutting down"));
    /*
     * Exit code 1, not 0: a worker that exits with 0 is unregistered by the
     * postmaster and never restarted, so a stray SIGTERM (pg_terminate_backend,
     * an operator's kill) would silently stop all lake mirroring until the
     * next server restart.  With 1 it is restarted after bgw_restart_time;
     * during server shutdown nothing is restarted either way.
     */
    proc_exit(1);
}

/*
 * Register the warehouse / staging GUCs.
 * Called from paimon_heap.cpp _PG_init so these GUCs are defined in every
 * process (backends need to read staging_dir to know if they should bother
 * connecting to the ring).
 */
extern "C"
PGDLLEXPORT void
paimon_bgworker_define_gucs(void)
{
    DefineCustomStringVariable(
        "paimon_heap.warehouse",
        "Root directory for the Paimon lake warehouse",
        NULL,
        &guc_warehouse,
        "/tmp/paimon-warehouse",
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "paimon_heap.staging_dir",
        "Local staging directory for Parquet files before promotion "
        "(empty = write directly to warehouse)",
        NULL,
        &guc_staging_dir,
        "",
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "paimon_heap.flush_interval_txns",
        "Number of committed transactions to batch before writing a Parquet file "
        "(0 = flush after every transaction)",
        NULL,
        &guc_flush_txns,
        100, 0, INT_MAX,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "paimon_heap.flush_interval_secs",
        "Maximum seconds to buffer rows before flushing, regardless of transaction count "
        "(0 = no time-based flush)",
        NULL,
        &guc_flush_secs,
        30, 0, INT_MAX,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "paimon_heap.min_rows_per_file",
        "Minimum buffered rows required before writing a Parquet file; "
        "the time-based flush ignores this limit to prevent indefinite deferral "
        "(0 = no minimum)",
        NULL,
        &guc_min_rows,
        0, 0, INT_MAX,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "paimon_heap.seq_field",
        "Parquet column name for the Paimon sequence field (default: _seq)",
        NULL, &g_paimon_seq_col_guc, "_seq", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.row_kind_field",
        "Parquet column name for the row-kind field (default: _row_kind)",
        NULL, &g_paimon_rk_col_guc, "_row_kind", PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "paimon_heap.s3_bucket",
        "S3 bucket to sync the warehouse to after each flush (empty = local only)",
        NULL, &guc_s3_bucket, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.s3_prefix",
        "Key prefix inside the S3 bucket (empty = bucket root)",
        NULL, &guc_s3_prefix, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.s3_region",
        "AWS region for S3 upload (empty = AWS CLI default)",
        NULL, &guc_s3_region, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.s3_endpoint",
        "Custom S3 endpoint URL, e.g. for MinIO (empty = AWS default)",
        NULL, &guc_s3_endpoint, "", PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "paimon_heap.hdfs_namenode",
        "WebHDFS namenode host:port (empty = auto-discover from hdfs-site.xml)",
        NULL, &guc_hdfs_namenode, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable(
        "paimon_heap.hdfs_use_https",
        "Use HTTPS for WebHDFS connections (default false)",
        NULL, &guc_hdfs_use_https, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.hdfs_path",
        "Base HDFS path for the warehouse (empty = HDFS offload disabled)",
        NULL, &guc_hdfs_path, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.hdfs_user",
        "HDFS user name for WebHDFS (empty = OS user)",
        NULL, &guc_hdfs_user, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.hdfs_conf_dir",
        "Directory containing hdfs-site.xml (empty = search standard locations)",
        NULL, &guc_hdfs_conf_dir, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.hdfs_krb_principal",
        "Kerberos principal for HDFS SPNEGO auth, e.g. hdfs/host@REALM (empty = simple auth)",
        NULL, &guc_hdfs_krb_principal, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.hdfs_krb_keytab",
        "Absolute path to Kerberos keytab file for HDFS auth (empty = simple auth)",
        NULL, &guc_hdfs_krb_keytab, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
}
