// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_heap — PostgreSQL Table Access Method that mirrors writes to a
 * local Paimon lake warehouse via a built-in background worker.
 *
 * Architecture
 * ────────────
 *  PostgreSQL backend   →  shared-memory ring buffer  →  paimon_bgworker
 *  (DML via TAM)            (32 MB, non-blocking)        (writes Parquet)
 *
 * The backend accumulates rows in process-local memory during a transaction.
 * At COMMIT the batch is serialised and pushed into the ring buffer in one
 * operation (fire-and-forget, never blocks the committing backend).
 * The bgworker drains the ring and calls PaimonTableWriter to write Parquet
 * files to a local staging directory; completed files are promoted to the
 * final warehouse directory (which may be on a remote/S3 filesystem).
 *
 * REQUIRED:  Add to postgresql.conf
 *   shared_preload_libraries = 'paimon_heap'
 *
 * GUCs (all PGC_SIGHUP except ring_size_mb which is PGC_POSTMASTER):
 *   paimon_heap.warehouse     — final warehouse root (default /tmp/paimon-warehouse)
 *   paimon_heap.staging_dir   — local staging dir   (default: same as warehouse)
 *   paimon_heap.ring_size_mb  — ring buffer size MB (default 32, PGC_POSTMASTER)
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/relation.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(paimon_heap_handler);

void _PG_init(void);

/* Forward-declared so we can reference them from _PG_init. */
extern void paimon_bgworker_main(Datum arg);
extern void paimon_bgworker_define_gucs(void);
extern void paimon_shmem_define_ring_guc(void);
extern void paimon_ddl_init(void);
} // extern "C"

#include "paimon_client.h"
#include "paimon_relmeta.h"
#include "paimon_shmem.h"
#include "paimon_wal.h"

#include <string>
#include <vector>
#include <cstring>

/* ── Metadata field GUCs ───────────────────────────────────────────── */
static char *guc_meta_xid_field = NULL;
static char *guc_meta_op_field  = NULL;
static char *guc_meta_tid_field = NULL;

/* Reserved field_id values for metadata columns (above any pg attnum). */
#define PAIMON_META_FID_XID  ((uint16_t)0xFF00u)   /* 65280 */
#define PAIMON_META_FID_OP   ((uint16_t)0xFF01u)   /* 65281 */
#define PAIMON_META_FID_TID  ((uint16_t)0xFF02u)   /* 65282 */

/* ── Shmem hooks ──────────────────────────────────────────────────── */
static shmem_request_hook_type prev_shmem_request  = NULL;
static shmem_startup_hook_type prev_shmem_startup  = NULL;

static void
paimon_shmem_request_hook(void)
{
    if (prev_shmem_request)
        prev_shmem_request();
    paimon_shmem_request();   /* RequestAddinShmemSpace + LWLock tranche */
}

static void
paimon_shmem_startup_hook(void)
{
    if (prev_shmem_startup)
        prev_shmem_startup();
    paimon_shmem_init();      /* ShmemInitStruct + GetNamedLWLockTranche */
}

static void paimon_xact_callback(XactEvent event, void *arg);
static void paimon_subxact_callback(SubXactEvent event,
                                    SubTransactionId mySubid,
                                    SubTransactionId parentSubid,
                                    void *arg);

/* ── _PG_init ─────────────────────────────────────────────────────── */
extern "C" void
_PG_init(void)
{
    /* GUCs must be defined here (before hooks run). */
    paimon_shmem_define_ring_guc();
    paimon_bgworker_define_gucs();
    paimon_wal_init();

    DefineCustomStringVariable(
        "paimon_heap.meta_field_xid",
        "Column name for the transaction ID metadata field (empty = disabled)",
        NULL, &guc_meta_xid_field, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.meta_field_op",
        "Column name for the operation sequence number metadata field (empty = disabled)",
        NULL, &guc_meta_op_field, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable(
        "paimon_heap.meta_field_tid",
        "Column name for the physical TID (block<<16|offset) metadata field (empty = disabled)",
        NULL, &guc_meta_tid_field, "", PGC_USERSET, 0, NULL, NULL, NULL);

    /* PG 15+: request memory from shmem_request_hook, not _PG_init. */
    prev_shmem_request   = shmem_request_hook;
    shmem_request_hook   = paimon_shmem_request_hook;

    prev_shmem_startup   = shmem_startup_hook;
    shmem_startup_hook   = paimon_shmem_startup_hook;

    /* DDL intercept hook. */
    paimon_ddl_init();

    /*
     * Transaction callbacks are registered up front (not lazily on the first
     * DML) so that savepoints started before the first paimon_heap write are
     * tracked, and DDL-only transactions are delivered too.
     */
    RegisterXactCallback(paimon_xact_callback, NULL);
    RegisterSubXactCallback(paimon_subxact_callback, NULL);

    /* Register the background worker. */
    BackgroundWorker bgw;
    memset(&bgw, 0, sizeof(bgw));
    bgw.bgw_flags        = BGWORKER_SHMEM_ACCESS;
    bgw.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    bgw.bgw_restart_time = 5;   /* restart 5 s after crash */
    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "paimon_heap");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "paimon_bgworker_main");
    snprintf(bgw.bgw_name, BGW_MAXLEN, "paimon writer");
    snprintf(bgw.bgw_type, BGW_MAXLEN, "paimon writer");
    bgw.bgw_main_arg     = (Datum) 0;
    RegisterBackgroundWorker(&bgw);
}

/* ── Transaction callbacks ────────────────────────────────────────── */

static void
paimon_xact_callback(XactEvent event, void *arg)
{
    PaimonClient &client = PaimonClient::instance();

    switch (event) {
        case XACT_EVENT_PRE_COMMIT:
            /*
             * Serialise queued rows + DDL → pending_msgs_.
             * When wal_level = 'local', also writes a WAL record so the
             * messages survive a crash.  Fires before RecordTransactionCommit,
             * so the WAL flush is absorbed into the transaction's commit fsync.
             */
            client.prepareCommit();
            break;
        case XACT_EVENT_COMMIT:
            /* Push pending_msgs_ (built at PRE_COMMIT) to the ring buffer. */
            client.flushAll();
            break;
        case XACT_EVENT_PARALLEL_COMMIT:
            /*
             * Parallel workers have no PARALLEL_PRE_COMMIT event, so do both
             * steps here.  WAL durability is best-effort in parallel workers.
             */
            client.prepareCommit();
            client.flushAll();
            break;
        case XACT_EVENT_PRE_PREPARE:
            /*
             * The ring is fed at COMMIT by the backend that did the work; a
             * prepared transaction is committed later (possibly by another
             * backend, possibly never), so its lake changes could neither be
             * delivered nor safely kept.  Refuse, like PostgreSQL does for
             * temporary tables.
             */
            if (client.hasPending())
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("cannot PREPARE a transaction that has modified "
                                "paimon_heap tables")));
            break;
        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
        case XACT_EVENT_PREPARE:
            client.discardAll();
            break;
        default:
            break;
    }
}

static void
paimon_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                        SubTransactionId parentSubid, void *arg)
{
    PaimonClient &client = PaimonClient::instance();

    switch (event) {
        case SUBXACT_EVENT_START_SUB:
            client.startSubXact((uint32_t) mySubid);
            break;
        case SUBXACT_EVENT_COMMIT_SUB:
            client.commitSubXact((uint32_t) mySubid);
            break;
        case SUBXACT_EVENT_ABORT_SUB:
            client.abortSubXact((uint32_t) mySubid);
            break;
        default:
            break;
    }
}

/* Kept for the DML wrappers; callbacks are registered in _PG_init. */
static inline void
ensure_xact_cb(void)
{
}

/* ── Type mapping (PG OID → Paimon type code + typmod) ─────────────── */
static PaimonTypeCode
pg_oid_to_ptype(Oid typoid, uint32_t *typmod_out, int32_t pg_typmod)
{
    *typmod_out = 0;
    switch (typoid) {
        case BOOLOID:        return PTYPE_BOOLEAN;
        case INT2OID:        return PTYPE_SMALLINT;
        case INT4OID:        return PTYPE_INT;
        case INT8OID:        return PTYPE_BIGINT;
        case FLOAT4OID:      return PTYPE_FLOAT;
        case FLOAT8OID:      return PTYPE_DOUBLE;
        case NUMERICOID:
            if (pg_typmod > 4) {
                int raw   = (int)(pg_typmod - 4);
                int prec  = (raw >> 16) & 0xFFFF;
                int scale = raw & 0xFFFF;
                *typmod_out = ((uint32_t)prec << 16) | (uint32_t)scale;
            }
            return PTYPE_DECIMAL;
        case TEXTOID: case VARCHAROID: case BPCHAROID: case UUIDOID:
            return PTYPE_STRING;
        case BYTEAOID:       return PTYPE_BYTES;
        case DATEOID:        return PTYPE_DATE;
        case TIMESTAMPOID:   return PTYPE_TIMESTAMP;
        case TIMESTAMPTZOID: return PTYPE_TIMESTAMPTZ;
        default:             return PTYPE_STRING;
    }
}

/* ── Datum → wire bytes ─────────────────────────────────────────────── */
static void
serialize_datum(Oid typoid, Datum d, bool isnull,
                int32_t *out_len, std::string *out_val)
{
    if (isnull) { *out_len = -1; return; }

    switch (typoid) {
        case BOOLOID: {
            char v = (char)DatumGetBool(d);
            *out_len = 1; out_val->assign(1, v); return;
        }
        case INT2OID: {
            int16_t v = DatumGetInt16(d);
            *out_len = 2; out_val->assign((char*)&v, 2); return;
        }
        case INT4OID: {
            int32_t v = DatumGetInt32(d);
            *out_len = 4; out_val->assign((char*)&v, 4); return;
        }
        case INT8OID: {
            int64_t v = DatumGetInt64(d);
            *out_len = 8; out_val->assign((char*)&v, 8); return;
        }
        case FLOAT4OID: {
            float v = DatumGetFloat4(d);
            *out_len = 4; out_val->assign((char*)&v, 4); return;
        }
        case FLOAT8OID: {
            double v = DatumGetFloat8(d);
            *out_len = 8; out_val->assign((char*)&v, 8); return;
        }
        case DATEOID: {
            int32_t v = (int32_t)DatumGetDateADT(d) + 10957;
            *out_len = 4; out_val->assign((char*)&v, 4); return;
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            int64_t v = DatumGetTimestamp(d) + INT64CONST(946684800000000);
            *out_len = 8; out_val->assign((char*)&v, 8); return;
        }
        case TEXTOID: case VARCHAROID: case BPCHAROID: case UUIDOID: {
            text *t = DatumGetTextPP(d);
            int32_t len = VARSIZE_ANY_EXHDR(t);
            *out_len = len;
            out_val->assign(VARDATA_ANY(t), len); return;
        }
        case BYTEAOID: {
            bytea *b = DatumGetByteaPP(d);
            int32_t len = VARSIZE_ANY_EXHDR(b);
            *out_len = len;
            out_val->assign(VARDATA_ANY(b), len); return;
        }
        case NUMERICOID: {
            char *s = DatumGetCString(DirectFunctionCall1(numeric_out, d));
            *out_len = (int32_t)strlen(s);
            *out_val = s;
            pfree(s); return;
        }
        default: {
            Oid outfunc; bool typIsVarlena;
            getTypeOutputInfo(typoid, &outfunc, &typIsVarlena);
            char *s = OidOutputFunctionCall(outfunc, d);
            *out_len = (int32_t)strlen(s);
            *out_val = s;
            pfree(s); return;
        }
    }
}

/* ── Schema fingerprint ─────────────────────────────────────────────── */
/*
 * Covers everything that ends up in the Paimon schema (attnum, type, typmod,
 * nullability, name), so any difference makes the backend re-send the full
 * schema and the bgworker reconciles — even if an individual DDL event was
 * never applied (e.g. it was dropped from a full ring).
 */
uint32_t
paimon_schema_fingerprint(TupleDesc td)
{
    uint32_t h = 0x811c9dc5u;
    for (int i = 0; i < td->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(td, i);
        if (att->attisdropped) continue;
        h ^= (uint32_t)att->attnum;    h *= 0x01000193u;
        h ^= (uint32_t)att->atttypid;  h *= 0x01000193u;
        h ^= (uint32_t)att->atttypmod; h *= 0x01000193u;
        h ^= (uint32_t)att->attnotnull; h *= 0x01000193u;
        for (const char *c = NameStr(att->attname); *c; c++) {
            h ^= (uint8_t)*c; h *= 0x01000193u;
        }
    }
    /* Include presence of metadata fields so schema re-bootstraps if config changes. */
    uint32_t meta = ((guc_meta_xid_field && guc_meta_xid_field[0]) ? 1u : 0u)
                  | ((guc_meta_op_field  && guc_meta_op_field[0])  ? 2u : 0u)
                  | ((guc_meta_tid_field && guc_meta_tid_field[0]) ? 4u : 0u);
    h ^= meta; h *= 0x01000193u;
    return h;
}

/* ── Lake identity of a relation ────────────────────────────────────── */

static std::string
make_ident(Oid nspid, const char *relname)
{
    std::string db = "default";
    char *nsp = get_namespace_name(nspid);
    if (nsp && strcmp(nsp, "public") != 0)
        db = nsp;
    std::string ident = db;
    ident.push_back('\0');
    ident += relname;
    return ident;
}

std::string
paimon_rel_ident(Relation rel)
{
    return make_ident(RelationGetNamespace(rel), RelationGetRelationName(rel));
}

std::string
paimon_rel_ident_oid(Oid relid)
{
    char *relname = get_rel_name(relid);
    if (!relname)
        return std::string();
    return make_ident(get_rel_namespace(relid), relname);
}

/*
 * make_new_heap() names the transient heap of a table rewrite
 * "pg_temp_<oid of the table being rewritten>".
 */
static bool
is_transient_rewrite_heap(Oid relid, const char *relname)
{
    if (strncmp(relname, "pg_temp_", 8) != 0)
        return false;
    const char *p = relname + 8;
    if (*p == '\0')
        return false;
    for (const char *q = p; *q; q++)
        if (*q < '0' || *q > '9')
            return false;
    Oid orig = (Oid) strtoul(p, NULL, 10);
    return OidIsValid(orig) && orig != relid && get_rel_name(orig) != NULL;
}

bool
paimon_rel_is_mirrored(Relation rel)
{
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
        return false;
    if (rel->rd_rel->relkind != RELKIND_RELATION)
        return false;
    return !is_transient_rewrite_heap(RelationGetRelid(rel),
                                      RelationGetRelationName(rel));
}

bool
paimon_relid_is_mirrored(Oid relid)
{
    if (get_rel_persistence(relid) == RELPERSISTENCE_TEMP)
        return false;
    if (get_rel_relkind(relid) != RELKIND_RELATION)
        return false;
    char *relname = get_rel_name(relid);
    return relname && !is_transient_rewrite_heap(relid, relname);
}

/* ── Column metadata from Relation ─────────────────────────────────── */
void
paimon_build_col_meta(Relation rel,
               std::vector<PaimonColumn> &cols,
               std::vector<uint16_t>     &pk_attnums)
{
    TupleDesc td = RelationGetDescr(rel);
    for (int i = 0; i < td->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(td, i);
        if (att->attisdropped) continue;
        PaimonColumn c;
        c.attnum    = (uint16_t)att->attnum;
        c.type_code = pg_oid_to_ptype(att->atttypid, &c.typmod,
                                       (int32_t)att->atttypmod);
        c.not_null  = att->attnotnull;
        c.name      = NameStr(att->attname);
        cols.push_back(c);
    }

    /*
     * rd_pkindex is only valid once the relcache has computed the index list
     * (RelationGetIndexList fills it in lazily).  Right after CREATE TABLE —
     * or for a relation the executor has not opened indexes on yet — it is
     * still InvalidOid, which would bootstrap the lake table without its
     * primary key.
     */
    list_free(RelationGetIndexList(rel));
    if (OidIsValid(rel->rd_pkindex)) {
        Relation pkidx = index_open(rel->rd_pkindex, AccessShareLock);
        IndexInfo *ii  = BuildIndexInfo(pkidx);
        for (int i = 0; i < ii->ii_NumIndexAttrs; i++)
            if (ii->ii_IndexAttrNumbers[i] > 0)
                pk_attnums.push_back((uint16_t)ii->ii_IndexAttrNumbers[i]);
        index_close(pkidx, AccessShareLock);
    }

    /* Append metadata columns if their names are configured. */
    auto add_meta = [&](uint16_t fid, const char *name) {
        PaimonColumn c;
        c.attnum    = fid;
        c.type_code = PTYPE_BIGINT;
        c.typmod    = 0;
        c.not_null  = true;
        c.name      = name;
        cols.push_back(c);
    };
    if (guc_meta_xid_field && guc_meta_xid_field[0])
        add_meta(PAIMON_META_FID_XID, guc_meta_xid_field);
    if (guc_meta_op_field  && guc_meta_op_field[0])
        add_meta(PAIMON_META_FID_OP,  guc_meta_op_field);
    if (guc_meta_tid_field && guc_meta_tid_field[0])
        add_meta(PAIMON_META_FID_TID, guc_meta_tid_field);
}

/* ── XID-based sequence counter ─────────────────────────────────────── */
static TransactionId s_last_xid    = InvalidTransactionId;
static uint32_t      s_xact_op_cnt = 0;

static uint64_t
next_seq(void)
{
    TransactionId xid = GetCurrentTransactionId();
    if (xid != s_last_xid) {
        s_last_xid    = xid;
        s_xact_op_cnt = 0;
    }
    return ((uint64_t)xid << 32) | (uint64_t)s_xact_op_cnt++;
}

/* ── Serialize slot and buffer it ───────────────────────────────────── */
static void
buffer_slot(Relation rel, TupleTableSlot *slot, PaimonRowKind kind)
{
    if (!paimon_rel_is_mirrored(rel))
        return;

    TupleDesc td = slot->tts_tupleDescriptor;
    uint32_t  fp = paimon_schema_fingerprint(td);

    std::vector<PaimonColumn>  cols;
    std::vector<uint16_t>      pk_attnums;
    paimon_build_col_meta(rel, cols, pk_attnums);

    std::vector<uint16_t>        attnums;
    std::vector<PaimonTypeCode>  types;
    std::vector<int32_t>         lengths;
    std::vector<std::string>     values;

    slot_getallattrs(slot);
    for (int i = 0; i < td->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(td, i);
        if (att->attisdropped) continue;
        int32_t len = -1;
        std::string val;
        serialize_datum(att->atttypid,
                        slot->tts_values[i], slot->tts_isnull[i],
                        &len, &val);
        attnums.push_back((uint16_t)att->attnum);
        uint32_t tm = 0;
        types.push_back(pg_oid_to_ptype(att->atttypid, &tm,
                                         (int32_t)att->atttypmod));
        lengths.push_back(len);
        values.push_back(std::move(val));
    }

    uint64_t seq     = next_seq();
    uint32_t cur_xid = (uint32_t)(seq >> 32);
    uint32_t cur_op  = (uint32_t)(seq & 0xFFFFFFFFu);

    /* Append metadata column values if configured. */
    auto push_i64 = [&](uint16_t fid, int64_t v) {
        std::string val(8, '\0');
        memcpy(val.data(), &v, 8);
        attnums.push_back(fid);
        types.push_back(PTYPE_BIGINT);
        lengths.push_back(8);
        values.push_back(std::move(val));
    };
    if (guc_meta_xid_field && guc_meta_xid_field[0])
        push_i64(PAIMON_META_FID_XID, (int64_t)(uint64_t)cur_xid);
    if (guc_meta_op_field && guc_meta_op_field[0])
        push_i64(PAIMON_META_FID_OP,  (int64_t)(uint64_t)cur_op);
    if (guc_meta_tid_field && guc_meta_tid_field[0]) {
        BlockNumber  blkno = BlockIdGetBlockNumber(&slot->tts_tid.ip_blkid);
        OffsetNumber posid = slot->tts_tid.ip_posid;
        push_i64(PAIMON_META_FID_TID, ((int64_t)blkno << 16) | (int64_t)posid);
    }

    PaimonClient::instance().bufferRow(
        RelationGetRelid(rel),
        paimon_rel_ident(rel),
        fp, cols, pk_attnums, kind, seq,
        attnums, types, lengths, values);
}

/* ── Fetch old heap tuple by TID and buffer it ──────────────────────── */
static void
fetch_and_buffer_old(Relation rel, ItemPointer tid,
                     Snapshot snapshot, PaimonRowKind kind)
{
    Buffer          buf      = InvalidBuffer;
    HeapTupleData   tuple;
    ItemPointerCopy(tid, &tuple.t_self);

    /*
     * The before-image is the exact row version at tid, which is what the
     * heap is about to update/delete.  It is fetched with SnapshotAny (as
     * the executor does for row triggers, see GetTupleForTrigger): after an
     * EvalPlanQual recheck under READ COMMITTED, tid points at a version
     * committed after the statement's snapshot, which that snapshot cannot
     * see — the -U / -D event would silently be lost.  If the heap call then
     * fails (TM_Updated, ...), the caller retracts the event.
     */
    (void) snapshot;
    bool found = heap_fetch(rel, SnapshotAny, &tuple, &buf, false);
    if (!found) {
        if (BufferIsValid(buf)) ReleaseBuffer(buf);
        return;
    }

    TupleTableSlot *slot = MakeSingleTupleTableSlot(
        RelationGetDescr(rel), &TTSOpsHeapTuple);
    ExecStoreHeapTuple(&tuple, slot, false);
    buffer_slot(rel, slot, kind);
    ExecDropSingleTupleTableSlot(slot);
    ReleaseBuffer(buf);
}

/* ── Scan wrappers (proxy-to-heap-AM) ───────────────────────────────── */
/*
 * paimon_heap is write-path only.  All reads delegate to the heap AM via a
 * shallow proxy RelationData whose only difference from the real relation is
 * rd_tableam = GetHeapamTableAmRoutine().  This satisfies PG19 heap_getnext()'s
 * direct check:  sscan->rs_rd->rd_tableam != GetHeapamTableAmRoutine().
 *
 * Proxy lifetime fix
 * ------------------
 * heap_beginscan(proxy) calls RelationIncrementReferenceCount(proxy), which
 * registers the proxy pointer with CurrentResourceOwner.  The original code
 * used palloc() (CurrentMemoryContext), whose lifetime depends on query phase:
 *
 *   Simple protocol (psql):  commit → AtEOXact → ResourceOwnerRelease runs
 *                             BEFORE the query MemoryContext is reset → safe.
 *   Extended protocol (JDBC): PortalDrop frees the portal MemoryContext
 *                             BEFORE exec_sync_message calls
 *                             finish_xact_command → ResourceOwnerRelease
 *                             → proxy already freed → SIGSEGV.
 *   Parallel workers:         worker exit order is non-deterministic.
 *
 * Fix: allocate the proxy in TopTransactionContext.  PostgreSQL's abort
 * sequence always runs ResourceOwnerRelease BEFORE resetting
 * TopTransactionContext, so the proxy pointer is valid whenever the resource
 * owner tries to close it.  The proxy is a pure value-copy (no owned heap
 * resources), so freeing it at transaction end is safe.
 */

static TableAmRoutine  g_paimon_routine;
static bool            g_paimon_routine_init = false;

static TableScanDesc
ph_scan_begin(Relation rel, Snapshot snapshot, int nkeys,
              ScanKey key, ParallelTableScanDesc parallel_scan, uint32 flags)
{
    Relation proxy = (Relation) MemoryContextAlloc(TopTransactionContext,
                                                   sizeof(RelationData));
    memcpy(proxy, rel, sizeof(RelationData));
    const_cast<const TableAmRoutine*&>(proxy->rd_tableam) =
        GetHeapamTableAmRoutine();
    return GetHeapamTableAmRoutine()->scan_begin(
        proxy, snapshot, nkeys, key, parallel_scan, flags);
}

/*
 * scan_end / scan_rescan / scan_getnextslot are registered in g_paimon_routine
 * but are unreachable via the normal proxy path: once ph_scan_begin stores the
 * proxy (rd_tableam = heap_am) in sscan->rs_rd, all subsequent scan dispatch
 * goes through the proxy directly to the heap AM implementations.
 * Kept as a safety net for any non-proxy call path.
 */
static void
ph_scan_end(TableScanDesc scan)
{
    GetHeapamTableAmRoutine()->scan_end(scan);
}

static void
ph_scan_rescan(TableScanDesc scan, ScanKey key, bool set_params,
               bool allow_strat, bool allow_sync, bool allow_pagemode)
{
    GetHeapamTableAmRoutine()->scan_rescan(
        scan, key, set_params, allow_strat, allow_sync, allow_pagemode);
}

static bool
ph_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
                    TupleTableSlot *slot)
{
    return GetHeapamTableAmRoutine()->scan_getnextslot(sscan, direction, slot);
}

/* ── DML wrappers ───────────────────────────────────────────────────── */

/*
 * PostgreSQL 19 changed the TAM callback signatures relative to PG 16-18:
 *   - tuple_insert/multi_insert/tuple_insert_speculative: options uint32 → int
 *   - tuple_delete: removed uint32 options, added bool changingPart at end
 *   - tuple_update: removed uint32 options
 */
#if PG_VERSION_NUM >= 190000
# define TAM_OPT_TYPE uint32
#else
# define TAM_OPT_TYPE int
#endif

static void
ph_tuple_insert(Relation rel, TupleTableSlot *slot,
                CommandId cid, TAM_OPT_TYPE options, BulkInsertStateData *bistate)
{
    GetHeapamTableAmRoutine()->tuple_insert(rel, slot, cid, options, bistate);
    ensure_xact_cb();
    buffer_slot(rel, slot, PROW_INSERT);
}

static void
ph_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
                CommandId cid, TAM_OPT_TYPE options, BulkInsertStateData *bistate)
{
    GetHeapamTableAmRoutine()->multi_insert(rel, slots, nslots, cid, options, bistate);
    ensure_xact_cb();
    for (int i = 0; i < nslots; i++)
        buffer_slot(rel, slots[i], PROW_INSERT);
}

#if PG_VERSION_NUM >= 190000
static TM_Result
ph_tuple_delete(Relation rel, ItemPointer tid,
                CommandId cid, uint32 options,
                Snapshot snapshot, Snapshot crosscheck,
                bool wait, TM_FailureData *tmfd)
{
    size_t mark = PaimonClient::instance().rowMark(RelationGetRelid(rel));
    fetch_and_buffer_old(rel, tid, snapshot, PROW_DELETE);
    TM_Result r = GetHeapamTableAmRoutine()->tuple_delete(
        rel, tid, cid, options, snapshot, crosscheck, wait, tmfd);
    if (r == TM_Ok)
        ensure_xact_cb();
    else    /* concurrent update/delete: no change happened, retract -D */
        PaimonClient::instance().truncateRows(RelationGetRelid(rel), mark);
    return r;
}

static TM_Result
ph_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
                CommandId cid, uint32 options,
                Snapshot snapshot, Snapshot crosscheck, bool wait,
                TM_FailureData *tmfd, LockTupleMode *lockmode,
                TU_UpdateIndexes *update_indexes)
{
    size_t mark = PaimonClient::instance().rowMark(RelationGetRelid(rel));
    fetch_and_buffer_old(rel, otid, snapshot, PROW_UPDATE_BEFORE);
    TM_Result r = GetHeapamTableAmRoutine()->tuple_update(
        rel, otid, slot, cid, options, snapshot, crosscheck,
        wait, tmfd, lockmode, update_indexes);
    if (r == TM_Ok) {
        ensure_xact_cb();
        buffer_slot(rel, slot, PROW_UPDATE_AFTER);
    } else {
        /* concurrent update/delete: no change happened, retract -U */
        PaimonClient::instance().truncateRows(RelationGetRelid(rel), mark);
    }
    return r;
}
#else  /* PG 16-18 */
static TM_Result
ph_tuple_delete(Relation rel, ItemPointer tid,
                CommandId cid,
                Snapshot snapshot, Snapshot crosscheck,
                bool wait, TM_FailureData *tmfd, bool changingPart)
{
    size_t mark = PaimonClient::instance().rowMark(RelationGetRelid(rel));
    fetch_and_buffer_old(rel, tid, snapshot, PROW_DELETE);
    TM_Result r = GetHeapamTableAmRoutine()->tuple_delete(
        rel, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart);
    if (r == TM_Ok)
        ensure_xact_cb();
    else    /* concurrent update/delete: no change happened, retract -D */
        PaimonClient::instance().truncateRows(RelationGetRelid(rel), mark);
    return r;
}

static TM_Result
ph_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
                CommandId cid,
                Snapshot snapshot, Snapshot crosscheck, bool wait,
                TM_FailureData *tmfd, LockTupleMode *lockmode,
                TU_UpdateIndexes *update_indexes)
{
    size_t mark = PaimonClient::instance().rowMark(RelationGetRelid(rel));
    fetch_and_buffer_old(rel, otid, snapshot, PROW_UPDATE_BEFORE);
    TM_Result r = GetHeapamTableAmRoutine()->tuple_update(
        rel, otid, slot, cid, snapshot, crosscheck,
        wait, tmfd, lockmode, update_indexes);
    if (r == TM_Ok) {
        ensure_xact_cb();
        buffer_slot(rel, slot, PROW_UPDATE_AFTER);
    } else {
        /* concurrent update/delete: no change happened, retract -U */
        PaimonClient::instance().truncateRows(RelationGetRelid(rel), mark);
    }
    return r;
}
#endif /* PG_VERSION_NUM */

static void
ph_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
                             CommandId cid, TAM_OPT_TYPE options,
                             BulkInsertStateData *bistate, uint32 specToken)
{
    GetHeapamTableAmRoutine()->tuple_insert_speculative(
        rel, slot, cid, options, bistate, specToken);
}

static void
ph_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
                               uint32 specToken, bool succeeded)
{
    GetHeapamTableAmRoutine()->tuple_complete_speculative(rel, slot, specToken, succeeded);
    if (succeeded) {
        ensure_xact_cb();
        buffer_slot(rel, slot, PROW_INSERT);
    }
}

/*
 * Override index_build_range_scan to avoid the proxy-RelationData path.
 *
 * heapam_index_build_range_scan calls heap_getnext() directly (bypassing the
 * AM vtable), which checks sscan->rs_rd->rd_tableam == heap_AM.  ph_scan_begin
 * satisfies this via a palloc'd proxy RelationData, but the proxy's lifecycle
 * under JDBC's extended query protocol (PortalDrop before commit) causes a
 * use-after-free SIGSEGV.
 *
 * Fix: temporarily swap rd_tableam on the real relation to heap_AM for the
 * duration of the index build scan, then restore it.  No proxy is created, so
 * there is nothing to dangle.  The swap is safe because index builds hold an
 * AccessShareLock and no concurrent DML can change rd_tableam.
 */
static double
ph_index_build_range_scan(Relation heapRelation,
                          Relation indexRelation,
                          IndexInfo *indexInfo,
                          bool allow_sync,
                          bool anyvisible,
                          bool progress,
                          BlockNumber start_blockno,
                          BlockNumber numblocks,
                          IndexBuildCallback callback,
                          void *callback_state,
                          TableScanDesc scan)
{
    const TableAmRoutine *saved_am = heapRelation->rd_tableam;
    const_cast<const TableAmRoutine*&>(heapRelation->rd_tableam) =
        GetHeapamTableAmRoutine();
    double result = GetHeapamTableAmRoutine()->index_build_range_scan(
        heapRelation, indexRelation, indexInfo,
        allow_sync, anyvisible, progress,
        start_blockno, numblocks,
        callback, callback_state, scan);
    const_cast<const TableAmRoutine*&>(heapRelation->rd_tableam) = saved_am;
    return result;
}

/* ── Handler ────────────────────────────────────────────────────────── */

extern "C" Datum
paimon_heap_handler(PG_FUNCTION_ARGS)
{
    if (!g_paimon_routine_init) {
        memcpy(&g_paimon_routine, GetHeapamTableAmRoutine(), sizeof(TableAmRoutine));

        g_paimon_routine.tuple_insert              = ph_tuple_insert;
        g_paimon_routine.tuple_insert_speculative  = ph_tuple_insert_speculative;
        g_paimon_routine.tuple_complete_speculative= ph_tuple_complete_speculative;
        g_paimon_routine.multi_insert              = ph_multi_insert;
        g_paimon_routine.tuple_delete              = ph_tuple_delete;
        g_paimon_routine.tuple_update              = ph_tuple_update;
        g_paimon_routine.scan_begin                = ph_scan_begin;
        g_paimon_routine.scan_end                  = ph_scan_end;
        g_paimon_routine.scan_rescan               = ph_scan_rescan;
        g_paimon_routine.scan_getnextslot          = ph_scan_getnextslot;
        g_paimon_routine.index_build_range_scan    = ph_index_build_range_scan;

        g_paimon_routine_init = true;
    }
    PG_RETURN_POINTER(&g_paimon_routine);
}
