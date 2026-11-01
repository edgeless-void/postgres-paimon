// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_client.cpp — backend-side transactional queue + ring-buffer producer.
 *
 * Each PostgreSQL backend accumulates DML rows and DDL events in
 * process-local memory during a transaction (see paimon_client.h).  On
 * COMMIT the ordered message list is pushed into the shared-memory ring in a
 * single all-or-nothing write for the paimon_bgworker to consume.
 *
 * On ABORT / ROLLBACK TO SAVEPOINT the affected messages are discarded, so
 * nothing PostgreSQL rolled back ever reaches the lake.
 *
 * A transaction larger than the free ring space is dropped as a whole (the
 * heap remains authoritative); the session gets a WARNING and the
 * bgworker logs the drop counter.
 */

#include "paimon_client.h"
#include "paimon_event.h"
#include "paimon_shmem.h"
#include "paimon_ring.h"
#include "paimon_wal.h"

#include <cstring>
#include <cstdlib>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

/* ── little-endian serialisation helpers ─────────────────────────── */
namespace {

void put_u8 (std::string& b, uint8_t  v) { b.push_back((char)v); }
void put_u16(std::string& b, uint16_t v) {
    b.push_back((char)(v));
    b.push_back((char)(v >> 8));
}
void put_u32(std::string& b, uint32_t v) {
    b.push_back((char)(v));
    b.push_back((char)(v >>  8));
    b.push_back((char)(v >> 16));
    b.push_back((char)(v >> 24));
}
void put_i32(std::string& b, int32_t v)  { put_u32(b, (uint32_t)v); }
void put_u64(std::string& b, uint64_t v) {
    put_u32(b, (uint32_t)v);
    put_u32(b, (uint32_t)(v >> 32));
}
void put_str(std::string& b, const std::string& s) {
    put_u16(b, (uint16_t)s.size());
    b.append(s);
}
/* Build a complete framed message: [uint32 total_len][uint8 type][payload] */
std::string frame(uint8_t msg_type, const std::string& payload) {
    std::string msg;
    uint32_t total = 4 + 1 + (uint32_t)payload.size();
    put_u32(msg, total);
    put_u8 (msg, msg_type);
    msg.append(payload);
    return msg;
}

} // anonymous namespace

/* ── PaimonClient singleton (per backend process) ─────────────────── */

PaimonClient& PaimonClient::instance() {
    static PaimonClient inst;
    return inst;
}

PaimonClient::~PaimonClient() {}

/* "Connect" just checks that the ring is available. */
bool PaimonClient::connect() {
    if (connected_) return true;
    PaimonRingHdr *hdr = paimon_ring_hdr();
    connected_ = (hdr != NULL && hdr->magic == PAIMON_RING_MAGIC);
    return connected_;
}

/* ── Message builders ─────────────────────────────────────────────── */

static std::string
schema_payload(uint32_t table_oid, const std::string& table_name,
               const std::vector<PaimonColumn>& cols,
               const std::vector<uint16_t>& pk_attnums)
{
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    put_u32(payload, (uint32_t)cols.size());
    for (const auto& c : cols) {
        put_u16(payload, c.attnum);
        put_u8 (payload, (uint8_t)c.type_code);
        put_u32(payload, c.typmod);
        put_u8 (payload, c.not_null ? 1 : 0);
        put_str(payload, c.name);
    }
    put_u32(payload, (uint32_t)pk_attnums.size());
    for (uint16_t pk : pk_attnums)
        put_u16(payload, pk);
    return payload;
}

/* Build a DDL_CREATE framed message. */
std::string PaimonClient::makeSchemaMsg(const PaimonRelBuffer& buf) const {
    return frame(PMSG_DDL_CREATE,
                 schema_payload(buf.table_oid, buf.table_name,
                                buf.columns, buf.pk_attnums));
}

/* Build a DML_BATCH framed message. */
std::string PaimonClient::makeBatchMsg(const PaimonRelBuffer& buf) const {
    std::string payload;
    put_u32(payload, buf.table_oid);
    put_str(payload, buf.table_name);
    put_u32(payload, buf.schema_version);
    put_u32(payload, (uint32_t)buf.rows.size());
    for (const auto& row : buf.rows) {
        put_u8 (payload, (uint8_t)row.kind);
        put_u64(payload, row.seq);
        put_u16(payload, (uint16_t)row.attnums.size());
        for (size_t i = 0; i < row.attnums.size(); ++i) {
            put_u16(payload, row.attnums[i]);
            put_u8 (payload, (uint8_t)row.types[i]);
            put_i32(payload, row.lengths[i]);
            if (row.lengths[i] > 0)
                payload.append(row.values[i]);
        }
    }
    return frame(PMSG_DML_BATCH, payload);
}

/* ── Schema bookkeeping ───────────────────────────────────────────── */

/*
 * The schema version the bgworker will have for oid once the messages queued
 * so far are applied: this transaction's emissions first, then what earlier
 * committed transactions of this backend delivered.
 */
uint32_t PaimonClient::knownSchema(uint32_t oid, bool *found) const {
    auto t = txn_schema_version_.find(oid);
    if (t != txn_schema_version_.end()) { *found = true; return t->second; }
    auto s = sent_schema_version_.find(oid);
    if (s != sent_schema_version_.end()) { *found = true; return s->second; }
    *found = false;
    return 0;
}

/* Move one relation's buffered rows into the ordered txn message list. */
void PaimonClient::spillBuffer(uint32_t oid, PaimonRelBuffer& buf) {
    if (buf.rows.empty()) return;

    bool     found = false;
    uint32_t known = knownSchema(oid, &found);
    if (!found || known != buf.schema_version) {
        txn_msgs_.push_back(makeSchemaMsg(buf));
        txn_schema_version_[oid] = buf.schema_version;
    }

    txn_msgs_.push_back(makeBatchMsg(buf));
    buf.rows.clear();
    buf.byte_size = 0;
}

void PaimonClient::spillAll() {
    for (auto& [oid, buf] : buffers_)
        spillBuffer(oid, buf);
}

/* ── Buffer a DML row ─────────────────────────────────────────────── */

void PaimonClient::bufferRow(uint32_t table_oid,
                             const std::string& table_name,
                             uint32_t schema_version,
                             const std::vector<PaimonColumn>& cols,
                             const std::vector<uint16_t>& pk_attnums,
                             PaimonRowKind kind,
                             uint64_t seq,
                             const std::vector<uint16_t>& attnums,
                             const std::vector<PaimonTypeCode>& types,
                             const std::vector<int32_t>& lengths,
                             const std::vector<std::string>& values) {
    if (!connect()) return;

    PaimonRelBuffer& buf = buffers_[table_oid];
    if (buf.table_oid == 0) {
        buf.table_oid      = table_oid;
        buf.table_name     = table_name;
        buf.schema_version = schema_version;
        buf.columns        = cols;
        buf.pk_attnums     = pk_attnums;
    } else if (buf.schema_version != schema_version ||
               buf.table_name != table_name) {
        /*
         * Schema changed mid-transaction: rows buffered so far were captured
         * under the old schema, so ship them (with that schema) first.
         */
        spillBuffer(table_oid, buf);
        buf.table_name     = table_name;
        buf.schema_version = schema_version;
        buf.columns        = cols;
        buf.pk_attnums     = pk_attnums;
    }

    PaimonRow row;
    row.kind    = kind;
    row.seq     = seq;
    row.attnums = attnums;
    row.types   = types;
    row.lengths = lengths;
    row.values  = values;

    size_t row_bytes = 1 + 8 + 2;
    for (size_t i = 0; i < attnums.size(); ++i)
        row_bytes += 2 + 1 + 4 + (lengths[i] > 0 ? lengths[i] : 0);
    buf.byte_size += row_bytes;
    buf.rows.push_back(std::move(row));
}

size_t PaimonClient::rowMark(uint32_t table_oid) const {
    auto it = buffers_.find(table_oid);
    return it == buffers_.end() ? 0 : it->second.rows.size();
}

void PaimonClient::truncateRows(uint32_t table_oid, size_t mark) {
    auto it = buffers_.find(table_oid);
    if (it == buffers_.end()) return;
    PaimonRelBuffer& buf = it->second;
    /* A spill in between would have emptied the buffer; never grow it. */
    if (mark < buf.rows.size())
        buf.rows.resize(mark);
}

/* ── DDL (transactional, ordered with DML) ───────────────────────── */

void PaimonClient::queueSchema(uint32_t table_oid,
                               const std::string& table_name,
                               uint32_t schema_version,
                               const std::vector<PaimonColumn>& cols,
                               const std::vector<uint16_t>& pk_attnums) {
    if (!connect()) return;
    /* Rows captured before this DDL belong to the previous schema. */
    spillAll();
    txn_msgs_.push_back(frame(PMSG_DDL_CREATE,
                              schema_payload(table_oid, table_name,
                                             cols, pk_attnums)));
    txn_schema_version_[table_oid] = schema_version;
}

void PaimonClient::queueTruncate(uint32_t table_oid,
                                 const std::string& table_name) {
    if (!connect()) return;
    spillAll();
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    txn_msgs_.push_back(frame(PMSG_DDL_TRUNCATE, payload));
}

void PaimonClient::queueDropTable(uint32_t table_oid,
                                  const std::string& table_name) {
    if (!connect()) return;
    /* Rows for a table that is being dropped are moot — don't ship them. */
    auto it = buffers_.find(table_oid);
    if (it != buffers_.end()) {
        it->second.rows.clear();
        it->second.byte_size = 0;
    }
    spillAll();
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    txn_msgs_.push_back(frame(PMSG_DDL_DROP_TBL, payload));
    txn_schema_version_.erase(table_oid);
    txn_dropped_.insert(table_oid);
}

bool PaimonClient::hasPending() const {
    if (!txn_msgs_.empty() || !pending_msgs_.empty()) return true;
    for (const auto& [oid, buf] : buffers_)
        if (!buf.rows.empty()) return true;
    return false;
}

/* ── Subtransactions ──────────────────────────────────────────────── */

void PaimonClient::startSubXact(uint32_t subid) {
    /*
     * Everything buffered so far belongs to the parent; spill it so that a
     * later ROLLBACK TO can simply cut txn_msgs_ back to this mark.
     */
    spillAll();
    subxact_marks_.emplace_back(subid, txn_msgs_.size());
}

void PaimonClient::commitSubXact(uint32_t subid) {
    while (!subxact_marks_.empty()) {
        uint32_t id = subxact_marks_.back().first;
        subxact_marks_.pop_back();
        if (id == subid) break;
    }
}

void PaimonClient::abortSubXact(uint32_t subid) {
    size_t mark  = txn_msgs_.size();
    bool   found = false;
    while (!subxact_marks_.empty()) {
        auto [id, m] = subxact_marks_.back();
        subxact_marks_.pop_back();
        mark = m;
        if (id == subid) { found = true; break; }
    }
    if (!found) {
        /* Should not happen (callbacks are registered at load time). */
        ereport(WARNING,
                errmsg("paimon_heap: unknown subtransaction %u rolled back; "
                       "discarding all lake changes of this transaction", subid));
        mark = 0;
    }

    /* Rows still in the per-relation buffers were captured after the mark. */
    for (auto& [oid, buf] : buffers_) {
        buf.rows.clear();
        buf.byte_size = 0;
    }
    if (mark < txn_msgs_.size())
        txn_msgs_.resize(mark);

    /*
     * Schema messages emitted inside the aborted part are gone; forget all
     * per-transaction schema knowledge and let the next spill re-emit the
     * schema (the bgworker treats a repeated schema as a no-op).  Drops
     * queued in the aborted part are undone as well, so recompute the set.
     */
    txn_schema_version_.clear();
    txn_dropped_.clear();
    for (const auto& m : txn_msgs_) {
        if (m.size() >= 9 && (uint8_t)m[4] == PMSG_DDL_DROP_TBL) {
            uint32_t oid = (uint8_t)m[5] | ((uint8_t)m[6] << 8) |
                           ((uint8_t)m[7] << 16) | ((uint32_t)(uint8_t)m[8] << 24);
            txn_dropped_.insert(oid);
        }
    }
}

/* ── Commit / abort ───────────────────────────────────────────────── */

/*
 * prepareCommit — spill all buffered rows, append SYNC and, when
 * paimon_heap.wal_level = 'local', write a WAL record so the messages are
 * durable before the transaction commit record hits the WAL.
 *
 * Must be called at XACT_EVENT_PRE_COMMIT (before RecordTransactionCommit)
 * so the WAL flush is absorbed into the commit's fsync at no extra I/O.
 */
void PaimonClient::prepareCommit() {
    pending_msgs_.clear();

    if (!connect()) { discardAll(); return; }

    spillAll();
    if (txn_msgs_.empty()) return;

    txn_msgs_.push_back(frame(PMSG_SYNC, ""));
    pending_msgs_.swap(txn_msgs_);
    txn_msgs_.clear();

    /* WAL record: all pending messages in one XLogInsert. */
    if (paimon_wal_is_local())
        paimon_wal_write(pending_msgs_);
}

/* ── Write pending_msgs_ to ring (called at XACT_EVENT_COMMIT) ───── */

void PaimonClient::flushAll() {
    if (pending_msgs_.empty() || !connect()) {
        discardAll();
        return;
    }

    std::vector<const void*> datas;
    std::vector<size_t>      lens;
    datas.reserve(pending_msgs_.size());
    lens.reserve(pending_msgs_.size());
    for (const auto& m : pending_msgs_) {
        datas.push_back(m.data());
        lens.push_back(m.size());
    }

    bool ok = paimon_ring_write_batch(datas.data(), lens.data(),
                                      (int)datas.size());
    if (ok) {
        for (const auto& [oid, ver] : txn_schema_version_)
            sent_schema_version_[oid] = ver;
        for (uint32_t oid : txn_dropped_)
            sent_schema_version_.erase(oid);
    } else {
        /*
         * Nothing of this transaction reached the lake.  Forget which schemas
         * the bgworker has seen so the next transaction re-sends them (the
         * bgworker reconciles), instead of silently writing under a stale or
         * missing schema.
         */
        sent_schema_version_.clear();
        ereport(WARNING,
                errmsg("paimon_heap: lake update for this transaction was dropped "
                       "(ring buffer full)"),
                errhint("Increase paimon_heap.ring_size_mb."));
    }
    discardAll();
}

void PaimonClient::discardAll() {
    for (auto& [oid, buf] : buffers_) {
        buf.rows.clear();
        buf.byte_size = 0;
    }
    txn_msgs_.clear();
    pending_msgs_.clear();
    txn_schema_version_.clear();
    txn_dropped_.clear();
    subxact_marks_.clear();
}
