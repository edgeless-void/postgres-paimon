// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_client.h — per-backend row accumulator + ring-buffer producer.
 *
 * Everything a transaction does to paimon_heap tables — DML rows and DDL
 * events alike — is queued in process-local memory and only reaches the
 * shared-memory ring at COMMIT, in the order it happened.  On ABORT (or
 * ROLLBACK TO SAVEPOINT) the queued work is discarded, so the lake never
 * sees changes that PostgreSQL rolled back.
 *
 * DML rows are first collected per relation in PaimonRelBuffer.  Whenever
 * ordering matters (a DDL event is queued, a savepoint starts, the relation's
 * schema changes) the buffered rows are "spilled": serialised into the
 * ordered per-transaction message list txn_msgs_.  At PRE_COMMIT the
 * remaining rows are spilled and a SYNC is appended; at COMMIT the whole list
 * is written to the ring in one all-or-nothing operation.
 */

#ifdef __cplusplus

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>

#include "paimon_event.h"

struct PaimonColumn {
    uint16_t    attnum;
    PaimonTypeCode type_code;
    uint32_t    typmod;
    bool        not_null;
    std::string name;
};

struct PaimonRow {
    PaimonRowKind kind;
    uint64_t      seq;
    std::vector<uint16_t>        attnums;
    std::vector<PaimonTypeCode>  types;
    std::vector<int32_t>         lengths;
    std::vector<std::string>     values;
};

struct PaimonRelBuffer {
    uint32_t    table_oid    = 0;
    std::string table_name;          /* wire identifier, see paimon_event.h */
    uint32_t    schema_version = 0;
    std::vector<PaimonRow>     rows;
    size_t      byte_size    = 0;
    std::vector<PaimonColumn>  columns;
    std::vector<uint16_t>      pk_attnums;
};

class PaimonClient {
public:
    static PaimonClient& instance();

    /* Returns true if shared memory ring is available. */
    bool connect();
    bool isConnected() const { return connected_; }

    /* Buffer a DML row (accumulates until commit). */
    void bufferRow(uint32_t table_oid,
                   const std::string& table_name,
                   uint32_t schema_version,
                   const std::vector<PaimonColumn>& cols,
                   const std::vector<uint16_t>& pk_attnums,
                   PaimonRowKind kind,
                   uint64_t seq,
                   const std::vector<uint16_t>& attnums,
                   const std::vector<PaimonTypeCode>& types,
                   const std::vector<int32_t>& lengths,
                   const std::vector<std::string>& values);

    /*
     * Row marks: rowMark() returns the number of rows currently buffered for
     * a relation; truncateRows() drops rows buffered after that mark.  Used
     * to retract a before-image when the heap UPDATE/DELETE did not happen
     * (TM_Updated / TM_Deleted / ...).
     */
    size_t rowMark(uint32_t table_oid) const;
    void   truncateRows(uint32_t table_oid, size_t mark);

    /*
     * Queue the relation's full current schema (PMSG_DDL_CREATE).  The
     * bgworker creates the Paimon table if needed, otherwise reconciles its
     * schema to this one (add / drop / rename / retype by field id).
     */
    void queueSchema(uint32_t table_oid,
                     const std::string& table_name,
                     uint32_t schema_version,
                     const std::vector<PaimonColumn>& cols,
                     const std::vector<uint16_t>& pk_attnums);
    void queueTruncate(uint32_t table_oid, const std::string& table_name);
    void queueDropTable(uint32_t table_oid, const std::string& table_name);

    /* True when this transaction has anything queued for the lake. */
    bool hasPending() const;

    /* Subtransaction boundaries (SAVEPOINT / RELEASE / ROLLBACK TO). */
    void startSubXact(uint32_t subid);
    void commitSubXact(uint32_t subid);
    void abortSubXact(uint32_t subid);

    /*
     * Spill buffered rows, append SYNC, and write a WAL record when
     * paimon_heap.wal_level = 'local'.  Must be called at
     * XACT_EVENT_PRE_COMMIT so the WAL record is included in the
     * transaction's commit flush.
     */
    void prepareCommit();

    /* Push the prepared messages to the ring (call at XACT_EVENT_COMMIT). */
    void flushAll();

    /* Discard everything queued by this transaction (call at ABORT). */
    void discardAll();

private:
    PaimonClient() = default;
    ~PaimonClient();

    void spillBuffer(uint32_t oid, PaimonRelBuffer& buf);
    void spillAll();
    uint32_t knownSchema(uint32_t oid, bool *found) const;
    std::string makeSchemaMsg(const PaimonRelBuffer& buf) const;
    std::string makeBatchMsg(const PaimonRelBuffer& buf) const;

    bool connected_ = false;
    std::unordered_map<uint32_t, PaimonRelBuffer> buffers_;

    /* Ordered messages queued by the current transaction. */
    std::vector<std::string> txn_msgs_;
    /* Messages built at PRE_COMMIT, written to the ring at COMMIT. */
    std::vector<std::string> pending_msgs_;

    /*
     * Schema versions known to have reached the bgworker (committed), and
     * the versions emitted by the current transaction (merged on commit).
     */
    std::unordered_map<uint32_t, uint32_t> sent_schema_version_;
    std::unordered_map<uint32_t, uint32_t> txn_schema_version_;
    std::unordered_set<uint32_t>           txn_dropped_;

    /* Savepoint stack: (subxact id, txn_msgs_ length at its start). */
    std::vector<std::pair<uint32_t, size_t>> subxact_marks_;
};

#endif /* __cplusplus */
