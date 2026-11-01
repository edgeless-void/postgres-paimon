// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * PaimonTableWriter: per-table write coordinator.
 *
 * Accumulates rows from DML_BATCH events, flushes to a Parquet file
 * when the row-group limit is reached, writes Paimon manifest + snapshot.
 *
 * One instance lives per active table in the daemon.
 */
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

#include "paimon_schema.h"
#include "paimon_parquet.h"
#include "paimon_event.h"
#include "paimon_manifest.h"

namespace paimon {

class PaimonTableWriter {
public:
    /*
     * table_root: path like <warehouse>/<db>/<table_name>
     * The directory is created if it does not exist.
     */
    PaimonTableWriter(const std::string& table_root,
                      const std::string& table_name);

    /* ── DML ──────────────────────────────────────────────────────── */

    /*
     * Bootstrap schema from a CREATE event (idempotent).
     * Must be called before the first addRow() for a new table.
     */
    bool initSchema(const std::vector<FieldDef>& fields,
                    const std::vector<std::string>& pk_names);

    /*
     * Buffer one row.  Triggers a Parquet flush when the row-group
     * accumulator hits PAIMON_ROWGROUP_ROWS.
     */
    void addRow(PaimonRowKind kind, uint64_t seq,
                const std::vector<uint16_t>&     attnums,
                const std::vector<PaimonTypeCode>& types,
                const std::vector<int32_t>&      lengths,
                const std::vector<std::string>&  values);

    /*
     * Flush any buffered rows to a Parquet file and commit a snapshot.
     * Called at SYNC, TRUNCATE, or when the row-group limit is reached.
     */
    bool flush();

    /* ── DDL ──────────────────────────────────────────────────────── */

    bool ddlAddField(const FieldDef& f);
    bool ddlDropField(uint16_t field_id);
    bool ddlRenameField(uint16_t field_id, const std::string& new_name);
    bool ddlChangeType(uint16_t field_id, PaimonTypeCode new_type, uint32_t typmod);

    /* Drop all Parquet files and reset snapshot state */
    bool truncate();

    /* Remove the entire table directory (used internally / for testing) */
    bool drop();

    /*
     * Metadata-only drop: remove schema/, snapshot/, manifest/ from local
     * staging but leave bucket-0/ data files intact.  Call flush() and
     * promote_table() before this so S3/HDFS has the final complete copy.
     */
    bool dropMetadataOnly();

    const std::string &tableName() const { return name_; }
    const std::string &root() const { return root_; }

    /* True once the table has a schema (on disk). */
    bool hasSchema() const;
    /* Current schema id (-1 if none). */
    int64_t schemaId() const { return hasSchema() ? schema_mgr_.current().id : -1; }

private:
    std::string             root_;
    std::string             name_;
    PaimonSchemaManager     schema_mgr_;

    /* Accumulated row-group data */
    std::vector<PaimonRowKind> rk_buf_;
    std::vector<uint64_t>      seq_buf_;
    /* col_buf_: indexed by position in schema.fields (excl. _seq) */
    std::unordered_map<uint16_t, ColumnValues> col_buf_;
    int64_t buf_rows_ = 0;

    /* Snapshot state */
    int64_t latest_snapshot_id_  = -1;
    int64_t total_record_count_  = 0;

    /*
     * Ordered list of every manifest file committed so far (in-memory).
     * Used to write the cumulative baseManifestList on each flush so that
     * the latest snapshot's base+delta covers all data files.
     * Rebuilt from disk on construction if latest_snapshot_id_ >= 0.
     */
    std::vector<ManifestRef> committed_manifests_;

    void ensureColBuf(uint16_t attnum, PaimonTypeCode tc, uint32_t typmod);
    bool flushInternal();
    void rebuildManifestsFromDisk();
};

} // namespace paimon
