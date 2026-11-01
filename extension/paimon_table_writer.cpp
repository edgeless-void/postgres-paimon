// SPDX-License-Identifier: Apache-2.0
#include "paimon_table_writer.h"
#include "paimon_manifest.h"
#include "paimon_snapshot.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <chrono>

namespace fs = std::filesystem;
namespace paimon {

static int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}

PaimonTableWriter::PaimonTableWriter(const std::string& table_root,
                                     const std::string& table_name)
    : root_(table_root), name_(table_name), schema_mgr_(table_root) {
    fs::create_directories(table_root);
    latest_snapshot_id_ = read_latest_snapshot(table_root);
    if (latest_snapshot_id_ >= 0)
        rebuildManifestsFromDisk();
}

void PaimonTableWriter::rebuildManifestsFromDisk() {
    /*
     * After a daemon restart, reconstruct committed_manifests_ by reading
     * the base + delta manifest-list files of the latest snapshot.
     * The base manifest-list already contains all manifests from all prior
     * snapshots; the delta contains the manifests added in that snapshot.
     */
    std::string manifest_dir = root_ + "/manifest";
    int64_t snap_id = latest_snapshot_id_;

    /* Collect: base manifest-list names first, then delta */
    auto collect = [&](const std::string& mlist_name) {
        if (mlist_name.empty()) return;
        std::string path = manifest_dir + "/" + mlist_name;
        for (const auto& mname : read_manifest_names(path)) {
            ManifestRef ref;
            ref.name      = mname;
            ref.num_added = 0;   /* exact count not required for base */
            ref.schema_id = 0;
            committed_manifests_.push_back(ref);
        }
    };

    /*
     * Walk from snapshot 0 to latest_snapshot_id_ and collect all
     * manifest names in order.  The base manifest-list of each snapshot
     * already covers snapshots 0..N-1, so reading the latest snapshot's
     * base + delta is sufficient.
     */
    /* base manifest-list of the latest snapshot covers all prior flushes */
    auto read_field = [&](const std::string& field) -> std::string {
        fs::path p = fs::path(root_) / "snapshot" /
                     ("snapshot-" + std::to_string(snap_id));
        std::ifstream f(p);
        if (!f) return "";
        std::string line;
        while (std::getline(f, line)) {
            auto pos = line.find("\"" + field + "\"");
            if (pos == std::string::npos) continue;
            auto q1 = line.find('"', pos + field.size() + 2);
            if (q1 == std::string::npos) continue;
            auto q2 = line.find('"', q1 + 1);
            if (q2 == std::string::npos) continue;
            return line.substr(q1 + 1, q2 - q1 - 1);
        }
        return "";
    };

    collect(read_field("baseManifestList"));
    collect(read_field("deltaManifestList"));

    /* Restore the running record count so the next snapshot's
       totalRecordCount continues from the committed state. */
    {
        fs::path p = fs::path(root_) / "snapshot" /
                     ("snapshot-" + std::to_string(snap_id));
        std::ifstream f(p);
        std::string json((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
        auto pos = json.find("\"totalRecordCount\"");
        if (pos != std::string::npos) {
            pos = json.find(':', pos);
            if (pos != std::string::npos) {
                try { total_record_count_ = std::stoll(json.substr(pos + 1)); }
                catch (...) {}
            }
        }
    }
}

bool PaimonTableWriter::initSchema(const std::vector<FieldDef>& fields,
                                    const std::vector<std::string>& pk_names) {
    /*
     * Existing table: if the incoming schema differs, the rows buffered so
     * far were captured under the current schema — write them out first.
     */
    if (schema_mgr_.exists() && schema_mgr_.reconcile(fields, pk_names, true))
        flush();
    return schema_mgr_.bootstrap(fields, pk_names);
}

bool PaimonTableWriter::hasSchema() const {
    return schema_mgr_.exists();
}

void PaimonTableWriter::ensureColBuf(uint16_t attnum, PaimonTypeCode tc,
                                      uint32_t typmod) {
    if (col_buf_.count(attnum)) return;
    ColumnValues cv;
    cv.attnum    = attnum;
    cv.type_code = tc;
    cv.typmod    = typmod;
    col_buf_[attnum] = std::move(cv);
}

void PaimonTableWriter::addRow(PaimonRowKind kind, uint64_t seq,
                                const std::vector<uint16_t>&      attnums,
                                const std::vector<PaimonTypeCode>& types,
                                const std::vector<int32_t>&       lengths,
                                const std::vector<std::string>&   values) {
    rk_buf_.push_back(kind);
    seq_buf_.push_back(seq);

    /* Fill column buffers; NULL-pad any column missing from this row */
    const auto& schema_fields = schema_mgr_.current().fields;

    /* Build a quick lookup of what arrived in this row */
    std::unordered_map<uint16_t, size_t> row_col_idx;
    for (size_t i = 0; i < attnums.size(); ++i)
        row_col_idx[attnums[i]] = i;

    for (const auto& f : schema_fields) {
        if (f.field_id == 32767) continue; /* _seq handled separately */
        ensureColBuf(f.field_id, f.type_code, f.typmod);
        auto& cv = col_buf_[f.field_id];

        auto it = row_col_idx.find(f.field_id);
        if (it == row_col_idx.end()) {
            cv.lengths.push_back(-1); /* NULL */
            cv.values.push_back("");
        } else {
            cv.lengths.push_back(lengths[it->second]);
            cv.values.push_back(values[it->second]);
        }
    }
    ++buf_rows_;

    if (buf_rows_ >= PAIMON_ROWGROUP_ROWS)
        flushInternal();
}

bool PaimonTableWriter::flush() {
    if (buf_rows_ == 0) return true;
    return flushInternal();
}

bool PaimonTableWriter::flushInternal() {
    if (buf_rows_ == 0) return true;
    if (!schema_mgr_.exists()) return false;

    const SchemaVersion& sv = schema_mgr_.current();

    /* Build col_data vector in schema field order */
    std::vector<ColumnValues> col_data;
    for (const auto& f : sv.fields) {
        if (f.field_id == 32767) continue;
        auto it = col_buf_.find(f.field_id);
        if (it != col_buf_.end())
            col_data.push_back(it->second);
    }

    /* Write Parquet — for primary-key tables with VK=1 rows, split into two
       files (ADD rows + DELETE rows) so SortMergeReaderWithMinHeap sees k=2
       sorted runs and correctly deduplicates same-key records across files. */
    std::string bucket_dir = root_ + "/bucket-0";
    std::string manifest_dir = root_ + "/manifest";
    std::vector<DataFileStat> file_stats;
    int64_t flush_row_count = 0;

    bool has_pk = !sv.primary_keys.empty();
    bool need_split = false;
    if (has_pk) {
        for (auto k : rk_buf_)
            if (k == PROW_UPDATE_BEFORE || k == PROW_DELETE) { need_split = true; break; }
    }

    if (!need_split) {
        /* No split needed: single file */
        ParquetFileResult pf = write_parquet_file(
            bucket_dir, sv, rk_buf_, seq_buf_, col_data);
        DataFileStat dfs;
        dfs.file_name        = pf.file_name;
        dfs.file_size        = pf.file_size;
        dfs.row_count        = pf.row_count;
        dfs.delete_row_count = pf.delete_row_count;
        dfs.schema_id        = sv.id;
        dfs.creation_time_ms = now_ms();
        file_stats.push_back(dfs);
        flush_row_count = pf.row_count;
    } else {
        /* Split into ADD file (VK=0) and DELETE file (VK=1) */
        auto split_rows = [&](bool want_delete) {
            std::vector<PaimonRowKind> rk;
            std::vector<uint64_t>     sq;
            /* col_data parallel vectors sliced by row subset */
            std::vector<ColumnValues> cd;
            cd.resize(col_data.size());
            for (size_t c = 0; c < col_data.size(); c++) {
                cd[c].attnum     = col_data[c].attnum;
                cd[c].type_code  = col_data[c].type_code;
                cd[c].typmod     = col_data[c].typmod;
            }
            for (size_t i = 0; i < rk_buf_.size(); i++) {
                bool is_delete = (rk_buf_[i] == PROW_UPDATE_BEFORE ||
                                  rk_buf_[i] == PROW_DELETE);
                if (is_delete != want_delete) continue;
                rk.push_back(rk_buf_[i]);
                sq.push_back(seq_buf_[i]);
                for (size_t c = 0; c < col_data.size(); c++) {
                    cd[c].lengths.push_back(col_data[c].lengths[i]);
                    cd[c].values.push_back(col_data[c].values[i]);
                }
            }
            return std::make_tuple(rk, sq, cd);
        };

        /* ADD rows first (VK=0) */
        {
            auto [rk, sq, cd] = split_rows(false);
            if (!rk.empty()) {
                ParquetFileResult pf = write_parquet_file(bucket_dir, sv, rk, sq, cd);
                DataFileStat dfs;
                dfs.file_name        = pf.file_name;
                dfs.file_size        = pf.file_size;
                dfs.row_count        = pf.row_count;
                dfs.delete_row_count = 0;
                dfs.schema_id        = sv.id;
                dfs.creation_time_ms = now_ms();
                file_stats.push_back(dfs);
                flush_row_count += pf.row_count;
            }
        }
        /* DELETE rows second (VK=1) */
        {
            auto [rk, sq, cd] = split_rows(true);
            if (!rk.empty()) {
                ParquetFileResult pf = write_parquet_file(bucket_dir, sv, rk, sq, cd);
                DataFileStat dfs;
                dfs.file_name        = pf.file_name;
                dfs.file_size        = pf.file_size;
                dfs.row_count        = pf.row_count;
                dfs.delete_row_count = pf.delete_row_count;
                dfs.schema_id        = sv.id;
                dfs.creation_time_ms = now_ms();
                file_stats.push_back(dfs);
                flush_row_count += pf.row_count;
            }
        }
    }

    std::string manifest_name = write_manifest(manifest_dir, file_stats, sv.id);

    /* delta: manifest-list for THIS flush only */
    std::string delta_mlist = write_manifest_list(
        manifest_dir, manifest_name, (int64_t)file_stats.size(), sv.id);

    /* base: cumulative manifest-list covering all PREVIOUS flushes */
    std::string base_mlist = write_manifest_list(manifest_dir, committed_manifests_);

    total_record_count_ += flush_row_count;
    int64_t new_snap = commit_snapshot(
        root_, latest_snapshot_id_, sv.id,
        base_mlist, delta_mlist,
        total_record_count_, flush_row_count);

    if (new_snap < 0) return false;
    latest_snapshot_id_ = new_snap;

    /* Track this manifest for future base writes */
    committed_manifests_.push_back({manifest_name, (int64_t)file_stats.size(), sv.id});

    /* Clear buffers */
    rk_buf_.clear();
    seq_buf_.clear();
    col_buf_.clear();
    buf_rows_ = 0;
    return true;
}

/* ── DDL ─────────────────────────────────────────────────────────── */

bool PaimonTableWriter::ddlAddField(const FieldDef& f) {
    flush(); /* flush before schema change */
    return schema_mgr_.addField(f);
}

bool PaimonTableWriter::ddlDropField(uint16_t field_id) {
    flush();
    col_buf_.erase(field_id);
    return schema_mgr_.dropField(field_id);
}

bool PaimonTableWriter::ddlRenameField(uint16_t field_id,
                                        const std::string& new_name) {
    flush();
    return schema_mgr_.renameField(field_id, new_name);
}

bool PaimonTableWriter::ddlChangeType(uint16_t field_id,
                                       PaimonTypeCode new_type,
                                       uint32_t typmod) {
    flush();
    /* Update the in-flight col_buf entry type so the next writes use the new type */
    auto it = col_buf_.find(field_id);
    if (it != col_buf_.end()) {
        it->second.type_code = new_type;
        it->second.typmod    = typmod;
    }
    return schema_mgr_.changeFieldType(field_id, new_type, typmod);
}

bool PaimonTableWriter::truncate() {
    /* Discard in-memory buffer */
    rk_buf_.clear(); seq_buf_.clear(); col_buf_.clear(); buf_rows_ = 0;
    total_record_count_ = 0;

    /*
     * Pre-truncate manifests must not be carried into the next snapshot's
     * baseManifestList, or the truncated rows reappear on the next flush.
     */
    committed_manifests_.clear();

    if (!schema_mgr_.exists()) return true;   /* nothing in the lake yet */

    /* Write an empty snapshot (no delta files, TRUNCATE kind) */
    /* We use a manifest-list pointing to an empty manifest file */
    std::string manifest_dir = root_ + "/manifest";
    const SchemaVersion& sv  = schema_mgr_.current();

    std::string manifest_name = write_manifest(manifest_dir, {}, sv.id);
    std::string mlist_name    = write_manifest_list(manifest_dir, manifest_name, 0, sv.id);

    fs::path snap_dir = fs::path(root_) / "snapshot";
    fs::create_directories(snap_dir);
    int64_t new_id = latest_snapshot_id_ + 1;

    std::string json =
        "{\n  \"version\": 3,\n  \"id\": " + std::to_string(new_id) +
        ",\n  \"schemaId\": " + std::to_string(sv.id) +
        ",\n  \"baseManifestList\": null"
        ",\n  \"deltaManifestList\": \"" + mlist_name + "\""
        ",\n  \"changelogManifestList\": null"
        ",\n  \"indexManifest\": null"
        ",\n  \"commitUser\": \"clouddb\""
        ",\n  \"commitIdentifier\": " + std::to_string(new_id) +
        ",\n  \"commitKind\": \"OVERWRITE\""
        ",\n  \"timeMillis\": " + std::to_string(now_ms()) +
        ",\n  \"logOffsets\": {}"
        ",\n  \"totalRecordCount\": 0"
        ",\n  \"deltaRecordCount\": 0"
        ",\n  \"changelogRecordCount\": 0"
        ",\n  \"watermark\": -9223372036854775808"
        ",\n  \"statistics\": null\n}\n";

    fs::path dest = snap_dir / ("snapshot-" + std::to_string(new_id));
    fs::path tmp  = snap_dir / ("snapshot-" + std::to_string(new_id) + ".tmp");
    { std::ofstream f(tmp); f << json; }
    fs::rename(tmp, dest);

    { std::ofstream f(snap_dir / "LATEST.tmp"); f << new_id << "\n"; }
    fs::rename(snap_dir / "LATEST.tmp", snap_dir / "LATEST");

    latest_snapshot_id_ = new_id;

    /* The TRUNCATE snapshot's delta manifest is the new (empty) baseline. */
    committed_manifests_.push_back({manifest_name, 0, sv.id});
    return true;
}

bool PaimonTableWriter::drop() {
    rk_buf_.clear(); seq_buf_.clear(); col_buf_.clear(); buf_rows_ = 0;
    std::error_code ec;
    fs::remove_all(root_, ec);
    return !ec;
}

bool PaimonTableWriter::dropMetadataOnly() {
    rk_buf_.clear(); seq_buf_.clear(); col_buf_.clear(); buf_rows_ = 0;
    latest_snapshot_id_ = -1;
    total_record_count_ = 0;
    committed_manifests_.clear();
    std::error_code ec;
    fs::remove_all(fs::path(root_) / "schema",   ec);
    fs::remove_all(fs::path(root_) / "snapshot", ec);
    fs::remove_all(fs::path(root_) / "manifest", ec);
    return true;
}

} // namespace paimon
