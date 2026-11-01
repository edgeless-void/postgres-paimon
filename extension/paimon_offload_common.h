// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_offload_common.h — upload ordering shared by the S3 and HDFS
 * offload paths.
 *
 * A Paimon reader trusts snapshot/LATEST → snapshot-N → manifest lists →
 * manifests → data files.  To never expose a snapshot whose files are not
 * there yet, a table is synced in that dependency order, bottom-up, and a
 * phase is only started when every upload of the previous phases succeeded.
 */
#include <filesystem>
#include <string>
#include <vector>
#include <algorithm>
#include <system_error>

namespace paimon {

enum OffloadPhase {
    OFFLOAD_DATA     = 0,   /* bucket-N/ data files                       */
    OFFLOAD_MANIFEST = 1,   /* manifest/ (manifests + manifest lists)     */
    OFFLOAD_SCHEMA   = 2,   /* schema/schema-N                            */
    OFFLOAD_SNAPSHOT = 3,   /* snapshot/snapshot-N                        */
    OFFLOAD_POINTER  = 4,   /* snapshot/LATEST, snapshot/EARLIEST         */
    OFFLOAD_NPHASES  = 5
};

struct OffloadFile {
    std::filesystem::path local;
    std::string           rel;         /* path relative to the table root */
    OffloadPhase          phase;
    bool                  name_reused; /* same name may get new content   */
};

/* Directories under the table root that hold table metadata. */
static inline const std::vector<std::string> &
offload_metadata_dirs()
{
    static const std::vector<std::string> dirs = {"snapshot", "manifest", "schema"};
    return dirs;
}

static inline std::vector<OffloadFile>
offload_collect(const std::filesystem::path &local_root)
{
    namespace fs = std::filesystem;
    std::vector<OffloadFile> out;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(local_root, ec);
         !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        if (!it->is_regular_file(ec)) continue;
        std::string fname = it->path().filename().string();
        if (fname.size() > 4 && fname.compare(fname.size() - 4, 4, ".tmp") == 0)
            continue;   /* in-progress write */

        OffloadFile f;
        f.local = it->path();
        f.rel   = fs::relative(it->path(), local_root, ec).generic_string();
        std::string top = f.rel.substr(0, f.rel.find('/'));

        f.name_reused = false;
        if (top == "snapshot") {
            if (fname == "LATEST" || fname == "EARLIEST") {
                f.phase = OFFLOAD_POINTER;
            } else {
                f.phase = OFFLOAD_SNAPSHOT;
                f.name_reused = true;    /* snapshot-N restarts after DROP */
            }
        } else if (top == "schema") {
            f.phase = OFFLOAD_SCHEMA;
            f.name_reused = true;        /* schema-N restarts after DROP */
        } else if (top == "manifest") {
            f.phase = OFFLOAD_MANIFEST;  /* UUID names */
        } else {
            f.phase = OFFLOAD_DATA;      /* bucket-N/data-<uuid>.parquet */
        }
        out.push_back(std::move(f));
    }
    std::stable_sort(out.begin(), out.end(),
                     [](const OffloadFile &a, const OffloadFile &b) {
                         return a.phase < b.phase;
                     });
    return out;
}

/* Remote location of a table: "<prefix>/<rel_path>" (prefix may be empty). */
static inline std::string
offload_join(const std::string &prefix, const std::string &rel)
{
    if (prefix.empty()) return rel;
    if (prefix.back() == '/') return prefix + rel;
    return prefix + "/" + rel;
}

} // namespace paimon
