// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>

namespace paimon {

struct S3Config {
    std::string bucket;
    std::string prefix;    // key prefix inside bucket, no trailing slash
    std::string region;    // empty → SDK credential/config chain
    std::string endpoint;  // empty → AWS; set to MinIO/Ceph URL otherwise
};

// SDK lifecycle — call once at bgworker start/stop.
void s3_sdk_init();
void s3_sdk_shutdown();

// Sync the local table directory local_root → s3://<bucket>/<prefix>/<remote_rel>/
// bottom-up (data, manifests, schema, snapshots, LATEST last), skipping
// objects that are already identical.  Does not delete local files.
// Returns true when everything is in sync; logs WARNING and returns false
// otherwise (the next call retries).
bool s3_upload_table(const S3Config &cfg,
                     const std::string &local_root,
                     const std::string &remote_rel);

// DROP TABLE: remove snapshot/, manifest/ and schema/ under
// s3://<bucket>/<prefix>/<remote_rel>/ so readers no longer see the table and
// a re-created table with the same name starts from clean metadata.  Data
// files are left in place (same as the local warehouse).
bool s3_drop_table_metadata(const S3Config &cfg, const std::string &remote_rel);

} // namespace paimon
