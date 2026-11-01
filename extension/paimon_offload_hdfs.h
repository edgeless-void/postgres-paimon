// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>

namespace paimon {

struct HdfsConfig {
    /* WebHDFS namenode HTTP(S) host and port, e.g. "namenode1:9870". */
    std::string namenode;       // empty → try hdfs-site.xml auto-discovery
    bool        use_https = false;

    /* Base HDFS path under which table dirs are placed. */
    std::string base_path;      // e.g. "/warehouse/paimon"

    /* HDFS user name (empty = current OS user). */
    std::string user;

    /* Directory containing hdfs-site.xml (empty = search standard locations). */
    std::string conf_dir;

    /* Kerberos: both must be set together; empty = simple auth. */
    std::string krb_principal;  // e.g. "hdfs/host@REALM"
    std::string krb_keytab;     // absolute path to keytab file
};

// Sync the local table directory local_root →
// hdfs://<namenode>/<base_path>/<remote_rel>/ bottom-up (data, manifests,
// schema, snapshots, LATEST last), skipping files that are already identical.
// Does not delete local files.  Returns true when everything is in sync;
// logs WARNING and returns false otherwise (the next call retries).
bool hdfs_upload_table(const HdfsConfig  &cfg,
                       const std::string &local_root,
                       const std::string &remote_rel);

// DROP TABLE: remove snapshot/, manifest/ and schema/ under
// <base_path>/<remote_rel>/.  Data files are left in place.
bool hdfs_drop_table_metadata(const HdfsConfig &cfg, const std::string &remote_rel);

} // namespace paimon
