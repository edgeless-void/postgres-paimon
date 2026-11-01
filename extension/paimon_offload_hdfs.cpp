// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_offload_hdfs.cpp — HDFS offload for paimon_heap via libhdfs3.
 *
 * libhdfs3 is a native C++ HDFS client (no JVM) that speaks the Hadoop RPC
 * wire protocol directly.  We use only the stable C API (<hdfs/hdfs.h>).
 *
 * Sync semantics match the S3 implementation (paimon_offload_common.h):
 *   - Upload bottom-up: data, manifests, schema, snapshots, LATEST last; a
 *     phase only starts when the previous phases fully succeeded.
 *   - Skip unchanged files: size check for UUID-named files, byte-for-byte
 *     comparison for schema-N / snapshot-N (names reused after DROP TABLE).
 *   - Upload missing / changed files with hdfsOpenFile(O_WRONLY) + hdfsWrite().
 *   - Local .parquet files are removed by the caller once every configured
 *     target has them.
 *
 * Kerberos
 * --------
 * When krb_principal + krb_keytab are set we perform a programmatic kinit
 * (krb5_get_init_creds_keytab) to refresh the TGT in the default ccache
 * before connecting.  libhdfs3 then picks up the TGT automatically via GSSAPI.
 *
 * Config / namenode discovery
 * ---------------------------
 * If cfg.namenode is non-empty we parse it as "host" or "host:port" (RPC port,
 * default 8020) and pass it explicitly to the builder.
 * If cfg.namenode is empty we pass "default" to hdfsBuilderSetNameNode(), which
 * makes libhdfs3 load hdfs-site.xml from the paths in $HADOOP_CONF_DIR /
 * $HADOOP_HOME/etc/hadoop / standard system locations.
 * If cfg.conf_dir is set we export $HADOOP_CONF_DIR before connecting.
 */

#include "paimon_offload_hdfs.h"
#include "paimon_offload_common.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

/* libhdfs3 C API */
#include <hdfs/hdfs.h>

/* Kerberos programmatic kinit */
#include <krb5.h>

/* PostgreSQL logging — after all C++ / C headers. */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace paimon {

namespace fs = std::filesystem;

/* ── Kerberos TGT refresh ───────────────────────────────────────────────── */

static bool
krb5_kinit_keytab(const std::string &principal_str,
                  const std::string &keytab_path)
{
    krb5_context   context  = nullptr;
    krb5_principal principal = nullptr;
    krb5_keytab    keytab   = nullptr;
    krb5_creds     creds;
    krb5_ccache    ccache   = nullptr;
    krb5_get_init_creds_opt *opts = nullptr;
    bool           success  = false;

    memset(&creds, 0, sizeof(creds));

    do {
        if (krb5_init_context(&context) != 0) {
            ereport(WARNING, errmsg("paimon_hdfs: krb5_init_context failed"));
            break;
        }
        if (krb5_parse_name(context, principal_str.c_str(), &principal) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: krb5_parse_name failed for '%s'",
                           principal_str.c_str()));
            break;
        }

        std::string kt_name = "FILE:" + keytab_path;
        if (krb5_kt_resolve(context, kt_name.c_str(), &keytab) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: cannot open keytab '%s'",
                           keytab_path.c_str()));
            break;
        }
        if (krb5_get_init_creds_opt_alloc(context, &opts) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: krb5_get_init_creds_opt_alloc failed"));
            break;
        }

        krb5_error_code rc = krb5_get_init_creds_keytab(
                context, &creds, principal, keytab, 0, NULL, opts);
        if (rc != 0) {
            const char *msg = krb5_get_error_message(context, rc);
            ereport(WARNING,
                    errmsg("paimon_hdfs: kinit from keytab failed: %s",
                           msg ? msg : "unknown error"));
            if (msg) krb5_free_error_message(context, msg);
            break;
        }

        if (krb5_cc_default(context, &ccache) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: cannot open default credentials cache"));
            krb5_free_cred_contents(context, &creds);
            break;
        }
        if (krb5_cc_initialize(context, ccache, principal) != 0 ||
            krb5_cc_store_cred(context, ccache, &creds) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: cannot store Kerberos credentials"));
            krb5_free_cred_contents(context, &creds);
            break;
        }

        krb5_free_cred_contents(context, &creds);
        success = true;

    } while (false);

    if (opts)      krb5_get_init_creds_opt_free(context, opts);
    if (keytab)    krb5_kt_close(context, keytab);
    if (principal) krb5_free_principal(context, principal);
    if (ccache)    krb5_cc_close(context, ccache);
    if (context)   krb5_free_context(context);
    return success;
}

/* ── Connection builder ─────────────────────────────────────────────────── */

/*
 * Parse "host" or "host:port" from cfg.namenode.
 * Returns the host part; sets *port to the numeric port (default 8020).
 */
static std::string
parse_namenode(const std::string &nn, int *port)
{
    *port = 8020;
    auto colon = nn.rfind(':');
    if (colon == std::string::npos)
        return nn;
    try { *port = std::stoi(nn.substr(colon + 1)); }
    catch (...) {}
    return nn.substr(0, colon);
}

static hdfsFS
connect_hdfs(const HdfsConfig &cfg)
{
    /* Kerberos: refresh TGT before building connection */
    if (!cfg.krb_principal.empty() && !cfg.krb_keytab.empty()) {
        if (!krb5_kinit_keytab(cfg.krb_principal, cfg.krb_keytab))
            return nullptr;
    }

    /* Export HADOOP_CONF_DIR if caller specified a conf directory. */
    if (!cfg.conf_dir.empty())
        setenv("HADOOP_CONF_DIR", cfg.conf_dir.c_str(), /*overwrite=*/1);

    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld) {
        ereport(WARNING, errmsg("paimon_hdfs: hdfsNewBuilder failed"));
        return nullptr;
    }

    if (cfg.namenode.empty()) {
        /* Auto-discover from hdfs-site.xml */
        hdfsBuilderSetNameNode(bld, "default");
    } else {
        int port;
        std::string host = parse_namenode(cfg.namenode, &port);
        hdfsBuilderSetNameNode(bld, host.c_str());
        hdfsBuilderSetNameNodePort(bld, (tPort)port);
    }

    if (!cfg.user.empty())
        hdfsBuilderSetUserName(bld, cfg.user.c_str());

    /* hdfsBuilderConnect frees the builder regardless of outcome. */
    hdfsFS fsh = hdfsBuilderConnect(bld);
    if (!fsh)
        ereport(WARNING,
                errmsg("paimon_hdfs: hdfsBuilderConnect failed: %s",
                       hdfsGetLastError()));
    return fsh;
}

/* ── Upload helpers ─────────────────────────────────────────────────────── */

/*
 * True if the remote file already holds this local file: same size and, with
 * check_content, the same bytes (only used for small metadata files).
 */
static bool
remote_file_matches(hdfsFS fsh, const std::string &path,
                    const fs::path &local_path, int64_t expected_size,
                    bool check_content)
{
    hdfsFileInfo *info = hdfsGetPathInfo(fsh, path.c_str());
    if (!info) return false;
    bool match = (info->mSize == (tOffset)expected_size);
    hdfsFreeFileInfo(info, 1);
    if (!match || !check_content) return match;

    std::ifstream in(local_path, std::ios::binary);
    if (!in.is_open()) return false;
    std::string local((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());

    hdfsFile rf = hdfsOpenFile(fsh, path.c_str(), O_RDONLY, 0, 0, 0);
    if (!rf) return false;
    std::string remote(local.size(), '\0');
    size_t got = 0;
    while (got < remote.size()) {
        tSize n = hdfsRead(fsh, rf, &remote[got], (tSize)(remote.size() - got));
        if (n <= 0) break;
        got += (size_t)n;
    }
    hdfsCloseFile(fsh, rf);
    return got == local.size() && remote == local;
}

/* Create remote directory (and all parents) idempotently. */
static bool
ensure_dir(hdfsFS fsh, const std::string &path)
{
    if (hdfsExists(fsh, path.c_str()) == 0)
        return true;
    if (hdfsCreateDirectory(fsh, path.c_str()) != 0) {
        ereport(WARNING,
                errmsg("paimon_hdfs: cannot create directory %s: %s",
                       path.c_str(), hdfsGetLastError()));
        return false;
    }
    return true;
}

/* Upload one file. Returns true on success. */
static bool
put_file(hdfsFS fsh, const std::string &remote_path, const fs::path &local_path)
{
    /* Ensure the remote parent directory exists. */
    std::string remote_dir = remote_path.substr(0, remote_path.rfind('/'));
    if (!ensure_dir(fsh, remote_dir))
        return false;

    hdfsFile out = hdfsOpenFile(fsh, remote_path.c_str(),
                                O_WRONLY, /*bufferSize=*/0,
                                /*replication=*/0, /*blocksize=*/0);
    if (!out) {
        ereport(WARNING,
                errmsg("paimon_hdfs: cannot open remote file %s for write: %s",
                       remote_path.c_str(), hdfsGetLastError()));
        return false;
    }

    std::ifstream in(local_path, std::ios::binary);
    if (!in.is_open()) {
        hdfsCloseFile(fsh, out);
        ereport(WARNING,
                errmsg("paimon_hdfs: cannot open local file %s",
                       local_path.c_str()));
        return false;
    }

    static constexpr size_t BUF = 1 << 20; /* 1 MB write buffer */
    std::vector<char> buf(BUF);
    bool ok = true;

    while (in.good()) {
        in.read(buf.data(), BUF);
        std::streamsize n = in.gcount();
        if (n <= 0) break;
        tSize written = hdfsWrite(fsh, out, buf.data(), (tSize)n);
        if (written != (tSize)n) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: write error for %s: %s",
                           remote_path.c_str(), hdfsGetLastError()));
            ok = false;
            break;
        }
    }

    if (hdfsHFlush(fsh, out) != 0) ok = false;
    hdfsCloseFile(fsh, out);
    return ok;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

bool
hdfs_upload_table(const HdfsConfig     &cfg,
                  const std::string    &local_root_str,
                  const std::string    &remote_rel)
{
    if (cfg.base_path.empty()) return true;

    fs::path local_root(local_root_str);
    std::error_code ec;
    if (!fs::exists(local_root, ec)) return true;

    hdfsFS fsh = connect_hdfs(cfg);
    if (!fsh) return false;

    std::string remote_root = offload_join(cfg.base_path, remote_rel);
    ensure_dir(fsh, remote_root);

    bool all_ok = true;
    int  phase  = -1;

    for (const auto &f : offload_collect(local_root)) {
        if (f.phase != phase) {
            /* Never publish metadata on top of a failed lower layer. */
            if (!all_ok) break;
            phase = f.phase;
        }

        std::string remote = remote_root + "/" + f.rel;
        int64_t     size   = (int64_t)fs::file_size(f.local, ec);
        if (ec) { all_ok = false; continue; }

        if (f.phase != OFFLOAD_POINTER &&
            remote_file_matches(fsh, remote, f.local, size, f.name_reused))
            continue;

        if (!put_file(fsh, remote, f.local))
            all_ok = false;
    }

    hdfsDisconnect(fsh);

    if (!all_ok)
        ereport(WARNING,
                errmsg("paimon_hdfs: sync of %s incomplete; "
                       "will retry on the next flush", remote_root.c_str()));
    return all_ok;
}

bool
hdfs_drop_table_metadata(const HdfsConfig &cfg, const std::string &remote_rel)
{
    if (cfg.base_path.empty()) return true;

    hdfsFS fsh = connect_hdfs(cfg);
    if (!fsh) return false;

    std::string remote_root = offload_join(cfg.base_path, remote_rel);
    bool ok = true;
    /* snapshot/ first: once LATEST is gone readers stop seeing the table. */
    for (const auto &dir : offload_metadata_dirs()) {
        std::string p = remote_root + "/" + dir;
        if (hdfsExists(fsh, p.c_str()) != 0)
            continue;
        if (hdfsDelete(fsh, p.c_str(), /*recursive=*/1) != 0) {
            ereport(WARNING,
                    errmsg("paimon_hdfs: cannot delete %s: %s",
                           p.c_str(), hdfsGetLastError()));
            ok = false;
        }
    }
    hdfsDisconnect(fsh);
    return ok;
}

} // namespace paimon
