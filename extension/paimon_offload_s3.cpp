// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_offload_s3.cpp — S3 offload for paimon_heap via AWS SDK C++.
 *
 * Sync semantics (see paimon_offload_common.h):
 *   Files are uploaded bottom-up — data, manifests, schema, snapshots and
 *   finally snapshot/LATEST — and a phase only starts when every upload of
 *   the previous phases succeeded, so S3 never exposes a snapshot that
 *   references objects which are not there yet.
 *
 *   Unchanged files are skipped: UUID-named files (data, manifests) by a
 *   HeadObject size check, files whose names are reused after DROP TABLE +
 *   CREATE TABLE (schema-N, snapshot-N) by comparing MD5 with the ETag.
 *   LATEST is always uploaded.
 *
 *   Removing the uploaded local .parquet files is left to the caller, which
 *   does it only once every configured offload target has them.
 */

#include "paimon_offload_s3.h"
#include "paimon_offload_common.h"

#include <filesystem>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

/* PostgreSQL logging — included AFTER all C++ headers. */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace paimon {

namespace fs = std::filesystem;

/* ── SDK lifecycle ──────────────────────────────────────────────────────── */

static Aws::SDKOptions g_sdk_options;

void s3_sdk_init()
{
    /* Don't let the SDK install a SIGPIPE handler — PostgreSQL owns signals. */
    g_sdk_options.httpOptions.installSigPipeHandler = false;
    Aws::InitAPI(g_sdk_options);
}

void s3_sdk_shutdown()
{
    Aws::ShutdownAPI(g_sdk_options);
}

/* ── Internal helpers ───────────────────────────────────────────────────── */

static std::unique_ptr<Aws::S3::S3Client>
make_client(const S3Config &cfg)
{
    Aws::Client::ClientConfiguration cc;
    if (!cfg.region.empty())
        cc.region = cfg.region;
    if (!cfg.endpoint.empty()) {
        cc.endpointOverride = cfg.endpoint;
        /* Path-style addressing is required for non-AWS endpoints (e.g. MinIO). */
        cc.scheme = Aws::Http::Scheme::HTTP;
    }
    /* Use the full credential chain: env vars, ~/.aws/credentials, IAM role. */
    return std::make_unique<Aws::S3::S3Client>(
        cc,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing=*/cfg.endpoint.empty());
}

/*
 * True if the S3 object already holds this local file.  With check_content
 * the MD5 of the local file must equal the object's ETag (single-part
 * uploads, which is all this module does); otherwise the size must match.
 */
static bool
remote_file_matches(Aws::S3::S3Client &client,
                    const std::string &bucket,
                    const std::string &key,
                    const fs::path    &local_path,
                    int64_t            expected_size,
                    bool               check_content)
{
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    auto outcome = client.HeadObject(req);
    if (!outcome.IsSuccess()) return false;
    if (outcome.GetResult().GetContentLength() != expected_size) return false;
    if (!check_content) return true;

    Aws::FStream in(local_path.string().c_str(),
                    std::ios_base::in | std::ios_base::binary);
    if (!in.good()) return false;
    Aws::String md5 = Aws::Utils::HashingUtils::HexEncode(
        Aws::Utils::HashingUtils::CalculateMD5(in));

    std::string etag(outcome.GetResult().GetETag().c_str());
    etag.erase(std::remove(etag.begin(), etag.end(), '"'), etag.end());
    std::string local_md5(md5.c_str());
    if (etag.size() != local_md5.size()) return false;
    for (size_t i = 0; i < etag.size(); i++)
        if (std::tolower((unsigned char)etag[i]) !=
            std::tolower((unsigned char)local_md5[i]))
            return false;
    return true;
}

/* Upload a single local file to S3. Returns true on success. */
static bool
put_file(Aws::S3::S3Client &client,
         const std::string  &bucket,
         const std::string  &key,
         const fs::path     &local_path)
{
    auto stream = Aws::MakeShared<Aws::FStream>(
        "PaimonS3Upload",
        local_path.string().c_str(),
        std::ios_base::in | std::ios_base::binary);

    if (!stream->good()) {
        ereport(WARNING, errmsg("paimon_s3: cannot open %s for upload",
                                local_path.c_str()));
        return false;
    }

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetBody(stream);

    auto outcome = client.PutObject(req);
    if (!outcome.IsSuccess()) {
        ereport(WARNING,
                errmsg("paimon_s3: PutObject failed for key %s: %s",
                       key.c_str(),
                       outcome.GetError().GetMessage().c_str()));
        return false;
    }
    return true;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

bool
s3_upload_table(const S3Config     &cfg,
                const std::string  &local_root_str,
                const std::string  &remote_rel)
{
    if (cfg.bucket.empty()) return true;

    fs::path local_root(local_root_str);
    std::error_code ec;
    if (!fs::exists(local_root, ec)) return true; /* nothing to upload */

    std::string key_prefix = offload_join(cfg.prefix, remote_rel);
    auto client = make_client(cfg);

    bool all_ok = true;
    int  phase  = -1;

    for (const auto &f : offload_collect(local_root)) {
        if (f.phase != phase) {
            /* Never publish metadata on top of a failed lower layer. */
            if (!all_ok) break;
            phase = f.phase;
        }

        std::string key  = key_prefix + "/" + f.rel;
        int64_t     size = (int64_t)fs::file_size(f.local, ec);
        if (ec) { all_ok = false; continue; }

        if (f.phase != OFFLOAD_POINTER &&
            remote_file_matches(*client, cfg.bucket, key, f.local, size,
                                f.name_reused))
            continue;

        if (!put_file(*client, cfg.bucket, key, f.local))
            all_ok = false;
    }

    if (!all_ok)
        ereport(WARNING,
                errmsg("paimon_s3: sync of s3://%s/%s incomplete; "
                       "will retry on the next flush",
                       cfg.bucket.c_str(), key_prefix.c_str()));
    return all_ok;
}

/* Delete every object under s3://bucket/<prefix>/ . */
static bool
delete_prefix(Aws::S3::S3Client &client, const std::string &bucket,
              const std::string &prefix)
{
    bool        ok = true;
    Aws::String token;
    for (;;) {
        Aws::S3::Model::ListObjectsV2Request lreq;
        lreq.SetBucket(bucket);
        lreq.SetPrefix(prefix);
        if (!token.empty())
            lreq.SetContinuationToken(token);
        auto lout = client.ListObjectsV2(lreq);
        if (!lout.IsSuccess()) {
            ereport(WARNING,
                    errmsg("paimon_s3: ListObjectsV2 failed for prefix %s: %s",
                           prefix.c_str(),
                           lout.GetError().GetMessage().c_str()));
            return false;
        }
        for (const auto &obj : lout.GetResult().GetContents()) {
            Aws::S3::Model::DeleteObjectRequest dreq;
            dreq.SetBucket(bucket);
            dreq.SetKey(obj.GetKey());
            auto dout = client.DeleteObject(dreq);
            if (!dout.IsSuccess()) {
                ereport(WARNING,
                        errmsg("paimon_s3: DeleteObject failed for key %s: %s",
                               obj.GetKey().c_str(),
                               dout.GetError().GetMessage().c_str()));
                ok = false;
            }
        }
        if (!lout.GetResult().GetIsTruncated()) break;
        token = lout.GetResult().GetNextContinuationToken();
        if (token.empty()) break;
    }
    return ok;
}

bool
s3_drop_table_metadata(const S3Config &cfg, const std::string &remote_rel)
{
    if (cfg.bucket.empty()) return true;

    std::string key_prefix = offload_join(cfg.prefix, remote_rel);
    auto client = make_client(cfg);

    /* snapshot/ first: once LATEST is gone readers stop seeing the table. */
    bool ok = true;
    for (const auto &dir : offload_metadata_dirs())
        if (!delete_prefix(*client, cfg.bucket, key_prefix + "/" + dir + "/"))
            ok = false;

    if (!ok)
        ereport(WARNING,
                errmsg("paimon_s3: could not remove metadata of dropped table "
                       "s3://%s/%s", cfg.bucket.c_str(), key_prefix.c_str()));
    return ok;
}

} // namespace paimon
