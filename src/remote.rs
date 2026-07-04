use anyhow::{Result, anyhow};
use aws_config::retry::RetryConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, GlacierJobParameters, RestoreRequest, StorageClass,
    Tier,
};
use std::path::Path;
use tokio::io::AsyncReadExt;

use crate::config::S3Config;

const PART_SIZE: usize = 64 * 1024 * 1024;

/// Whether an object can be downloaded right now, considering archive storage classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreStatus {
    /// Downloadable now: not archived, or an archive retrieval has completed.
    Ready,
    /// An archive retrieval was requested and is still running.
    InProgress,
    /// Archived and no retrieval has been requested yet.
    NotRequested,
}

#[allow(async_fn_in_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait RemoteBackend: Send + Sync {
    /// `tags` is a URL-query-encoded tag set (e.g. "bcb-backup-level=0") stamped on the
    /// object so S3 lifecycle rules can filter by it.
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        storage_class: &str,
        tags: &str,
    ) -> Result<()>;
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        storage_class: &str,
        tags: &str,
    ) -> Result<()>;
    /// Returns the object size in bytes, or None if the key does not exist.
    async fn head(&self, key: &str) -> Result<Option<u64>>;
    async fn download(&self, key: &str) -> Result<Vec<u8>>;
    /// Streams the object to a local file without buffering it in memory.
    async fn download_to_file(&self, key: &str, path: &Path) -> Result<()>;
    /// Reports whether the object is downloadable, in archive retrieval, or needs one.
    async fn restore_status(&self, key: &str) -> Result<RestoreStatus>;
    /// Initiates an archive retrieval so the object becomes downloadable for `days`.
    async fn request_restore(&self, key: &str, days: i32, tier: &str) -> Result<()>;
    async fn verify_credentials(&self) -> Result<()>;
    async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: String,
    upload_speed_limit_bps: Option<f64>,
}

impl S3Backend {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let region = aws_sdk_s3::config::Region::new(config.region.clone());
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let retry = RetryConfig::standard().with_max_attempts(config.retry.max_attempts);
        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config).retry_config(retry);
        if !config.endpoint.is_empty() {
            builder = builder
                .endpoint_url(config.endpoint.clone())
                .force_path_style(true);
        }

        let upload_speed_limit_bps = (config.upload_speed_limit_kbps > 0)
            .then_some(config.upload_speed_limit_kbps as f64 * 1024.0);

        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            upload_speed_limit_bps,
        })
    }
}

pub(crate) fn parse_storage_class(s: &str) -> Result<StorageClass> {
    match s {
        "DEEP_ARCHIVE" => Ok(StorageClass::DeepArchive),
        "GLACIER" => Ok(StorageClass::Glacier),
        "GLACIER_IR" => Ok(StorageClass::GlacierIr),
        "INTELLIGENT_TIERING" => Ok(StorageClass::IntelligentTiering),
        "ONEZONE_IA" => Ok(StorageClass::OnezoneIa),
        "REDUCED_REDUNDANCY" => Ok(StorageClass::ReducedRedundancy),
        "STANDARD" => Ok(StorageClass::Standard),
        "STANDARD_IA" => Ok(StorageClass::StandardIa),
        other => Err(anyhow!("unknown S3 storage class: {other}")),
    }
}

// Test-only pause point: parks here right after a part upload succeeds (upload_id exists on S3
// with at least one part already uploaded, nothing completed/aborted yet) so the VM e2e suite can
// SIGKILL bcb at a deterministic point to exercise orphaned-multipart-upload cleanup (see
// abort_existing_multipart_uploads) instead of racing on sleep timing.
#[cfg(debug_assertions)]
async fn test_pause_after_s3_part(part_number: i32) {
    if let Ok(target) = std::env::var("BCB_TEST_PAUSE_AFTER_S3_PART_NUMBER")
        && target.parse() == Ok(part_number)
    {
        use std::io::Write;
        eprintln!("BCB_TEST_PAUSE_REACHED:s3_part:{part_number}");
        let _ = std::io::stderr().flush();
        std::future::pending::<()>().await;
    }
}

#[cfg(not(debug_assertions))]
async fn test_pause_after_s3_part(_part_number: i32) {}

async fn abort_existing_multipart_uploads(client: &Client, bucket: &str, key: &str) {
    // Some S3-compatible servers don't reliably honor the `prefix` filter on
    // ListMultipartUploads (observed against MinIO: a prefix that's a genuine string-prefix of
    // an in-progress upload's key still comes back empty), so list unfiltered and rely entirely
    // on the exact-key match below for correctness rather than trusting server-side filtering.
    let mut key_marker: Option<String> = None;
    let mut upload_id_marker: Option<String> = None;
    loop {
        let mut req = client.list_multipart_uploads().bucket(bucket);
        if let Some(km) = &key_marker {
            req = req.key_marker(km);
        }
        if let Some(uim) = &upload_id_marker {
            req = req.upload_id_marker(uim);
        }
        let resp = match req.send().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("failed to list multipart uploads for {key}: {e}");
                return;
            }
        };

        for upload in resp.uploads.into_iter().flatten() {
            if upload.key() != Some(key) {
                continue;
            }
            let Some(upload_id) = upload.upload_id() else {
                continue;
            };
            if let Err(e) = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
            {
                tracing::warn!(
                    "failed to abort orphaned multipart upload {upload_id} for {key}: {e}"
                );
            }
        }

        if !resp.is_truncated.unwrap_or(false) {
            break;
        }
        key_marker = resp.next_key_marker;
        upload_id_marker = resp.next_upload_id_marker;
    }
}

async fn multipart_upload<R: tokio::io::AsyncRead + Unpin>(
    client: &Client,
    bucket: &str,
    key: &str,
    storage_class: StorageClass,
    tags: &str,
    mut reader: R,
) -> Result<()> {
    // No explicit checksum_algorithm is set
    // aws-sdk-s3's default RequestChecksumCalculation (WhenSupported) already attaches a per-part CRC32 that
    // S3 validates before accepting each UploadPart, so transmission corruption is already rejected immediately.
    // A full-object checksum (CRC64NVME) or stored BLAKE3 metadata wouldn't add coverage beyond that,
    // since bcb's own BLAKE3 manifest hash already verifies the whole stream at restore time.

    // The PID lock prevents concurrent bcb instances, so any incomplete upload already sitting
    // under this key can only be an orphan from a prior run that was killed mid-upload; clean it
    // up before starting a fresh session so it doesn't linger (and keep costing storage) forever.
    abort_existing_multipart_uploads(client, bucket, key).await;

    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .storage_class(storage_class)
        .tagging(tags)
        .send()
        .await
        .map_err(|e| anyhow!("create_multipart_upload failed: {e}"))?
        .upload_id()
        .ok_or_else(|| anyhow!("create_multipart_upload returned no upload_id"))?
        .to_string();

    let result: Result<Vec<CompletedPart>> = async {
        let mut parts = Vec::new();
        let mut part_buf = vec![0u8; PART_SIZE];
        let mut part_number = 1i32;

        loop {
            let mut filled = 0;
            while filled < PART_SIZE {
                let n = reader.read(&mut part_buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }

            let resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .content_length(filled as i64)
                .body(ByteStream::from(bytes::Bytes::copy_from_slice(
                    &part_buf[..filled],
                )))
                .send()
                .await
                .map_err(|e| anyhow!("upload_part {part_number} failed: {e}"))?;

            let etag = resp
                .e_tag()
                .ok_or_else(|| anyhow!("upload_part {part_number} returned no ETag"))?
                .to_string();

            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );
            test_pause_after_s3_part(part_number).await;
            part_number += 1;
        }

        Ok(parts)
    }
    .await;

    match result {
        Ok(parts) => {
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map_err(|e| anyhow!("complete_multipart_upload failed: {e}"))?;
            Ok(())
        }
        Err(e) => {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(e)
        }
    }
}

impl RemoteBackend for S3Backend {
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        storage_class: &str,
        tags: &str,
    ) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(parse_storage_class(storage_class)?)
            .tagging(tags)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| anyhow!("S3 put_object failed: {e}"))?;
        Ok(())
    }

    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        storage_class: &str,
        tags: &str,
    ) -> Result<()> {
        let sc = parse_storage_class(storage_class)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| anyhow!("failed to open file for upload: {e}"))?;

        match self.upload_speed_limit_bps {
            Some(bps) => {
                use async_speed_limit::Limiter;
                use async_speed_limit::clock::StandardClock;
                use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
                let reader = Limiter::<StandardClock>::new(bps)
                    .limit(file.compat())
                    .compat();
                multipart_upload(&self.client, &self.bucket, key, sc, tags, reader).await
            }
            None => multipart_upload(&self.client, &self.bucket, key, sc, tags, file).await,
        }
    }

    async fn head(&self, key: &str) -> Result<Option<u64>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => Ok(resp.content_length().map(|n| n as u64)),
            Err(e) => {
                if matches!(e.as_service_error(), Some(HeadObjectError::NotFound(_))) {
                    Ok(None)
                } else {
                    Err(anyhow!("S3 head_object failed: {e}"))
                }
            }
        }
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 get_object failed: {e}"))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| anyhow!("failed to read S3 response body: {e}"))?
            .into_bytes();

        Ok(bytes.to_vec())
    }

    async fn download_to_file(&self, key: &str, path: &Path) -> Result<()> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 get_object failed: {e}"))?;

        let mut body = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(path)
            .await
            .map_err(|e| anyhow!("failed to create {:?}: {e}", path))?;
        tokio::io::copy(&mut body, &mut file)
            .await
            .map_err(|e| anyhow!("failed to write S3 object to {:?}: {e}", path))?;
        Ok(())
    }

    async fn restore_status(&self, key: &str) -> Result<RestoreStatus> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("S3 head_object failed for {key}: {e}"))?;

        // Only these classes require an explicit retrieval before GET; GLACIER_IR and the
        // rest are directly readable.
        let archived = matches!(
            resp.storage_class(),
            Some(StorageClass::DeepArchive) | Some(StorageClass::Glacier)
        );
        if !archived {
            return Ok(RestoreStatus::Ready);
        }

        // x-amz-restore: absent = never requested; ongoing-request="true" = running;
        // ongoing-request="false" = a restored copy is available until its expiry date.
        match resp.restore() {
            None => Ok(RestoreStatus::NotRequested),
            Some(r) if r.contains("ongoing-request=\"true\"") => Ok(RestoreStatus::InProgress),
            Some(_) => Ok(RestoreStatus::Ready),
        }
    }

    async fn request_restore(&self, key: &str, days: i32, tier: &str) -> Result<()> {
        let request = RestoreRequest::builder()
            .days(days)
            .glacier_job_parameters(
                GlacierJobParameters::builder()
                    .tier(Tier::from(tier))
                    .build()
                    .map_err(|e| anyhow!("invalid retrieval tier: {e}"))?,
            )
            .build();

        match self
            .client
            .restore_object()
            .bucket(&self.bucket)
            .key(key)
            .restore_request(request)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                use aws_sdk_s3::error::ProvideErrorMetadata;
                // A retrieval already running (or the object already readable) means the
                // caller's goal is met; only one retrieval needs to be in flight.
                match e.code() {
                    Some("RestoreAlreadyInProgress") | Some("ObjectAlreadyInActiveTierError") => {
                        Ok(())
                    }
                    _ => Err(anyhow!("S3 restore_object failed for {key}: {e}")),
                }
            }
        }
    }

    async fn verify_credentials(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| anyhow!("cannot access S3 bucket: {e}"))?;

        // head_bucket only proves read access; backups need write and delete, so probe with
        // a throwaway object under the configured prefix.
        let probe_key = format!("{}bcb_check_probe", self.prefix);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&probe_key)
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .map_err(|e| anyhow!("cannot write to S3 bucket: {e}"))?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&probe_key)
            .send()
            .await
            .map_err(|e| anyhow!("cannot delete from S3 bucket: {e}"))?;

        Ok(())
    }

    async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| anyhow!("S3 list_objects_v2 failed: {e}"))?;

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                match resp.next_continuation_token() {
                    Some(token) => continuation_token = Some(token.to_string()),
                    None => {
                        return Err(anyhow!(
                            "S3 list_objects_v2 returned is_truncated=true with no NextContinuationToken"
                        ));
                    }
                }
            } else {
                break;
            }
        }

        Ok(keys)
    }
}
