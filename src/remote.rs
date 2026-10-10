//! The object store, behind [`RemoteBackend`] so backup and restore can be tested against a
//! mock. [`S3Backend`] implements it with the AWS SDK.

use anyhow::{Context, Result, anyhow};
use aws_config::retry::RetryConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, GlacierJobParameters, RestoreRequest,
    StorageClass as SdkStorageClass, Tier,
};
use std::fmt;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::config::{S3Config, StorageClass};
use crate::fsutil::fsync_dir;
use crate::layout::check_tag;
use crate::test_hooks;
use crate::util::{BYTES_PER_KBPS, IO_BUF_SIZE};

/// Size of each chunk of a multipart upload.
const MULTIPART_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// An error from S3 or the network on the way to it, as opposed to a local one. It is a
/// distinct type so a restore can tell the two apart: a part whose download failed remotely
/// is retried on the next run, while a local error such as a full disk fails the run.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RemoteError(String);

impl RemoteError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

fn remote_error(context: impl fmt::Display, e: impl std::error::Error) -> anyhow::Error {
    RemoteError(format!("{context}: {}", DisplayErrorContext(&e))).into()
}

/// Whether an object can be downloaded right now, given archive storage classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrievalStatus {
    /// Downloadable now: not archived, or an archive retrieval has completed.
    Ready,
    /// An archive retrieval was requested and is still running.
    InProgress,
    /// Archived and no retrieval has been requested yet.
    NotRequested,
}

#[derive(Debug, Clone)]
pub struct Retrieval {
    pub status: RetrievalStatus,
    /// Unix time the retrieved copy of an archived object expires; None if not archived or not
    /// retrieved yet.
    pub expires_at: Option<i64>,
}

/// Archive retrieval tier, named as in the S3 API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "PascalCase")]
pub enum RetrievalTier {
    Standard,
    Bulk,
    Expedited,
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size_bytes: u64,
    pub storage_class: String,
}

#[allow(async_fn_in_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait RemoteBackend: Send + Sync {
    /// `tags` is a URL-query-encoded tag set (see `layout::level_tag`) stamped on the object
    /// so S3 lifecycle rules can filter by it.
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        storage_class: StorageClass,
        tags: &str,
    ) -> Result<()>;
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        storage_class: StorageClass,
        tags: &str,
    ) -> Result<()>;
    async fn download(&self, key: &str) -> Result<Vec<u8>>;
    /// Streams the inclusive byte range `start..=end` of the object to a local file, creating
    /// it when `start` is 0 and appending otherwise.
    async fn download_range_to_file(
        &self,
        key: &str,
        start: u64,
        end: u64,
        path: &Path,
    ) -> Result<()>;
    /// The object's size and storage class, or None if it doesn't exist. Never requests or
    /// waits on an archive retrieval.
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>>;
    async fn retrieval_status(&self, key: &str) -> Result<Retrieval>;
    /// Starts an archive retrieval that keeps the object downloadable for `days`.
    async fn request_retrieval(&self, key: &str, days: i32, tier: RetrievalTier) -> Result<()>;
    /// Checks that the bucket can be read, written, tagged and deleted from.
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
            .then_some(config.upload_speed_limit_kbps as f64 * BYTES_PER_KBPS);

        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            upload_speed_limit_bps,
        })
    }

    /// HeadObject, with a missing object as None.
    async fn head(&self, key: &str) -> Result<Option<HeadObjectOutput>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp)),
            Err(e) if matches!(e.as_service_error(), Some(HeadObjectError::NotFound(_))) => {
                Ok(None)
            }
            Err(e) => Err(remote_error(format!("S3 head_object failed for {key}"), e)),
        }
    }

    /// The instance lock keeps other bcb processes out, so an incomplete upload already under
    /// `key` is an orphan of a run killed mid-upload. It is aborted so it doesn't keep costing
    /// storage.
    async fn abort_orphaned_uploads(&self, key: &str) {
        // Some S3-compatible servers don't reliably honor ListMultipartUploads' `prefix`
        // filter, returning nothing for a prefix of an upload's key, so the listing is
        // unfiltered and matched on the exact key here.
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        loop {
            let resp = match self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .set_key_marker(key_marker.take())
                .set_upload_id_marker(upload_id_marker.take())
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "failed to list multipart uploads for {key}: {}",
                        DisplayErrorContext(&e)
                    );
                    return;
                }
            };

            for upload in resp.uploads() {
                let (Some(upload_key), Some(upload_id)) = (upload.key(), upload.upload_id()) else {
                    continue;
                };
                if upload_key != key {
                    continue;
                }
                if let Err(e) = self.abort_upload(key, upload_id).await {
                    tracing::warn!("failed to abort orphaned multipart upload {upload_id}: {e}");
                }
            }

            if !resp.is_truncated.unwrap_or(false) {
                return;
            }
            key_marker = resp.next_key_marker;
            upload_id_marker = resp.next_upload_id_marker;
        }
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| remote_error(format!("abort_multipart_upload failed for {key}"), e))?;
        Ok(())
    }

    async fn multipart_upload(
        &self,
        key: &str,
        storage_class: StorageClass,
        tags: &str,
        reader: impl AsyncRead + Unpin,
    ) -> Result<()> {
        self.abort_orphaned_uploads(key).await;

        // No explicit checksum_algorithm: the SDK's default per-chunk CRC32 already rejects
        // transmission corruption, and the manifest's BLAKE3 verifies the whole stream at
        // restore time, so a full-object checksum would add no coverage.
        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(sdk_storage_class(storage_class))
            .tagging(tags)
            .send()
            .await
            .map_err(|e| remote_error(format!("create_multipart_upload failed for {key}"), e))?
            .upload_id()
            .ok_or_else(|| anyhow!("create_multipart_upload returned no upload_id"))?
            .to_string();

        let parts = match self.upload_chunks(key, &upload_id, reader).await {
            Ok(parts) => parts,
            Err(e) => {
                let _ = self.abort_upload(key, &upload_id).await;
                return Err(e);
            }
        };
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| remote_error(format!("complete_multipart_upload failed for {key}"), e))?;
        Ok(())
    }

    async fn upload_chunks(
        &self,
        key: &str,
        upload_id: &str,
        mut reader: impl AsyncRead + Unpin,
    ) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        let mut buf = vec![0u8; MULTIPART_CHUNK_SIZE];
        for chunk_number in 1.. {
            let mut filled = 0;
            while filled < buf.len() {
                let n = reader
                    .read(&mut buf[filled..])
                    .await
                    .context("failed to read the file being uploaded")?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }

            let resp = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(chunk_number)
                .content_length(filled as i64)
                .body(ByteStream::from(bytes::Bytes::copy_from_slice(
                    &buf[..filled],
                )))
                .send()
                .await
                .map_err(|e| remote_error(format!("upload_part {chunk_number} failed"), e))?;
            let etag = resp
                .e_tag()
                .ok_or_else(|| anyhow!("upload_part {chunk_number} returned no ETag"))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(chunk_number)
                    .e_tag(etag)
                    .build(),
            );
            test_hooks::pause_at(
                "BCB_TEST_PAUSE_AFTER_S3_PART_NUMBER",
                "s3_part",
                chunk_number as u64,
            )
            .await;
        }
        Ok(parts)
    }
}

fn sdk_storage_class(class: StorageClass) -> SdkStorageClass {
    match class {
        StorageClass::Standard => SdkStorageClass::Standard,
        StorageClass::StandardIa => SdkStorageClass::StandardIa,
        StorageClass::OnezoneIa => SdkStorageClass::OnezoneIa,
        StorageClass::GlacierIr => SdkStorageClass::GlacierIr,
        StorageClass::Glacier => SdkStorageClass::Glacier,
        StorageClass::DeepArchive => SdkStorageClass::DeepArchive,
    }
}

/// Parses the expiry date out of an `x-amz-restore` header such as
/// `ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"`.
fn parse_expiry_date(restore_header: &str) -> Option<i64> {
    const FIELD: &str = "expiry-date=\"";
    let start = restore_header.find(FIELD)? + FIELD.len();
    let len = restore_header[start..].find('"')?;
    let expiry = httpdate::parse_http_date(&restore_header[start..start + len]).ok()?;
    Some(
        expiry
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
    )
}

impl RemoteBackend for S3Backend {
    async fn upload_bytes(
        &self,
        key: &str,
        data: Vec<u8>,
        storage_class: StorageClass,
        tags: &str,
    ) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(sdk_storage_class(storage_class))
            .tagging(tags)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| remote_error(format!("S3 put_object failed for {key}"), e))?;
        Ok(())
    }

    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        storage_class: StorageClass,
        tags: &str,
    ) -> Result<()> {
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("failed to open {path:?} for upload"))?;
        match self.upload_speed_limit_bps {
            Some(bps) => {
                use async_speed_limit::{Limiter, clock::StandardClock};
                use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
                let reader = Limiter::<StandardClock>::new(bps)
                    .limit(file.compat())
                    .compat();
                self.multipart_upload(key, storage_class, tags, reader)
                    .await
            }
            None => self.multipart_upload(key, storage_class, tags, file).await,
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
            .map_err(|e| remote_error(format!("S3 get_object failed for {key}"), e))?;
        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| remote_error(format!("failed to read the body of {key}"), e))?;
        Ok(body.into_bytes().to_vec())
    }

    async fn download_range_to_file(
        &self,
        key: &str,
        start: u64,
        end: u64,
        path: &Path,
    ) -> Result<()> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={start}-{end}"))
            .send()
            .await
            .map_err(|e| remote_error(format!("S3 get_object of a range of {key} failed"), e))?;

        let mut body = resp.body.into_async_read();
        let mut file = if start == 0 {
            tokio::fs::File::create(path)
                .await
                .with_context(|| format!("failed to create {path:?}"))?
        } else {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(path)
                .await
                .with_context(|| format!("failed to open {path:?}"))?
        };
        let write_err = || format!("failed to write range to {path:?}");
        let mut buf = vec![0u8; IO_BUF_SIZE];
        loop {
            let n = body
                .read(&mut buf)
                .await
                .map_err(|e| remote_error(format!("failed to read the body of {key}"), e))?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n]).await.with_context(write_err)?;
        }
        // The caller records the range as downloaded next; after a power loss it must be.
        file.sync_all().await.with_context(write_err)?;
        if start == 0
            && let Some(parent) = path.parent()
        {
            fsync_dir(parent).with_context(|| format!("failed to sync {parent:?}"))?;
        }
        Ok(())
    }

    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        Ok(self.head(key).await?.map(|resp| ObjectMetadata {
            size_bytes: resp.content_length().unwrap_or(0).max(0) as u64,
            // HeadObject omits x-amz-storage-class for the default STANDARD class.
            storage_class: resp
                .storage_class()
                .map_or(StorageClass::Standard.as_str(), |sc| sc.as_str())
                .to_string(),
        }))
    }

    async fn retrieval_status(&self, key: &str) -> Result<Retrieval> {
        let resp = self
            .head(key)
            .await?
            .ok_or_else(|| RemoteError::new(format!("{key} does not exist on S3")))?;
        let archived = matches!(
            resp.storage_class(),
            Some(SdkStorageClass::DeepArchive | SdkStorageClass::Glacier)
        );
        let (status, expires_at) = match resp.restore() {
            _ if !archived => (RetrievalStatus::Ready, None),
            None => (RetrievalStatus::NotRequested, None),
            Some(r) if r.contains("ongoing-request=\"true\"") => {
                (RetrievalStatus::InProgress, None)
            }
            Some(r) => (RetrievalStatus::Ready, parse_expiry_date(r)),
        };
        Ok(Retrieval { status, expires_at })
    }

    async fn request_retrieval(&self, key: &str, days: i32, tier: RetrievalTier) -> Result<()> {
        let tier = match tier {
            RetrievalTier::Standard => Tier::Standard,
            RetrievalTier::Bulk => Tier::Bulk,
            RetrievalTier::Expedited => Tier::Expedited,
        };
        let request = RestoreRequest::builder()
            .days(days)
            .glacier_job_parameters(GlacierJobParameters::builder().tier(tier).build()?)
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
            // A retrieval already running, or the object already readable, meets the goal.
            Err(e)
                if matches!(
                    e.code(),
                    Some("RestoreAlreadyInProgress" | "ObjectAlreadyInActiveTierError")
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(remote_error(
                format!("S3 restore_object failed for {key}"),
                e,
            )),
        }
    }

    async fn verify_credentials(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| remote_error("cannot access S3 bucket", e))?;

        // head_bucket only proves read access; backups also need write and tagging, so a
        // multipart upload is started under the configured prefix, tagged like a real part.
        // AWS checks its tags against s3:PutObjectTagging separately from s3:PutObject, so an
        // untagged probe would pass without the tagging permission. Aborting it leaves no
        // object behind, so the policy needs no s3:DeleteObject.
        let probe_key = format!("{}bcb_check_probe", self.prefix);
        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&probe_key)
            .tagging(check_tag())
            .send()
            .await
            .map_err(|e| remote_error("cannot write to S3 bucket", e))?
            .upload_id()
            .ok_or_else(|| anyhow!("create_multipart_upload returned no upload_id"))?
            .to_string();
        self.abort_upload(&probe_key, &upload_id).await
    }

    async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(continuation_token.take())
                .send()
                .await
                .map_err(|e| remote_error(format!("S3 list_objects_v2 failed for {prefix}"), e))?;
            keys.extend(
                resp.contents()
                    .iter()
                    .filter_map(|o| o.key())
                    .map(String::from),
            );
            if !resp.is_truncated().unwrap_or(false) {
                return Ok(keys);
            }
            let token = resp.next_continuation_token().ok_or_else(|| {
                anyhow!(
                    "S3 list_objects_v2 returned is_truncated=true with no NextContinuationToken"
                )
            })?;
            continuation_token = Some(token.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_header_expiry_date_should_parse_to_unix_time() {
        let header = r#"ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT""#;
        assert_eq!(parse_expiry_date(header), Some(1356048000));
    }

    #[test]
    fn restore_header_without_valid_expiry_date_should_be_none() {
        for header in [
            r#"ongoing-request="true""#,
            r#"expiry-date="Fri, 21 Dec 2012"#,
            r#"expiry-date="not a date""#,
            "",
        ] {
            assert!(parse_expiry_date(header).is_none(), "{header}");
        }
    }
}
