use anyhow::{Result, anyhow};
use aws_config::retry::RetryConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, StorageClass};
use std::path::Path;
use tokio::io::AsyncReadExt;

use crate::config::S3Config;

const PART_SIZE: usize = 64 * 1024 * 1024;

#[allow(async_fn_in_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait RemoteBackend: Send + Sync {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str) -> Result<()>;
    async fn upload_file(&self, key: &str, path: &Path, storage_class: &str) -> Result<()>;
    /// Returns the object size in bytes, or None if the key does not exist.
    async fn head(&self, key: &str) -> Result<Option<u64>>;
    async fn download(&self, key: &str) -> Result<Vec<u8>>;
    async fn verify_credentials(&self) -> Result<()>;
    async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

pub struct S3Backend {
    client: Client,
    bucket: String,
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
            upload_speed_limit_bps,
        })
    }
}

fn parse_storage_class(s: &str) -> Result<StorageClass> {
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

async fn multipart_upload<R: tokio::io::AsyncRead + Unpin>(
    client: &Client,
    bucket: &str,
    key: &str,
    storage_class: StorageClass,
    mut reader: R,
) -> Result<()> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .storage_class(storage_class)
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
    async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(parse_storage_class(storage_class)?)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| anyhow!("S3 put_object failed: {e}"))?;
        Ok(())
    }

    async fn upload_file(&self, key: &str, path: &Path, storage_class: &str) -> Result<()> {
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
                multipart_upload(&self.client, &self.bucket, key, sc, reader).await
            }
            None => multipart_upload(&self.client, &self.bucket, key, sc, file).await,
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

    async fn verify_credentials(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| anyhow!("cannot access S3 bucket: {e}"))?;

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
