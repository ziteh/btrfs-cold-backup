use anyhow::{Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::StorageClass;
use std::path::Path;

use crate::config::S3Config;

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
}

impl S3Backend {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let region = aws_sdk_s3::config::Region::new(config.region.clone());
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);
        if !config.endpoint.is_empty() {
            builder = builder
                .endpoint_url(config.endpoint.clone())
                .force_path_style(true);
        }

        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
        })
    }
}

fn parse_storage_class(s: &str) -> StorageClass {
    match s {
        "DEEP_ARCHIVE" => StorageClass::DeepArchive,
        "GLACIER" => StorageClass::Glacier,
        "GLACIER_IR" => StorageClass::GlacierIr,
        "INTELLIGENT_TIERING" => StorageClass::IntelligentTiering,
        "ONEZONE_IA" => StorageClass::OnezoneIa,
        "REDUCED_REDUNDANCY" => StorageClass::ReducedRedundancy,
        "STANDARD_IA" => StorageClass::StandardIa,
        _ => StorageClass::Standard,
    }
}

impl RemoteBackend for S3Backend {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(parse_storage_class(storage_class))
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| anyhow!("S3 put_object failed: {e}"))?;
        Ok(())
    }

    async fn upload_file(&self, key: &str, path: &Path, storage_class: &str) -> Result<()> {
        let metadata = tokio::fs::metadata(path).await?;

        let body = ByteStream::from_path(path)
            .await
            .map_err(|e| anyhow!("failed to open file for upload: {e}"))?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(parse_storage_class(storage_class))
            .content_length(metadata.len() as i64)
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow!("S3 put_object failed: {e}"))?;

        Ok(())
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
                continuation_token = resp.next_continuation_token().map(str::to_string);
            } else {
                break;
            }
        }

        Ok(keys)
    }
}
