use anyhow::{Result, anyhow};
use aws_config::retry::RetryConfig;
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
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .storage_class(parse_storage_class(storage_class)?);

        let body = if let Some(bps) = self.upload_speed_limit_bps {
            use async_speed_limit::Limiter;
            use async_speed_limit::clock::StandardClock;
            use aws_smithy_types::body::SdkBody;
            use tokio::io::AsyncReadExt;
            use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

            let (mut sender, hyper_body) = hyper::Body::channel();
            let path_owned = path.to_owned();
            tokio::spawn(async move {
                let file = match tokio::fs::File::open(&path_owned).await {
                    Ok(f) => f,
                    Err(_) => {
                        sender.abort();
                        return;
                    }
                };
                let mut limited = Limiter::<StandardClock>::new(bps)
                    .limit(file.compat())
                    .compat();
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    match limited.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if sender
                                .send_data(bytes::Bytes::copy_from_slice(&buf[..n]))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => {
                            sender.abort();
                            break;
                        }
                    }
                }
            });
            ByteStream::new(SdkBody::from_body_0_4(hyper_body))
        } else {
            let metadata = tokio::fs::metadata(path).await?;
            req = req.content_length(metadata.len() as i64);
            ByteStream::from_path(path)
                .await
                .map_err(|e| anyhow!("failed to open file for upload: {e}"))?
        };

        req.body(body)
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
