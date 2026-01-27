use std::path::Path;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

use crate::{ConfigError, FloeResult};

use super::StorageClient;

pub struct S3Client {
    bucket: String,
    client: Client,
    runtime: Runtime,
}

impl S3Client {
    pub fn new(bucket: String, region: Option<&str>) -> FloeResult<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Box::new(ConfigError(format!("failed to build aws runtime: {err}"))))?;
        let config = runtime.block_on(async {
            let region_provider = match region {
                Some(region) => RegionProviderChain::first_try(Region::new(region.to_string()))
                    .or_default_provider(),
                None => RegionProviderChain::default_provider(),
            };
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region_provider)
                .load()
                .await
        });
        let client = Client::new(&config);
        Ok(Self {
            bucket,
            client,
            runtime,
        })
    }

    fn bucket(&self) -> &str {
        self.bucket.as_str()
    }
}

impl StorageClient for S3Client {
    fn list(&self, prefix: &str) -> FloeResult<Vec<String>> {
        let bucket = self.bucket().to_string();
        let prefix = prefix.to_string();
        self.runtime.block_on(async {
            let mut keys = Vec::new();
            let mut continuation = None;
            loop {
                let mut request = self.client.list_objects_v2().bucket(&bucket);
                if !prefix.is_empty() {
                    request = request.prefix(&prefix);
                }
                if let Some(token) = continuation {
                    request = request.continuation_token(token);
                }
                let response = request.send().await.map_err(|err| {
                    Box::new(ConfigError(format!(
                        "s3 list objects failed for bucket {}: {err}",
                        bucket
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
                if let Some(contents) = response.contents {
                    for object in contents {
                        if let Some(key) = object.key {
                            keys.push(key);
                        }
                    }
                }
                if response.is_truncated.unwrap_or(false) {
                    continuation = response.next_continuation_token;
                    if continuation.is_none() {
                        break;
                    }
                } else {
                    break;
                }
            }
            Ok(keys)
        })
    }

    fn download(&self, key: &str, dest: &Path) -> FloeResult<()> {
        let bucket = self.bucket().to_string();
        let key = key.to_string();
        let dest = dest.to_path_buf();
        self.runtime.block_on(async move {
            let response = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|err| {
                    Box::new(ConfigError(format!("s3 get object failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut file = tokio::fs::File::create(&dest).await?;
            let mut reader = response.body.into_async_read();
            tokio::io::copy(&mut reader, &mut file).await?;
            file.flush().await?;
            Ok(())
        })
    }

    fn upload(&self, key: &str, path: &Path) -> FloeResult<()> {
        let bucket = self.bucket().to_string();
        let key = key.to_string();
        let path = path.to_path_buf();
        self.runtime.block_on(async move {
            let body = ByteStream::from_path(path).await.map_err(|err| {
                Box::new(ConfigError(format!("s3 upload body failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;
            self.client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .map_err(|err| {
                    Box::new(ConfigError(format!("s3 put object failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }
}
