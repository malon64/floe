use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

use crate::errors::{RunError, StorageError};
use crate::{config, io, ConfigError, FloeResult};

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
            .map_err(|err| Box::new(StorageError(format!("failed to build aws runtime: {err}"))))?;
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
                    Box::new(StorageError(format!(
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
                    Box::new(StorageError(format!("s3 get object failed: {err}")))
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
                Box::new(StorageError(format!("s3 upload body failed: {err}")))
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
                    Box::new(StorageError(format!("s3 put object failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }

    fn delete(&self, key: &str) -> FloeResult<()> {
        let bucket = self.bucket().to_string();
        let key = key.to_string();
        self.runtime.block_on(async move {
            self.client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("s3 delete object failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub key: String,
}

pub fn parse_s3_uri(uri: &str) -> FloeResult<S3Location> {
    let stripped = uri.strip_prefix("s3://").ok_or_else(|| {
        Box::new(ConfigError(format!("expected s3 uri, got {}", uri)))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "missing bucket in s3 uri: {}",
            uri
        ))));
    }
    let key = parts.next().unwrap_or("").to_string();
    Ok(S3Location { bucket, key })
}

pub fn format_s3_uri(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("s3://{}", bucket)
    } else {
        format!("s3://{}/{}", bucket, key)
    }
}

pub fn filter_keys_by_suffixes(mut keys: Vec<String>, suffixes: &[String]) -> Vec<String> {
    let suffixes = suffixes
        .iter()
        .map(|suffix| suffix.to_ascii_lowercase())
        .collect::<Vec<_>>();
    keys.retain(|key| {
        let lower = key.to_ascii_lowercase();
        !lower.ends_with('/') && suffixes.iter().any(|suffix| lower.ends_with(suffix))
    });
    keys.sort();
    keys
}

pub fn temp_path_for_key(temp_dir: &Path, key: &str) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let name = file_name_from_key(key).unwrap_or_else(|| "object".to_string());
    let sanitized = sanitize_filename(&name);
    temp_dir.join(format!("{hash:016x}_{sanitized}"))
}

pub fn file_name_from_key(key: &str) -> Option<String> {
    Path::new(key)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn file_stem_from_name(name: &str) -> Option<String> {
    Path::new(name)
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_string())
}

pub fn build_input_files(
    client: &dyn StorageClient,
    bucket: &str,
    prefix: &str,
    adapter: &dyn io::format::InputAdapter,
    temp_dir: &Path,
    entity: &config::EntityConfig,
    storage: &str,
) -> FloeResult<Vec<io::format::InputFile>> {
    let suffixes = adapter.suffixes()?;
    let keys = client.list(prefix)?;
    let keys = filter_keys_by_suffixes(keys, &suffixes);
    if keys.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} source.storage={} no input objects matched (bucket={}, prefix={}, suffixes={})",
            entity.name,
            storage,
            bucket,
            prefix,
            suffixes.join(",")
        ))));
    }
    let mut inputs = Vec::with_capacity(keys.len());
    for key in keys {
        let local_path = temp_path_for_key(temp_dir, &key);
        client.download(&key, &local_path)?;
        let source_name = file_name_from_key(&key).unwrap_or_else(|| entity.name.clone());
        let source_stem = file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
        let source_uri = format_s3_uri(bucket, &key);
        inputs.push(io::format::InputFile {
            source_uri,
            source_local_path: local_path,
            source_name,
            source_stem,
        });
    }
    Ok(inputs)
}
