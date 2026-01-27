use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_extracts_bucket_and_key() {
        let loc = parse_s3_uri("s3://my-bucket/path/to/file.csv").expect("parse");
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(loc.key, "path/to/file.csv");
    }

    #[test]
    fn parse_s3_uri_allows_bucket_only() {
        let loc = parse_s3_uri("s3://my-bucket").expect("parse");
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(loc.key, "");
    }

    #[test]
    fn filter_keys_by_suffix_sorts_and_filters() {
        let keys = vec![
            "b.csv".to_string(),
            "a.CSV".to_string(),
            "c.txt".to_string(),
        ];
        let filtered = filter_keys_by_suffixes(keys, &[".csv".to_string()]);
        assert_eq!(filtered, vec!["a.CSV", "b.csv"]);
    }

    #[test]
    fn temp_path_for_key_avoids_separator_collisions() {
        let temp_dir = Path::new("tmp");
        let first = temp_path_for_key(temp_dir, "dir/a/b.csv");
        let second = temp_path_for_key(temp_dir, "dir/a__b.csv");
        assert_ne!(first, second);
    }
}
