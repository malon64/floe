use std::path::{Path, PathBuf};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub key: String,
}

pub struct S3Client {
    client: Client,
    runtime: Runtime,
}

impl S3Client {
    pub fn new(region: Option<&str>) -> FloeResult<Self> {
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
        Ok(Self { client, runtime })
    }

    pub fn list_objects(&self, bucket: &str, prefix: &str) -> FloeResult<Vec<String>> {
        self.runtime.block_on(async {
            let mut keys = Vec::new();
            let mut continuation = None;
            loop {
                let mut request = self.client.list_objects_v2().bucket(bucket);
                if !prefix.is_empty() {
                    request = request.prefix(prefix);
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

    pub fn download_object(&self, bucket: &str, key: &str, dest: &Path) -> FloeResult<()> {
        let bucket = bucket.to_string();
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

    pub fn upload_file(&self, bucket: &str, key: &str, path: &Path) -> FloeResult<()> {
        let bucket = bucket.to_string();
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

pub fn normalize_base_key(base_key: &str) -> String {
    base_key.trim_matches('/').to_string()
}

pub fn suffix_for_format(format: &str) -> FloeResult<&'static str> {
    match format {
        "csv" => Ok(".csv"),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported source format for s3: {format}"
        )))),
    }
}

pub fn filter_keys_by_suffix(mut keys: Vec<String>, suffix: &str) -> Vec<String> {
    let suffix = suffix.to_ascii_lowercase();
    keys.retain(|key| {
        let lower = key.to_ascii_lowercase();
        lower.ends_with(&suffix) && !lower.ends_with('/')
    });
    keys.sort();
    keys
}

pub fn temp_path_for_key(temp_dir: &Path, key: &str) -> PathBuf {
    let sanitized = key.replace('/', "__");
    temp_dir.join(sanitized)
}

pub fn file_name_from_key(key: &str) -> Option<String> {
    Path::new(key)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
}

pub fn file_stem_from_name(name: &str) -> Option<String> {
    Path::new(name)
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_string())
}

pub fn build_parquet_key(base_key: &str, source_stem: &str) -> String {
    let base = normalize_base_key(base_key);
    if Path::new(&base).extension().is_some() {
        base
    } else if base.is_empty() {
        format!("{source_stem}.parquet")
    } else {
        format!("{base}/{source_stem}.parquet")
    }
}

pub fn build_rejected_csv_key(base_key: &str, source_stem: &str) -> String {
    let base = normalize_base_key(base_key);
    if Path::new(&base).extension().is_some() {
        base
    } else if base.is_empty() {
        format!("{source_stem}_rejected.csv")
    } else {
        format!("{base}/{source_stem}_rejected.csv")
    }
}

pub fn build_rejected_raw_key(base_key: &str, source_name: &str) -> String {
    let base = normalize_base_key(base_key);
    if Path::new(&base).extension().is_some() {
        base
    } else if base.is_empty() {
        source_name.to_string()
    } else {
        format!("{base}/{source_name}")
    }
}

pub fn build_reject_errors_key(base_key: &str, source_stem: &str) -> String {
    let base = normalize_base_key(base_key);
    let dir = if Path::new(&base).extension().is_some() {
        parent_key(&base)
    } else {
        base
    };
    if dir.is_empty() {
        format!("{source_stem}_reject_errors.json")
    } else {
        format!("{dir}/{source_stem}_reject_errors.json")
    }
}

fn parent_key(base: &str) -> String {
    match base.rsplit_once('/') {
        Some((parent, _)) => parent.to_string(),
        None => base.to_string(),
    }
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
    fn build_keys_from_prefix() {
        assert_eq!(build_parquet_key("out", "file"), "out/file.parquet");
        assert_eq!(
            build_parquet_key("out/file.parquet", "file"),
            "out/file.parquet"
        );
        assert_eq!(
            build_rejected_csv_key("out", "file"),
            "out/file_rejected.csv"
        );
        assert_eq!(build_rejected_raw_key("out", "file.csv"), "out/file.csv");
        assert_eq!(
            build_reject_errors_key("out/errors.csv", "file"),
            "out/file_reject_errors.json"
        );
    }

    #[test]
    fn filter_keys_by_suffix_sorts_and_filters() {
        let keys = vec![
            "b.csv".to_string(),
            "a.CSV".to_string(),
            "c.txt".to_string(),
        ];
        let filtered = filter_keys_by_suffix(keys, ".csv");
        assert_eq!(filtered, vec!["a.CSV", "b.csv"]);
    }
}
