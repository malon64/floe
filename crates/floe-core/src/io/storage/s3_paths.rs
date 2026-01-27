use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use crate::{ConfigError, FloeResult};

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

pub fn normalize_base_key(base_key: &str) -> String {
    base_key.trim_matches('/').to_string()
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
