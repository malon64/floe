use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketLocation {
    pub bucket: String,
    pub key: String,
}

pub fn parse_bucket_uri(scheme: &str, uri: &str) -> FloeResult<BucketLocation> {
    let expected = format!("{scheme}://");
    let stripped = uri.strip_prefix(&expected).ok_or_else(|| {
        Box::new(ConfigError(format!("expected {} uri, got {}", scheme, uri)))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "missing bucket in {} uri: {}",
            scheme, uri
        ))));
    }
    let key = parts.next().unwrap_or("").to_string();
    Ok(BucketLocation { bucket, key })
}

pub fn format_bucket_uri(scheme: &str, bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("{}://{}", scheme, bucket)
    } else {
        format!("{}://{}/{}", scheme, bucket, key)
    }
}
