use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::path::PathBuf;

use crate::FloeResult;
use crate::io::storage::StorageClient;

#[derive(Debug, Clone)]
pub struct ObjectRef {
    pub uri: String,
    pub key: String,
    pub last_modified: Option<String>,
    pub size: Option<u64>,
}

pub fn join_prefix(prefix: &str, name: &str) -> String {
    let left = prefix.trim_matches('/');
    let right = name.trim_matches('/');
    if left.is_empty() {
        right.to_string()
    } else if right.is_empty() {
        left.to_string()
    } else {
        format!("{left}/{right}")
    }
}

pub fn normalize_separators(value: &str) -> String {
    value.replace('\\', "/").trim_matches('/').to_string()
}

pub fn stable_sort_refs(mut refs: Vec<ObjectRef>) -> Vec<ObjectRef> {
    refs.sort_by(|a, b| a.uri.cmp(&b.uri));
    refs
}

pub fn object_ref(
    uri: String,
    key: String,
    last_modified: Option<String>,
    size: Option<u64>,
) -> ObjectRef {
    ObjectRef {
        uri,
        key,
        last_modified,
        size,
    }
}

pub fn filter_by_suffixes(refs: Vec<ObjectRef>, suffixes: &[String]) -> Vec<ObjectRef> {
    let suffixes = suffixes
        .iter()
        .map(|suffix| suffix.to_ascii_lowercase())
        .collect::<Vec<_>>();
    refs.into_iter()
        .filter(|obj| {
            let lower = obj.uri.to_ascii_lowercase();
            !lower.ends_with('/') && suffixes.iter().any(|suffix| lower.ends_with(suffix))
        })
        .collect()
}

pub fn ensure_parent_dir(path: &Path) -> FloeResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn temp_path_for_key(temp_dir: &Path, key: &str) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let name = Path::new(key)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("object");
    let sanitized = sanitize_filename(name);
    temp_dir.join(format!("{hash:016x}_{sanitized}"))
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

pub fn exists_by_key(client: &dyn StorageClient, key: &str) -> FloeResult<bool> {
    if key.is_empty() {
        return Ok(false);
    }
    let refs = client.list(key)?;
    Ok(refs.iter().any(|object| object.key == key))
}

pub fn copy_via_temp(client: &dyn StorageClient, src_uri: &str, dst_uri: &str) -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let temp_path = client.download_to_temp(src_uri, temp_dir.path())?;
    client.upload_from_path(&temp_path, dst_uri)?;
    Ok(())
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
