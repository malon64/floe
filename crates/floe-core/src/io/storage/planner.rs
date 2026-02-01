use std::path::Path;
use std::path::PathBuf;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::FloeResult;

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
