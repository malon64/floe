use std::path::Path;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_prefix_handles_empty_edges() {
        assert_eq!(join_prefix("", "file.txt"), "file.txt");
        assert_eq!(join_prefix("root", ""), "root");
        assert_eq!(join_prefix("root/", "/file.txt"), "root/file.txt");
    }

    #[test]
    fn stable_sort_orders_by_uri() {
        let refs = vec![
            ObjectRef {
                uri: "s3://b/zzz".to_string(),
                key: "zzz".to_string(),
                last_modified: None,
                size: None,
            },
            ObjectRef {
                uri: "s3://b/aaa".to_string(),
                key: "aaa".to_string(),
                last_modified: None,
                size: None,
            },
        ];
        let sorted = stable_sort_refs(refs);
        assert_eq!(sorted[0].uri, "s3://b/aaa");
        assert_eq!(sorted[1].uri, "s3://b/zzz");
    }
}
