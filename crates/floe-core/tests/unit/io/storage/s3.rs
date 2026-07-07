use std::path::Path;

use floe_core::io::storage::s3::{parse_s3_uri, resolve_path_style_access_from_env};
use floe_core::io::storage::{filter_by_suffixes, stable_sort_refs, temp_path_for_key, ObjectRef};

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
fn path_style_access_prefers_explicit_config() {
    let resolve = resolve_path_style_access_from_env;

    assert!(resolve(Some(true), None));
    assert!(resolve(Some(true), Some("false")));
    assert!(!resolve(Some(false), Some("true")));
}

#[test]
fn path_style_access_falls_back_to_aws_env_convention() {
    let resolve = resolve_path_style_access_from_env;

    assert!(!resolve(None, None));
    assert!(resolve(None, Some("true")));
    assert!(resolve(None, Some("1")));
    assert!(resolve(None, Some(" TRUE ")));
    assert!(!resolve(None, Some("false")));
    assert!(!resolve(None, Some("garbage")));
}

#[test]
fn filter_keys_by_suffix_sorts_and_filters() {
    let refs = vec![
        ObjectRef {
            uri: "s3://bucket/b.csv".to_string(),
            key: "b.csv".to_string(),
            last_modified: None,
            size: None,
        },
        ObjectRef {
            uri: "s3://bucket/a.CSV".to_string(),
            key: "a.CSV".to_string(),
            last_modified: None,
            size: None,
        },
        ObjectRef {
            uri: "s3://bucket/c.txt".to_string(),
            key: "c.txt".to_string(),
            last_modified: None,
            size: None,
        },
    ];
    let filtered = filter_by_suffixes(refs, &[".csv".to_string()]);
    let sorted = stable_sort_refs(filtered);
    let keys = sorted.into_iter().map(|obj| obj.key).collect::<Vec<_>>();
    assert_eq!(keys, vec!["a.CSV", "b.csv"]);
}

#[test]
fn temp_path_for_key_avoids_separator_collisions() {
    let temp_dir = Path::new("tmp");
    let first = temp_path_for_key(temp_dir, "dir/a/b.csv");
    let second = temp_path_for_key(temp_dir, "dir/a__b.csv");
    assert_ne!(first, second);
}
