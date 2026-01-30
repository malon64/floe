use std::path::Path;

use floe_core::io::storage::s3::{filter_keys_by_suffixes, parse_s3_uri, temp_path_for_key};

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
