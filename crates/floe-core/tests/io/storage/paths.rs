use std::path::PathBuf;

use floe_core::io::storage::paths::{
    build_output_filename, build_part_stem, resolve_output_dir_key, resolve_output_dir_path,
    resolve_output_key, resolve_sibling_key,
};

#[test]
fn build_output_filename_includes_extension() {
    assert_eq!(build_output_filename("file", "", "parquet"), "file.parquet");
    assert_eq!(
        build_output_filename("file", "_rejected", ".csv"),
        "file_rejected.csv"
    );
}

#[test]
fn build_part_stem_zero_pads() {
    assert_eq!(build_part_stem(0), "part-00000");
    assert_eq!(build_part_stem(12), "part-00012");
}

#[test]
fn resolve_output_key_respects_file_base() {
    assert_eq!(
        resolve_output_key("out/file.parquet", "ignored.parquet"),
        "out/file.parquet"
    );
    assert_eq!(
        resolve_output_key("out", "file.parquet"),
        "out/file.parquet"
    );
    assert_eq!(resolve_output_key("", "file.parquet"), "file.parquet");
}

#[test]
fn resolve_output_dir_path_treats_base_as_directory() {
    assert_eq!(
        resolve_output_dir_path("out/file.parquet", "part-00000.parquet"),
        PathBuf::from("out/file.parquet/part-00000.parquet")
    );
    assert_eq!(
        resolve_output_dir_path("out", "part-00000.parquet"),
        PathBuf::from("out/part-00000.parquet")
    );
    assert_eq!(
        resolve_output_dir_path("", "part-00000.parquet"),
        PathBuf::from("part-00000.parquet")
    );
}

#[test]
fn resolve_output_dir_key_treats_base_as_directory() {
    assert_eq!(
        resolve_output_dir_key("out/file.parquet", "part-00000.parquet"),
        "out/file.parquet/part-00000.parquet"
    );
    assert_eq!(
        resolve_output_dir_key("out", "part-00000.parquet"),
        "out/part-00000.parquet"
    );
    assert_eq!(
        resolve_output_dir_key("", "part-00000.parquet"),
        "part-00000.parquet"
    );
}

#[test]
fn resolve_sibling_key_uses_parent_for_file_base() {
    assert_eq!(
        resolve_sibling_key("out/errors.csv", "file_reject_errors.json"),
        "out/file_reject_errors.json"
    );
    assert_eq!(
        resolve_sibling_key("out", "file_reject_errors.json"),
        "out/file_reject_errors.json"
    );
}
