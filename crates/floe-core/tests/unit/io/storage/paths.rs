use std::path::PathBuf;

use floe_core::io::storage::paths::{
    archive_relative_path, build_output_filename, build_part_stem, resolve_archive_key,
    resolve_archive_path, resolve_output_dir_key, resolve_output_dir_path, resolve_output_key,
    resolve_sibling_key,
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

#[test]
fn archive_relative_path_includes_entity_and_filename() {
    assert_eq!(
        archive_relative_path("orders", "input.csv"),
        "orders/input.csv"
    );
    assert_eq!(
        archive_relative_path("orders", "nested/input.csv"),
        "orders/input.csv"
    );
    assert_eq!(archive_relative_path("", "input.csv"), "input.csv");
}

#[test]
fn resolve_archive_paths_use_directory_semantics() {
    assert_eq!(
        resolve_archive_path("archive", "orders", "input.csv"),
        PathBuf::from("archive/orders/input.csv")
    );
    assert_eq!(
        resolve_archive_key("archive", "orders", "input.csv"),
        "archive/orders/input.csv"
    );
}
