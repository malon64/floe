use floe_core::io::write::metrics::{
    default_small_file_threshold_bytes, summarize_written_file_sizes,
    DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
};

#[test]
fn summarize_written_file_sizes_computes_totals_average_and_small_count() {
    let metrics = summarize_written_file_sizes(&[4, 10, 20], 10);
    assert_eq!(metrics.total_bytes_written, Some(34));
    assert_eq!(metrics.small_files_count, Some(1));
    let avg = metrics.avg_file_size_mb.expect("avg_file_size_mb");
    let expected = 34.0 / 3.0 / (1024.0 * 1024.0);
    assert!((avg - expected).abs() < 1e-12);
}

#[test]
fn summarize_written_file_sizes_empty_is_unset() {
    let metrics = summarize_written_file_sizes(&[], 1024);
    assert_eq!(metrics.total_bytes_written, None);
    assert_eq!(metrics.avg_file_size_mb, None);
    assert_eq!(metrics.small_files_count, None);
}

#[test]
fn default_small_file_threshold_uses_half_target_or_default() {
    assert_eq!(default_small_file_threshold_bytes(Some(200)), 100);
    assert_eq!(
        default_small_file_threshold_bytes(None),
        DEFAULT_SMALL_FILE_THRESHOLD_BYTES
    );
}
