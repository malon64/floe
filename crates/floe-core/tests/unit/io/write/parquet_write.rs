use floe_core::config;
use floe_core::io::write::parquet::parquet_write_runtime_options;

fn sink_target(max_size_per_file: Option<u64>) -> config::SinkTarget {
    config::SinkTarget {
        format: "parquet".to_string(),
        path: "out/accepted/orders".to_string(),
        storage: None,
        options: Some(config::SinkOptions {
            compression: None,
            row_group_size: None,
            max_size_per_file,
        }),
        iceberg: None,
        partition_by: None,
        partition_spec: None,
        write_mode: config::WriteMode::Overwrite,
    }
}

#[test]
fn parquet_runtime_options_default_max_size_when_missing() {
    let mut target = sink_target(None);
    target.options = None;
    let options = parquet_write_runtime_options(&target);
    assert_eq!(options.max_size_per_file_bytes, 256 * 1024 * 1024);
    assert_eq!(options.small_file_threshold_bytes, 128 * 1024 * 1024);
}

#[test]
fn parquet_runtime_options_maps_max_size_per_file() {
    let options = parquet_write_runtime_options(&sink_target(Some(64 * 1024 * 1024)));
    assert_eq!(options.max_size_per_file_bytes, 64 * 1024 * 1024);
    assert_eq!(options.small_file_threshold_bytes, 32 * 1024 * 1024);
}
