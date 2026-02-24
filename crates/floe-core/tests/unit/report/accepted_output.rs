use floe_core::report::AcceptedOutputSummary;

#[test]
fn accepted_output_summary_deserializes_legacy_payload_without_new_metrics() {
    let json = r#"{
        "path": "/tmp/out",
        "accepted_rows": 10,
        "files_written": 1,
        "parts_written": 1,
        "part_files": ["part-00000.parquet"]
    }"#;

    let summary: AcceptedOutputSummary = serde_json::from_str(json).expect("deserialize");

    assert_eq!(summary.path, "/tmp/out");
    assert_eq!(summary.accepted_rows, 10);
    assert_eq!(summary.files_written, 1);
    assert_eq!(summary.parts_written, 1);
    assert_eq!(summary.total_bytes_written, None);
    assert_eq!(summary.avg_file_size_mb, None);
    assert_eq!(summary.small_files_count, None);
}

#[test]
fn accepted_output_summary_serializes_new_metrics_when_present() {
    let summary = AcceptedOutputSummary {
        path: "/tmp/out".to_string(),
        table_root_uri: Some("/tmp/out".to_string()),
        write_mode: Some("append".to_string()),
        accepted_rows: 10,
        files_written: 2,
        parts_written: 2,
        part_files: vec![
            "part-00000.parquet".to_string(),
            "part-00001.parquet".to_string(),
        ],
        table_version: Some(3),
        snapshot_id: Some(42),
        iceberg_catalog_name: None,
        iceberg_database: None,
        iceberg_namespace: None,
        iceberg_table: None,
        total_bytes_written: Some(8_388_608),
        avg_file_size_mb: Some(4.0),
        small_files_count: Some(1),
    };

    let value = serde_json::to_value(summary).expect("serialize");
    let obj = value.as_object().expect("object");

    assert_eq!(obj.get("files_written").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(
        obj.get("total_bytes_written").and_then(|v| v.as_u64()),
        Some(8_388_608)
    );
    assert_eq!(
        obj.get("avg_file_size_mb").and_then(|v| v.as_f64()),
        Some(4.0)
    );
    assert_eq!(
        obj.get("small_files_count").and_then(|v| v.as_u64()),
        Some(1)
    );
}

#[test]
fn accepted_output_summary_omits_new_metrics_when_absent() {
    let summary = AcceptedOutputSummary {
        path: "/tmp/out".to_string(),
        table_root_uri: None,
        write_mode: None,
        accepted_rows: 10,
        files_written: 0,
        parts_written: 0,
        part_files: Vec::new(),
        table_version: None,
        snapshot_id: None,
        iceberg_catalog_name: None,
        iceberg_database: None,
        iceberg_namespace: None,
        iceberg_table: None,
        total_bytes_written: None,
        avg_file_size_mb: None,
        small_files_count: None,
    };

    let value = serde_json::to_value(summary).expect("serialize");
    let obj = value.as_object().expect("object");

    assert!(!obj.contains_key("total_bytes_written"));
    assert!(!obj.contains_key("avg_file_size_mb"));
    assert!(!obj.contains_key("small_files_count"));
}
