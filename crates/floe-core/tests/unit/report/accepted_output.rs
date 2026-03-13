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
    assert_eq!(summary.files_written, Some(1));
    assert_eq!(summary.parts_written, 1);
    assert_eq!(summary.total_bytes_written, None);
    assert_eq!(summary.avg_file_size_mb, None);
    assert_eq!(summary.small_files_count, None);
    assert_eq!(summary.closed_count, None);
    assert_eq!(summary.unchanged_count, None);
}

#[test]
fn accepted_output_summary_serializes_new_metrics_when_present() {
    let summary = AcceptedOutputSummary {
        path: "/tmp/out".to_string(),
        table_root_uri: Some("/tmp/out".to_string()),
        write_mode: Some("append".to_string()),
        accepted_rows: 10,
        files_written: Some(2),
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
        merge_key: vec!["id".to_string()],
        inserted_count: Some(1),
        updated_count: Some(1),
        closed_count: Some(1),
        unchanged_count: Some(2),
        target_rows_before: Some(1),
        target_rows_after: Some(2),
        merge_elapsed_ms: Some(123),
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
    assert_eq!(obj.get("closed_count").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(obj.get("unchanged_count").and_then(|v| v.as_u64()), Some(2));
}

#[test]
fn accepted_output_summary_serializes_exact_zero_metrics_as_zero() {
    let summary = AcceptedOutputSummary {
        path: "/tmp/out".to_string(),
        table_root_uri: None,
        write_mode: Some("overwrite".to_string()),
        accepted_rows: 0,
        files_written: Some(0),
        parts_written: 0,
        part_files: Vec::new(),
        table_version: None,
        snapshot_id: None,
        iceberg_catalog_name: None,
        iceberg_database: None,
        iceberg_namespace: None,
        iceberg_table: None,
        total_bytes_written: Some(0),
        avg_file_size_mb: None,
        small_files_count: Some(0),
        merge_key: Vec::new(),
        inserted_count: None,
        updated_count: None,
        closed_count: None,
        unchanged_count: None,
        target_rows_before: None,
        target_rows_after: None,
        merge_elapsed_ms: None,
    };

    let value = serde_json::to_value(summary).expect("serialize");
    let obj = value.as_object().expect("object");

    assert_eq!(obj.get("files_written").and_then(|v| v.as_u64()), Some(0));
    assert_eq!(
        obj.get("total_bytes_written").and_then(|v| v.as_u64()),
        Some(0)
    );
    assert_eq!(
        obj.get("small_files_count").and_then(|v| v.as_u64()),
        Some(0)
    );
    assert!(obj.get("avg_file_size_mb").is_some_and(|v| v.is_null()));
}

#[test]
fn accepted_output_summary_serializes_unknown_metrics_as_null() {
    let summary = AcceptedOutputSummary {
        path: "/tmp/out".to_string(),
        table_root_uri: None,
        write_mode: None,
        accepted_rows: 10,
        files_written: None,
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
        merge_key: Vec::new(),
        inserted_count: None,
        updated_count: None,
        closed_count: None,
        unchanged_count: None,
        target_rows_before: None,
        target_rows_after: None,
        merge_elapsed_ms: None,
    };

    let value = serde_json::to_value(summary).expect("serialize");
    let obj = value.as_object().expect("object");

    assert!(obj.get("files_written").is_some_and(|v| v.is_null()));
    assert!(obj.get("total_bytes_written").is_some_and(|v| v.is_null()));
    assert!(obj.get("avg_file_size_mb").is_some_and(|v| v.is_null()));
    assert!(obj.get("small_files_count").is_some_and(|v| v.is_null()));
    assert!(!obj.contains_key("merge_key"));
    assert!(!obj.contains_key("inserted_count"));
    assert!(!obj.contains_key("updated_count"));
    assert!(!obj.contains_key("closed_count"));
    assert!(!obj.contains_key("unchanged_count"));
}
