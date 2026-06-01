use floe_core::io::format::{
    input_adapter, AcceptedSchemaEvolution, AcceptedWriteMetrics, AcceptedWriteOutput,
    AcceptedWritePerfBreakdown, CatalogRegistration,
};

#[test]
fn input_registry_returns_csv_adapter() {
    let adapter = input_adapter("csv").expect("adapter");
    assert_eq!(adapter.format(), "csv");
}

#[test]
fn input_registry_returns_fixed_adapter() {
    let adapter = input_adapter("fixed").expect("adapter");
    assert_eq!(adapter.format(), "fixed");
}

#[test]
fn input_registry_returns_tsv_adapter() {
    let adapter = input_adapter("tsv").expect("adapter");
    assert_eq!(adapter.format(), "tsv");
}

fn output_with_parts(
    files: u64,
    parts: u64,
    part_files: Vec<String>,
    bytes: u64,
) -> AcceptedWriteOutput {
    AcceptedWriteOutput {
        files_written: Some(files),
        parts_written: parts,
        part_files,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: Some(bytes),
            avg_file_size_mb: if parts == 0 {
                None
            } else {
                Some((bytes as f64) / (parts as f64) / (1024.0 * 1024.0))
            },
            small_files_count: Some(0),
        },
        ..AcceptedWriteOutput::default()
    }
}

#[test]
fn merge_in_sums_counters_and_concats_part_files() {
    let mut acc = output_with_parts(2, 2, vec!["part-00000.parquet".into()], 2 * 1024 * 1024);
    let next = output_with_parts(1, 3, vec!["part-uuid-1.parquet".into()], 6 * 1024 * 1024);
    acc.merge_in(next);
    assert_eq!(acc.files_written, Some(3));
    assert_eq!(acc.parts_written, 5);
    assert_eq!(
        acc.part_files,
        vec![
            "part-00000.parquet".to_string(),
            "part-uuid-1.parquet".to_string()
        ]
    );
    assert_eq!(acc.metrics.total_bytes_written, Some(8 * 1024 * 1024));
    // Average uses files_written (3) as denominator, not parts_written (5).
    let expected = (8.0 * 1024.0 * 1024.0) / 3.0 / (1024.0 * 1024.0);
    assert!((acc.metrics.avg_file_size_mb.unwrap() - expected).abs() < 1e-9);
}

#[test]
fn merge_in_avg_file_size_uses_files_written_for_delta_like_outputs() {
    // Delta: one commit (one "part" in our accounting) may write multiple
    // data files. With `parts_written` as the denominator the average is
    // wildly off; the reducer must use `files_written` when available.
    let mut acc = AcceptedWriteOutput {
        files_written: Some(4),
        parts_written: 1,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: Some(64 * 1024 * 1024),
            avg_file_size_mb: Some(16.0),
            small_files_count: Some(0),
        },
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(AcceptedWriteOutput {
        files_written: Some(4),
        parts_written: 1,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: Some(64 * 1024 * 1024),
            avg_file_size_mb: Some(16.0),
            small_files_count: Some(0),
        },
        ..AcceptedWriteOutput::default()
    });
    assert_eq!(acc.files_written, Some(8));
    assert_eq!(acc.parts_written, 2);
    assert_eq!(acc.metrics.total_bytes_written, Some(128 * 1024 * 1024));
    // 128 MiB / 8 files = 16 MiB. With parts_written=2 the buggy formula
    // would give 64 MiB.
    let avg = acc.metrics.avg_file_size_mb.expect("avg present");
    assert!((avg - 16.0).abs() < 1e-9, "got {avg}");
}

#[test]
fn merge_in_avg_file_size_falls_back_to_parts_when_files_unknown() {
    let mut acc = AcceptedWriteOutput {
        files_written: None,
        parts_written: 1,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: Some(8 * 1024 * 1024),
            avg_file_size_mb: Some(8.0),
            small_files_count: Some(0),
        },
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(AcceptedWriteOutput {
        files_written: None,
        parts_written: 1,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: Some(8 * 1024 * 1024),
            avg_file_size_mb: Some(8.0),
            small_files_count: Some(0),
        },
        ..AcceptedWriteOutput::default()
    });
    assert_eq!(acc.files_written, None);
    assert_eq!(acc.parts_written, 2);
    let avg = acc.metrics.avg_file_size_mb.expect("avg present");
    assert!((avg - 8.0).abs() < 1e-9, "got {avg}");
}

#[test]
fn merge_in_caps_part_files_at_50_across_flushes() {
    let mut acc = AcceptedWriteOutput {
        part_files: (0..40).map(|i| format!("part-{i:05}.parquet")).collect(),
        parts_written: 40,
        ..AcceptedWriteOutput::default()
    };
    let next = AcceptedWriteOutput {
        part_files: (0..40).map(|i| format!("part-uuid-{i}.parquet")).collect(),
        parts_written: 40,
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(next);
    assert_eq!(acc.parts_written, 80);
    assert_eq!(acc.part_files.len(), 50);
    // First 40 (existing) preserved verbatim, then 10 from `next`.
    assert_eq!(acc.part_files[0], "part-00000.parquet");
    assert_eq!(acc.part_files[39], "part-00039.parquet");
    assert_eq!(acc.part_files[40], "part-uuid-0.parquet");
    assert_eq!(acc.part_files[49], "part-uuid-9.parquet");

    // Further flushes do not grow the list.
    let later = AcceptedWriteOutput {
        part_files: vec!["part-late.parquet".to_string()],
        parts_written: 1,
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(later);
    assert_eq!(acc.parts_written, 81);
    assert_eq!(acc.part_files.len(), 50);
    assert_eq!(acc.part_files[49], "part-uuid-9.parquet");
}

#[test]
fn merge_in_takes_latest_table_version_and_snapshot_id() {
    let mut acc = AcceptedWriteOutput {
        table_version: Some(1),
        snapshot_id: Some(101),
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(AcceptedWriteOutput {
        table_version: Some(2),
        snapshot_id: Some(102),
        ..AcceptedWriteOutput::default()
    });
    acc.merge_in(AcceptedWriteOutput {
        table_version: Some(3),
        snapshot_id: Some(103),
        ..AcceptedWriteOutput::default()
    });
    assert_eq!(acc.table_version, Some(3));
    assert_eq!(acc.snapshot_id, Some(103));
}

#[test]
fn merge_in_takes_first_table_root_uri_and_catalog_and_schema_evolution() {
    let first_catalog = CatalogRegistration::UnityDelta {
        catalog_name: "main".into(),
        schema: "sales".into(),
        table: "orders".into(),
    };
    let mut acc = AcceptedWriteOutput {
        table_root_uri: Some("file:///lake/orders".into()),
        catalog: Some(first_catalog),
        schema_evolution: AcceptedSchemaEvolution {
            enabled: true,
            mode: "add_columns".into(),
            applied: true,
            added_columns: vec!["extra".into()],
            incompatible_changes_detected: false,
        },
        ..AcceptedWriteOutput::default()
    };
    let second_catalog = CatalogRegistration::UnityDelta {
        catalog_name: "other".into(),
        schema: "ops".into(),
        table: "orders".into(),
    };
    acc.merge_in(AcceptedWriteOutput {
        table_root_uri: Some("file:///other".into()),
        catalog: Some(second_catalog),
        schema_evolution: AcceptedSchemaEvolution {
            enabled: true,
            mode: "add_columns".into(),
            applied: false,
            added_columns: Vec::new(),
            incompatible_changes_detected: false,
        },
        ..AcceptedWriteOutput::default()
    });
    assert_eq!(acc.table_root_uri.as_deref(), Some("file:///lake/orders"));
    match acc.catalog {
        Some(CatalogRegistration::UnityDelta { catalog_name, .. }) => {
            assert_eq!(catalog_name, "main");
        }
        other => panic!("expected UnityDelta(main), got {other:?}"),
    }
    assert!(acc.schema_evolution.applied);
    assert_eq!(
        acc.schema_evolution.added_columns,
        vec!["extra".to_string()]
    );
}

#[test]
fn merge_in_sums_perf_breakdown_when_both_present() {
    let mut acc = AcceptedWriteOutput {
        perf: Some(AcceptedWritePerfBreakdown {
            conversion_ms: Some(10),
            source_df_build_ms: None,
            merge_exec_ms: None,
            data_write_ms: Some(40),
            commit_ms: Some(5),
            metrics_read_ms: None,
        }),
        ..AcceptedWriteOutput::default()
    };
    acc.merge_in(AcceptedWriteOutput {
        perf: Some(AcceptedWritePerfBreakdown {
            conversion_ms: Some(7),
            source_df_build_ms: Some(3),
            merge_exec_ms: None,
            data_write_ms: Some(20),
            commit_ms: None,
            metrics_read_ms: Some(2),
        }),
        ..AcceptedWriteOutput::default()
    });
    let perf = acc.perf.expect("perf");
    assert_eq!(perf.conversion_ms, Some(17));
    assert_eq!(perf.source_df_build_ms, Some(3));
    assert_eq!(perf.data_write_ms, Some(60));
    assert_eq!(perf.commit_ms, Some(5));
    assert_eq!(perf.metrics_read_ms, Some(2));
}

#[test]
fn merge_in_preserves_partial_bytes_when_next_is_unknown() {
    let mut acc = output_with_parts(1, 1, vec!["part-00000.parquet".into()], 1024);
    acc.merge_in(AcceptedWriteOutput {
        files_written: Some(1),
        parts_written: 1,
        part_files: vec!["part-uuid.parquet".into()],
        metrics: AcceptedWriteMetrics {
            total_bytes_written: None,
            avg_file_size_mb: None,
            small_files_count: None,
        },
        ..AcceptedWriteOutput::default()
    });
    assert_eq!(acc.files_written, Some(2));
    assert_eq!(acc.parts_written, 2);
    assert_eq!(acc.metrics.total_bytes_written, Some(1024));
    let expected = 1024.0 / 2.0 / (1024.0 * 1024.0);
    assert!((acc.metrics.avg_file_size_mb.unwrap() - expected).abs() < 1e-12);
    assert_eq!(acc.metrics.small_files_count, Some(0));
}
