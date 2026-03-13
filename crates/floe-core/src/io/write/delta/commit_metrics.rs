use std::path::Path;

use deltalake::logstore::read_commit_entry;
use deltalake::table::builder::DeltaTableBuilder;
use serde_json::Value;

use crate::errors::RunError;
use crate::io::storage::{object_store, Target};
use crate::io::write::metrics;
use crate::{config, FloeResult};

pub(super) fn delta_commit_metrics_for_target(
    runtime: &tokio::runtime::Runtime,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    version: i64,
    small_file_threshold_bytes: u64,
) -> FloeResult<(
    Option<u64>,
    Vec<String>,
    crate::io::format::AcceptedWriteMetrics,
)> {
    match target {
        Target::Local { base_path, .. } => {
            let stats = delta_commit_add_stats(Path::new(base_path), version)?;
            Ok(delta_commit_stats_to_output(
                stats,
                small_file_threshold_bytes,
            ))
        }
        // Best-effort metrics for remote targets: never fail a successful write because the
        // commit log could not be read or parsed after commit.
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            match delta_commit_add_stats_via_object_store(
                runtime, target, resolver, entity, version,
            ) {
                Ok(stats) => Ok(delta_commit_stats_to_output(
                    stats,
                    small_file_threshold_bytes,
                )),
                Err(_) => Ok(delta_commit_metrics_fallback_unknown()),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeltaCommitAddStats {
    files_written: u64,
    part_files: Vec<String>,
    file_sizes: Vec<u64>,
}

fn delta_commit_add_stats(table_root: &Path, version: i64) -> FloeResult<DeltaCommitAddStats> {
    let log_path = table_root
        .join("_delta_log")
        .join(format!("{version:020}.json"));
    let bytes = std::fs::read(&log_path).map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics failed to open commit log {}: {err}",
            log_path.display()
        )))
    })?;
    parse_delta_commit_add_stats_bytes_with_context(&bytes, &log_path.display().to_string())
}

fn delta_commit_add_stats_via_object_store(
    runtime: &tokio::runtime::Runtime,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    version: i64,
) -> FloeResult<DeltaCommitAddStats> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let builder = DeltaTableBuilder::from_url(store.table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta metrics builder failed: {err}"))))?
        .with_storage_options(store.storage_options);
    let log_store = builder.build_storage().map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics log store init failed: {err}"
        )))
    })?;
    let bytes = runtime
        .block_on(async { read_commit_entry(log_store.object_store(None).as_ref(), version).await })
        .map_err(|err| Box::new(RunError(format!("delta metrics commit read failed: {err}"))))?
        .ok_or_else(|| {
            Box::new(RunError(format!(
                "delta metrics commit log missing for version {version}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    parse_delta_commit_add_stats_bytes_with_context(
        bytes.as_ref(),
        &format!("remote delta commit version {version}"),
    )
}

#[doc(hidden)]
pub fn parse_delta_commit_add_stats_bytes(bytes: &[u8]) -> FloeResult<DeltaCommitAddStats> {
    parse_delta_commit_add_stats_bytes_with_context(bytes, "delta commit log bytes")
}

fn parse_delta_commit_add_stats_bytes_with_context(
    bytes: &[u8],
    context: &str,
) -> FloeResult<DeltaCommitAddStats> {
    let content = std::str::from_utf8(bytes).map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics failed to decode {context} as utf-8: {err}"
        )))
    })?;
    let mut stats = DeltaCommitAddStats::default();
    for line in content.lines() {
        let record: Value = serde_json::from_str(line).map_err(|err| {
            Box::new(RunError(format!(
                "delta metrics failed to parse {context}: {err}"
            )))
        })?;
        let Some(add) = record.get("add") else {
            continue;
        };
        stats.files_written += 1;
        if stats.part_files.len() < 50 {
            if let Some(path) = add.get("path").and_then(|value| value.as_str()) {
                let display_name = Path::new(path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| path.to_string());
                stats.part_files.push(display_name);
            }
        }
        if let Some(size) = add.get("size").and_then(|value| value.as_u64()) {
            stats.file_sizes.push(size);
        }
    }
    Ok(stats)
}

#[doc(hidden)]
pub fn delta_commit_metrics_from_log_bytes(
    bytes: &[u8],
    small_file_threshold_bytes: u64,
) -> FloeResult<(
    Option<u64>,
    Vec<String>,
    crate::io::format::AcceptedWriteMetrics,
)> {
    let stats = parse_delta_commit_add_stats_bytes(bytes)?;
    Ok(delta_commit_stats_to_output(
        stats,
        small_file_threshold_bytes,
    ))
}

#[doc(hidden)]
pub fn delta_commit_metrics_from_log_bytes_best_effort(
    bytes: &[u8],
    small_file_threshold_bytes: u64,
) -> (
    Option<u64>,
    Vec<String>,
    crate::io::format::AcceptedWriteMetrics,
) {
    match delta_commit_metrics_from_log_bytes(bytes, small_file_threshold_bytes) {
        Ok(output) => output,
        Err(_) => delta_commit_metrics_fallback_unknown(),
    }
}

fn delta_commit_stats_to_output(
    stats: DeltaCommitAddStats,
    small_file_threshold_bytes: u64,
) -> (
    Option<u64>,
    Vec<String>,
    crate::io::format::AcceptedWriteMetrics,
) {
    let metrics = metrics::summarize_written_file_sizes(
        &stats.file_sizes,
        stats.files_written,
        small_file_threshold_bytes,
    );
    (Some(stats.files_written), stats.part_files, metrics)
}

fn delta_commit_metrics_fallback_unknown() -> (
    Option<u64>,
    Vec<String>,
    crate::io::format::AcceptedWriteMetrics,
) {
    (None, Vec::new(), metrics::null_accepted_write_metrics())
}
