use std::collections::HashMap;
use std::path::Path;

use crate::errors::ConfigError;
use crate::io::format::{AcceptedWriteOutput, AcceptedWriteRequest};
use crate::io::storage::Target;
use crate::{check, config, io, FloeResult};

/// Per-call context passed to `SinkFormat::seed_unique_tracker`.
pub struct SeedContext<'a> {
    pub target: &'a Target,
    pub temp_dir: Option<&'a Path>,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub catalogs: &'a config::CatalogResolver,
    pub entity: &'a config::EntityConfig,
    pub scan_cols: &'a [String],
    pub rename_back: &'a HashMap<String, String>,
}

/// Unified sink format trait — write data and seed duplicate detection, all in one place.
pub trait SinkFormat: Send + Sync {
    fn format_name(&self) -> &'static str;

    /// Write modes this format supports. Used for capability validation.
    fn supported_modes(&self) -> &'static [config::WriteMode];

    /// Storage backends this format supports (lowercase: "local", "s3", "gcs", "adls").
    /// Used for capability validation.
    fn supported_storages(&self) -> &'static [&'static str];

    fn write(&self, req: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput>;

    /// Seed `tracker` from existing sink data. Default: no-op (Overwrite-only formats skip this).
    fn seed_unique_tracker(
        &self,
        _tracker: &mut check::UniqueTracker,
        _ctx: &mut SeedContext<'_>,
    ) -> FloeResult<()> {
        Ok(())
    }
}

// ── Static registry ──────────────────────────────────────────────────────────

use super::delta::DELTA_SINK_FORMAT;
use super::duckdb::DUCKDB_SINK_FORMAT;
use super::iceberg::ICEBERG_SINK_FORMAT;
use super::parquet::PARQUET_SINK_FORMAT;

pub(crate) static SINK_FORMATS: &[&dyn SinkFormat] = &[
    &DELTA_SINK_FORMAT,
    &PARQUET_SINK_FORMAT,
    &ICEBERG_SINK_FORMAT,
    &DUCKDB_SINK_FORMAT,
];

pub(crate) fn sink_format(name: &str) -> FloeResult<&'static dyn SinkFormat> {
    SINK_FORMATS
        .iter()
        .find(|f| f.format_name() == name)
        .copied()
        .ok_or_else(|| {
            Box::new(ConfigError(format!(
                "unsupported accepted sink format: {name}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })
}
