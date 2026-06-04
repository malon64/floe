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

#[cfg(feature = "delta")]
use super::delta::DELTA_SINK_FORMAT;
#[cfg(feature = "duckdb")]
use super::duckdb::DUCKDB_SINK_FORMAT;
#[cfg(feature = "iceberg")]
use super::iceberg::ICEBERG_SINK_FORMAT;
use super::parquet::PARQUET_SINK_FORMAT;

pub(crate) static SINK_FORMATS: &[&dyn SinkFormat] = &[
    &PARQUET_SINK_FORMAT,
    #[cfg(feature = "delta")]
    &DELTA_SINK_FORMAT,
    #[cfg(feature = "iceberg")]
    &ICEBERG_SINK_FORMAT,
    #[cfg(feature = "duckdb")]
    &DUCKDB_SINK_FORMAT,
];

/// Sink formats that exist but are compiled out unless their Cargo feature is on.
/// Lets `sink_format` tell the difference between an unknown format and a known
/// one the user simply didn't build, so the error can point at the right
/// `--features` flag instead of "unsupported".
const FEATURE_GATED_SINK_FORMATS: &[(&str, &str)] = &[
    ("delta", "delta"),
    ("iceberg", "iceberg"),
    ("duckdb", "duckdb"),
];

pub(crate) fn sink_format(name: &str) -> FloeResult<&'static dyn SinkFormat> {
    if let Some(found) = SINK_FORMATS.iter().find(|f| f.format_name() == name) {
        return Ok(*found);
    }

    if let Some((_, feature)) = FEATURE_GATED_SINK_FORMATS
        .iter()
        .find(|(format, _)| *format == name)
    {
        return Err(Box::new(ConfigError(format!(
            "accepted sink format '{name}' is not available in this build; \
             rebuild with --features {feature}"
        ))) as Box<dyn std::error::Error + Send + Sync>);
    }

    Err(Box::new(ConfigError(format!(
        "unsupported accepted sink format: {name}"
    ))) as Box<dyn std::error::Error + Send + Sync>)
}
