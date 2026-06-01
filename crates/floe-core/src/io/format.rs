use std::collections::HashMap;
use std::path::{Path, PathBuf};

use polars::chunked_array::cast::CastOptions;
use polars::prelude::{Column, DataFrame, DataType, NamedFrom, Schema, Series};

use crate::io::storage::Target;
use crate::{check, config, io, ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct InputFile {
    pub source_uri: String,
    pub source_name: String,
    pub source_stem: String,
    pub source_size: Option<u64>,
    pub source_mtime: Option<String>,
}

/// An `InputFile` paired with a guaranteed local filesystem path.
///
/// For local sources the path is the normalized source path (no copy).
/// For cloud sources the path is a temp file downloaded just-in-time;
/// `is_ephemeral` is true and the file should be deleted after use.
#[derive(Debug, Clone)]
pub struct LocalInputFile {
    pub file: InputFile,
    pub local_path: PathBuf,
    pub is_ephemeral: bool,
}

#[derive(Debug, Clone)]
pub struct FileReadError {
    pub rule: String,
    pub message: String,
}

pub enum ReadInput {
    Data {
        input_file: InputFile,
        raw_df: Option<DataFrame>,
        typed_df: DataFrame,
    },
    FileError {
        input_file: InputFile,
        error: FileReadError,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AcceptedWriteMetrics {
    pub total_bytes_written: Option<u64>,
    pub avg_file_size_mb: Option<f64>,
    pub small_files_count: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct AcceptedSchemaEvolution {
    pub enabled: bool,
    pub mode: String,
    pub applied: bool,
    pub added_columns: Vec<String>,
    pub incompatible_changes_detected: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AcceptedWritePerfBreakdown {
    pub conversion_ms: Option<u64>,
    pub source_df_build_ms: Option<u64>,
    pub merge_exec_ms: Option<u64>,
    pub data_write_ms: Option<u64>,
    pub commit_ms: Option<u64>,
    pub metrics_read_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AcceptedMergeMetrics {
    pub merge_key: Vec<String>,
    pub inserted_count: u64,
    pub updated_count: u64,
    pub closed_count: Option<u64>,
    pub unchanged_count: Option<u64>,
    pub target_rows_before: u64,
    pub target_rows_after: u64,
    pub merge_elapsed_ms: u64,
}

#[derive(Debug, Clone)]
pub enum CatalogRegistration {
    UnityDelta {
        catalog_name: String,
        schema: String,
        table: String,
    },
    IcebergGlue {
        catalog_name: String,
        database: Option<String>,
        namespace: String,
        table: String,
    },
    IcebergRest {
        catalog_name: String,
        namespace: String,
        table: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AcceptedWriteOutput {
    pub files_written: Option<u64>,
    pub parts_written: u64,
    pub part_files: Vec<String>,
    pub table_version: Option<i64>,
    pub snapshot_id: Option<i64>,
    pub table_root_uri: Option<String>,
    pub catalog: Option<CatalogRegistration>,
    pub metrics: AcceptedWriteMetrics,
    pub merge: Option<AcceptedMergeMetrics>,
    pub schema_evolution: AcceptedSchemaEvolution,
    pub perf: Option<AcceptedWritePerfBreakdown>,
}

/// Per-write sinks cap their reported `part_files` list at this many entries
/// (see `parquet.rs`). The reducer applies the same cap across flushes so
/// the run report does not grow to N × 50 entries for high-fanout entities.
pub const MAX_REPORTED_PART_FILES: usize = 50;

impl AcceptedWriteOutput {
    /// Fold a later flush's output into this one. The receiver represents the
    /// running total across N completed flushes; `next` is the output of the
    /// (N+1)th flush.
    ///
    /// Field semantics across flushes:
    /// - `parts_written` (always known, the count of successful sink writes)
    ///   sums.
    /// - `files_written` and the `Option<u64>` metric fields
    ///   (`total_bytes_written`, `small_files_count`, perf entries) sum
    ///   when *both* sides are `Some`; if either side is `None` the merged
    ///   result is `None`. "Unknown poisons" matches the per-flush
    ///   semantics: when any single flush could not determine its file
    ///   count (for example a remote Delta commit whose post-commit log
    ///   could not be read), reporting a partial sum would silently
    ///   under-count the total. The run report instead surfaces the value
    ///   as unknown.
    /// - `part_files` concatenates and is capped at `MAX_REPORTED_PART_FILES`
    ///   so the reducer preserves the same cap the individual sink writers
    ///   apply per-flush.
    /// - `table_version` / `snapshot_id` take the latest (Delta commit /
    ///   Iceberg snapshot move forward with every commit; the final state
    ///   is what readers see).
    /// - `table_root_uri`, `catalog`, `schema_evolution` take the first
    ///   non-default value seen — table location and catalog registration
    ///   are established by the first write; schema evolution only fires on
    ///   the first (Overwrite) write because subsequent flushes are Append.
    /// - `avg_file_size_mb` is recomputed from `total_bytes_written` divided
    ///   by `files_written` when available (so it matches the per-flush
    ///   semantics: for Parquet/Iceberg `files == parts`, but for Delta one
    ///   commit can write multiple `add` files and `parts != files`).
    ///   Falls back to `parts_written` when `files_written` is unknown.
    /// - `perf` accumulates by summing each `Option<u64>` field.
    /// - `merge` is unreachable in the buffered path (merge modes use the
    ///   legacy accumulate-then-write code path); the running value is
    ///   preserved if anything ever does pass one.
    pub fn merge_in(&mut self, next: AcceptedWriteOutput) {
        let AcceptedWriteOutput {
            files_written,
            parts_written,
            part_files,
            table_version,
            snapshot_id,
            table_root_uri,
            catalog,
            metrics,
            merge,
            schema_evolution,
            perf,
        } = next;

        // `parts_written == 0` on the receiver means no prior flush has been
        // merged. In that case `Option<u64>` fields on `self` start at `None`
        // not because a flush returned unknown but because nothing has been
        // recorded yet — distinguishing "vacuous" from "poisoned" matters
        // because adopting the next flush's value verbatim on the first merge
        // is correct, while applying poison-on-unknown semantics from `None`
        // would always poison the very first merge.
        let first_merge = self.parts_written == 0;

        self.files_written = merge_option_u64(self.files_written, files_written, first_merge);
        self.parts_written += parts_written;
        let remaining = MAX_REPORTED_PART_FILES.saturating_sub(self.part_files.len());
        if remaining > 0 {
            self.part_files
                .extend(part_files.into_iter().take(remaining));
        }

        if table_version.is_some() {
            self.table_version = table_version;
        }
        if snapshot_id.is_some() {
            self.snapshot_id = snapshot_id;
        }

        if self.table_root_uri.is_none() {
            self.table_root_uri = table_root_uri;
        }
        if self.catalog.is_none() {
            self.catalog = catalog;
        }
        if !self.schema_evolution.enabled
            && !self.schema_evolution.applied
            && self.schema_evolution.added_columns.is_empty()
            && !self.schema_evolution.incompatible_changes_detected
            && self.schema_evolution.mode.is_empty()
        {
            self.schema_evolution = schema_evolution;
        }

        self.metrics.total_bytes_written = merge_option_u64(
            self.metrics.total_bytes_written,
            metrics.total_bytes_written,
            first_merge,
        );
        self.metrics.small_files_count = merge_option_u64(
            self.metrics.small_files_count,
            metrics.small_files_count,
            first_merge,
        );
        self.metrics.avg_file_size_mb = recompute_avg_file_size_mb(
            self.metrics.total_bytes_written,
            self.files_written,
            self.parts_written,
        );

        if self.merge.is_none() {
            self.merge = merge;
        }

        match (self.perf.take(), perf) {
            (Some(a), Some(b)) => self.perf = Some(sum_perf_breakdown(a, b)),
            (Some(a), None) => self.perf = Some(a),
            (None, Some(b)) => self.perf = Some(b),
            (None, None) => self.perf = None,
        }
    }
}

/// Sum two `Option<u64>` values with poison-on-unknown semantics: if either
/// side is `None`, the result is `None`. Reporting a partial sum as if it
/// were the total would silently under-count for any aggregation across
/// flushes where one flush could not determine the underlying count
/// (e.g. remote Delta commit-log read failures).
fn sum_option_u64(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        _ => None,
    }
}

/// Progressive `Option<u64>` merge used by `merge_in`. On the first merge
/// (when the accumulator has no flush recorded yet) the next flush's value is
/// taken verbatim; on subsequent merges `sum_option_u64`'s poison-on-unknown
/// semantics apply.
fn merge_option_u64(acc: Option<u64>, next: Option<u64>, first_merge: bool) -> Option<u64> {
    if first_merge {
        next
    } else {
        sum_option_u64(acc, next)
    }
}

fn recompute_avg_file_size_mb(
    total_bytes: Option<u64>,
    files_written: Option<u64>,
    parts_written: u64,
) -> Option<f64> {
    let bytes = total_bytes?;
    let denominator = files_written.unwrap_or(parts_written);
    if denominator == 0 {
        return None;
    }
    let mb = (bytes as f64) / (denominator as f64) / (1024.0 * 1024.0);
    Some(mb)
}

fn sum_perf_breakdown(
    a: AcceptedWritePerfBreakdown,
    b: AcceptedWritePerfBreakdown,
) -> AcceptedWritePerfBreakdown {
    AcceptedWritePerfBreakdown {
        conversion_ms: sum_option_u64(a.conversion_ms, b.conversion_ms),
        source_df_build_ms: sum_option_u64(a.source_df_build_ms, b.source_df_build_ms),
        merge_exec_ms: sum_option_u64(a.merge_exec_ms, b.merge_exec_ms),
        data_write_ms: sum_option_u64(a.data_write_ms, b.data_write_ms),
        commit_ms: sum_option_u64(a.commit_ms, b.commit_ms),
        metrics_read_ms: sum_option_u64(a.metrics_read_ms, b.metrics_read_ms),
    }
}

pub trait InputAdapter: Send + Sync {
    fn format(&self) -> &'static str;

    fn default_globs(&self) -> FloeResult<Vec<String>> {
        io::storage::extensions::glob_patterns_for_format(self.format())
    }

    fn suffixes(&self) -> FloeResult<Vec<String>> {
        io::storage::extensions::suffixes_for_format(self.format())
    }

    fn resolve_local_inputs(
        &self,
        config_dir: &Path,
        entity_name: &str,
        source: &config::SourceConfig,
        storage: &str,
    ) -> FloeResult<io::storage::local::ResolvedLocalInputs> {
        let default_globs = self.default_globs()?;
        io::storage::local::resolve_local_inputs(
            config_dir,
            entity_name,
            source,
            storage,
            &default_globs,
        )
    }

    fn read_input_columns(
        &self,
        entity: &config::EntityConfig,
        input_file: &LocalInputFile,
        columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError>;

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[LocalInputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>>;

    fn read_inputs_with_prechecked_columns(
        &self,
        entity: &config::EntityConfig,
        files: &[LocalInputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
        prechecked_input_columns: Option<&[String]>,
    ) -> FloeResult<Vec<ReadInput>> {
        let _ = prechecked_input_columns;
        self.read_inputs(entity, files, columns, normalize_strategy, collect_raw)
    }
}

pub struct AcceptedWriteRequest<'a> {
    pub target: &'a Target,
    pub df: &'a mut DataFrame,
    pub mode: config::WriteMode,
    pub output_stem: &'a str,
    pub temp_dir: Option<&'a Path>,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub catalogs: &'a config::CatalogResolver,
    pub entity: &'a config::EntityConfig,
}

pub struct RejectedWriteRequest<'a> {
    pub target: &'a Target,
    pub df: &'a mut DataFrame,
    pub source_stem: &'a str,
    pub temp_dir: Option<&'a Path>,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub entity: &'a config::EntityConfig,
    pub mode: config::WriteMode,
}

pub trait RejectedSinkAdapter: Send + Sync {
    fn write_rejected(&self, request: RejectedWriteRequest<'_>) -> FloeResult<String>;
}

#[derive(Debug, Clone, Copy)]
pub enum FormatKind {
    Source,
    SinkAccepted,
    SinkRejected,
}

impl FormatKind {
    fn field_path(self) -> &'static str {
        match self {
            FormatKind::Source => "source.format",
            FormatKind::SinkAccepted => "sink.accepted.format",
            FormatKind::SinkRejected => "sink.rejected.format",
        }
    }

    fn description(self) -> &'static str {
        match self {
            FormatKind::Source => "source format",
            FormatKind::SinkAccepted => "accepted sink format",
            FormatKind::SinkRejected => "rejected sink format",
        }
    }
}

fn unsupported_format_error(
    kind: FormatKind,
    format: &str,
    entity_name: Option<&str>,
) -> ConfigError {
    if let Some(entity_name) = entity_name {
        return ConfigError(format!(
            "entity.name={} {}={} is unsupported",
            entity_name,
            kind.field_path(),
            format
        ));
    }
    ConfigError(format!("unsupported {}: {format}", kind.description()))
}

pub fn ensure_input_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if input_adapter(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::Source,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn ensure_accepted_sink_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if crate::io::write::sink_format::sink_format(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::SinkAccepted,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn ensure_rejected_sink_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if rejected_sink_adapter(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::SinkRejected,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn resolve_read_columns(
    entity: &config::EntityConfig,
    normalized_columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Vec<config::ColumnConfig>> {
    if entity.source.format == "json" || entity.source.format == "xml" {
        check::normalize::resolve_source_columns(&entity.schema.columns, normalize_strategy, true)
    } else {
        Ok(normalized_columns.to_vec())
    }
}

pub fn sink_options_warning(
    entity_name: &str,
    format: &str,
    options: Option<&config::SinkOptions>,
) -> Option<String> {
    let options = options?;
    if format == "parquet" {
        return None;
    }
    let mut keys = Vec::new();
    if options.compression.is_some() {
        keys.push("compression");
    }
    if options.row_group_size.is_some() {
        keys.push("row_group_size");
    }
    if options.max_size_per_file.is_some() {
        keys.push("max_size_per_file");
    }
    let detail = if keys.is_empty() {
        "options".to_string()
    } else {
        keys.join(", ")
    };
    Some(format!(
        "entity.name={} sink.accepted.options ({detail}) ignored for format={}",
        entity_name, format
    ))
}

pub fn validate_sink_options(
    entity_name: &str,
    format: &str,
    options: Option<&config::SinkOptions>,
) -> FloeResult<()> {
    let options = match options {
        Some(options) => options,
        None => return Ok(()),
    };
    if format != "parquet" {
        return Ok(());
    }
    if let Some(compression) = &options.compression {
        match compression.as_str() {
            "snappy" | "gzip" | "zstd" | "uncompressed" => {}
            _ => {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.options.compression={} is unsupported (allowed: snappy, gzip, zstd, uncompressed)",
                    entity_name, compression
                ))))
            }
        }
    }
    if let Some(row_group_size) = options.row_group_size {
        if row_group_size == 0 {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.options.row_group_size must be greater than 0",
                entity_name
            ))));
        }
    }
    if let Some(max_size_per_file) = options.max_size_per_file {
        if max_size_per_file == 0 {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.options.max_size_per_file must be greater than 0",
                entity_name
            ))));
        }
    }
    Ok(())
}

pub fn input_adapter(format: &str) -> FloeResult<&'static dyn InputAdapter> {
    match format {
        "csv" => Ok(io::read::csv::csv_input_adapter()),
        "tsv" => Ok(io::read::csv::tsv_input_adapter()),
        "fixed" => Ok(io::read::fixed_width::fixed_width_input_adapter()),
        "orc" => Ok(io::read::orc::orc_input_adapter()),
        "parquet" => Ok(io::read::parquet::parquet_input_adapter()),
        "json" => Ok(io::read::json::json_input_adapter()),
        "xlsx" => Ok(io::read::xlsx::xlsx_input_adapter()),
        "avro" => Ok(io::read::avro::avro_input_adapter()),
        "xml" => Ok(io::read::xml::xml_input_adapter()),
        _ => Err(Box::new(unsupported_format_error(
            FormatKind::Source,
            format,
            None,
        ))),
    }
}

pub fn rejected_sink_adapter(format: &str) -> FloeResult<&'static dyn RejectedSinkAdapter> {
    match format {
        "csv" => Ok(io::write::csv::csv_rejected_adapter()),
        _ => Err(Box::new(unsupported_format_error(
            FormatKind::SinkRejected,
            format,
            None,
        ))),
    }
}

pub(crate) fn read_input_from_df(
    input_file: &LocalInputFile,
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
    collect_raw: bool,
) -> FloeResult<ReadInput> {
    let input_columns = df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
    let raw_df = if collect_raw {
        Some(cast_df_to_string(df)?)
    } else {
        None
    };
    let typed_df = cast_df_to_schema(df, &typed_schema)?;
    finalize_read_input(input_file, raw_df, typed_df, normalize_strategy)
}

pub(crate) fn finalize_read_input(
    input_file: &LocalInputFile,
    mut raw_df: Option<DataFrame>,
    mut typed_df: DataFrame,
    normalize_strategy: Option<&str>,
) -> FloeResult<ReadInput> {
    if let Some(strategy) = normalize_strategy {
        if let Some(raw_df) = raw_df.as_mut() {
            crate::checks::normalize::normalize_dataframe_columns(raw_df, strategy)?;
        }
        crate::checks::normalize::normalize_dataframe_columns(&mut typed_df, strategy)?;
    }
    Ok(ReadInput::Data {
        input_file: input_file.file.clone(),
        raw_df,
        typed_df,
    })
}

pub(crate) fn build_typed_schema(
    input_columns: &[String],
    declared_columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Schema> {
    let mut declared_types = HashMap::new();
    for column in declared_columns {
        declared_types.insert(
            column.name.as_str(),
            config::parse_data_type(&column.column_type)?,
        );
    }

    let mut schema = Schema::with_capacity(input_columns.len());
    for name in input_columns {
        let normalized = if let Some(strategy) = normalize_strategy {
            crate::checks::normalize::normalize_name(name, strategy)
        } else {
            name.to_string()
        };
        let dtype = declared_types
            .get(normalized.as_str())
            .cloned()
            .unwrap_or(DataType::String);
        schema.insert(name.as_str().into(), dtype);
    }
    Ok(schema)
}

pub(crate) fn cast_df_to_string(df: &DataFrame) -> FloeResult<DataFrame> {
    cast_df_with_type(df, &DataType::String)
}

pub(crate) fn cast_df_to_schema(df: &DataFrame, schema: &Schema) -> FloeResult<DataFrame> {
    let mut columns = Vec::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        let series = df.column(name.as_str()).map_err(|err| {
            Box::new(ConfigError(format!(
                "input column {} not found: {err}",
                name.as_str()
            )))
        })?;
        let casted =
            if matches!(dtype, DataType::Boolean) && matches!(series.dtype(), DataType::String) {
                cast_string_to_bool(name.as_str(), series)?
            } else {
                series
                    .cast_with_options(dtype, CastOptions::NonStrict)
                    .map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to cast input column {}: {err}",
                            name.as_str()
                        )))
                    })?
            };
        columns.push(casted);
    }
    DataFrame::new(columns).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to build typed dataframe: {err}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

fn cast_string_to_bool(name: &str, series: &Column) -> FloeResult<Column> {
    let string_values = series.as_materialized_series().str().map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to read boolean column {} as string: {err}",
            name
        )))
    })?;
    let mut values = Vec::with_capacity(series.len());
    for value in string_values {
        let parsed = value.and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        });
        values.push(parsed);
    }
    Ok(Series::new(name.into(), values).into())
}

fn cast_df_with_type(df: &DataFrame, dtype: &DataType) -> FloeResult<DataFrame> {
    let mut out = df.clone();
    let names = out
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    for name in names {
        let series = out.column(&name).map_err(|err| {
            Box::new(ConfigError(format!(
                "input column {} not found: {err}",
                name
            )))
        })?;
        let casted = series
            .cast_with_options(dtype, CastOptions::NonStrict)
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "failed to cast input column {}: {err}",
                    name
                )))
            })?;
        let idx = out.get_column_index(&name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "input column {} not found for update",
                name
            )))
        })?;
        out.replace_column(idx, casted).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to update input column {}: {err}",
                name
            )))
        })?;
    }
    Ok(out)
}
pub fn collect_row_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
    raw_indices: &check::ColumnIndex,
    typed_indices: &check::ColumnIndex,
) -> FloeResult<Vec<Vec<check::RowError>>> {
    let mut error_lists = check::not_null_errors(typed_df, required_cols, typed_indices)?;
    if track_cast_errors {
        let cast_errors =
            check::cast_mismatch_errors(raw_df, typed_df, columns, raw_indices, typed_indices)?;
        for (errors, cast) in error_lists.iter_mut().zip(cast_errors) {
            errors.extend(cast);
        }
    }
    Ok(error_lists)
}
