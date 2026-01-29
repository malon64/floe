use std::path::Path;

use polars::prelude::DataFrame;

use crate::errors::RunError;
use crate::{check, config, io, ConfigError, FloeResult};

use io::format::{self, InputFile};
use io::storage::Target;
pub(super) fn write_accepted_output(
    format: &str,
    target: &Target,
    df: &mut DataFrame,
    output_stem: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<format::AcceptedWriteOutput> {
    let adapter = format::accepted_sink_adapter(format)?;
    adapter.write_accepted(target, df, output_stem, temp_dir, cloud, resolver, entity)
}
pub(super) fn write_rejected_output(
    format: &str,
    target: &Target,
    df: &mut DataFrame,
    source_stem: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let adapter = format::rejected_sink_adapter(format)?;
    adapter.write_rejected(target, df, source_stem, temp_dir, cloud, resolver, entity)
}

pub(super) fn write_rejected_raw_output(
    target: &Target,
    input_file: &InputFile,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    io::storage::output::write_output(
        target,
        io::storage::output::OutputPlacement::Output,
        &input_file.source_name,
        temp_dir,
        cloud,
        resolver,
        entity,
        |path| {
            io::write::write_rejected_raw(&input_file.source_local_path, path)?;
            Ok(())
        },
    )
}

pub(super) fn write_error_report_output(
    target: &Target,
    source_stem: &str,
    errors_json: &[Option<String>],
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let filename = io::storage::paths::build_output_filename(source_stem, "_reject_errors", "json");
    io::storage::output::write_output(
        target,
        io::storage::output::OutputPlacement::Sibling,
        &filename,
        temp_dir,
        cloud,
        resolver,
        entity,
        |path| {
            io::write::write_error_report(path, errors_json)?;
            Ok(())
        },
    )
}

pub(super) fn validate_rejected_target<'a>(
    entity: &'a config::EntityConfig,
    severity: &str,
) -> FloeResult<&'a config::SinkTarget> {
    let rejected_target = entity.sink.rejected.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "sink.rejected is required for {severity} severity"
        )))
    })?;
    if format::rejected_sink_adapter(rejected_target.format.as_str()).is_err() {
        return Err(Box::new(ConfigError(format!(
            "unsupported rejected sink format: {}",
            rejected_target.format
        ))));
    }
    Ok(rejected_target)
}

pub(super) fn append_rejection_columns(
    df: &mut DataFrame,
    errors_per_row: &[Option<String>],
    include_all_rows: bool,
) -> FloeResult<()> {
    let (row_index, errors) = check::rejected_error_columns(errors_per_row, include_all_rows);
    df.with_column(row_index)
        .map_err(|err| Box::new(RunError(format!("failed to add __floe_row_index: {err}"))))?;
    df.with_column(errors)
        .map_err(|err| Box::new(RunError(format!("failed to add __floe_errors: {err}"))))?;
    Ok(())
}
