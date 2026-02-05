use std::path::Path;

use polars::prelude::DataFrame;

use crate::errors::RunError;
use crate::{check, config, io, ConfigError, FloeResult};

use io::format::{self, InputFile};
use io::storage::Target;

pub(super) struct AcceptedOutputContext<'a> {
    pub(super) format: &'a str,
    pub(super) target: &'a Target,
    pub(super) df: &'a mut DataFrame,
    pub(super) output_stem: &'a str,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) cloud: &'a mut io::storage::CloudClient,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) mode: config::WriteMode,
}

pub(super) fn write_accepted_output(
    context: AcceptedOutputContext<'_>,
) -> FloeResult<format::AcceptedWriteOutput> {
    let AcceptedOutputContext {
        format,
        target,
        df,
        output_stem,
        temp_dir,
        cloud,
        resolver,
        entity,
        mode,
    } = context;
    let adapter = format::accepted_sink_adapter(format)?;
    io::write::accepted::write_with_adapter(
        adapter,
        io::write::accepted::AcceptedWriteRequest {
            target,
            df,
            output_stem,
            temp_dir,
            cloud,
            resolver,
            entity,
            mode,
        },
    )
}
pub(super) fn write_rejected_output(
    format: &str,
    request: format::RejectedWriteRequest<'_>,
) -> FloeResult<String> {
    let adapter = format::rejected_sink_adapter(format)?;
    adapter.write_rejected(request)
}

pub(super) fn write_rejected_raw_output(
    target: &Target,
    input_file: &InputFile,
    mode: config::WriteMode,
    run_id: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let (placement, filename) = rejected_output_location(mode, run_id, &input_file.source_name);
    io::storage::output::write_output(
        target,
        placement,
        &filename,
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
    context: ErrorReportOutputContext<'_>,
) -> FloeResult<String> {
    let ErrorReportOutputContext {
        target,
        source_stem,
        errors_json,
        mode,
        run_id,
        temp_dir,
        cloud,
        resolver,
        entity,
    } = context;
    let base_filename =
        io::storage::paths::build_output_filename(source_stem, "_reject_errors", "json");
    let (placement, filename) = rejected_output_location(mode, run_id, &base_filename);
    io::storage::output::write_output(
        target,
        placement,
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

pub(super) struct ErrorReportOutputContext<'a> {
    pub(super) target: &'a Target,
    pub(super) source_stem: &'a str,
    pub(super) errors_json: &'a [Option<String>],
    pub(super) mode: config::WriteMode,
    pub(super) run_id: &'a str,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) cloud: &'a mut io::storage::CloudClient,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) entity: &'a config::EntityConfig,
}

fn rejected_output_location(
    mode: config::WriteMode,
    run_id: &str,
    filename: &str,
) -> (io::storage::output::OutputPlacement, String) {
    match mode {
        config::WriteMode::Overwrite => (
            io::storage::output::OutputPlacement::Output,
            filename.to_string(),
        ),
        config::WriteMode::Append => (
            io::storage::output::OutputPlacement::Directory,
            io::storage::paths::run_partition_relative(run_id, filename),
        ),
    }
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
