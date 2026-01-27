use std::collections::HashMap;
use std::path::Path;

use polars::prelude::DataFrame;

use crate::{check, config, io, ConfigError, FloeResult};

use io::format::{self, InputFile, StorageTarget};

#[allow(clippy::too_many_arguments)]
pub(super) fn write_accepted_output(
    format: &str,
    target: &StorageTarget,
    df: &mut DataFrame,
    source_stem: &str,
    temp_dir: Option<&Path>,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let adapter = format::accepted_sink_adapter(format)?;
    adapter.write_accepted(
        target,
        df,
        source_stem,
        temp_dir,
        s3_clients,
        resolver,
        entity,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_rejected_output(
    format: &str,
    target: &StorageTarget,
    df: &mut DataFrame,
    source_stem: &str,
    temp_dir: Option<&Path>,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let adapter = format::rejected_sink_adapter(format)?;
    adapter.write_rejected(
        target,
        df,
        source_stem,
        temp_dir,
        s3_clients,
        resolver,
        entity,
    )
}

pub(super) fn write_rejected_raw_output(
    target: &StorageTarget,
    input_file: &InputFile,
    _temp_dir: Option<&Path>,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    match target {
        StorageTarget::Local { base_path } => {
            let output_path = io::write::write_rejected_raw(&input_file.local_path, base_path)?;
            Ok(output_path.display().to_string())
        }
        StorageTarget::S3 {
            storage,
            bucket,
            base_key,
        } => {
            let key = io::fs::s3::build_rejected_raw_key(base_key, &input_file.source_name);
            let client = super::entity::s3_client_for(s3_clients, resolver, storage, entity)?;
            client.upload_file(bucket, &key, &input_file.local_path)?;
            Ok(io::fs::s3::format_s3_uri(bucket, &key))
        }
    }
}

pub(super) fn write_error_report_output(
    target: &StorageTarget,
    source_stem: &str,
    errors_json: &[Option<String>],
    temp_dir: Option<&Path>,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    match target {
        StorageTarget::Local { base_path } => {
            let output_path = io::write::write_error_report(base_path, source_stem, errors_json)?;
            Ok(output_path.display().to_string())
        }
        StorageTarget::S3 {
            storage,
            bucket,
            base_key,
        } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_base = temp_dir.display().to_string();
            let local_path = io::write::write_error_report(&temp_base, source_stem, errors_json)?;
            let key = io::fs::s3::build_reject_errors_key(base_key, source_stem);
            let client = super::entity::s3_client_for(s3_clients, resolver, storage, entity)?;
            client.upload_file(bucket, &key, &local_path)?;
            Ok(io::fs::s3::format_s3_uri(bucket, &key))
        }
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
    df.with_column(row_index).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to add __floe_row_index: {err}"
        )))
    })?;
    df.with_column(errors)
        .map_err(|err| Box::new(ConfigError(format!("failed to add __floe_errors: {err}"))))?;
    Ok(())
}
