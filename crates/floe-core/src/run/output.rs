use std::path::Path;

use polars::prelude::DataFrame;

use crate::{check, config, io, ConfigError, FloeResult};

use io::format::{self, InputFile};
use io::storage::Target;

#[allow(clippy::too_many_arguments)]
pub(super) fn write_accepted_output(
    format: &str,
    target: &Target,
    df: &mut DataFrame,
    source_stem: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    let adapter = format::accepted_sink_adapter(format)?;
    match target {
        Target::Local { .. } => {
            adapter.write_accepted(target, df, source_stem, temp_dir, cloud, resolver, entity)
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_target = temp_local_target(temp_dir);
            let local_path = adapter.write_accepted(
                &temp_target,
                df,
                source_stem,
                None,
                cloud,
                resolver,
                entity,
            )?;
            let key = match format {
                "parquet" => {
                    let filename =
                        io::storage::paths::build_output_filename(source_stem, "", "parquet");
                    io::storage::paths::resolve_output_key(base_key, &filename)
                }
                "delta" | "iceberg" => {
                    return Err(Box::new(ConfigError(format!(
                        "entity.name={} sink.accepted.format={} is only supported on local storage",
                        entity.name, format
                    ))))
                }
                _ => {
                    return Err(Box::new(ConfigError(format!(
                        "entity.name={} sink.accepted.format={} is unsupported for s3 output",
                        entity.name, format
                    ))))
                }
            };
            let client = cloud.client_for(resolver, storage, entity)?;
            client.upload(&key, Path::new(&local_path))?;
            Ok(io::storage::s3::format_s3_uri(bucket, &key))
        }
    }
}

#[allow(clippy::too_many_arguments)]
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
    match target {
        Target::Local { .. } => {
            adapter.write_rejected(target, df, source_stem, temp_dir, cloud, resolver, entity)
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_target = temp_local_target(temp_dir);
            let local_path = adapter.write_rejected(
                &temp_target,
                df,
                source_stem,
                None,
                cloud,
                resolver,
                entity,
            )?;
            let key = match format {
                "csv" => {
                    let filename =
                        io::storage::paths::build_output_filename(source_stem, "_rejected", "csv");
                    io::storage::paths::resolve_output_key(base_key, &filename)
                }
                _ => {
                    return Err(Box::new(ConfigError(format!(
                        "entity.name={} sink.rejected.format={} is unsupported for s3 output",
                        entity.name, format
                    ))))
                }
            };
            let client = cloud.client_for(resolver, storage, entity)?;
            client.upload(&key, Path::new(&local_path))?;
            Ok(io::storage::s3::format_s3_uri(bucket, &key))
        }
    }
}

pub(super) fn write_rejected_raw_output(
    target: &Target,
    input_file: &InputFile,
    _temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    match target {
        Target::Local { base_path, .. } => {
            let output_path = io::write::write_rejected_raw(&input_file.local_path, base_path)?;
            Ok(output_path.display().to_string())
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let key = io::storage::paths::resolve_output_key(base_key, &input_file.source_name);
            let client = cloud.client_for(resolver, storage, entity)?;
            client.upload(&key, &input_file.local_path)?;
            Ok(io::storage::s3::format_s3_uri(bucket, &key))
        }
    }
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
    match target {
        Target::Local { base_path, .. } => {
            let output_path = io::write::write_error_report(base_path, source_stem, errors_json)?;
            Ok(output_path.display().to_string())
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_base = temp_dir.display().to_string();
            let local_path = io::write::write_error_report(&temp_base, source_stem, errors_json)?;
            let filename =
                io::storage::paths::build_output_filename(source_stem, "_reject_errors", "json");
            let key = io::storage::paths::resolve_sibling_key(base_key, &filename);
            let client = cloud.client_for(resolver, storage, entity)?;
            client.upload(&key, &local_path)?;
            Ok(io::storage::s3::format_s3_uri(bucket, &key))
        }
    }
}

fn temp_local_target(temp_dir: &Path) -> Target {
    let base_path = temp_dir.display().to_string();
    Target::Local {
        storage: "local".to_string(),
        uri: format!("local://{base_path}"),
        base_path,
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
