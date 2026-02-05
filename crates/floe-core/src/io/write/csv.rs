use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, SerWriter};

use crate::errors::IoError;
use crate::io::format::{RejectedSinkAdapter, RejectedWriteRequest};
use crate::io::storage::Target;
use crate::{config, errors::StorageError, io, FloeResult};

struct CsvRejectedAdapter;

static CSV_REJECTED_ADAPTER: CsvRejectedAdapter = CsvRejectedAdapter;

pub(crate) fn csv_rejected_adapter() -> &'static dyn RejectedSinkAdapter {
    &CSV_REJECTED_ADAPTER
}

pub fn write_rejected_csv(
    df: &mut DataFrame,
    output_path: &Path,
    mode: config::WriteMode,
) -> FloeResult<PathBuf> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let include_header = match mode {
        config::WriteMode::Overwrite => true,
        config::WriteMode::Append => {
            !output_path.exists() || std::fs::metadata(output_path)?.len() == 0
        }
    };
    let file = match mode {
        config::WriteMode::Overwrite => OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?,
        config::WriteMode::Append => OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?,
    };
    CsvWriter::new(file)
        .include_header(include_header)
        .finish(df)
        .map_err(|err| Box::new(IoError(format!("rejected csv write failed: {err}"))))?;
    Ok(output_path.to_path_buf())
}

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(&self, request: RejectedWriteRequest<'_>) -> FloeResult<String> {
        let filename =
            io::storage::paths::build_output_filename(request.source_stem, "_rejected", "csv");
        match request.mode {
            config::WriteMode::Overwrite => io::storage::output::write_output(
                request.target,
                io::storage::output::OutputPlacement::Output,
                &filename,
                request.temp_dir,
                request.cloud,
                request.resolver,
                request.entity,
                |path| {
                    write_rejected_csv(request.df, path, config::WriteMode::Overwrite)?;
                    Ok(())
                },
            ),
            config::WriteMode::Append => write_rejected_csv_append(
                request.target,
                request.df,
                &filename,
                request.temp_dir,
                request.cloud,
                request.resolver,
                request.entity,
            ),
        }
    }
}

fn write_rejected_csv_append(
    target: &Target,
    df: &mut DataFrame,
    filename: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<String> {
    match target {
        Target::Local { base_path, .. } => {
            let output_path = io::storage::paths::resolve_output_path(base_path, filename);
            write_rejected_csv(df, &output_path, config::WriteMode::Append)?;
            Ok(output_path.display().to_string())
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => write_remote_rejected_csv_append(
            df,
            temp_dir,
            cloud,
            resolver,
            entity,
            storage,
            &io::storage::s3::format_s3_uri(
                bucket,
                &io::storage::paths::resolve_output_key(base_key, filename),
            ),
            filename,
        ),
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => write_remote_rejected_csv_append(
            df,
            temp_dir,
            cloud,
            resolver,
            entity,
            storage,
            &io::storage::gcs::format_gcs_uri(
                bucket,
                &io::storage::paths::resolve_output_key(base_key, filename),
            ),
            filename,
        ),
        Target::Adls {
            storage,
            account,
            container,
            base_path,
            ..
        } => write_remote_rejected_csv_append(
            df,
            temp_dir,
            cloud,
            resolver,
            entity,
            storage,
            &io::storage::adls::format_abfs_uri(
                container,
                account,
                &io::storage::paths::resolve_output_key(base_path, filename),
            ),
            filename,
        ),
    }
}

fn write_remote_rejected_csv_append(
    df: &mut DataFrame,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    uri: &str,
    filename: &str,
) -> FloeResult<String> {
    let temp_dir = temp_dir.ok_or_else(|| {
        Box::new(StorageError(format!(
            "entity.name={} missing temp dir for append rejected output",
            entity.name
        )))
    })?;
    let temp_path = temp_dir.join(filename);
    let client = cloud.client_for(resolver, storage, entity)?;

    if client.exists(uri)? {
        let existing_path = client.download_to_temp(uri, temp_dir)?;
        if existing_path != temp_path {
            if let Some(parent) = temp_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(existing_path, &temp_path)?;
        }
    } else if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }

    write_rejected_csv(df, &temp_path, config::WriteMode::Append)?;
    client.upload_from_path(&temp_path, uri)?;
    Ok(uri.to_string())
}

// Filename construction is shared via io::storage::paths helpers.
