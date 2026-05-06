use std::collections::HashMap;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use df_interchange::Interchange;

use crate::checks::normalize::{
    output_column_mapping, rename_output_columns, resolve_normalize_strategy,
};
use crate::errors::RunError;
use crate::io::storage::Target;
use crate::{check, config, io, FloeResult};

mod delta;
mod iceberg;
mod parquet;

/// Implemented by each sink format to seed the UniqueTracker from its existing data.
///
/// `scan_cols` are the stored/output column names to project from the sink.
/// `rename_back` maps stored name → runtime name for columns whose names differ.
pub(crate) trait FormatSeeder {
    fn seed(
        &mut self,
        unique_tracker: &mut check::UniqueTracker,
        scan_cols: &[String],
        rename_back: &HashMap<String, String>,
    ) -> FloeResult<()>;
}

#[allow(clippy::too_many_arguments)]
pub fn seed_unique_tracker_for_append(
    unique_tracker: &mut check::UniqueTracker,
    write_mode: config::WriteMode,
    accepted_format: &str,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    catalogs: &config::CatalogResolver,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    if write_mode != config::WriteMode::Append || unique_tracker.is_empty() {
        return Ok(());
    }
    let unique_columns = unique_tracker.runtime_columns();
    if unique_columns.is_empty() {
        return Ok(());
    }
    let (scan_cols, rename_back) = accepted_scan_projection(entity, &unique_columns)?;
    match accepted_format {
        "parquet" => parquet::ParquetSeeder {
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
        }
        .seed(unique_tracker, &scan_cols, &rename_back),
        "delta" => delta::DeltaSeeder {
            target,
            resolver,
            entity,
        }
        .seed(unique_tracker, &scan_cols, &rename_back),
        "iceberg" => iceberg::IcebergSeeder {
            target,
            cloud,
            resolver,
            catalogs,
            entity,
        }
        .seed(unique_tracker, &scan_cols, &rename_back),
        _ => Ok(()),
    }
}

// Builds two parallel lists from unique_columns (runtime/input names):
// - scan_cols: the stored/output names to project from the accepted sink files
// - rename_back: map from stored name -> runtime name, for columns that differ
//
// Accepted files always store output names (after rename_output_columns is applied on write),
// so all seeding paths use this to project the right columns and rename them back.
pub(crate) fn accepted_scan_projection(
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<(Vec<String>, HashMap<String, String>)> {
    let strategy = resolve_normalize_strategy(entity)?;
    let runtime_to_output = output_column_mapping(&entity.schema.columns, strategy.as_deref())?;

    let mut scan_cols = Vec::with_capacity(unique_columns.len());
    let mut rename_back = HashMap::new();
    for runtime in unique_columns {
        if let Some(output) = runtime_to_output.get(runtime) {
            scan_cols.push(output.clone());
            rename_back.insert(output.clone(), runtime.clone());
        } else {
            scan_cols.push(runtime.clone());
        }
    }
    Ok((scan_cols, rename_back))
}

pub(crate) fn seed_from_batches(
    unique_tracker: &mut check::UniqueTracker,
    batches: Vec<RecordBatch>,
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    for batch in batches {
        let mut df = Interchange::from_arrow_57(vec![batch])
            .and_then(|ic| ic.to_polars_0_52())
            .map_err(|err| {
                Box::new(RunError(format!(
                    "batch to DataFrame conversion failed: {err}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        rename_output_columns(&mut df, rename_back)?;
        unique_tracker.seed_from_df(&df)?;
    }
    Ok(())
}
