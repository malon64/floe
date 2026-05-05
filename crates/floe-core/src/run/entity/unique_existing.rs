use std::collections::HashMap;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use deltalake::table::builder::DeltaTableBuilder;
use df_interchange::Interchange;

use crate::checks::normalize::{
    output_column_mapping, rename_output_columns, resolve_normalize_strategy,
};
use crate::errors::{RunError, StorageError};
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::{object_store, Target};
use crate::io::write::iceberg::metadata::{
    latest_gcs_metadata_location, latest_local_metadata_location, latest_s3_metadata_location,
};
use crate::io::write::iceberg::{
    load_glue_table_state, GlueIcebergCatalogConfig, ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE,
};
use crate::io::write::{parts, strategy};
use crate::{check, config, io, FloeResult};

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
    match accepted_format {
        "parquet" => seed_from_parquet(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
            &unique_columns,
        ),
        "delta" => seed_from_delta(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
            &unique_columns,
        ),
        "iceberg" => seed_from_iceberg(
            unique_tracker,
            target,
            cloud,
            resolver,
            catalogs,
            entity,
            &unique_columns,
        ),
        _ => Ok(()),
    }
}

fn seed_from_parquet(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    let (scan_cols, rename_back) = accepted_scan_projection(entity, unique_columns)?;
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let part_files = parts::list_local_part_paths(base_path, "parquet")?;
            for part_path in part_files {
                seed_from_parquet_path(unique_tracker, &part_path, &scan_cols, &rename_back)?;
            }
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for parquet read",
                    entity.name
                )))
            })?;
            let spec = strategy::accepted_parquet_spec();
            let (list_prefix, objects) = {
                let mut ctx = strategy::WriteContext {
                    target,
                    cloud,
                    resolver,
                    entity,
                };
                strategy::list_part_objects(&mut ctx, spec)?
            };
            let client = cloud.client_for(resolver, target.storage(), entity)?;
            for object in objects
                .into_iter()
                .filter(|obj| obj.key.starts_with(&list_prefix))
                .filter(|obj| parts::is_part_key(&obj.key, spec.extension))
            {
                let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                seed_from_parquet_path(unique_tracker, &local_path, &scan_cols, &rename_back)?;
            }
        }
    }
    Ok(())
}

fn seed_from_parquet_path(
    unique_tracker: &mut check::UniqueTracker,
    path: &Path,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    let mut df = read_parquet_lazy(path, Some(scan_cols))?;
    rename_output_columns(&mut df, rename_back)?;
    unique_tracker.seed_from_df(&df)?;
    Ok(())
}

// Builds two parallel lists from unique_columns (runtime/input names):
// - scan_cols: the stored/output names to project from the accepted sink files
// - rename_back: map from stored name -> runtime name, for columns that differ
//
// Used by all three seeding paths (parquet, delta, iceberg) because accepted files
// always contain output names after rename_output_columns is applied before writing.
fn accepted_scan_projection(
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<(Vec<String>, HashMap<String, String>)> {
    let strategy = resolve_normalize_strategy(entity)?;
    // maps runtime_name -> output_name for columns that differ
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

fn seed_from_iceberg(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    catalogs: &config::CatalogResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    // When a Glue catalog is configured, the writer uses glue_target.table_location as the
    // real table root (not target). Seed from the same location so duplicate detection is
    // consistent with what was actually written.
    if let Some(glue_target) =
        catalogs.resolve_iceberg_target(resolver, entity, &entity.sink.accepted)?
    {
        if glue_target.catalog_type == "glue" {
            return seed_from_glue_iceberg(
                unique_tracker,
                &glue_target,
                resolver,
                entity,
                unique_columns,
            );
        }
    }

    let store = object_store::iceberg_store_config(target, resolver, entity)?;

    let metadata_location: Option<String> = match target {
        Target::Local { base_path, .. } => latest_local_metadata_location(Path::new(base_path))?,
        Target::S3 {
            storage, base_key, ..
        } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            latest_s3_metadata_location(client, base_key)?
        }
        Target::Gcs {
            storage, base_key, ..
        } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            latest_gcs_metadata_location(client, base_key)?
        }
        Target::Adls { .. } => return Ok(()),
    };
    let Some(metadata_location) = metadata_location else {
        return Ok(());
    };

    let (scan_cols, rename_back) = accepted_scan_projection(entity, unique_columns)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("iceberg seed runtime init failed: {err}"))))?;

    let batches = runtime
        .block_on(collect_iceberg_batches(
            metadata_location,
            store.file_io_props,
            store.warehouse_location,
            &entity.name,
            &scan_cols,
        ))
        .map_err(|err| Box::new(RunError(format!("iceberg seed failed: {err}"))))?;

    seed_from_batches(unique_tracker, batches, &rename_back)
}

fn seed_from_glue_iceberg(
    unique_tracker: &mut check::UniqueTracker,
    glue_target: &config::ResolvedIcebergCatalogTarget,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    let catalog_target = Target::from_resolved(&glue_target.table_location)?;
    let store = object_store::iceberg_store_config(&catalog_target, resolver, entity)?;

    let glue_cfg = GlueIcebergCatalogConfig {
        catalog_name: glue_target.catalog_name.clone(),
        region: glue_target.region.clone(),
        database: glue_target.database.clone(),
        namespace: glue_target.namespace.clone(),
        table: glue_target.table.clone(),
    };

    let (scan_cols, rename_back) = accepted_scan_projection(entity, unique_columns)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(RunError(format!(
                "glue iceberg seed runtime init failed: {err}"
            )))
        })?;

    let glue_state = runtime
        .block_on(load_glue_table_state(&glue_cfg))
        .map_err(|err| {
            Box::new(RunError(format!(
                "glue get_table for iceberg seed failed: {err}"
            )))
        })?;

    let Some(metadata_location) = glue_state.metadata_location else {
        return Ok(());
    };

    let batches = runtime
        .block_on(collect_iceberg_batches(
            metadata_location,
            store.file_io_props,
            store.warehouse_location,
            &entity.name,
            &scan_cols,
        ))
        .map_err(|err| Box::new(RunError(format!("glue iceberg seed failed: {err}"))))?;

    seed_from_batches(unique_tracker, batches, &rename_back)
}

// Uses table.scan().to_arrow() so the Iceberg reader applies positional and equality deletes
// before returning rows — only live rows are seeded.
async fn collect_iceberg_batches(
    metadata_location: String,
    catalog_props: HashMap<String, String>,
    warehouse_location: String,
    entity_name: &str,
    scan_columns: &[String],
) -> Result<Vec<RecordBatch>, iceberg::Error> {
    use futures::TryStreamExt;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

    let mut props = catalog_props;
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location);

    let catalog = MemoryCatalogBuilder::default()
        .load(ICEBERG_CATALOG_NAME, props)
        .await?;

    let namespace = NamespaceIdent::new(ICEBERG_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    let table_name = iceberg_table_name(entity_name);
    let table_ident = TableIdent::new(namespace, table_name);

    let table = catalog
        .register_table(&table_ident, metadata_location)
        .await?;

    let scan = table.scan().select(scan_columns.iter().cloned()).build()?;
    let batch_stream = scan.to_arrow().await?;
    batch_stream
        .try_filter(|b| std::future::ready(b.num_rows() > 0))
        .try_collect()
        .await
}

fn seed_from_batches(
    unique_tracker: &mut check::UniqueTracker,
    batches: Vec<RecordBatch>,
    // Maps stored/output column name -> runtime/input name, for columns that differ.
    // Applied after converting to DataFrame so seed_from_df sees runtime names.
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    for batch in batches {
        let mut df = Interchange::from_arrow_57(vec![batch])
            .and_then(|ic| ic.to_polars_0_52())
            .map_err(|err| {
                Box::new(RunError(format!(
                    "iceberg batch to DataFrame conversion failed: {err}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        rename_output_columns(&mut df, rename_back)?;
        unique_tracker.seed_from_df(&df)?;
    }
    Ok(())
}

fn iceberg_table_name(entity_name: &str) -> String {
    let mut out = String::with_capacity(entity_name.len());
    for ch in entity_name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "table".to_string()
    } else {
        out
    }
}

fn seed_from_delta(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    _temp_dir: Option<&Path>,
    _cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    let (scan_cols, rename_back) = accepted_scan_projection(entity, unique_columns)?;
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let table_url = store.table_url;
    let storage_options = store.storage_options;
    let builder = DeltaTableBuilder::from_url(table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(storage_options.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
    let table = runtime.block_on(async move { builder.load().await });
    let table = match table {
        Ok(table) => table,
        Err(err) => match err {
            deltalake::DeltaTableError::NotATable(_) => return Ok(()),
            other => return Err(Box::new(RunError(format!("delta load failed: {other}")))),
        },
    };
    let batches = runtime
        .block_on(async {
            let (_t, stream) = table.scan_table().with_columns(scan_cols.clone()).await?;
            deltalake::operations::collect_sendable_stream(stream).await
        })
        .map_err(|err| Box::new(RunError(format!("delta scan failed: {err}"))))?;
    seed_from_batches(unique_tracker, batches, &rename_back)
}
