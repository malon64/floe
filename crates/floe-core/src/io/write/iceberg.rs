use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_glue::config::Region as GlueRegion;
use aws_sdk_glue::types::{
    StorageDescriptor as GlueStorageDescriptor, TableInput as GlueTableInput,
};
use aws_sdk_glue::Client as GlueClient;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use polars::prelude::{DataFrame, DataType, Series, TimeUnit};
use uuid::Uuid;

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput};
use crate::io::storage::{object_store::iceberg_store_config, ObjectRef, Target};
use crate::{config, io, FloeResult};

struct IcebergAcceptedAdapter;

static ICEBERG_ACCEPTED_ADAPTER: IcebergAcceptedAdapter = IcebergAcceptedAdapter;

const ICEBERG_NAMESPACE: &str = "floe";

pub(crate) fn iceberg_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &ICEBERG_ACCEPTED_ADAPTER
}

#[derive(Debug)]
struct PreparedIcebergWrite {
    iceberg_schema: Schema,
    batch: RecordBatch,
}

#[derive(Debug)]
struct IcebergWriteResult {
    files_written: u64,
    snapshot_id: Option<i64>,
    metadata_version: Option<i64>,
    file_paths: Vec<String>,
    table_root_uri: String,
    iceberg_catalog_name: Option<String>,
    iceberg_database: Option<String>,
    iceberg_namespace: Option<String>,
    iceberg_table: Option<String>,
}

struct IcebergRemoteContext<'a> {
    cloud: &'a mut io::storage::CloudClient,
    resolver: &'a config::StorageResolver,
    catalogs: &'a config::CatalogResolver,
}

struct IcebergWriteContext {
    table_root_uri: String,
    catalog_name: &'static str,
    catalog_props: HashMap<String, String>,
    metadata_location: Option<String>,
    glue_catalog: Option<GlueIcebergCatalogConfig>,
}

#[derive(Debug, Clone)]
struct GlueIcebergCatalogConfig {
    catalog_name: String,
    region: String,
    database: String,
    namespace: String,
    table: String,
}

impl AcceptedSinkAdapter for IcebergAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        mode: config::WriteMode,
        _output_stem: &str,
        _temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        write_iceberg_table_with_remote_context(
            df,
            target,
            entity,
            mode,
            Some(IcebergRemoteContext {
                cloud,
                resolver,
                catalogs,
            }),
        )
    }
}

pub fn write_iceberg_table(
    df: &mut DataFrame,
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<AcceptedWriteOutput> {
    write_iceberg_table_with_remote_context(df, target, entity, mode, None)
}

fn write_iceberg_table_with_remote_context(
    df: &mut DataFrame,
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    mut remote: Option<IcebergRemoteContext<'_>>,
) -> FloeResult<AcceptedWriteOutput> {
    let write_ctx = build_iceberg_write_context(target, entity, mode, remote.as_mut())?;
    let prepared = prepare_iceberg_write(df, entity)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("iceberg runtime init failed: {err}"))))?;

    let result = runtime.block_on(write_iceberg_table_async(write_ctx, prepared, entity, mode))?;
    Ok(AcceptedWriteOutput {
        files_written: result.files_written,
        parts_written: result.files_written,
        part_files: result.file_paths,
        table_version: result.metadata_version,
        snapshot_id: result.snapshot_id,
        table_root_uri: result
            .iceberg_catalog_name
            .as_ref()
            .map(|_| result.table_root_uri.clone()),
        iceberg_catalog_name: result.iceberg_catalog_name,
        iceberg_database: result.iceberg_database,
        iceberg_namespace: result.iceberg_namespace,
        iceberg_table: result.iceberg_table,
        metrics: AcceptedWriteMetrics {
            total_bytes_written: None,
            avg_file_size_mb: None,
            small_files_count: None,
        },
    })
}

async fn write_iceberg_table_async(
    write_ctx: IcebergWriteContext,
    prepared: PreparedIcebergWrite,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<IcebergWriteResult> {
    let IcebergWriteContext {
        table_root_uri,
        catalog_name,
        mut catalog_props,
        mut metadata_location,
        glue_catalog,
    } = write_ctx;
    let mut glue_table_state = None;
    if let Some(glue_cfg) = glue_catalog.as_ref() {
        let state = load_glue_table_state(glue_cfg).await?;
        metadata_location = state.metadata_location.clone();
        glue_table_state = Some(state);
    }
    catalog_props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), table_root_uri.clone());

    let catalog = MemoryCatalogBuilder::default()
        .load(catalog_name, catalog_props)
        .await
        .map_err(map_iceberg_err("iceberg catalog init failed"))?;
    let namespace_name = glue_catalog
        .as_ref()
        .map(|cfg| cfg.namespace.clone())
        .unwrap_or_else(|| ICEBERG_NAMESPACE.to_string());
    let namespace = NamespaceIdent::new(namespace_name);
    ensure_namespace(&catalog, &namespace).await?;
    let table_name = glue_catalog
        .as_ref()
        .map(|cfg| cfg.table.clone())
        .unwrap_or_else(|| sanitize_table_name(&entity.name));
    let table_ident = TableIdent::new(namespace.clone(), table_name);

    let existing_table = if let Some(location) = metadata_location.as_ref() {
        Some(
            catalog
                .register_table(&table_ident, location.clone())
                .await
                .map_err(map_iceberg_err("iceberg register existing table failed"))?,
        )
    } else {
        None
    };

    if let Some(existing) = existing_table.as_ref() {
        ensure_schema_matches(
            existing.metadata().current_schema(),
            &prepared.iceberg_schema,
            entity,
        )?;
    }

    let table = match mode {
        config::WriteMode::Append => match existing_table {
            Some(table) => table,
            None => {
                create_table(
                    &catalog,
                    &namespace,
                    &table_ident,
                    table_root_uri.clone(),
                    &prepared.iceberg_schema,
                )
                .await?
            }
        },
        config::WriteMode::Overwrite => {
            if existing_table.is_some() {
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(map_iceberg_err("iceberg drop table mapping failed"))?;
            }
            create_table(
                &catalog,
                &namespace,
                &table_ident,
                table_root_uri.clone(),
                &prepared.iceberg_schema,
            )
            .await?
        }
    };

    let mut file_paths = Vec::new();
    let mut files_written = 0_u64;
    let mut table_after_write = table;

    if prepared.batch.num_rows() > 0 {
        let data_files = write_data_files(&table_after_write, prepared.batch).await?;
        files_written = data_files.len() as u64;
        file_paths = data_files
            .iter()
            .map(|file| {
                let file_path = file.file_path().to_string();
                Path::new(file_path.as_str())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or(file_path)
            })
            .take(50)
            .collect();

        let tx = Transaction::new(&table_after_write);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(map_iceberg_err("iceberg append transaction apply failed"))?;
        table_after_write = tx
            .commit(&catalog)
            .await
            .map_err(map_iceberg_err("iceberg commit failed"))?;
    }

    let snapshot_id = table_after_write
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let metadata_version = table_after_write
        .metadata_location()
        .and_then(parse_metadata_version_from_location);
    let final_metadata_location = table_after_write
        .metadata_location()
        .map(|value| value.to_string())
        .ok_or_else(|| {
            Box::new(RunError(
                "iceberg table metadata location missing after commit".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

    if let Some(glue_cfg) = glue_catalog.as_ref() {
        upsert_glue_table(
            glue_cfg,
            &table_root_uri,
            &final_metadata_location,
            glue_table_state
                .as_ref()
                .and_then(|state| state.version_id.as_deref()),
        )
        .await?;
    }

    Ok(IcebergWriteResult {
        files_written,
        snapshot_id,
        metadata_version,
        file_paths,
        table_root_uri,
        iceberg_catalog_name: glue_catalog.as_ref().map(|cfg| cfg.catalog_name.clone()),
        iceberg_database: glue_catalog.as_ref().map(|cfg| cfg.database.clone()),
        iceberg_namespace: glue_catalog.as_ref().map(|cfg| cfg.namespace.clone()),
        iceberg_table: glue_catalog.as_ref().map(|cfg| cfg.table.clone()),
    })
}

fn build_iceberg_write_context(
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    mut remote: Option<&mut IcebergRemoteContext<'_>>,
) -> FloeResult<IcebergWriteContext> {
    if entity.sink.accepted.iceberg.is_some() && remote.is_none() {
        return Err(Box::new(RunError(format!(
            "iceberg catalog writes require runtime catalog context for entity {}",
            entity.name
        ))));
    }
    match target {
        Target::Local { base_path, .. } => {
            let table_root = PathBuf::from(base_path);
            fs::create_dir_all(&table_root)?;
            let metadata_location = latest_local_metadata_location(&table_root)?;
            Ok(IcebergWriteContext {
                table_root_uri: base_path.to_string(),
                catalog_name: "floe_iceberg",
                catalog_props: HashMap::new(),
                metadata_location,
                glue_catalog: None,
            })
        }
        Target::S3 {
            storage,
            uri,
            bucket,
            base_key,
        } => {
            if let Some(ctx) = remote.as_mut() {
                let ctx = &mut **ctx;
                if let Some(glue_target) = ctx.catalogs.resolve_iceberg_target(
                    ctx.resolver,
                    entity,
                    &entity.sink.accepted,
                )? {
                    if glue_target.catalog_type == "glue" {
                        let catalog_target = Target::from_resolved(&glue_target.table_location)?;
                        let store = iceberg_store_config(&catalog_target, ctx.resolver, entity)?;
                        return Ok(IcebergWriteContext {
                            table_root_uri: store.warehouse_location,
                            catalog_name: "floe_iceberg",
                            catalog_props: store.file_io_props,
                            metadata_location: None,
                            glue_catalog: Some(GlueIcebergCatalogConfig {
                                catalog_name: glue_target.catalog_name,
                                region: glue_target.region,
                                database: glue_target.database,
                                namespace: glue_target.namespace,
                                table: glue_target.table,
                            }),
                        });
                    }
                }
            }

            let metadata_location = if matches!(mode, config::WriteMode::Append) {
                match remote.as_mut() {
                    Some(ctx) => {
                        let ctx = &mut **ctx;
                        let client = ctx.cloud.client_for(ctx.resolver, storage, entity)?;
                        latest_s3_metadata_location(client, base_key)?
                    }
                    None => {
                        let mut client = io::storage::s3::S3Client::new(bucket.clone(), None)?;
                        latest_s3_metadata_location(&mut client, base_key)?
                    }
                }
            } else {
                None
            };

            match remote.as_mut() {
                Some(ctx) => {
                    let ctx = &mut **ctx;
                    let store = iceberg_store_config(target, ctx.resolver, entity)?;
                    Ok(IcebergWriteContext {
                        table_root_uri: store.warehouse_location,
                        catalog_name: "floe_iceberg",
                        catalog_props: store.file_io_props,
                        metadata_location,
                        glue_catalog: None,
                    })
                }
                None => Ok(IcebergWriteContext {
                    table_root_uri: uri.clone(),
                    catalog_name: "floe_iceberg",
                    catalog_props: HashMap::new(),
                    metadata_location,
                    glue_catalog: None,
                }),
            }
        }
        Target::Gcs {
            storage,
            uri,
            bucket,
            base_key,
        } => {
            let metadata_location = if matches!(mode, config::WriteMode::Append) {
                match remote.as_mut() {
                    Some(ctx) => {
                        let ctx = &mut **ctx;
                        let client = ctx.cloud.client_for(ctx.resolver, storage, entity)?;
                        latest_gcs_metadata_location(client, base_key)?
                    }
                    None => {
                        let mut client = io::storage::gcs::GcsClient::new(bucket.clone())?;
                        latest_gcs_metadata_location(&mut client, base_key)?
                    }
                }
            } else {
                None
            };

            match remote.as_mut() {
                Some(ctx) => {
                    let ctx = &mut **ctx;
                    let store = iceberg_store_config(target, ctx.resolver, entity)?;
                    Ok(IcebergWriteContext {
                        table_root_uri: store.warehouse_location,
                        catalog_name: "floe_iceberg",
                        catalog_props: store.file_io_props,
                        metadata_location,
                        glue_catalog: None,
                    })
                }
                None => Ok(IcebergWriteContext {
                    table_root_uri: uri.clone(),
                    catalog_name: "floe_iceberg",
                    catalog_props: HashMap::new(),
                    metadata_location,
                    glue_catalog: None,
                }),
            }
        }
        Target::Adls { .. } => Err(Box::new(RunError(format!(
            "iceberg sink currently supports local, s3, or gcs storage only for entity {}",
            entity.name
        )))),
    }
}

async fn ensure_namespace(
    catalog: &iceberg::MemoryCatalog,
    namespace: &NamespaceIdent,
) -> FloeResult<()> {
    let exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(map_iceberg_err("iceberg namespace exists check failed"))?;
    if !exists {
        catalog
            .create_namespace(namespace, HashMap::new())
            .await
            .map_err(map_iceberg_err("iceberg namespace create failed"))?;
    }
    Ok(())
}

async fn create_table(
    catalog: &iceberg::MemoryCatalog,
    namespace: &NamespaceIdent,
    table_ident: &TableIdent,
    table_root: String,
    schema: &Schema,
) -> FloeResult<iceberg::table::Table> {
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location(table_root)
        .schema(schema.clone())
        .build();
    catalog
        .create_table(namespace, creation)
        .await
        .map_err(map_iceberg_err("iceberg create table failed"))
}

async fn write_data_files(
    table: &iceberg::table::Table,
    batch: RecordBatch,
) -> FloeResult<Vec<iceberg::spec::DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(map_iceberg_err("iceberg location generator failed"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "floe".to_string(),
        Some(Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        Default::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let mut writer = DataFileWriterBuilder::new(rolling_writer_builder)
        .build(None)
        .await
        .map_err(map_iceberg_err("iceberg data writer build failed"))?;
    writer
        .write(batch)
        .await
        .map_err(map_iceberg_err("iceberg data write failed"))?;
    writer
        .close()
        .await
        .map_err(map_iceberg_err("iceberg data writer close failed"))
}

fn prepare_iceberg_write(
    df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<PreparedIcebergWrite> {
    let columns = resolve_output_columns(df, entity)?;
    if columns.is_empty() {
        return Err(Box::new(RunError(format!(
            "iceberg sink requires at least one column for entity {}",
            entity.name
        ))));
    }

    let mut field_id = 1_i32;
    let mut iceberg_fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());

    for (name, nullable, series) in columns {
        if !nullable && series.null_count() > 0 {
            return Err(Box::new(RunError(format!(
                "iceberg write rejected nulls for non-nullable column {}",
                name
            ))));
        }

        let primitive = polars_dtype_to_iceberg_type(series, &entity.name)?;
        let field = if nullable {
            NestedField::optional(field_id, name.clone(), Type::Primitive(primitive))
        } else {
            NestedField::required(field_id, name.clone(), Type::Primitive(primitive))
        };
        iceberg_fields.push(field.into());
        arrays.push(series_to_arrow_array(series, &name, &entity.name)?);
        field_id += 1;
    }

    let iceberg_schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(iceberg_fields)
        .build()
        .map_err(map_iceberg_err("iceberg schema build failed"))?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(&iceberg_schema)
            .map_err(map_iceberg_err("iceberg arrow schema conversion failed"))?,
    );
    let batch = RecordBatch::try_new(arrow_schema, arrays).map_err(|err| {
        Box::new(RunError(format!(
            "iceberg record batch build failed: {err}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;

    Ok(PreparedIcebergWrite {
        iceberg_schema,
        batch,
    })
}

fn resolve_output_columns<'a>(
    df: &'a DataFrame,
    entity: &'a config::EntityConfig,
) -> FloeResult<Vec<(String, bool, &'a Series)>> {
    if entity.schema.columns.is_empty() {
        let mut columns = Vec::with_capacity(df.width());
        for column in df.get_columns() {
            let series = column.as_materialized_series();
            let name = series.name().to_string();
            // Keep inferred schemas stable across runs when no explicit schema is provided.
            // Batch-dependent null distributions should not flip required/optional.
            let nullable = true;
            columns.push((name, nullable, series));
        }
        return Ok(columns);
    }

    let schema_columns = normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize::resolve_normalize_strategy(entity)?.as_deref(),
    );

    let mut columns = Vec::with_capacity(schema_columns.len());
    for column in &schema_columns {
        let series = df
            .column(column.name.as_str())
            .map_err(|err| Box::new(RunError(format!("iceberg column lookup failed: {err}"))))?
            .as_materialized_series();
        columns.push((column.name.clone(), column.nullable.unwrap_or(true), series));
    }
    Ok(columns)
}

fn polars_dtype_to_iceberg_type(series: &Series, entity_name: &str) -> FloeResult<PrimitiveType> {
    let primitive = match series.dtype() {
        DataType::String => PrimitiveType::String,
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Date => PrimitiveType::Date,
        DataType::Time => PrimitiveType::Time,
        DataType::Datetime(_, tz) => {
            if tz.is_some() {
                PrimitiveType::Timestamptz
            } else {
                PrimitiveType::Timestamp
            }
        }
        dtype => {
            return Err(Box::new(RunError(format!(
                "iceberg sink supports scalar types only; unsupported dtype {dtype:?} for column {} (entity {})",
                series.name(),
                entity_name
            ))))
        }
    };
    Ok(primitive)
}

fn series_to_arrow_array(
    series: &Series,
    column_name: &str,
    entity_name: &str,
) -> FloeResult<ArrayRef> {
    let array: ArrayRef = match series.dtype() {
        DataType::String => Arc::new(StringArray::from_iter(series.str()?)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(series.bool()?)),
        DataType::Int8 => {
            let values = series.i8()?;
            Arc::new(Int32Array::from_iter(values.into_iter().map(|opt| opt.map(i32::from))))
        }
        DataType::Int16 => {
            let values = series.i16()?;
            Arc::new(Int32Array::from_iter(values.into_iter().map(|opt| opt.map(i32::from))))
        }
        DataType::Int32 => Arc::new(Int32Array::from_iter(series.i32()?)),
        DataType::Int64 => Arc::new(Int64Array::from_iter(series.i64()?)),
        DataType::Float32 => Arc::new(Float32Array::from_iter(series.f32()?)),
        DataType::Float64 => Arc::new(Float64Array::from_iter(series.f64()?)),
        DataType::Date => {
            let values = series.date()?;
            Arc::new(Date32Array::from_iter(values.phys.iter()))
        }
        DataType::Time => {
            let values = series.time()?;
            let micros = values.phys.iter().map(|opt| opt.map(|value| value / 1_000));
            Arc::new(Time64MicrosecondArray::from_iter(micros))
        }
        DataType::Datetime(unit, _) => {
            let values = series.datetime()?;
            let micros = values.phys.iter().map(|opt| match unit {
                TimeUnit::Milliseconds => opt.map(|value| value.saturating_mul(1_000)),
                TimeUnit::Microseconds => opt,
                TimeUnit::Nanoseconds => opt.map(|value| value / 1_000),
            });
            Arc::new(TimestampMicrosecondArray::from_iter(micros))
        }
        dtype => {
            return Err(Box::new(RunError(format!(
                "iceberg sink supports scalar types only; unsupported dtype {dtype:?} for column {} (entity {})",
                column_name, entity_name
            ))))
        }
    };
    Ok(array)
}

fn ensure_schema_matches(
    existing: &Schema,
    expected: &Schema,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    let existing_fields = existing.as_struct().fields();
    let expected_fields = expected.as_struct().fields();
    if existing_fields.len() != expected_fields.len() {
        return Err(Box::new(RunError(format!(
            "entity.name={} iceberg schema evolution is not supported (column count differs: existing={}, incoming={})",
            entity.name,
            existing_fields.len(),
            expected_fields.len()
        ))));
    }

    for (index, (existing_field, expected_field)) in existing_fields
        .iter()
        .zip(expected_fields.iter())
        .enumerate()
    {
        if existing_field.name != expected_field.name
            || existing_field.required != expected_field.required
            || existing_field.field_type != expected_field.field_type
        {
            return Err(Box::new(RunError(format!(
                "entity.name={} iceberg schema evolution is not supported (column {} differs: existing={} incoming={})",
                entity.name,
                index,
                describe_field(existing_field),
                describe_field(expected_field)
            ))));
        }
    }

    Ok(())
}

fn describe_field(field: &Arc<iceberg::spec::NestedField>) -> String {
    let required = if field.required {
        "required"
    } else {
        "optional"
    };
    format!("{}:{}:{required}", field.name, field.field_type)
}

fn latest_local_metadata_location(table_root: &Path) -> FloeResult<Option<String>> {
    let metadata_dir = table_root.join("metadata");
    if !metadata_dir.exists() {
        return Ok(None);
    }

    let mut best: Option<(i64, PathBuf)> = None;
    for entry in fs::read_dir(&metadata_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_path)) => {
                version > *best_version || (version == *best_version && path > *best_path)
            }
        };
        if replace {
            best = Some((version, path));
        }
    }

    Ok(best.map(|(_, path)| path.display().to_string()))
}

fn latest_s3_metadata_location(
    client: &mut dyn io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

fn latest_gcs_metadata_location(
    client: &mut dyn io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

fn latest_metadata_location_from_objects(objects: Vec<ObjectRef>) -> FloeResult<Option<String>> {
    let mut best: Option<(i64, String, String)> = None;
    for object in objects {
        let file_name = object
            .key
            .rsplit('/')
            .next()
            .unwrap_or(object.key.as_str())
            .to_string();
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(&file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_key, _)) => {
                version > *best_version || (version == *best_version && object.key > *best_key)
            }
        };
        if replace {
            best = Some((version, object.key.clone(), object.uri.clone()));
        }
    }
    Ok(best.map(|(_, _, uri)| uri))
}

#[derive(Debug, Clone)]
struct GlueTableState {
    metadata_location: Option<String>,
    version_id: Option<String>,
}

async fn build_glue_client(region: &str) -> FloeResult<GlueClient> {
    let region_provider =
        RegionProviderChain::first_try(GlueRegion::new(region.to_string())).or_default_provider();
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    Ok(GlueClient::new(&config))
}

async fn load_glue_table_state(glue_cfg: &GlueIcebergCatalogConfig) -> FloeResult<GlueTableState> {
    let client = build_glue_client(&glue_cfg.region).await?;
    let response = client
        .get_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .send()
        .await;
    match response {
        Ok(output) => {
            let table = output.table().ok_or_else(|| {
                Box::new(RunError(format!(
                    "glue get_table returned no table for {}.{}",
                    glue_cfg.database, glue_cfg.table
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let parameters = table.parameters();
            let metadata_location =
                parameters.and_then(|params| params.get("metadata_location").cloned());
            let iceberg_param = parameters
                .and_then(|params| params.get("table_type"))
                .map(|value| value.eq_ignore_ascii_case("ICEBERG"))
                .unwrap_or(false);
            if !iceberg_param || metadata_location.is_none() {
                return Err(Box::new(RunError(format!(
                    "glue table {}.{} exists but is not an Iceberg table managed by Floe (missing Iceberg parameters/metadata_location)",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
            Ok(GlueTableState {
                metadata_location,
                version_id: table.version_id().map(ToOwned::to_owned),
            })
        }
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(|service_err| service_err.is_entity_not_found_exception())
            {
                return Ok(GlueTableState {
                    metadata_location: None,
                    version_id: None,
                });
            }
            Err(Box::new(RunError(format!(
                "glue get_table failed for {}.{}: {err}",
                glue_cfg.database, glue_cfg.table
            ))))
        }
    }
}

async fn upsert_glue_table(
    glue_cfg: &GlueIcebergCatalogConfig,
    table_root_uri: &str,
    metadata_location: &str,
    version_id: Option<&str>,
) -> FloeResult<()> {
    let client = build_glue_client(&glue_cfg.region).await?;
    let table_input = build_glue_table_input(glue_cfg, table_root_uri, metadata_location);

    let create_result = client
        .create_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .table_input(table_input.clone())
        .send()
        .await;
    match create_result {
        Ok(_) => return Ok(()),
        Err(err) => {
            if !err
                .as_service_error()
                .is_some_and(|service_err| service_err.is_already_exists_exception())
            {
                return Err(Box::new(RunError(format!(
                    "glue create_table failed for {}.{}: {err}",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
        }
    }

    let mut update = client
        .update_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .table_input(table_input)
        .skip_archive(true);
    if let Some(version_id) = version_id {
        update = update.version_id(version_id);
    }
    update.send().await.map_err(|err| {
        Box::new(RunError(format!(
            "glue update_table failed for {}.{}: {err}",
            glue_cfg.database, glue_cfg.table
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(())
}

fn build_glue_table_input(
    glue_cfg: &GlueIcebergCatalogConfig,
    table_root_uri: &str,
    metadata_location: &str,
) -> GlueTableInput {
    let storage_descriptor = GlueStorageDescriptor::builder()
        .location(table_root_uri)
        .build();

    GlueTableInput::builder()
        .name(glue_cfg.table.as_str())
        .table_type("EXTERNAL_TABLE")
        .storage_descriptor(storage_descriptor)
        .set_partition_keys(Some(Vec::new()))
        .parameters("table_type", "ICEBERG")
        .parameters("EXTERNAL", "TRUE")
        .parameters("metadata_location", metadata_location)
        .parameters("floe.iceberg.namespace", glue_cfg.namespace.as_str())
        .build()
        .expect("glue table input builder validated")
}

fn parse_metadata_version_from_location(location: &str) -> Option<i64> {
    let file_name = Path::new(location).file_name()?.to_str()?;
    parse_metadata_version_from_filename(file_name)
}

fn parse_metadata_version_from_filename(file_name: &str) -> Option<i64> {
    let prefix = file_name.split_once('-')?.0;
    prefix.parse::<i64>().ok()
}

fn sanitize_table_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
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

fn map_iceberg_err(
    context: &'static str,
) -> impl FnOnce(iceberg::Error) -> Box<dyn std::error::Error + Send + Sync> {
    move |err| Box::new(RunError(format!("{context}: {err}")))
}
