use std::path::Path;
use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use deltalake::arrow::datatypes::{Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::{DataFrame, DataType, TimeUnit};

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::{object_store, Target};
use crate::{config, io, FloeResult};

struct DeltaAcceptedAdapter;

static DELTA_ACCEPTED_ADAPTER: DeltaAcceptedAdapter = DeltaAcceptedAdapter;

pub(crate) fn delta_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &DELTA_ACCEPTED_ADAPTER
}

pub fn write_delta_table(
    df: &mut DataFrame,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<i64> {
    if let Target::Local { base_path, .. } = target {
        std::fs::create_dir_all(Path::new(base_path))?;
    }
    let batch = dataframe_to_record_batch(df, entity)?;
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
    let version = runtime
        .block_on(async move {
            let table = match builder.load().await {
                Ok(table) => table,
                Err(err) => match err {
                    deltalake::DeltaTableError::NotATable(_) => {
                        let builder = DeltaTableBuilder::from_url(table_url)?
                            .with_storage_options(storage_options);
                        builder.build()?
                    }
                    other => return Err(other),
                },
            };
            let table = table
                .write(vec![batch])
                .with_save_mode(save_mode_for_write_mode(mode))
                .await?;
            let version = table.version().ok_or_else(|| {
                deltalake::DeltaTableError::Generic(
                    "delta table version missing after write".to_string(),
                )
            })?;
            Ok::<i64, deltalake::DeltaTableError>(version)
        })
        .map_err(|err| Box::new(RunError(format!("delta write failed: {err}"))))?;

    Ok(version)
}

impl AcceptedSinkAdapter for DeltaAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        mode: config::WriteMode,
        _output_stem: &str,
        _temp_dir: Option<&Path>,
        _cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let version = write_delta_table(df, target, resolver, entity, mode)?;
        Ok(AcceptedWriteOutput {
            parts_written: 1,
            part_files: Vec::new(),
            table_version: Some(version),
        })
    }
}

fn dataframe_to_record_batch(
    df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<RecordBatch> {
    if entity.schema.columns.is_empty() {
        let mut fields = Vec::with_capacity(df.width());
        let mut arrays = Vec::with_capacity(df.width());
        for column in df.get_columns() {
            let series = column.as_materialized_series();
            let name = series.name().to_string();
            let array = series_to_arrow_array(series)?;
            let nullable = array.null_count() > 0;
            fields.push(Field::new(name, array.data_type().clone(), nullable));
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, arrays).map_err(|err| {
            Box::new(RunError(format!("delta record batch build failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        });
    }

    let schema_columns = if let Some(strategy) = normalize::resolve_normalize_strategy(entity)? {
        normalize::normalize_schema_columns(&entity.schema.columns, &strategy)?
    } else {
        entity.schema.columns.clone()
    };
    let mut fields = Vec::with_capacity(schema_columns.len());
    let mut arrays = Vec::with_capacity(schema_columns.len());
    for column in &schema_columns {
        let series = df
            .column(column.name.as_str())
            .map_err(|err| Box::new(RunError(format!("delta column lookup failed: {err}"))))?;
        let series = series.as_materialized_series();
        let array = series_to_arrow_array(series)?;
        let nullable = column.nullable.unwrap_or(true);
        if !nullable && array.null_count() > 0 {
            return Err(Box::new(RunError(format!(
                "delta write rejected nulls for non-nullable column {}",
                column.name
            ))));
        }
        fields.push(Field::new(
            column.name.clone(),
            array.data_type().clone(),
            nullable,
        ));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        Box::new(RunError(format!("delta record batch build failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    match mode {
        config::WriteMode::Overwrite => SaveMode::Overwrite,
        config::WriteMode::Append => SaveMode::Append,
    }
}

fn series_to_arrow_array(series: &polars::prelude::Series) -> FloeResult<ArrayRef> {
    let array: ArrayRef = match series.dtype() {
        DataType::String => {
            let values = series.str()?;
            Arc::new(StringArray::from_iter(values))
        }
        DataType::Boolean => {
            let values = series.bool()?;
            Arc::new(BooleanArray::from_iter(values))
        }
        DataType::Int8 => {
            let values = series.i8()?;
            Arc::new(Int8Array::from_iter(values))
        }
        DataType::Int16 => {
            let values = series.i16()?;
            Arc::new(Int16Array::from_iter(values))
        }
        DataType::Int32 => {
            let values = series.i32()?;
            Arc::new(Int32Array::from_iter(values))
        }
        DataType::Int64 => {
            let values = series.i64()?;
            Arc::new(Int64Array::from_iter(values))
        }
        DataType::UInt8 => {
            let values = series.u8()?;
            Arc::new(UInt8Array::from_iter(values))
        }
        DataType::UInt16 => {
            let values = series.u16()?;
            Arc::new(UInt16Array::from_iter(values))
        }
        DataType::UInt32 => {
            let values = series.u32()?;
            Arc::new(UInt32Array::from_iter(values))
        }
        DataType::UInt64 => {
            let values = series.u64()?;
            Arc::new(UInt64Array::from_iter(values))
        }
        DataType::Float32 => {
            let values = series.f32()?;
            Arc::new(Float32Array::from_iter(values))
        }
        DataType::Float64 => {
            let values = series.f64()?;
            Arc::new(Float64Array::from_iter(values))
        }
        DataType::Date => {
            let values = series.date()?;
            Arc::new(Date32Array::from_iter(values.phys.iter()))
        }
        DataType::Datetime(unit, _) => {
            let values = series.datetime()?;
            let micros = values.phys.iter().map(|opt| match unit {
                TimeUnit::Milliseconds => opt.map(|value| value.saturating_mul(1000)),
                TimeUnit::Microseconds => opt,
                TimeUnit::Nanoseconds => opt.map(|value| value / 1000),
            });
            Arc::new(TimestampMicrosecondArray::from_iter(micros))
        }
        DataType::Time => {
            let values = series.time()?;
            Arc::new(Time64NanosecondArray::from_iter(values.phys.iter()))
        }
        DataType::Null => Arc::new(NullArray::new(series.len())),
        dtype => {
            return Err(Box::new(RunError(format!(
                "delta sink does not support dtype {dtype:?} for {}",
                series.name()
            ))))
        }
    };
    Ok(array)
}
