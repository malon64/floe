use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use deltalake::arrow::datatypes::{Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use polars::prelude::{DataFrame, DataType, TimeUnit};

use crate::checks::normalize;
use crate::errors::RunError;
use crate::{config, FloeResult};

pub(super) fn dataframe_to_record_batch(
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

    let schema_columns = normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize::resolve_normalize_strategy(entity)?.as_deref(),
    );
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

pub(super) fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    match mode {
        config::WriteMode::Overwrite => SaveMode::Overwrite,
        config::WriteMode::Append => SaveMode::Append,
        config::WriteMode::MergeScd1 => SaveMode::Overwrite,
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
