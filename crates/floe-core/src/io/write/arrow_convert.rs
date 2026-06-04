use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
#[cfg(any(feature = "delta", feature = "duckdb"))]
use arrow::datatypes::{Field, Schema};
#[cfg(any(feature = "delta", feature = "duckdb"))]
use arrow::record_batch::RecordBatch;
#[cfg(any(feature = "delta", feature = "duckdb"))]
use polars::prelude::DataFrame;
use polars::prelude::{DataType, Series, TimeUnit};

#[cfg(any(feature = "delta", feature = "duckdb"))]
use crate::checks::normalize;
#[cfg(any(feature = "delta", feature = "duckdb"))]
use crate::config;
use crate::errors::RunError;
use crate::FloeResult;

/// Convert a Polars `DataFrame` into an Arrow `RecordBatch`, honoring the entity's
/// declared output schema when present (column selection, renaming, nullability).
/// Shared by the Delta and DuckDB sinks; gated to those features.
#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn dataframe_to_record_batch(
    df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<RecordBatch> {
    if entity.schema.columns.is_empty() {
        return dataframe_to_record_batch_all(df);
    }

    let schema_columns = normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize::resolve_normalize_strategy(entity)?.as_deref(),
    );
    dataframe_to_record_batch_with_schema(df, &schema_columns)
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn dataframe_to_record_batch_with_schema(
    df: &DataFrame,
    schema_columns: &[config::ColumnConfig],
) -> FloeResult<RecordBatch> {
    let mut fields = Vec::with_capacity(schema_columns.len());
    let mut arrays = Vec::with_capacity(schema_columns.len());
    for column in schema_columns {
        let series = df
            .column(column.name.as_str())
            .map_err(|err| Box::new(RunError(format!("column lookup failed: {err}"))))?;
        let series = series.as_materialized_series();
        let array = record_batch_series_to_arrow(series)?;
        let nullable = column.nullable.unwrap_or(true);
        if !nullable && array.null_count() > 0 {
            return Err(Box::new(RunError(format!(
                "write rejected nulls for non-nullable column {}",
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
        Box::new(RunError(format!("record batch build failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn dataframe_to_record_batch_all(df: &DataFrame) -> FloeResult<RecordBatch> {
    let mut fields = Vec::with_capacity(df.width());
    let mut arrays = Vec::with_capacity(df.width());
    for column in df.get_columns() {
        let series = column.as_materialized_series();
        let name = series.name().to_string();
        let array = record_batch_series_to_arrow(series)?;
        let nullable = array.null_count() > 0;
        fields.push(Field::new(name, array.data_type().clone(), nullable));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        Box::new(RunError(format!("record batch build failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
fn record_batch_series_to_arrow(series: &Series) -> FloeResult<ArrayRef> {
    series_to_arrow_array(
        series,
        ArrowConversionOptions {
            upcast_i8_i16_to_i32: false,
            time_encoding: ArrowTimeEncoding::Nanoseconds,
        },
        |dtype| {
            RunError(format!(
                "sink does not support dtype {dtype:?} for {}",
                series.name()
            ))
        },
    )
}

// Each variant is used by a different sink (Nanoseconds → Delta/DuckDB record
// batches, Microseconds → Iceberg), so single-feature builds legitimately leave
// one variant unconstructed.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    not(all(any(feature = "delta", feature = "duckdb"), feature = "iceberg")),
    allow(dead_code)
)]
pub(crate) enum ArrowTimeEncoding {
    Nanoseconds,
    Microseconds,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrowConversionOptions {
    pub(crate) upcast_i8_i16_to_i32: bool,
    pub(crate) time_encoding: ArrowTimeEncoding,
}

pub(crate) fn series_to_arrow_array<F>(
    series: &Series,
    options: ArrowConversionOptions,
    unsupported_dtype: F,
) -> FloeResult<ArrayRef>
where
    F: Fn(&DataType) -> RunError,
{
    let array: ArrayRef = match series.dtype() {
        DataType::String => Arc::new(StringArray::from_iter(series.str()?)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(series.bool()?)),
        DataType::Int8 => {
            let values = series.i8()?;
            if options.upcast_i8_i16_to_i32 {
                Arc::new(Int32Array::from_iter(
                    values.into_iter().map(|opt| opt.map(i32::from)),
                ))
            } else {
                Arc::new(Int8Array::from_iter(values))
            }
        }
        DataType::Int16 => {
            let values = series.i16()?;
            if options.upcast_i8_i16_to_i32 {
                Arc::new(Int32Array::from_iter(
                    values.into_iter().map(|opt| opt.map(i32::from)),
                ))
            } else {
                Arc::new(Int16Array::from_iter(values))
            }
        }
        DataType::Int32 => Arc::new(Int32Array::from_iter(series.i32()?)),
        DataType::Int64 => Arc::new(Int64Array::from_iter(series.i64()?)),
        DataType::UInt8 => Arc::new(UInt8Array::from_iter(series.u8()?)),
        DataType::UInt16 => Arc::new(UInt16Array::from_iter(series.u16()?)),
        DataType::UInt32 => Arc::new(UInt32Array::from_iter(series.u32()?)),
        DataType::UInt64 => Arc::new(UInt64Array::from_iter(series.u64()?)),
        DataType::Float32 => Arc::new(Float32Array::from_iter(series.f32()?)),
        DataType::Float64 => Arc::new(Float64Array::from_iter(series.f64()?)),
        DataType::Date => {
            let values = series.date()?;
            Arc::new(Date32Array::from_iter(values.phys.iter()))
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
        DataType::Time => {
            let values = series.time()?;
            match options.time_encoding {
                ArrowTimeEncoding::Nanoseconds => {
                    Arc::new(Time64NanosecondArray::from_iter(values.phys.iter()))
                }
                ArrowTimeEncoding::Microseconds => {
                    let micros = values.phys.iter().map(|opt| opt.map(|value| value / 1_000));
                    Arc::new(Time64MicrosecondArray::from_iter(micros))
                }
            }
        }
        DataType::Null => Arc::new(NullArray::new(series.len())),
        dtype => return Err(Box::new(unsupported_dtype(dtype))),
    };
    Ok(array)
}
