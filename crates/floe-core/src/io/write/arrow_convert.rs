use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use polars::prelude::{DataType, Series, TimeUnit};

use crate::errors::RunError;
use crate::FloeResult;

#[derive(Debug, Clone, Copy)]
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
