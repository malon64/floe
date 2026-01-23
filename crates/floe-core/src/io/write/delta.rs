use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use polars::prelude::{DataFrame, DataType, TimeUnit};

use crate::io::format::{AcceptedSinkAdapter, StorageTarget};
use crate::{config, io, ConfigError, FloeResult};

struct DeltaAcceptedAdapter;

static DELTA_ACCEPTED_ADAPTER: DeltaAcceptedAdapter = DeltaAcceptedAdapter;

pub(crate) fn delta_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &DELTA_ACCEPTED_ADAPTER
}

pub fn write_delta_table(df: &mut DataFrame, base_path: &str) -> FloeResult<PathBuf> {
    let table_path = Path::new(base_path).to_path_buf();
    std::fs::create_dir_all(&table_path)?;

    let batch = dataframe_to_record_batch(df)?;
    let table_uri = table_path.display().to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(ConfigError(format!("delta runtime init failed: {err}"))))?;
    runtime
        .block_on(async move {
            let ops = DeltaOps::try_from_uri(&table_uri).await?;
            ops.write(vec![batch])
                .with_save_mode(SaveMode::Overwrite)
                .await?;
            Ok::<(), deltalake::DeltaTableError>(())
        })
        .map_err(|err| Box::new(ConfigError(format!("delta write failed: {err}"))))?;

    Ok(table_path)
}

impl AcceptedSinkAdapter for DeltaAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &StorageTarget,
        df: &mut DataFrame,
        _source_stem: &str,
        _temp_dir: Option<&Path>,
        _s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        _resolver: &config::FilesystemResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        match target {
            StorageTarget::Local { base_path } => {
                let output_path = write_delta_table(df, base_path)?;
                Ok(output_path.display().to_string())
            }
            StorageTarget::S3 { .. } => Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.format=delta is only supported on local filesystem",
                entity.name
            )))),
        }
    }
}

fn dataframe_to_record_batch(df: &DataFrame) -> FloeResult<RecordBatch> {
    let mut columns = Vec::with_capacity(df.width());
    for column in df.get_columns() {
        let series = column.as_materialized_series();
        let name = series.name().to_string();
        let array = series_to_arrow_array(series)?;
        columns.push((name, array));
    }
    Ok(RecordBatch::try_from_iter(columns).map_err(|err| {
        Box::new(ConfigError(format!(
            "delta record batch build failed: {err}"
        )))
    })?)
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
            return Err(Box::new(ConfigError(format!(
                "delta sink does not support dtype {dtype:?} for {}",
                series.name()
            ))))
        }
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{df, ParquetReader, SerReader};

    #[test]
    fn write_delta_table_overwrite() -> FloeResult<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let table_path = temp_dir.path().join("delta_table");
        let mut df = df!(
            "id" => &[1i64, 2, 3],
            "name" => &["a", "b", "c"]
        )?;

        write_delta_table(&mut df, table_path.to_str().unwrap())?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "delta test runtime init failed: {err}"
                )))
            })?;
        let table = runtime
            .block_on(async { deltalake::open_table(table_path.to_str().unwrap()).await })
            .map_err(|err| Box::new(ConfigError(format!("delta test open failed: {err}"))))?;

        let schema = table.get_schema()?;
        let field_names = schema
            .fields()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert!(field_names.contains(&"id".to_string()));

        let mut row_count = 0usize;
        for uri in table.get_file_uris()? {
            let file = std::fs::File::open(&uri)?;
            let df_read = ParquetReader::new(file).finish()?;
            row_count += df_read.height();
        }
        assert_eq!(row_count, df.height());

        Ok(())
    }
}
