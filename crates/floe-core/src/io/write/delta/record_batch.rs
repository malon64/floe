use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use polars::prelude::DataFrame;

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::write::arrow_convert::{self, ArrowConversionOptions, ArrowTimeEncoding};
use crate::{config, FloeResult};

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

pub(crate) fn dataframe_to_record_batch_with_schema(
    df: &DataFrame,
    schema_columns: &[config::ColumnConfig],
) -> FloeResult<RecordBatch> {
    let mut fields = Vec::with_capacity(schema_columns.len());
    let mut arrays = Vec::with_capacity(schema_columns.len());
    for column in schema_columns {
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

pub(crate) fn dataframe_to_record_batch_all(df: &DataFrame) -> FloeResult<RecordBatch> {
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
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        Box::new(RunError(format!("delta record batch build failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

pub(crate) fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    match mode {
        config::WriteMode::Overwrite => SaveMode::Overwrite,
        config::WriteMode::Append => SaveMode::Append,
        config::WriteMode::MergeScd1 => SaveMode::Overwrite,
        config::WriteMode::MergeScd2 => SaveMode::Overwrite,
    }
}

fn series_to_arrow_array(series: &polars::prelude::Series) -> FloeResult<ArrayRef> {
    arrow_convert::series_to_arrow_array(
        series,
        ArrowConversionOptions {
            upcast_i8_i16_to_i32: false,
            time_encoding: ArrowTimeEncoding::Nanoseconds,
        },
        |dtype| {
            RunError(format!(
                "delta sink does not support dtype {dtype:?} for {}",
                series.name()
            ))
        },
    )
}
