use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use polars::prelude::{DataFrame, DataType, Series};

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::write::arrow_convert::{self, ArrowConversionOptions, ArrowTimeEncoding};
use crate::{config, FloeResult};

use super::{map_iceberg_err, PreparedIcebergWrite};

pub(super) fn prepare_iceberg_write(
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
    let partition_spec = build_unbound_partition_spec(&iceberg_schema, entity)?;
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
        partition_spec,
        batch,
    })
}

fn build_unbound_partition_spec(
    iceberg_schema: &Schema,
    entity: &config::EntityConfig,
) -> FloeResult<Option<UnboundPartitionSpec>> {
    let Some(fields) = entity.sink.accepted.partition_spec.as_ref() else {
        return Ok(None);
    };

    let mut builder = UnboundPartitionSpec::builder();
    for field in fields {
        let column = field.column.trim();
        let schema_field = iceberg_schema.field_by_name(column).ok_or_else(|| {
            Box::new(RunError(format!(
                "entity.name={} iceberg partition_spec column {} was not found in runtime schema",
                entity.name, column
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let normalized_transform = field.transform.trim().to_ascii_lowercase();
        let transform = iceberg_partition_transform(&normalized_transform, &entity.name, column)?;
        let partition_field_name = iceberg_partition_field_name(column, &normalized_transform);
        builder = builder
            .add_partition_field(schema_field.id, partition_field_name, transform)
            .map_err(map_iceberg_err("iceberg partition spec build failed"))?;
    }

    Ok(Some(builder.build()))
}

fn iceberg_partition_transform(
    transform: &str,
    entity_name: &str,
    column: &str,
) -> FloeResult<Transform> {
    let iceberg_transform = match transform {
        "identity" => Transform::Identity,
        "year" => Transform::Year,
        "month" => Transform::Month,
        "day" => Transform::Day,
        "hour" => Transform::Hour,
        _ => {
            return Err(Box::new(RunError(format!(
            "entity.name={} iceberg partition_spec column {} has unsupported runtime transform {}",
            entity_name, column, transform
        ))))
        }
    };
    Ok(iceberg_transform)
}

fn iceberg_partition_field_name(column: &str, transform: &str) -> String {
    if transform == "identity" {
        column.to_string()
    } else {
        format!("{column}_{transform}")
    }
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
    arrow_convert::series_to_arrow_array(
        series,
        ArrowConversionOptions {
            upcast_i8_i16_to_i32: true,
            time_encoding: ArrowTimeEncoding::Microseconds,
        },
        |dtype| {
            RunError(format!(
                "iceberg sink supports scalar types only; unsupported dtype {dtype:?} for column {} (entity {})",
                column_name, entity_name
            ))
        },
    )
}

pub(super) fn ensure_schema_matches(
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

pub(super) fn ensure_partition_spec_matches(
    existing: &iceberg::spec::PartitionSpec,
    expected: Option<&UnboundPartitionSpec>,
    expected_schema: &Schema,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    let Some(expected_unbound) = expected else {
        return Ok(());
    };

    let expected_bound = expected_unbound
        .clone()
        .bind(Arc::new(expected_schema.clone()))
        .map_err(map_iceberg_err("iceberg partition spec bind failed"))?;

    if !existing.is_compatible_with(&expected_bound) {
        return Err(Box::new(RunError(format!(
            "entity.name={} iceberg partition spec evolution is not supported (existing={} incoming={})",
            entity.name,
            describe_partition_spec(existing),
            describe_partition_spec(&expected_bound)
        ))));
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

fn describe_partition_spec(spec: &iceberg::spec::PartitionSpec) -> String {
    if spec.fields().is_empty() {
        return "unpartitioned".to_string();
    }

    spec.fields()
        .iter()
        .map(|field| format!("{}:{}:{:?}", field.source_id, field.name, field.transform))
        .collect::<Vec<_>>()
        .join(",")
}
