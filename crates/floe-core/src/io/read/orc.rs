use std::fs::File;
use std::path::Path;

use arrow::array::new_empty_array;
use arrow::record_batch::RecordBatch;
use df_interchange::Interchange;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use polars::prelude::DataFrame;

use crate::errors::IoError;
use crate::io::format::{self, FileReadError, InputAdapter, LocalInputFile, ReadInput};
use crate::{config, FloeResult};

struct OrcInputAdapter;

static ORC_INPUT_ADAPTER: OrcInputAdapter = OrcInputAdapter;

pub(crate) fn orc_input_adapter() -> &'static dyn InputAdapter {
    &ORC_INPUT_ADAPTER
}

pub fn read_orc_schema_names(input_path: &Path) -> FloeResult<Vec<String>> {
    let file = File::open(input_path).map_err(|err| {
        Box::new(IoError(format!(
            "failed to open orc at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let builder = ArrowReaderBuilder::try_new(file).map_err(|err| {
        Box::new(IoError(format!(
            "failed to read orc schema at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let schema = builder.schema();
    Ok(schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect())
}

fn read_orc_batches(
    input_path: &Path,
    projection: Option<&[String]>,
) -> FloeResult<Vec<RecordBatch>> {
    let file = File::open(input_path).map_err(|err| {
        Box::new(IoError(format!(
            "failed to open orc at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut builder = ArrowReaderBuilder::try_new(file).map_err(|err| {
        Box::new(IoError(format!(
            "failed to build orc reader at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if let Some(columns) = projection {
        let roots = builder.file_metadata().root_data_type();
        let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let mask = ProjectionMask::named_roots(roots, &names);
        builder = builder.with_projection(mask);
    }
    let schema = builder.schema();
    let reader = builder.build();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|err| {
        Box::new(IoError(format!("orc read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    if batches.is_empty() {
        let arrays = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema, arrays).map_err(|err| {
            Box::new(IoError(format!("orc read failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;
        return Ok(vec![batch]);
    }
    Ok(batches)
}

pub fn read_orc_df(input_path: &Path, projection: Option<&[String]>) -> FloeResult<DataFrame> {
    let batches = read_orc_batches(input_path, projection)?;
    if batches.is_empty() {
        return Ok(DataFrame::default());
    }
    let interchange = Interchange::from_arrow_57(batches).map_err(|err| {
        Box::new(IoError(format!("orc conversion failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    interchange.to_polars_0_52().map_err(|err| {
        Box::new(IoError(format!("orc conversion failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

impl InputAdapter for OrcInputAdapter {
    fn format(&self) -> &'static str {
        "orc"
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        input_file: &LocalInputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        read_orc_schema_names(&input_file.local_path).map_err(|err| FileReadError {
            rule: "orc_read_error".to_string(),
            message: err.to_string(),
        })
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        files: &[LocalInputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let input_columns = read_orc_schema_names(path)?;
            let projection = projected_columns(&input_columns, columns);
            let df = read_orc_df(path, projection.as_deref())?;
            let typed_schema = format::build_typed_schema(
                projection.as_deref().unwrap_or(input_columns.as_slice()),
                columns,
                normalize_strategy,
            )?;
            let raw_df = if collect_raw {
                Some(format::cast_df_to_string(&df)?)
            } else {
                None
            };
            let typed_df = format::cast_df_to_schema(&df, &typed_schema)?;
            let input =
                format::finalize_read_input(input_file, raw_df, typed_df, normalize_strategy)?;
            inputs.push(input);
        }
        Ok(inputs)
    }
}

fn projected_columns(
    input_columns: &[String],
    declared_columns: &[config::ColumnConfig],
) -> Option<Vec<String>> {
    let declared = declared_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    let projected = input_columns
        .iter()
        .filter(|name| declared.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if projected.is_empty() || projected.len() == input_columns.len() {
        None
    } else {
        Some(projected)
    }
}
