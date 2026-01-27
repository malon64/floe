use std::path::Path;

use polars::prelude::{col, DataFrame, LazyFrame, ParquetReader, PlPath, SerReader};

use crate::io::format::{self, InputAdapter, InputFile, ReadInput};
use crate::{config, ConfigError, FloeResult};

struct ParquetInputAdapter;

static PARQUET_INPUT_ADAPTER: ParquetInputAdapter = ParquetInputAdapter;

pub(crate) fn parquet_input_adapter() -> &'static dyn InputAdapter {
    &PARQUET_INPUT_ADAPTER
}

pub fn read_parquet_schema_names(input_path: &Path) -> FloeResult<Vec<String>> {
    let file = std::fs::File::open(input_path).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to open parquet at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut reader = ParquetReader::new(file);
    let schema = reader.schema().map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to read parquet schema at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(schema.iter().map(|(name, _)| name.to_string()).collect())
}

pub fn read_parquet_lazy(
    input_path: &Path,
    projection: Option<&[String]>,
) -> FloeResult<DataFrame> {
    let path_str = input_path.to_string_lossy();
    let mut lf = LazyFrame::scan_parquet(PlPath::new(path_str.as_ref()), Default::default())
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to scan parquet at {}: {err}",
                input_path.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    if let Some(columns) = projection {
        let exprs = columns.iter().map(col).collect::<Vec<_>>();
        lf = lf.select(exprs);
    }
    lf.collect().map_err(|err| {
        Box::new(ConfigError(format!("parquet read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

impl InputAdapter for ParquetInputAdapter {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let input_columns = read_parquet_schema_names(path)?;
            let projection = projected_columns(&input_columns, columns);
            let df = read_parquet_lazy(path, projection.as_deref())?;
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
            let input = format::finalize_read_input(
                input_file,
                input_columns,
                raw_df,
                typed_df,
                normalize_strategy,
            )?;
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
