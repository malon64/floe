use std::path::Path;

use polars::prelude::{DataFrame, ParquetReader, SerReader};

use crate::io::format::{self, InputAdapter, InputFile, ReadInput};
use crate::{config, ConfigError, FloeResult};

struct ParquetInputAdapter;

static PARQUET_INPUT_ADAPTER: ParquetInputAdapter = ParquetInputAdapter;

pub(crate) fn parquet_input_adapter() -> &'static dyn InputAdapter {
    &PARQUET_INPUT_ADAPTER
}

pub fn read_parquet_file(input_path: &Path) -> FloeResult<DataFrame> {
    let file = std::fs::File::open(input_path).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to open parquet at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let df = ParquetReader::new(file).finish().map_err(|err| {
        Box::new(ConfigError(format!("parquet read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(df)
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
            let df = read_parquet_file(path)?;
            let input = format::read_input_from_df(
                input_file,
                &df,
                columns,
                normalize_strategy,
                collect_raw,
            )?;
            inputs.push(input);
        }
        Ok(inputs)
    }
}
