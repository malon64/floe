use std::path::Path;

use polars::prelude::{DataFrame, ParquetReader, SerReader};

use crate::{ConfigError, FloeResult};

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
