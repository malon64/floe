use std::path::{Path, PathBuf};

use polars::prelude::{DataFrame, Schema, SerReader};

use crate::{config, ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct CsvReadPlan {
    pub schema: Schema,
    pub ignore_errors: bool,
}

impl CsvReadPlan {
    pub fn strict(schema: Schema) -> Self {
        Self {
            schema,
            ignore_errors: false,
        }
    }

    pub fn permissive(schema: Schema) -> Self {
        Self {
            schema,
            ignore_errors: true,
        }
    }
}

pub fn list_csv_files(input_dir: &Path) -> FloeResult<Vec<PathBuf>> {
    if !input_dir.is_dir() {
        return Err(Box::new(ConfigError(format!(
            "source path is not a directory: {}",
            input_dir.display()
        ))));
    }

    let mut files = Vec::new();
    for entry in std::fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("csv"))
            != Some(true)
        {
            continue;
        }
        files.push(path);
    }

    if files.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "no csv files found in directory {}",
            input_dir.display()
        ))));
    }

    files.sort();
    Ok(files)
}

pub fn read_csv_file(
    input_path: &Path,
    source_options: &config::SourceOptions,
    plan: &CsvReadPlan,
) -> FloeResult<DataFrame> {
    let read_options = source_options
        .to_csv_read_options(input_path)?
        .with_schema(Some(std::sync::Arc::new(plan.schema.clone())))
        .with_ignore_errors(plan.ignore_errors);
    let reader = read_options
        .try_into_reader_with_file_path(None)
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to open csv at {}: {err}",
                input_path.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    let df = reader
        .finish()
        .map_err(|err| {
            Box::new(ConfigError(format!("csv read failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        path.push(format!("{prefix}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn list_csv_files_filters_and_sorts() {
        let dir = temp_dir("floe-csv-list");
        fs::write(dir.join("b.csv"), "id\n1\n").expect("write b.csv");
        fs::write(dir.join("a.CSV"), "id\n2\n").expect("write a.CSV");
        fs::write(dir.join("note.txt"), "ignore").expect("write note");

        let files = list_csv_files(&dir).expect("list csv files");
        let names = files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["a.CSV", "b.csv"]);
    }

    #[test]
    fn list_csv_files_errors_when_empty() {
        let dir = temp_dir("floe-csv-empty");
        let result = list_csv_files(&dir);
        assert!(result.is_err());
    }
}
