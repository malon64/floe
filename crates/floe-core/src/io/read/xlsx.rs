use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek};
use std::path::Path;

use calamine::{open_workbook, Data, Reader, Xlsx};
use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::errors::IoError;
use crate::io::format::{self, FileReadError, InputAdapter, LocalInputFile, ReadInput};
use crate::{config, FloeResult};

struct XlsxInputAdapter;

static XLSX_INPUT_ADAPTER: XlsxInputAdapter = XlsxInputAdapter;

pub(crate) fn xlsx_input_adapter() -> &'static dyn InputAdapter {
    &XLSX_INPUT_ADAPTER
}

#[derive(Debug, Clone)]
pub struct XlsxReadError {
    pub rule: String,
    pub message: String,
}

impl std::fmt::Display for XlsxReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.rule, self.message)
    }
}

impl std::error::Error for XlsxReadError {}

#[derive(Debug, Clone)]
struct XlsxOptions {
    sheet: Option<String>,
    header_row: u64,
    data_row: u64,
}

impl XlsxOptions {
    fn from_source_options(source_options: Option<&config::SourceOptions>) -> Self {
        let defaults = config::SourceOptions::default();
        let options = source_options.unwrap_or(&defaults);
        let header_row = options.header_row.unwrap_or(1);
        let data_row = options.data_row.unwrap_or(header_row + 1);
        Self {
            sheet: options.sheet.clone(),
            header_row,
            data_row,
        }
    }
}

fn read_xlsx_header(
    input_path: &Path,
    options: &XlsxOptions,
) -> Result<Vec<String>, XlsxReadError> {
    let range = open_sheet_range(input_path, options)?;
    let (_height, width) = range.get_size();
    if width == 0 {
        return Err(XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!("sheet is empty in {}", input_path.display()),
        });
    }
    let header_idx = options.header_row.saturating_sub(1) as usize;
    if header_idx >= range.get_size().0 {
        return Err(XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!(
                "header_row={} is outside sheet bounds in {}",
                options.header_row,
                input_path.display()
            ),
        });
    }
    Ok(extract_header(&range, header_idx, width))
}

fn read_xlsx_file(input_path: &Path, options: &XlsxOptions) -> Result<DataFrame, XlsxReadError> {
    let range = open_sheet_range(input_path, options)?;
    let (height, width) = range.get_size();
    if width == 0 {
        return Err(XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!("sheet is empty in {}", input_path.display()),
        });
    }
    let header_idx = options.header_row.saturating_sub(1) as usize;
    if header_idx >= height {
        return Err(XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!(
                "header_row={} is outside sheet bounds in {}",
                options.header_row,
                input_path.display()
            ),
        });
    }

    let header = extract_header(&range, header_idx, width);
    let data_idx = options.data_row.saturating_sub(1) as usize;
    let row_count = height.saturating_sub(data_idx);
    let mut columns = vec![Vec::with_capacity(row_count); width];

    if data_idx < height {
        for row in data_idx..height {
            for (col, column) in columns.iter_mut().enumerate() {
                let cell = range.get((row, col)).and_then(cell_to_string);
                column.push(cell);
            }
        }
    }

    let mut series = Vec::with_capacity(width);
    for (index, name) in header.iter().enumerate() {
        let values = std::mem::take(&mut columns[index]);
        series.push(Series::new(name.as_str().into(), values).into());
    }

    DataFrame::new(series).map_err(|err| XlsxReadError {
        rule: "xlsx_read_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

fn open_sheet_range(
    input_path: &Path,
    options: &XlsxOptions,
) -> Result<calamine::Range<Data>, XlsxReadError> {
    let mut workbook: Xlsx<_> = open_workbook(input_path).map_err(|err| XlsxReadError {
        rule: "xlsx_read_error".to_string(),
        message: format!("failed to open xlsx at {}: {err}", input_path.display()),
    })?;
    let sheet_name = resolve_sheet_name(&mut workbook, options, input_path)?;
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|err| XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!(
                "failed to read sheet {} in {}: {err}",
                sheet_name,
                input_path.display()
            ),
        })?;
    Ok(range)
}

fn resolve_sheet_name<R: Read + Seek>(
    workbook: &mut Xlsx<R>,
    options: &XlsxOptions,
    input_path: &Path,
) -> Result<String, XlsxReadError> {
    let names = workbook.sheet_names().to_vec();
    if names.is_empty() {
        return Err(XlsxReadError {
            rule: "xlsx_read_error".to_string(),
            message: format!("no sheets found in {}", input_path.display()),
        });
    }
    if let Some(sheet) = options.sheet.as_ref() {
        if names.iter().any(|name| name == sheet) {
            return Ok(sheet.clone());
        }
        return Err(XlsxReadError {
            rule: "xlsx_sheet_error".to_string(),
            message: format!(
                "sheet {} not found in {} (available: {})",
                sheet,
                input_path.display(),
                names.join(", ")
            ),
        });
    }
    Ok(names[0].clone())
}

fn extract_header(range: &calamine::Range<Data>, row: usize, width: usize) -> Vec<String> {
    let mut header = Vec::with_capacity(width);
    for col in 0..width {
        let name = range
            .get((row, col))
            .and_then(cell_to_string)
            .unwrap_or_else(|| format!("column_{}", col + 1));
        let name = name.trim();
        if name.is_empty() {
            header.push(format!("column_{}", col + 1));
        } else {
            header.push(name.to_string());
        }
    }
    // Excel exports often repeat header labels. Polars requires unique column names,
    // so we dedupe here to avoid DataFrame::new errors during read.
    dedupe_header_names(header)
}

fn dedupe_header_names(names: Vec<String>) -> Vec<String> {
    let mut base_counts: HashMap<String, usize> = HashMap::new();
    let mut used: HashSet<String> = HashSet::new();
    let mut deduped = Vec::with_capacity(names.len());

    for name in names {
        let count = base_counts.entry(name.clone()).or_insert(0);
        *count += 1;
        let mut candidate = if *count == 1 {
            name.clone()
        } else {
            format!("{name}_dup_{}", *count)
        };
        while used.contains(&candidate) {
            *count += 1;
            candidate = format!("{name}_dup_{}", *count);
        }
        used.insert(candidate.clone());
        deduped.push(candidate);
    }

    deduped
}

fn cell_to_string(cell: &Data) -> Option<String> {
    match cell {
        Data::Empty => None,
        _ => Some(cell.to_string()),
    }
}

impl InputAdapter for XlsxInputAdapter {
    fn format(&self) -> &'static str {
        "xlsx"
    }

    fn read_input_columns(
        &self,
        entity: &config::EntityConfig,
        input_file: &LocalInputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        let options = XlsxOptions::from_source_options(entity.source.options.as_ref());
        let header =
            read_xlsx_header(&input_file.local_path, &options).map_err(|err| FileReadError {
                rule: err.rule,
                message: err.message,
            })?;
        Ok(header)
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[LocalInputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let options = XlsxOptions::from_source_options(entity.source.options.as_ref());
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let df = read_xlsx_file(path, &options).map_err(|err| {
                Box::new(IoError(err.to_string())) as Box<dyn std::error::Error + Send + Sync>
            })?;
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
