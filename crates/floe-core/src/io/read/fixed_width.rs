use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::io::format::{self, FileReadError, InputAdapter, InputFile, ReadInput};
use crate::{config, FloeResult};

struct FixedWidthInputAdapter;

static FIXED_WIDTH_INPUT_ADAPTER: FixedWidthInputAdapter = FixedWidthInputAdapter;

pub(crate) fn fixed_width_input_adapter() -> &'static dyn InputAdapter {
    &FIXED_WIDTH_INPUT_ADAPTER
}

#[derive(Debug, Clone)]
struct FixedWidthColumnSpec {
    name: String,
    width: usize,
    trim: bool,
}

#[derive(Debug, Clone)]
struct FixedWidthReadError {
    rule: String,
    message: String,
}

impl InputAdapter for FixedWidthInputAdapter {
    fn format(&self) -> &'static str {
        "fixed"
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        _input_file: &InputFile,
        columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        let specs = build_specs(columns).map_err(|err| FileReadError {
            rule: err.rule,
            message: err.message,
        })?;
        Ok(specs.iter().map(|spec| spec.name.clone()).collect())
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
            let path = &input_file.source_local_path;
            match read_fixed_width_file(path, columns) {
                Ok(df) => {
                    let input = format::read_input_from_df(
                        input_file,
                        &df,
                        columns,
                        normalize_strategy,
                        collect_raw,
                    )?;
                    inputs.push(input);
                }
                Err(err) => {
                    inputs.push(ReadInput::FileError {
                        input_file: input_file.clone(),
                        error: FileReadError {
                            rule: err.rule,
                            message: err.message,
                        },
                    });
                }
            }
        }
        Ok(inputs)
    }
}

fn build_specs(
    columns: &[config::ColumnConfig],
) -> Result<Vec<FixedWidthColumnSpec>, FixedWidthReadError> {
    let mut specs = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let width = column.width.ok_or_else(|| FixedWidthReadError {
            rule: "fixed_width_schema".to_string(),
            message: format!(
                "schema.columns[{}].width is required for fixed-width sources",
                index
            ),
        })?;
        if width == 0 {
            return Err(FixedWidthReadError {
                rule: "fixed_width_schema".to_string(),
                message: format!("schema.columns[{}].width must be greater than 0", index),
            });
        }
        let width = usize::try_from(width).map_err(|_| FixedWidthReadError {
            rule: "fixed_width_schema".to_string(),
            message: format!(
                "schema.columns[{}].width is too large for this platform",
                index
            ),
        })?;
        specs.push(FixedWidthColumnSpec {
            name: column.name.clone(),
            width,
            trim: column.trim.unwrap_or(true),
        });
    }
    Ok(specs)
}

fn read_fixed_width_file(
    input_path: &Path,
    columns: &[config::ColumnConfig],
) -> Result<DataFrame, FixedWidthReadError> {
    let specs = build_specs(columns)?;
    let total_width = specs.iter().map(|spec| spec.width).sum::<usize>();

    let file = File::open(input_path).map_err(|err| FixedWidthReadError {
        rule: "fixed_width_open_error".to_string(),
        message: format!(
            "failed to open fixed-width file at {}: {err}",
            input_path.display()
        ),
    })?;
    let reader = BufReader::new(file);

    let mut columns_data: Vec<Vec<Option<String>>> = specs.iter().map(|_| Vec::new()).collect();

    for (line_index, line_result) in reader.lines().enumerate() {
        let mut line = line_result.map_err(|err| FixedWidthReadError {
            rule: "fixed_width_read_error".to_string(),
            message: format!("failed to read fixed-width line {}: {err}", line_index + 1),
        })?;
        if line.ends_with('\r') {
            line.pop();
        }
        let char_offsets = char_offsets(&line);
        let line_len_chars = char_offsets.len().saturating_sub(1);
        if line_len_chars < total_width {
            return Err(FixedWidthReadError {
                rule: "fixed_width_line_length".to_string(),
                message: format!(
                    "line {} has length {} shorter than expected {}",
                    line_index + 1,
                    line_len_chars,
                    total_width
                ),
            });
        }
        if line_len_chars > total_width
            && line
                .chars()
                .skip(total_width)
                .any(|value| !value.is_whitespace())
        {
            return Err(FixedWidthReadError {
                rule: "fixed_width_line_length".to_string(),
                message: format!(
                    "line {} has length {} longer than expected {}",
                    line_index + 1,
                    line_len_chars,
                    total_width
                ),
            });
        }

        let mut start = 0usize;
        for (idx, spec) in specs.iter().enumerate() {
            let end = start + spec.width;
            let slice = slice_chars(&line, &char_offsets, start, end).ok_or_else(|| {
                FixedWidthReadError {
                    rule: "fixed_width_line_length".to_string(),
                    message: format!(
                        "line {} has length {} shorter than expected {}",
                        line_index + 1,
                        line_len_chars,
                        total_width
                    ),
                }
            })?;
            let raw = slice;
            let value = if spec.trim { raw.trim() } else { raw };
            let value = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
            columns_data[idx].push(value);
            start = end;
        }
    }

    let mut series = Vec::with_capacity(specs.len());
    for (spec, values) in specs.iter().zip(columns_data) {
        series.push(Series::new(spec.name.as_str().into(), values).into());
    }

    DataFrame::new(series).map_err(|err| FixedWidthReadError {
        rule: "fixed_width_parse_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

fn char_offsets(value: &str) -> Vec<usize> {
    let mut offsets = Vec::with_capacity(value.chars().count() + 1);
    for (idx, _) in value.char_indices() {
        offsets.push(idx);
    }
    offsets.push(value.len());
    offsets
}

fn slice_chars<'a>(value: &'a str, offsets: &[usize], start: usize, end: usize) -> Option<&'a str> {
    if start > end || end >= offsets.len() {
        return None;
    }
    let start_byte = *offsets.get(start)?;
    let end_byte = *offsets.get(end)?;
    value.get(start_byte..end_byte)
}
