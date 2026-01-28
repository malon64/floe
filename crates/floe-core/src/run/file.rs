use polars::prelude::DataFrame;

use crate::{check, config, io, FloeResult};

use io::format::{InputAdapter, InputFile, ReadInput};

pub(super) type ValidationCollect = io::format::ValidationCollect;

pub(super) fn required_columns(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}

pub(super) fn read_inputs(
    adapter: &dyn InputAdapter,
    entity: &config::EntityConfig,
    files: &[InputFile],
    columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
    collect_raw: bool,
) -> FloeResult<Vec<ReadInput>> {
    adapter.read_inputs(entity, files, columns, normalize_strategy, collect_raw)
}

pub(super) fn collect_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
    raw_indices: &check::ColumnIndex,
    typed_indices: &check::ColumnIndex,
    formatter: &dyn check::RowErrorFormatter,
) -> FloeResult<ValidationCollect> {
    io::format::collect_errors(
        raw_df,
        typed_df,
        required_cols,
        columns,
        track_cast_errors,
        raw_indices,
        typed_indices,
        formatter,
    )
}
