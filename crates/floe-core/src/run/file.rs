use polars::prelude::DataFrame;

use crate::{check, config, io, FloeResult};

pub(super) fn required_columns(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}

pub(super) fn collect_row_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
    raw_indices: &check::ColumnIndex,
    typed_indices: &check::ColumnIndex,
) -> FloeResult<Vec<Vec<check::RowError>>> {
    io::format::collect_row_errors(
        raw_df,
        typed_df,
        required_cols,
        columns,
        track_cast_errors,
        raw_indices,
        typed_indices,
    )
}
