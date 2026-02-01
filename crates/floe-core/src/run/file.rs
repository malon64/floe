use crate::config;

pub(super) fn required_columns(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}
