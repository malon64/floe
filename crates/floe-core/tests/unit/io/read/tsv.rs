use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config;
use polars::prelude::SerReader;

#[test]
fn tsv_default_separator_parses_tabs() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!("floe-test-tsv-default-{nanos}.tsv"));
    fs::write(&path, "id\tname\n1\talice\n2\tbob\n").expect("write tsv");

    let opts = config::SourceOptions::defaults_for_format("tsv");
    let reader = opts
        .to_csv_read_options(&path)
        .expect("tsv read options")
        .try_into_reader_with_file_path(None)
        .expect("tsv reader");
    let df = reader.finish().expect("read tsv");

    let columns = df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(df.height(), 2);
}
