use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config;
use polars::prelude::SerReader;

#[test]
fn csv_empty_fields_are_null_by_default() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!("floe-test-csv-null-default-{nanos}.csv"));
    fs::write(&path, "id;name\n1;alice\n2;\n").expect("write csv");

    let opts = config::SourceOptions::default();
    let reader = opts
        .to_csv_read_options(&path)
        .expect("csv read options")
        .try_into_reader_with_file_path(None)
        .expect("csv reader");
    let df = reader.finish().expect("read csv");

    let name = df.column("name").expect("name column");
    assert_eq!(name.null_count(), 1);
}
