use std::fs;
use std::path::{Path, PathBuf};

// Reach the DuckDB driver through floe-core's gated re-export instead of a
// dev-dependency: a non-optional `duckdb` dev-dep would compile the bundled C++
// amalgamation for every `cargo test` run, defeating the feature gate.
use floe_core::duckdb_driver::Connection;
use floe_core::io::write::duckdb::close_cached_connections;
use floe_core::{run, RunOptions, RunOutcome};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

/// Open a read-back connection to a local `.duckdb` file. The sink keeps its
/// write connection cached (DuckDB is single-writer), so callers must release it
/// before opening a fresh handle in-process.
fn open_readback(path: &Path) -> Connection {
    close_cached_connections();
    Connection::open(path).expect("open duckdb for read-back")
}

fn count_rows(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .expect("count rows")
}

fn run_once(config_path: &Path, run_id: &str) -> RunOutcome {
    run(
        config_path,
        RunOptions {
            profile: None,
            run_id: Some(run_id.to_string()),
            entities: Vec::new(),
            dry_run: false,
            full_refresh: false,
        },
    )
    .unwrap_or_else(|err| panic!("run {run_id} failed: {err}"))
}

#[test]
fn duckdb_overwrite_then_read_back() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n2;ca\n3;us\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "overwrite"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: orders
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run_once(&config_path, "it-duckdb-overwrite");
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "duckdb");
    assert_eq!(report.accepted_output.parts_written, 1);

    let conn = open_readback(&db_path);
    assert_eq!(count_rows(&conn, "main.orders"), 3);
    let countries: i64 = conn
        .query_row(
            "SELECT count(*) FROM main.orders WHERE country = 'us'",
            [],
            |row| row.get(0),
        )
        .expect("count us");
    assert_eq!(countries, 2);
}

#[test]
fn duckdb_overwrite_replaces_only_target_table() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n");

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "overwrite"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: orders
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
"#,
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    run_once(&config_path, "it-duckdb-coexist-1");

    // Create an unrelated table in the same database.
    {
        let conn = open_readback(&db_path);
        conn.execute_batch("CREATE TABLE other AS SELECT 42 AS x")
            .expect("create other table");
    }

    // A second overwrite run must replace only `orders`, leaving `other` intact.
    run_once(&config_path, "it-duckdb-coexist-2");

    let conn = open_readback(&db_path);
    assert_eq!(count_rows(&conn, "main.orders"), 1);
    assert_eq!(count_rows(&conn, "main.other"), 1);
}

#[test]
fn duckdb_append_accumulates_across_runs() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");

    fs::create_dir_all(&input_dir).expect("create input dir");

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "events"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "append"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: events
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    write_csv(&input_dir, "batch1.csv", "id\n1\n2\n");
    run_once(&config_path, "it-duckdb-append-1");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id\n3\n4\n5\n");
    run_once(&config_path, "it-duckdb-append-2");

    let conn = open_readback(&db_path);
    assert_eq!(count_rows(&conn, "main.events"), 5);
}

#[test]
fn duckdb_scd1_upsert() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");

    fs::create_dir_all(&input_dir).expect("create input dir");

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: customer
    policy:
      severity: "warn"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name\n1;fr;alice\n2;ca;bob\n",
    );
    run_once(&config_path, "it-duckdb-scd1-init");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name\n1;fr;alice-updated\n3;us;carol\n",
    );
    let outcome = run_once(&config_path, "it-duckdb-scd1-upsert");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.write_mode.as_deref(),
        Some("merge_scd1")
    );
    assert_eq!(report.accepted_output.updated_count, Some(1));
    assert_eq!(report.accepted_output.inserted_count, Some(1));
    assert_eq!(report.accepted_output.target_rows_before, Some(2));
    assert_eq!(report.accepted_output.target_rows_after, Some(3));

    let conn = open_readback(&db_path);
    assert_eq!(count_rows(&conn, "main.customer"), 3);
    let name: String = conn
        .query_row(
            "SELECT name FROM main.customer WHERE id = '1' AND country = 'fr'",
            [],
            |row| row.get(0),
        )
        .expect("read updated name");
    assert_eq!(name, "alice-updated");
}

#[test]
fn duckdb_scd1_upsert_with_normalized_primary_key() {
    // Regression: when `schema.normalize_columns` renames the primary key, the
    // accepted DataFrame is already renamed (e.g. "Customer ID" -> customer_id), so
    // the MERGE predicate must compare against the *output* column name. Otherwise a
    // second merge_scd1 run references s."Customer ID" and fails instead of upserting.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");

    fs::create_dir_all(&input_dir).expect("create input dir");

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: customer
    policy:
      severity: "warn"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      primary_key: ["Customer ID"]
      columns:
        - name: "Customer ID"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    write_csv(
        &input_dir,
        "batch1.csv",
        "Customer ID;name\n1;alice\n2;bob\n",
    );
    run_once(&config_path, "it-duckdb-scd1-norm-init");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(
        &input_dir,
        "batch2.csv",
        "Customer ID;name\n1;alice-updated\n3;carol\n",
    );
    let outcome = run_once(&config_path, "it-duckdb-scd1-norm-upsert");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.accepted_output.updated_count, Some(1));
    assert_eq!(report.accepted_output.inserted_count, Some(1));
    assert_eq!(report.accepted_output.target_rows_before, Some(2));
    assert_eq!(report.accepted_output.target_rows_after, Some(3));

    let conn = open_readback(&db_path);
    assert_eq!(count_rows(&conn, "main.customer"), 3);
    // The stored key column carries the normalized (output) name.
    let name: String = conn
        .query_row(
            "SELECT name FROM main.customer WHERE customer_id = '1'",
            [],
            |row| row.get(0),
        )
        .expect("read updated name");
    assert_eq!(name, "alice-updated");
}

#[test]
fn duckdb_scd2_history() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let db_path = root.join("out/warehouse.duckdb");

    fs::create_dir_all(&input_dir).expect("create input dir");

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "duckdb"
        path: "{db_path}"
        duckdb:
          table: customer
    policy:
      severity: "warn"
    schema:
      primary_key: ["id"]
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        db_path = db_path.display(),
    );
    let config_path = write_config(root, &yaml);

    write_csv(&input_dir, "batch1.csv", "id;name\n1;alice\n2;bob\n");
    run_once(&config_path, "it-duckdb-scd2-init");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;name\n1;alice-2\n");
    let outcome = run_once(&config_path, "it-duckdb-scd2-upsert");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.write_mode.as_deref(),
        Some("merge_scd2")
    );
    assert_eq!(report.accepted_output.inserted_count, Some(1));
    assert_eq!(report.accepted_output.closed_count, Some(1));

    let conn = open_readback(&db_path);
    // 2 originals + 1 new current version for id=1.
    assert_eq!(count_rows(&conn, "main.customer"), 3);
    // id=1 has exactly one current row.
    let current_id1: i64 = conn
        .query_row(
            "SELECT count(*) FROM main.customer WHERE id = '1' AND \"__floe_is_current\" = true",
            [],
            |row| row.get(0),
        )
        .expect("count current id1");
    assert_eq!(current_id1, 1);
    // The closed row has a non-null valid_to.
    let closed_id1: i64 = conn
        .query_row(
            "SELECT count(*) FROM main.customer WHERE id = '1' AND \"__floe_is_current\" = false AND \"__floe_valid_to\" IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("count closed id1");
    assert_eq!(closed_id1, 1);
}
