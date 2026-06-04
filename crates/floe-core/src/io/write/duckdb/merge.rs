//! SCD1 / SCD2 merge execution for the DuckDB sink, using DuckDB's native
//! `MERGE INTO` (available since DuckDB 1.4; the bundled engine is 1.5.0).
//!
//! Both strategies operate on a pre-materialized temporary source table so the
//! merge SQL needs no bound parameters and the source can be referenced more
//! than once. Metrics are extracted from `RETURNING merge_action`, which tags
//! every affected row with the action DuckDB took (`INSERT` / `UPDATE` /
//! `DELETE`).

use std::collections::HashSet;
use std::time::Instant;

use ::duckdb::Connection;

use crate::errors::RunError;
use crate::io::format::AcceptedMergeMetrics;
use crate::io::write::strategy::merge::keys;
use crate::{config, FloeResult};

use super::conn::{quote_ident, quoted_table};

/// Tally of the `merge_action` values returned by a `MERGE INTO ... RETURNING`.
#[derive(Debug, Default, Clone, Copy)]
struct MergeActionTally {
    inserted: u64,
    updated: u64,
    deleted: u64,
}

/// Run a `MERGE INTO ... RETURNING merge_action` statement and tally actions.
fn run_merge_with_tally(conn: &Connection, sql: &str) -> FloeResult<MergeActionTally> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| Box::new(RunError(format!("duckdb merge prepare failed: {err}"))))?;
    let rows = stmt
        .query_map([], |row| row.get::<usize, String>(0))
        .map_err(|err| Box::new(RunError(format!("duckdb merge execution failed: {err}"))))?;
    let mut tally = MergeActionTally::default();
    for action in rows {
        let action = action
            .map_err(|err| Box::new(RunError(format!("duckdb merge row read failed: {err}"))))?;
        match action.to_ascii_uppercase().as_str() {
            "INSERT" => tally.inserted += 1,
            "UPDATE" => tally.updated += 1,
            "DELETE" => tally.deleted += 1,
            _ => {}
        }
    }
    Ok(tally)
}

fn count_rows(conn: &Connection, schema: &str, table: &str) -> FloeResult<u64> {
    let sql = format!("SELECT count(*) FROM {}", quoted_table(schema, table));
    let count: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .map_err(|err| Box::new(RunError(format!("duckdb row count failed: {err}"))))?;
    Ok(count.max(0) as u64)
}

/// Build `t."k" = s."k" AND ...` for the merge key.
fn key_equality_predicate(merge_key: &[String], target_alias: &str, source_alias: &str) -> String {
    merge_key
        .iter()
        .map(|key| {
            format!(
                "{target_alias}.{col} = {source_alias}.{col}",
                col = quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Execute SCD1 (upsert). Mirrors the Delta SCD1 semantics: when a target table
/// does not yet exist the whole source is inserted; otherwise matched rows are
/// updated across all non-key, non-ignored columns and unmatched rows inserted.
pub(crate) fn execute_scd1(
    conn: &Connection,
    entity: &config::EntityConfig,
    schema: &str,
    table: &str,
    source_table: &str,
    source_columns: &[String],
    table_exists: bool,
) -> FloeResult<AcceptedMergeMetrics> {
    let started = Instant::now();
    // Map the primary key through the output-column naming so it matches the
    // already-renamed DataFrame columns (see resolve_merge_key_output).
    let merge_key = keys::resolve_merge_key_output(entity)?;

    if !table_exists {
        create_table_from_source(conn, schema, table, source_table)?;
        let inserted = count_rows(conn, schema, table)?;
        return Ok(AcceptedMergeMetrics {
            merge_key,
            inserted_count: inserted,
            updated_count: 0,
            closed_count: None,
            unchanged_count: None,
            target_rows_before: 0,
            target_rows_after: inserted,
            merge_elapsed_ms: started.elapsed().as_millis() as u64,
        });
    }

    let target_rows_before = count_rows(conn, schema, table)?;
    let merge_key_set = merge_key.iter().map(String::as_str).collect::<HashSet<_>>();
    let ignore_columns = keys::resolve_merge_ignore_columns(entity)?;
    let update_columns = source_columns
        .iter()
        .filter(|name| {
            !merge_key_set.contains(name.as_str()) && !ignore_columns.contains(name.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();

    let target = quoted_table(schema, table);
    let key_predicate = key_equality_predicate(&merge_key, "t", "s");
    let insert_columns = source_columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_values = source_columns
        .iter()
        .map(|c| format!("s.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut clauses = String::new();
    if !update_columns.is_empty() {
        let set_clause = update_columns
            .iter()
            .map(|c| format!("{col} = s.{col}", col = quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {set_clause}"));
    }
    clauses.push_str(&format!(
        " WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    ));

    let sql = format!(
        "MERGE INTO {target} AS t USING {source} AS s ON {key_predicate}{clauses} RETURNING merge_action",
        source = quote_ident(source_table)
    );
    let tally = run_merge_with_tally(conn, &sql)?;
    let target_rows_after = count_rows(conn, schema, table)?;

    Ok(AcceptedMergeMetrics {
        merge_key,
        inserted_count: tally.inserted,
        updated_count: tally.updated,
        closed_count: None,
        unchanged_count: None,
        target_rows_before,
        target_rows_after,
        merge_elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

/// Execute SCD2 (history-preserving). Two native merges mirror the Delta
/// implementation: phase 1 closes changed current rows
/// (`is_current = false`, `valid_to = now()`); phase 2 inserts a fresh current
/// version for changed and new keys.
pub(crate) fn execute_scd2(
    conn: &Connection,
    entity: &config::EntityConfig,
    schema: &str,
    table: &str,
    source_table: &str,
    source_columns: &[String],
    table_exists: bool,
) -> FloeResult<AcceptedMergeMetrics> {
    let started = Instant::now();
    // Map the primary key through the output-column naming so it matches the
    // already-renamed DataFrame columns (see resolve_merge_key_output).
    let merge_key = keys::resolve_merge_key_output(entity)?;
    let merge_key_set = merge_key.iter().map(String::as_str).collect::<HashSet<_>>();
    let (ignore_columns, compare_columns_opt) = keys::resolve_merge_column_mappings(entity)?;
    let compare_columns = compare_columns_opt.unwrap_or_else(|| {
        source_columns
            .iter()
            .filter(|name| {
                !merge_key_set.contains(name.as_str()) && !ignore_columns.contains(name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>()
    });
    let system = keys::resolve_scd2_system_columns(entity);
    let target = quoted_table(schema, table);
    let source = quote_ident(source_table);

    if !table_exists {
        let select_columns = source_columns
            .iter()
            .map(|c| format!("s.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "CREATE TABLE {target} AS SELECT {select_columns}, \
             true AS {is_current}, now() AS {valid_from}, \
             CAST(NULL AS TIMESTAMP) AS {valid_to} FROM {source} AS s",
            is_current = quote_ident(&system.is_current),
            valid_from = quote_ident(&system.valid_from),
            valid_to = quote_ident(&system.valid_to),
        );
        conn.execute(&sql, [])
            .map_err(|err| Box::new(RunError(format!("duckdb scd2 bootstrap failed: {err}"))))?;
        let inserted = count_rows(conn, schema, table)?;
        return Ok(AcceptedMergeMetrics {
            merge_key,
            inserted_count: inserted,
            updated_count: 0,
            closed_count: Some(0),
            unchanged_count: Some(0),
            target_rows_before: 0,
            target_rows_after: inserted,
            merge_elapsed_ms: started.elapsed().as_millis() as u64,
        });
    }

    let target_rows_before = count_rows(conn, schema, table)?;
    let key_predicate = key_equality_predicate(&merge_key, "t", "s");
    let changed_predicate = scd2_changed_predicate(&compare_columns);

    // Phase 1: close changed current rows.
    let close_sql = format!(
        "MERGE INTO {target} AS t USING {source} AS s ON {key_predicate} \
         WHEN MATCHED AND t.{is_current} = true AND ({changed_predicate}) \
         THEN UPDATE SET {is_current} = false, {valid_to} = now() RETURNING merge_action",
        is_current = quote_ident(&system.is_current),
        valid_to = quote_ident(&system.valid_to),
    );
    let close_tally = run_merge_with_tally(conn, &close_sql)?;

    // Phase 2: insert new current versions for keys lacking an active match.
    let active_predicate = format!(
        "{key_predicate} AND t.{is_current} = true",
        is_current = quote_ident(&system.is_current)
    );
    let insert_columns = source_columns
        .iter()
        .map(|c| quote_ident(c))
        .chain([
            quote_ident(&system.is_current),
            quote_ident(&system.valid_from),
            quote_ident(&system.valid_to),
        ])
        .collect::<Vec<_>>()
        .join(", ");
    let insert_values = source_columns
        .iter()
        .map(|c| format!("s.{}", quote_ident(c)))
        .chain([
            "true".to_string(),
            "now()".to_string(),
            "CAST(NULL AS TIMESTAMP)".to_string(),
        ])
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "MERGE INTO {target} AS t USING {source} AS s ON {active_predicate} \
         WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values}) \
         RETURNING merge_action",
    );
    let insert_tally = run_merge_with_tally(conn, &insert_sql)?;
    let target_rows_after = count_rows(conn, schema, table)?;

    let closed_count = close_tally.updated;
    let inserted_count = insert_tally.inserted;
    let source_rows = count_rows_in(conn, source_table)?;
    let unchanged_count = source_rows.saturating_sub(inserted_count);

    Ok(AcceptedMergeMetrics {
        merge_key,
        inserted_count,
        updated_count: closed_count,
        closed_count: Some(closed_count),
        unchanged_count: Some(unchanged_count),
        target_rows_before,
        target_rows_after,
        merge_elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

fn count_rows_in(conn: &Connection, table: &str) -> FloeResult<u64> {
    let sql = format!("SELECT count(*) FROM {}", quote_ident(table));
    let count: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .map_err(|err| Box::new(RunError(format!("duckdb row count failed: {err}"))))?;
    Ok(count.max(0) as u64)
}

fn create_table_from_source(
    conn: &Connection,
    schema: &str,
    table: &str,
    source_table: &str,
) -> FloeResult<()> {
    let sql = format!(
        "CREATE TABLE {} AS SELECT * FROM {}",
        quoted_table(schema, table),
        quote_ident(source_table)
    );
    conn.execute(&sql, [])
        .map_err(|err| Box::new(RunError(format!("duckdb table create failed: {err}"))))?;
    Ok(())
}

/// Build the SCD2 "row changed" predicate over `compare_columns`. Mirrors the
/// Delta predicate: a column counts as changed when the values differ or one
/// side is NULL while the other is not. With no compare columns, nothing is
/// ever considered changed (`false`).
fn scd2_changed_predicate(compare_columns: &[String]) -> String {
    if compare_columns.is_empty() {
        return "false".to_string();
    }
    compare_columns
        .iter()
        .map(|column| {
            let col = quote_ident(column);
            format!(
                "((t.{col} <> s.{col}) OR (t.{col} IS NULL AND s.{col} IS NOT NULL) OR \
                 (t.{col} IS NOT NULL AND s.{col} IS NULL))"
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_predicate_joins_with_and() {
        let predicate = key_equality_predicate(&["id".to_string(), "region".to_string()], "t", "s");
        assert_eq!(
            predicate,
            "t.\"id\" = s.\"id\" AND t.\"region\" = s.\"region\""
        );
    }

    #[test]
    fn changed_predicate_handles_nulls() {
        let predicate = scd2_changed_predicate(&["name".to_string()]);
        assert!(predicate.contains("t.\"name\" <> s.\"name\""));
        assert!(predicate.contains("t.\"name\" IS NULL AND s.\"name\" IS NOT NULL"));
    }

    #[test]
    fn changed_predicate_empty_is_false() {
        assert_eq!(scd2_changed_predicate(&[]), "false");
    }
}
