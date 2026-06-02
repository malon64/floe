//! DuckDB accepted sink.
//!
//! Writes accepted output into a DuckDB database — either a local `.duckdb`
//! file or a MotherDuck (managed remote) database. DuckDB cannot read-write a
//! database file over object storage, so remote writes go exclusively through
//! MotherDuck; object-store file paths are rejected at config-validation time
//! and again here as a runtime guard (see [`conn::resolve_target`]).
//!
//! Unlike the Delta/Iceberg sinks this writer is fully synchronous (no tokio).
//! Each `write()` call is one buffer flush: it opens/reuses a cached
//! connection, materializes the chunk as an Arrow-backed temp table, applies it
//! to the target table inside a single transaction, and commits. The
//! [`AcceptedBuffer`](crate::run::entity::accepted_buffer) contract drives the
//! first flush with the configured mode and forces later flushes to `Append`,
//! so `Overwrite` replaces the target table once and subsequent flushes
//! accumulate.

use arrow::record_batch::RecordBatch;

use ::duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use ::duckdb::Connection;
use uuid::Uuid;

use crate::errors::{ConfigError, RunError};
use crate::io::format::{
    AcceptedMergeMetrics, AcceptedSchemaEvolution, AcceptedWriteMetrics, AcceptedWriteOutput,
    AcceptedWriteRequest,
};
use crate::io::unique_seed::seed_from_batches;
use crate::io::write::delta::record_batch::dataframe_to_record_batch;
use crate::io::write::sink_format::{SeedContext, SinkFormat};
use crate::{check, config, FloeResult};

use super::metrics;

mod conn;
mod merge;

pub use self::conn::close_cached_connections;
use self::conn::{
    quote_ident, quoted_table, resolve_schema_and_table, resolve_target, DuckDbTarget,
};

pub(crate) struct DuckDbSinkFormat;

pub(crate) static DUCKDB_SINK_FORMAT: DuckDbSinkFormat = DuckDbSinkFormat;

impl SinkFormat for DuckDbSinkFormat {
    fn format_name(&self) -> &'static str {
        "duckdb"
    }

    fn supported_modes(&self) -> &'static [config::WriteMode] {
        &[
            config::WriteMode::Overwrite,
            config::WriteMode::Append,
            config::WriteMode::MergeScd1,
            config::WriteMode::MergeScd2,
        ]
    }

    fn supported_storages(&self) -> &'static [&'static str] {
        // Object-store file paths are rejected (DuckDB cannot read-write a
        // database file over object storage). MotherDuck targets carry no
        // storage definition, so the generic storage-capability check is
        // skipped for them and this list only constrains file targets.
        &["local"]
    }

    fn write(&self, req: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput> {
        let AcceptedWriteRequest {
            target,
            df,
            mode,
            entity,
            ..
        } = req;

        let cfg = entity.sink.accepted.duckdb.as_ref().ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.format=duckdb requires a sink.accepted.duckdb block",
                entity.name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let duck_target = resolve_target(target, cfg, &entity.name)?;
        let (schema, table) = resolve_schema_and_table(cfg);

        // Convert the chunk to Arrow before locking so the (potentially
        // expensive) conversion does not hold the per-database write lock.
        let batch = dataframe_to_record_batch(df, entity)?;
        let source_columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        let handle = conn::acquire(&duck_target)?;
        let conn = handle
            .lock()
            .map_err(|_| Box::new(RunError("duckdb connection lock poisoned".to_string())))?;

        let merge = apply_write(&conn, mode, entity, &schema, &table, &source_columns, batch)?;

        // Release the lock before stat-ing the file.
        drop(conn);

        let metrics = duckdb_write_metrics(&duck_target);

        Ok(AcceptedWriteOutput {
            files_written: Some(1),
            parts_written: 1,
            part_files: vec![duck_target.identity()],
            table_version: None,
            snapshot_id: None,
            table_root_uri: None,
            catalog: None,
            metrics,
            merge,
            schema_evolution: default_schema_evolution(entity),
            perf: None,
        })
    }

    fn seed_unique_tracker(
        &self,
        tracker: &mut check::UniqueTracker,
        ctx: &mut SeedContext<'_>,
    ) -> FloeResult<()> {
        if ctx.scan_cols.is_empty() {
            return Ok(());
        }
        let cfg = ctx.entity.sink.accepted.duckdb.as_ref().ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.format=duckdb requires a sink.accepted.duckdb block",
                ctx.entity.name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let duck_target = resolve_target(ctx.target, cfg, &ctx.entity.name)?;
        let (schema, table) = resolve_schema_and_table(cfg);

        let handle = conn::acquire(&duck_target)?;
        let conn = handle
            .lock()
            .map_err(|_| Box::new(RunError("duckdb connection lock poisoned".to_string())))?;

        if !table_exists(&conn, &schema, &table)? {
            return Ok(());
        }

        let columns = ctx
            .scan_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {columns} FROM {}", quoted_table(&schema, &table));
        let mut stmt = conn.prepare(&sql).map_err(|err| {
            Box::new(RunError(format!("duckdb seed prepare failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;
        let batches: Vec<RecordBatch> = stmt
            .query_arrow([])
            .map_err(|err| {
                Box::new(RunError(format!("duckdb seed scan failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })?
            .collect();

        seed_from_batches(tracker, batches, ctx.rename_back)
    }
}

/// Apply one chunk to the target table inside a transaction. Returns merge
/// metrics for the merge modes (`None` for overwrite/append).
fn apply_write(
    conn: &Connection,
    mode: config::WriteMode,
    entity: &config::EntityConfig,
    schema: &str,
    table: &str,
    source_columns: &[String],
    batch: RecordBatch,
) -> FloeResult<Option<AcceptedMergeMetrics>> {
    let source_table = format!("__floe_duckdb_src_{}", Uuid::new_v4().simple());

    with_transaction(conn, |conn| {
        exec(
            conn,
            &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema)),
            "duckdb create schema failed",
        )?;

        // Materialize the Arrow chunk into a temp table so it can be referenced
        // more than once (the MERGE statements scan the source twice) and so
        // metrics can be counted against it.
        let params = arrow_recordbatch_to_query_params(batch);
        let create_src = format!(
            "CREATE OR REPLACE TEMP TABLE {src} AS SELECT * FROM arrow(?, ?)",
            src = quote_ident(&source_table)
        );
        conn.execute(&create_src, params).map_err(|err| {
            Box::new(RunError(format!(
                "duckdb source materialization failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let exists = table_exists(conn, schema, table)?;

        let merge = match mode {
            config::WriteMode::Overwrite => {
                exec(
                    conn,
                    &format!(
                        "CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}",
                        target = quoted_table(schema, table),
                        src = quote_ident(&source_table)
                    ),
                    "duckdb overwrite failed",
                )?;
                None
            }
            config::WriteMode::Append => {
                if !exists {
                    exec(
                        conn,
                        &format!(
                            "CREATE TABLE {target} AS SELECT * FROM {src}",
                            target = quoted_table(schema, table),
                            src = quote_ident(&source_table)
                        ),
                        "duckdb append create failed",
                    )?;
                } else {
                    exec(
                        conn,
                        &format!(
                            "INSERT INTO {target} BY NAME SELECT * FROM {src}",
                            target = quoted_table(schema, table),
                            src = quote_ident(&source_table)
                        ),
                        "duckdb append insert failed",
                    )?;
                }
                None
            }
            config::WriteMode::MergeScd1 => Some(merge::execute_scd1(
                conn,
                entity,
                schema,
                table,
                &source_table,
                source_columns,
                exists,
            )?),
            config::WriteMode::MergeScd2 => Some(merge::execute_scd2(
                conn,
                entity,
                schema,
                table,
                &source_table,
                source_columns,
                exists,
            )?),
        };

        exec(
            conn,
            &format!("DROP TABLE IF EXISTS {}", quote_ident(&source_table)),
            "duckdb source cleanup failed",
        )?;

        Ok(merge)
    })
}

/// Run `body` between `BEGIN`/`COMMIT`, rolling back on error.
fn with_transaction<T>(
    conn: &Connection,
    body: impl FnOnce(&Connection) -> FloeResult<T>,
) -> FloeResult<T> {
    exec(conn, "BEGIN TRANSACTION", "duckdb begin failed")?;
    match body(conn) {
        Ok(value) => {
            exec(conn, "COMMIT", "duckdb commit failed")?;
            Ok(value)
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

fn exec(conn: &Connection, sql: &str, context: &str) -> FloeResult<()> {
    conn.execute_batch(sql).map_err(|err| {
        Box::new(RunError(format!("{context}: {err}"))) as Box<dyn std::error::Error + Send + Sync>
    })
}

/// True if `schema.table` already exists in the current database.
fn table_exists(conn: &Connection, schema: &str, table: &str) -> FloeResult<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
            [schema, table],
            |row| row.get(0),
        )
        .map_err(|err| {
            Box::new(RunError(format!(
                "duckdb table existence check failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    Ok(count > 0)
}

/// File-size based metrics for a local database, or null metrics for MotherDuck
/// (no local file to stat).
fn duckdb_write_metrics(target: &DuckDbTarget) -> AcceptedWriteMetrics {
    match target.local_path() {
        Some(path) => {
            let size = std::fs::metadata(path).ok().map(|meta| meta.len());
            match size {
                Some(size) => metrics::summarize_written_file_sizes(
                    &[size],
                    1,
                    metrics::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
                ),
                None => metrics::null_accepted_write_metrics(),
            }
        }
        None => metrics::null_accepted_write_metrics(),
    }
}

fn default_schema_evolution(entity: &config::EntityConfig) -> AcceptedSchemaEvolution {
    AcceptedSchemaEvolution {
        enabled: false,
        mode: entity
            .schema
            .resolved_schema_evolution()
            .mode
            .as_str()
            .to_string(),
        applied: false,
        added_columns: Vec::new(),
        incompatible_changes_detected: false,
    }
}
