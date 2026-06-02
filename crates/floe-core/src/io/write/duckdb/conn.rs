//! DuckDB connection resolution and a process-wide connection cache.
//!
//! DuckDB is a single-writer database: opening the same local file twice in one
//! process produces a file-lock conflict, and re-authenticating to MotherDuck on
//! every buffer flush is wasteful. `SinkFormat::write` is called once per
//! `AcceptedBuffer` flush, so the sink reuses a single cached connection per
//! canonical target id and serializes writes through the inner `Mutex`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use ::duckdb::{Config, Connection};

use crate::config::DuckDbSinkTargetConfig;
use crate::errors::{ConfigError, RunError};
use crate::io::storage::Target;
use crate::FloeResult;

/// MotherDuck connection strings are identified by the `md:` scheme prefix.
const MOTHERDUCK_SCHEME: &str = "md:";

/// A resolved DuckDB write target: either a local database file or a MotherDuck
/// (managed remote) database.
#[derive(Debug, Clone)]
pub(crate) enum DuckDbTarget {
    Local {
        path: PathBuf,
        cache_key: String,
    },
    MotherDuck {
        /// The full `md:<db>` connection string.
        connection: String,
        /// Already `${ENV}`-expanded token (never logged), or `None` to rely on
        /// the ambient `motherduck_token` environment variable.
        token: Option<String>,
        cache_key: String,
    },
}

impl DuckDbTarget {
    pub(crate) fn cache_key(&self) -> &str {
        match self {
            DuckDbTarget::Local { cache_key, .. } | DuckDbTarget::MotherDuck { cache_key, .. } => {
                cache_key
            }
        }
    }

    /// Human-readable identity used in the run report's `part_files` list.
    /// Never contains the MotherDuck token.
    pub(crate) fn identity(&self) -> String {
        match self {
            DuckDbTarget::Local { path, .. } => path.display().to_string(),
            DuckDbTarget::MotherDuck { connection, .. } => connection.clone(),
        }
    }

    pub(crate) fn local_path(&self) -> Option<&PathBuf> {
        match self {
            DuckDbTarget::Local { path, .. } => Some(path),
            DuckDbTarget::MotherDuck { .. } => None,
        }
    }
}

/// True if `connection` is a MotherDuck connection string (`md:` scheme).
pub(crate) fn is_motherduck_connection(connection: &str) -> bool {
    connection.trim_start().starts_with(MOTHERDUCK_SCHEME)
}

/// Resolve the physical DuckDB target from the storage `Target` and the
/// `duckdb:` config block.
///
/// Rules (also enforced at config-validation time, repeated here as a runtime
/// guard):
/// - A `duckdb.connection` of the form `md:<db>` selects MotherDuck. The
///   storage `Target` is ignored.
/// - Otherwise the target must be a local file (`Target::Local`); object-store
///   targets are rejected because DuckDB cannot read-write a database file over
///   object storage.
pub(crate) fn resolve_target(
    target: &Target,
    cfg: &DuckDbSinkTargetConfig,
    entity_name: &str,
) -> FloeResult<DuckDbTarget> {
    if let Some(connection) = cfg.connection.as_deref() {
        let connection = connection.trim();
        if is_motherduck_connection(connection) {
            let token = match cfg.token.as_deref() {
                Some(raw) => Some(expand_env_token(raw, entity_name)?),
                None => None,
            };
            // The cache key must distinguish the same `md:` database opened with
            // *different* credentials, otherwise a later entity/run would silently
            // reuse a connection authenticated with the first token. Mix a
            // non-reversible fingerprint of the resolved token into the key so the
            // raw secret never lives in the cache key (or anywhere it could leak).
            let cache_key = motherduck_cache_key(connection, token.as_deref());
            return Ok(DuckDbTarget::MotherDuck {
                connection: connection.to_string(),
                token,
                cache_key,
            });
        }
        return Err(Box::new(ConfigError(format!(
            "entity.name={entity_name} sink.accepted.duckdb.connection={connection:?} is unsupported; \
             only MotherDuck connection strings (md:<database>) are accepted"
        ))));
    }

    match target {
        Target::Local { base_path, .. } => {
            let path = PathBuf::from(base_path);
            let cache_key = canonical_cache_key(&path);
            Ok(DuckDbTarget::Local { path, cache_key })
        }
        Target::S3 { uri, .. } | Target::Gcs { uri, .. } | Target::Adls { uri, .. } => {
            Err(Box::new(ConfigError(format!(
                "entity.name={entity_name} sink.accepted.format=duckdb cannot write a database \
                 file to object storage ({uri}); DuckDB only supports read-write on local files. \
                 Use a MotherDuck target (sink.accepted.duckdb.connection: md:<database>) for \
                 remote writes"
            ))))
        }
    }
}

/// Build the cache key for a MotherDuck target: the `md:` connection string
/// plus a non-reversible fingerprint of the resolved token (or its absence).
///
/// Two configs pointing at the same database with different tokens must NOT share
/// a cached connection — the second would run under the first's credentials. We
/// hash the token rather than embedding it so the secret never appears in the
/// cache key. The hash is for differentiation only (not security), so the
/// standard non-cryptographic hasher is sufficient.
fn motherduck_cache_key(connection: &str, token: Option<&str>) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match token {
        Some(token) => {
            1u8.hash(&mut hasher);
            token.hash(&mut hasher);
        }
        None => 0u8.hash(&mut hasher),
    }
    format!("{connection}#{:016x}", hasher.finish())
}

/// Build a *stable* cache key for a local file so that the single-writer
/// invariant holds: every resolution of the same logical file — across buffer
/// flushes and across entities — must map to the same key regardless of whether
/// the file or its parent directory exists yet.
///
/// The naive approach (canonicalize the file if it exists, else fall back to the
/// raw path) is unstable: the *first* run sees neither the file nor its parent
/// and returns an un-canonicalized path, while a *later* run canonicalizes and
/// resolves symlinks (e.g. macOS `/var` -> `/private/var`). The two keys differ,
/// so the later run misses the cache and opens a *second* connection to the same
/// file — a single-writer violation whose writes are then invisible to readers of
/// the first connection.
///
/// To make the key stable we create the parent directory (which the connection
/// open needs anyway) and canonicalize *the parent* on every run, re-attaching
/// the file name. Once the file exists, canonicalizing the full path yields the
/// same string because the file component itself is not a symlink.
fn canonical_cache_key(path: &Path) -> String {
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        // Create the parent up front so the very first run can canonicalize it
        // too; `open_connection` creates it regardless, so this has no extra
        // side effect beyond ordering.
        let _ = std::fs::create_dir_all(parent);
        if let Ok(canonical_parent) = parent.canonicalize() {
            return canonical_parent.join(file_name).display().to_string();
        }
    }
    // Fall back to a best-effort canonicalization of the whole path, then the raw
    // path, if the parent could not be resolved.
    if let Ok(canonical) = path.canonicalize() {
        return canonical.display().to_string();
    }
    path.display().to_string()
}

/// Acquire (open + cache, or reuse) the shared connection for `target`.
pub(crate) fn acquire(target: &DuckDbTarget) -> FloeResult<Arc<Mutex<Connection>>> {
    let cache = connection_cache();
    let mut guard = cache.lock().map_err(|_| {
        Box::new(RunError(
            "duckdb connection cache lock poisoned".to_string(),
        ))
    })?;
    if let Some(existing) = guard.get(target.cache_key()) {
        return Ok(Arc::clone(existing));
    }
    let connection = open_connection(target)?;
    let handle = Arc::new(Mutex::new(connection));
    guard.insert(target.cache_key().to_string(), Arc::clone(&handle));
    Ok(handle)
}

fn connection_cache() -> &'static Mutex<HashMap<String, Arc<Mutex<Connection>>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Mutex<Connection>>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Close all cached DuckDB connections, releasing their file locks.
///
/// DuckDB is single-writer per database: the sink keeps each connection open for
/// the lifetime of the process so repeated buffer flushes (and multiple entities
/// targeting the same database) reuse one handle. In the normal CLI flow the
/// process exits after a run, which releases the locks. An embedding process
/// that wants to *read* a local `.duckdb` database in-process after a run must
/// call this first, otherwise the still-open write connection holds an
/// exclusive lock and the read open fails.
pub fn close_cached_connections() {
    if let Ok(mut guard) = connection_cache().lock() {
        guard.clear();
    }
}

fn open_connection(target: &DuckDbTarget) -> FloeResult<Connection> {
    let connection = match target {
        DuckDbTarget::Local { path, .. } => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            Connection::open(path).map_err(|err| {
                Box::new(RunError(format!(
                    "duckdb open failed for {}: {err}",
                    path.display()
                )))
            })?
        }
        DuckDbTarget::MotherDuck {
            connection, token, ..
        } => match token {
            Some(token) => {
                // Pass the token via the connection Config rather than a `SET`
                // statement so it never appears in any SQL string we build.
                let config = Config::default()
                    .with("motherduck_token", token)
                    .map_err(|err| {
                        Box::new(RunError(format!(
                            "duckdb motherduck token configuration failed: {err}"
                        )))
                    })?;
                Connection::open_with_flags(connection, config).map_err(|err| {
                    Box::new(RunError(format!(
                        "duckdb motherduck connection failed for {connection}: {err}"
                    )))
                })?
            }
            None => Connection::open(connection).map_err(|err| {
                Box::new(RunError(format!(
                    "duckdb motherduck connection failed for {connection}: {err}"
                )))
            })?,
        },
    };
    register_arrow_vtab(&connection)?;
    Ok(connection)
}

/// Register the Arrow scan virtual table (`arrow(ptr, ptr)`) so the sink can
/// reference an in-memory `RecordBatch` directly in SQL.
fn register_arrow_vtab(connection: &Connection) -> FloeResult<()> {
    connection
        .register_table_function::<::duckdb::vtab::arrow::ArrowVTab>("arrow")
        .map_err(|err| {
            Box::new(RunError(format!(
                "duckdb arrow virtual table registration failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })
}

/// Expand a `${ENV}` reference (or pass through a literal). Mirrors the
/// Unity/REST credential expansion: the value must be either a plain literal or
/// exactly one `${VAR}` reference. The expanded token is never logged.
pub(crate) fn expand_env_token(token: &str, entity_name: &str) -> FloeResult<String> {
    if !token.contains("${") {
        return Ok(token.to_string());
    }
    let Some(inner) = token.strip_prefix("${").and_then(|s| s.strip_suffix('}')) else {
        return Err(Box::new(ConfigError(format!(
            "entity.name={entity_name} sink.accepted.duckdb.token must be a plain value or a \
             single ${{VAR_NAME}} reference; mixing literal text with ${{...}} is not supported"
        ))));
    };
    if inner.is_empty() || inner.contains('{') || inner.contains('}') {
        return Err(Box::new(ConfigError(format!(
            "entity.name={entity_name} sink.accepted.duckdb.token has invalid placeholder syntax"
        ))));
    }
    std::env::var(inner).map_err(|_| {
        Box::new(ConfigError(format!(
            "entity.name={entity_name} sink.accepted.duckdb.token references env var {inner} which \
             is not set"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

/// Split a possibly schema-qualified table name into `(schema, table)`,
/// defaulting the schema to `cfg.schema` (or `main`).
pub(crate) fn resolve_schema_and_table(cfg: &DuckDbSinkTargetConfig) -> (String, String) {
    let raw = cfg.table.trim();
    if let Some((schema, table)) = raw.split_once('.') {
        let schema = schema.trim();
        let table = table.trim();
        if !schema.is_empty() && !table.is_empty() {
            return (schema.to_string(), table.to_string());
        }
    }
    let schema = cfg
        .schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("main")
        .to_string();
    (schema, raw.to_string())
}

/// Quote a single SQL identifier with double quotes, escaping embedded quotes.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Build a quoted `"schema"."table"` reference.
pub(crate) fn quoted_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(table: &str, schema: Option<&str>, connection: Option<&str>) -> DuckDbSinkTargetConfig {
        DuckDbSinkTargetConfig {
            table: table.to_string(),
            schema: schema.map(str::to_string),
            connection: connection.map(str::to_string),
            token: None,
        }
    }

    #[test]
    fn motherduck_detection() {
        assert!(is_motherduck_connection("md:analytics"));
        assert!(is_motherduck_connection("  md:db"));
        assert!(!is_motherduck_connection("s3://bucket/x.duckdb"));
        assert!(!is_motherduck_connection("./local.duckdb"));
    }

    #[test]
    fn schema_table_resolution_defaults_to_main() {
        let (schema, table) = resolve_schema_and_table(&cfg("customers", None, None));
        assert_eq!(schema, "main");
        assert_eq!(table, "customers");
    }

    #[test]
    fn schema_table_resolution_uses_explicit_schema() {
        let (schema, table) = resolve_schema_and_table(&cfg("customers", Some("staging"), None));
        assert_eq!(schema, "staging");
        assert_eq!(table, "customers");
    }

    #[test]
    fn schema_table_resolution_prefers_qualified_name() {
        let (schema, table) =
            resolve_schema_and_table(&cfg("warehouse.customers", Some("ignored"), None));
        assert_eq!(schema, "warehouse");
        assert_eq!(table, "customers");
    }

    #[test]
    fn quoting_escapes_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quoted_table("main", "t"), "\"main\".\"t\"");
    }

    #[test]
    fn motherduck_target_rejects_non_md_connection() {
        let err = resolve_target(
            &Target::Local {
                storage: "local".to_string(),
                uri: "file:///tmp/x.duckdb".to_string(),
                base_path: "/tmp/x.duckdb".to_string(),
            },
            &cfg("t", None, Some("s3://bucket/db.duckdb")),
            "orders",
        )
        .expect_err("non-md connection should be rejected");
        assert!(err.to_string().contains("MotherDuck"));
    }

    #[test]
    fn object_store_target_rejected_with_motherduck_hint() {
        let err = resolve_target(
            &Target::S3 {
                storage: "s3_out".to_string(),
                uri: "s3://bucket/db.duckdb".to_string(),
                bucket: "bucket".to_string(),
                base_key: "db.duckdb".to_string(),
            },
            &cfg("t", None, None),
            "orders",
        )
        .expect_err("object-store target should be rejected");
        assert!(err.to_string().contains("MotherDuck"));
    }

    #[test]
    fn token_expansion_passes_through_literal() {
        assert_eq!(expand_env_token("plain-token", "e").unwrap(), "plain-token");
    }

    #[test]
    fn token_expansion_reads_env_var() {
        std::env::set_var("FLOE_TEST_DUCKDB_TOKEN", "secret-value");
        assert_eq!(
            expand_env_token("${FLOE_TEST_DUCKDB_TOKEN}", "e").unwrap(),
            "secret-value"
        );
        std::env::remove_var("FLOE_TEST_DUCKDB_TOKEN");
    }

    #[test]
    fn token_expansion_rejects_mixed_literal() {
        assert!(expand_env_token("prefix-${VAR}", "e").is_err());
    }

    #[test]
    fn token_expansion_errors_on_missing_env() {
        std::env::remove_var("FLOE_TEST_DUCKDB_MISSING");
        assert!(expand_env_token("${FLOE_TEST_DUCKDB_MISSING}", "e").is_err());
    }

    fn cfg_with_token(connection: &str, token: Option<&str>) -> DuckDbSinkTargetConfig {
        DuckDbSinkTargetConfig {
            table: "t".to_string(),
            schema: None,
            connection: Some(connection.to_string()),
            token: token.map(str::to_string),
        }
    }

    fn local_target() -> Target {
        Target::Local {
            storage: "local".to_string(),
            uri: "file:///tmp/ignored.duckdb".to_string(),
            base_path: "/tmp/ignored.duckdb".to_string(),
        }
    }

    #[test]
    fn motherduck_cache_key_differs_by_token() {
        // Same database, different credentials must not share a cached connection.
        let a = resolve_target(
            &local_target(),
            &cfg_with_token("md:db", Some("tok-a")),
            "e",
        )
        .expect("resolve a");
        let b = resolve_target(
            &local_target(),
            &cfg_with_token("md:db", Some("tok-b")),
            "e",
        )
        .expect("resolve b");
        assert_ne!(a.cache_key(), b.cache_key());

        // Same database + same token must share a key (connection reuse).
        let a2 = resolve_target(
            &local_target(),
            &cfg_with_token("md:db", Some("tok-a")),
            "e",
        )
        .expect("resolve a2");
        assert_eq!(a.cache_key(), a2.cache_key());

        // A tokenless target is distinct from a tokened one.
        let none = resolve_target(&local_target(), &cfg_with_token("md:db", None), "e")
            .expect("resolve none");
        assert_ne!(a.cache_key(), none.cache_key());
    }

    #[test]
    fn motherduck_cache_key_never_contains_raw_token() {
        let target = resolve_target(
            &local_target(),
            &cfg_with_token("md:db", Some("super-secret-token")),
            "e",
        )
        .expect("resolve");
        assert!(
            !target.cache_key().contains("super-secret-token"),
            "cache key must not embed the raw token"
        );
    }
}
