use serde::Deserialize;

use crate::config::{
    ArchiveTarget, CatalogsConfig, ColumnConfig, EntityConfig, EntityStateConfig, IncrementalMode,
    LineageConfig, MergeOptionsConfig, PiiConfig, PolicyConfig, PolicySeverity, SchemaConfig,
    SchemaMismatchConfig, SinkConfig, SinkOptions, SinkTarget, SourceConfig, SourceOptions,
    StoragesConfig, WriteMode,
};
use crate::{ConfigError, FloeResult};

// Minimal deserializable mirror of CommonManifest — only the fields needed to reconstruct
// a RootConfig and run an entity.
#[derive(Debug, Deserialize)]
pub struct ManifestForRun {
    pub spec_version: String,
    pub report_base_uri: String,
    pub entities: Vec<ManifestEntityForRun>,
    pub storages: Option<serde_json::Value>,
    pub catalogs: Option<serde_json::Value>,
    pub lineage: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestEntityForRun {
    pub name: String,
    pub domain: Option<String>,
    pub source: ManifestSourceForRun,
    pub sinks: ManifestSinksForRun,
    pub policy_severity: Option<String>,
    pub write_mode: Option<String>,
    pub incremental_mode: Option<String>,
    pub schema: ManifestEntitySchemaForRun,
    pub pii: Option<serde_json::Value>,
    pub state_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSourceForRun {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub cast_mode: Option<String>,
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSinksForRun {
    pub accepted: ManifestSinkTargetForRun,
    pub rejected: Option<ManifestSinkTargetForRun>,
    pub archive: Option<ManifestArchiveTargetForRun>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSinkTargetForRun {
    pub format: String,
    pub storage: String,
    pub path: String,
    pub options: Option<serde_json::Value>,
    pub partition_by: Option<Vec<String>>,
    pub merge: Option<serde_json::Value>,
    pub iceberg: Option<serde_json::Value>,
    pub delta: Option<serde_json::Value>,
    pub write_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestArchiveTargetForRun {
    pub storage: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct ManifestEntitySchemaForRun {
    pub columns: Vec<ManifestColumnDefForRun>,
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<Vec<String>>,
    pub normalize_columns: Option<serde_json::Value>,
    pub mismatch: Option<serde_json::Value>,
    pub schema_evolution: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestColumnDefForRun {
    pub name: String,
    pub column_type: String,
    pub source: Option<String>,
    pub nullable: Option<bool>,
    pub unique: Option<bool>,
    pub width: Option<u64>,
    pub trim: Option<bool>,
}

/// Parse a manifest JSON string and reconstruct a minimal RootConfig.
/// Returns (config, report_base_uri).
pub fn config_from_manifest_json(json: &str) -> FloeResult<(crate::config::RootConfig, String)> {
    let manifest: ManifestForRun =
        serde_json::from_str(json).map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(ConfigError(format!("manifest parse error: {err}")))
        })?;

    let storages = manifest
        .storages
        .as_ref()
        .and_then(|v| serde_json::from_value::<StoragesConfig>(v.clone()).ok());
    let catalogs = manifest
        .catalogs
        .as_ref()
        .and_then(|v| serde_json::from_value::<CatalogsConfig>(v.clone()).ok());
    let lineage = manifest
        .lineage
        .as_ref()
        .and_then(|v| serde_json::from_value::<LineageConfig>(v.clone()).ok());

    let entities = manifest
        .entities
        .iter()
        .map(entity_from_manifest)
        .collect::<FloeResult<Vec<_>>>()?;

    let config = crate::config::RootConfig {
        version: manifest.spec_version,
        metadata: None,
        storages,
        catalogs,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage,
        entities,
    };

    Ok((config, manifest.report_base_uri))
}

fn entity_from_manifest(m: &ManifestEntityForRun) -> FloeResult<EntityConfig> {
    let policy_severity = parse_policy_severity(m.policy_severity.as_deref().unwrap_or("warn"));
    let write_mode = parse_write_mode(m.write_mode.as_deref().unwrap_or("overwrite"));
    let incremental_mode = parse_incremental_mode(m.incremental_mode.as_deref().unwrap_or("none"));

    let source_options: Option<SourceOptions> = m
        .source
        .options
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let source = SourceConfig {
        format: m.source.format.clone(),
        path: m.source.path.clone(),
        storage: if m.source.storage == "local" {
            None
        } else {
            Some(m.source.storage.clone())
        },
        options: source_options,
        cast_mode: m.source.cast_mode.clone(),
    };

    let accepted = sink_target_from_manifest(&m.sinks.accepted, write_mode);
    let rejected = m
        .sinks
        .rejected
        .as_ref()
        .map(|t| sink_target_from_manifest(t, write_mode));
    let archive = m.sinks.archive.as_ref().map(|a| ArchiveTarget {
        path: a.path.clone(),
        storage: if a.storage == "local" {
            None
        } else {
            Some(a.storage.clone())
        },
    });

    let schema = schema_from_manifest(&m.schema)?;
    let pii: Option<PiiConfig> = m
        .pii
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let state = m.state_path.as_ref().map(|p| EntityStateConfig {
        path: Some(p.clone()),
    });

    Ok(EntityConfig {
        name: m.name.clone(),
        metadata: None,
        domain: m.domain.clone(),
        incremental_mode,
        state,
        source,
        sink: SinkConfig {
            write_mode,
            accepted,
            rejected,
            archive,
        },
        policy: PolicyConfig {
            severity: policy_severity,
        },
        schema,
        pii,
    })
}

fn sink_target_from_manifest(
    m: &ManifestSinkTargetForRun,
    default_write_mode: WriteMode,
) -> SinkTarget {
    let write_mode = m
        .write_mode
        .as_deref()
        .map(parse_write_mode)
        .unwrap_or(default_write_mode);
    let options: Option<SinkOptions> = m
        .options
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let merge: Option<MergeOptionsConfig> = m
        .merge
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let iceberg = m
        .iceberg
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let delta = m
        .delta
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    SinkTarget {
        format: m.format.clone(),
        path: m.path.clone(),
        storage: if m.storage == "local" {
            None
        } else {
            Some(m.storage.clone())
        },
        options,
        merge,
        iceberg,
        delta,
        duckdb: None,
        partition_by: m.partition_by.clone(),
        partition_spec: None,
        write_mode,
    }
}

fn schema_from_manifest(m: &ManifestEntitySchemaForRun) -> FloeResult<SchemaConfig> {
    let columns = m
        .columns
        .iter()
        .map(|c| ColumnConfig {
            name: c.name.clone(),
            source: c.source.clone(),
            column_type: c.column_type.clone(),
            nullable: c.nullable,
            unique: c.unique,
            width: c.width,
            trim: c.trim,
        })
        .collect();

    let normalize_columns: Option<crate::config::NormalizeColumnsConfig> = m
        .normalize_columns
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let mismatch: Option<SchemaMismatchConfig> = m
        .mismatch
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let schema_evolution = m
        .schema_evolution
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    Ok(SchemaConfig {
        columns,
        normalize_columns,
        mismatch,
        schema_evolution,
        primary_key: if m.primary_key.is_empty() {
            None
        } else {
            Some(m.primary_key.clone())
        },
        unique_keys: if m.unique_keys.is_empty() {
            None
        } else {
            Some(m.unique_keys.clone())
        },
    })
}

fn parse_policy_severity(s: &str) -> PolicySeverity {
    match s {
        "reject" => PolicySeverity::Reject,
        "abort" => PolicySeverity::Abort,
        _ => PolicySeverity::Warn,
    }
}

fn parse_write_mode(s: &str) -> WriteMode {
    match s {
        "append" => WriteMode::Append,
        "merge_scd1" => WriteMode::MergeScd1,
        "merge_scd2" => WriteMode::MergeScd2,
        _ => WriteMode::Overwrite,
    }
}

fn parse_incremental_mode(s: &str) -> IncrementalMode {
    match s {
        "archive" => IncrementalMode::Archive,
        "file" => IncrementalMode::File,
        "row" => IncrementalMode::Row,
        _ => IncrementalMode::None,
    }
}
