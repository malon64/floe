use std::path::Path;

use yaml_rust2::Yaml;

use crate::config::{
    ColumnConfig, EntityConfig, EntityMetadata, NormalizeColumnsConfig, PolicyConfig,
    ProjectMetadata, QuarantineConfig, ReasonColumns, ReportTarget, RootConfig, SchemaConfig,
    SinkConfig, SinkTarget, SourceConfig, SourceOptions, ThresholdsConfig,
};
use crate::{ConfigError, FloeResult};
use crate::required::{
    optional_bool, optional_f64, optional_string, optional_u64, optional_vec_string,
    required_array, required_string, required_value,
};
use crate::yaml_decode::{hash_get, load_yaml, yaml_hash};

pub(crate) fn parse_config(path: &Path) -> FloeResult<RootConfig> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Err(Box::new(ConfigError("YAML is empty".to_string())));
    }
    if docs.len() > 1 {
        return Err(Box::new(ConfigError(
            "YAML contains multiple documents; expected one".to_string(),
        )));
    }
    parse_root(&docs[0])
}

fn parse_root(doc: &Yaml) -> FloeResult<RootConfig> {
    let root = yaml_hash(doc, "root")?;
    let version = required_string(root, "version", "root")?;

    let metadata = match hash_get(root, "metadata") {
        Some(value) => Some(parse_project_metadata(value)?),
        None => None,
    };

    let entities_yaml = required_array(root, "entities", "root")?;

    let mut entities = Vec::with_capacity(entities_yaml.len());
    for (index, entity_yaml) in entities_yaml.iter().enumerate() {
        let entity = parse_entity(entity_yaml)
            .map_err(|err| Box::new(ConfigError(format!("entities[{index}]: {err}"))))?;
        entities.push(entity);
    }

    Ok(RootConfig {
        version,
        metadata,
        entities,
    })
}

fn parse_project_metadata(value: &Yaml) -> FloeResult<ProjectMetadata> {
    let hash = yaml_hash(value, "metadata")?;
    Ok(ProjectMetadata {
        project: required_string(hash, "project", "metadata")?,
        description: optional_string(hash, "description", "metadata")?,
        owner: optional_string(hash, "owner", "metadata")?,
        tags: optional_vec_string(hash, "tags", "metadata")?,
    })
}

fn parse_entity(value: &Yaml) -> FloeResult<EntityConfig> {
    let hash = yaml_hash(value, "entity")?;
    let name = required_string(hash, "name", "entity")?;

    let metadata = match hash_get(hash, "metadata") {
        Some(value) => Some(parse_entity_metadata(value)?),
        None => None,
    };

    let source = parse_source(required_value(hash, "source", "entity")?)?;
    let sink = parse_sink(required_value(hash, "sink", "entity")?)?;
    let policy = parse_policy(required_value(hash, "policy", "entity")?)?;
    let schema = parse_schema(required_value(hash, "schema", "entity")?)?;

    Ok(EntityConfig {
        name,
        metadata,
        source,
        sink,
        policy,
        schema,
    })
}

fn parse_entity_metadata(value: &Yaml) -> FloeResult<EntityMetadata> {
    let hash = yaml_hash(value, "entity.metadata")?;
    Ok(EntityMetadata {
        data_product: optional_string(hash, "data_product", "entity.metadata")?,
        domain: optional_string(hash, "domain", "entity.metadata")?,
        owner: optional_string(hash, "owner", "entity.metadata")?,
        description: optional_string(hash, "description", "entity.metadata")?,
        tags: optional_vec_string(hash, "tags", "entity.metadata")?,
    })
}

fn parse_source(value: &Yaml) -> FloeResult<SourceConfig> {
    let hash = yaml_hash(value, "source")?;
    let options = match hash_get(hash, "options") {
        Some(value) => Some(parse_source_options(value)?),
        None => None,
    };

    Ok(SourceConfig {
        format: required_string(hash, "format", "source")?,
        path: required_string(hash, "path", "source")?,
        options,
        cast_mode: optional_string(hash, "cast_mode", "source")?,
    })
}

fn parse_source_options(value: &Yaml) -> FloeResult<SourceOptions> {
    let hash = yaml_hash(value, "source.options")?;
    Ok(SourceOptions {
        header: optional_bool(hash, "header", "source.options")?,
        separator: optional_string(hash, "separator", "source.options")?,
        encoding: optional_string(hash, "encoding", "source.options")?,
        null_values: optional_vec_string(hash, "null_values", "source.options")?,
    })
}

fn parse_sink(value: &Yaml) -> FloeResult<SinkConfig> {
    let hash = yaml_hash(value, "sink")?;
    Ok(SinkConfig {
        accepted: parse_sink_target(required_value(hash, "accepted", "sink")?, "sink.accepted")?,
        rejected: parse_sink_target(required_value(hash, "rejected", "sink")?, "sink.rejected")?,
        report: parse_report_target(required_value(hash, "report", "sink")?)?,
    })
}

fn parse_sink_target(value: &Yaml, ctx: &str) -> FloeResult<SinkTarget> {
    let hash = yaml_hash(value, ctx)?;
    Ok(SinkTarget {
        format: required_string(hash, "format", ctx)?,
        path: required_string(hash, "path", ctx)?,
    })
}

fn parse_report_target(value: &Yaml) -> FloeResult<ReportTarget> {
    let hash = yaml_hash(value, "sink.report")?;
    Ok(ReportTarget {
        path: required_string(hash, "path", "sink.report")?,
    })
}

fn parse_policy(value: &Yaml) -> FloeResult<PolicyConfig> {
    let hash = yaml_hash(value, "policy")?;
    let quarantine = match hash_get(hash, "quarantine") {
        Some(value) => Some(parse_quarantine(value)?),
        None => None,
    };
    let thresholds = match hash_get(hash, "thresholds") {
        Some(value) => Some(parse_thresholds(value)?),
        None => None,
    };

    Ok(PolicyConfig {
        default_severity: optional_string(hash, "default_severity", "policy")?,
        on_schema_error: optional_string(hash, "on_schema_error", "policy")?,
        quarantine,
        thresholds,
    })
}

fn parse_quarantine(value: &Yaml) -> FloeResult<QuarantineConfig> {
    let hash = yaml_hash(value, "policy.quarantine")?;
    let reason_columns = match hash_get(hash, "reason_columns") {
        Some(value) => Some(parse_reason_columns(value)?),
        None => None,
    };

    Ok(QuarantineConfig {
        mode: optional_string(hash, "mode", "policy.quarantine")?,
        add_reason_columns: optional_bool(hash, "add_reason_columns", "policy.quarantine")?,
        reason_columns,
    })
}

fn parse_reason_columns(value: &Yaml) -> FloeResult<ReasonColumns> {
    let hash = yaml_hash(value, "policy.quarantine.reason_columns")?;
    Ok(ReasonColumns {
        rule: optional_string(hash, "rule", "policy.quarantine.reason_columns")?,
        column: optional_string(hash, "column", "policy.quarantine.reason_columns")?,
        message: optional_string(hash, "message", "policy.quarantine.reason_columns")?,
    })
}

fn parse_thresholds(value: &Yaml) -> FloeResult<ThresholdsConfig> {
    let hash = yaml_hash(value, "policy.thresholds")?;
    Ok(ThresholdsConfig {
        max_reject_rate: optional_f64(hash, "max_reject_rate", "policy.thresholds")?,
        max_reject_count: optional_u64(hash, "max_reject_count", "policy.thresholds")?,
    })
}

fn parse_schema(value: &Yaml) -> FloeResult<SchemaConfig> {
    let hash = yaml_hash(value, "schema")?;
    let normalize_columns = match hash_get(hash, "normalize_columns") {
        Some(value) => Some(parse_normalize_columns(value)?),
        None => None,
    };
    let columns_yaml = required_array(hash, "columns", "schema")?;

    let mut columns = Vec::with_capacity(columns_yaml.len());
    for (index, column_yaml) in columns_yaml.iter().enumerate() {
        let column = parse_column(column_yaml)
            .map_err(|err| Box::new(ConfigError(format!("schema.columns[{index}]: {err}"))))?;
        columns.push(column);
    }

    Ok(SchemaConfig {
        normalize_columns,
        columns,
    })
}

fn parse_normalize_columns(value: &Yaml) -> FloeResult<NormalizeColumnsConfig> {
    let hash = yaml_hash(value, "schema.normalize_columns")?;
    Ok(NormalizeColumnsConfig {
        enabled: optional_bool(hash, "enabled", "schema.normalize_columns")?,
        strategy: optional_string(hash, "strategy", "schema.normalize_columns")?,
    })
}

fn parse_column(value: &Yaml) -> FloeResult<ColumnConfig> {
    let hash = yaml_hash(value, "schema.columns")?;
    Ok(ColumnConfig {
        name: required_string(hash, "name", "schema.columns")?,
        column_type: required_string(hash, "type", "schema.columns")?,
        nullable: optional_bool(hash, "nullable", "schema.columns")?,
        unique: optional_bool(hash, "unique", "schema.columns")?,
        unique_strategy: optional_string(hash, "unique_strategy", "schema.columns")?,
    })
}
