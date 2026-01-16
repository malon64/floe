use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::config::{
    ColumnConfig, EntityConfig, EntityMetadata, NormalizeColumnsConfig, PolicyConfig,
    ProjectMetadata, QuarantineConfig, ReasonColumns, ReportTarget, RootConfig, SchemaConfig,
    SinkConfig, SinkTarget, SourceConfig, SourceOptions, ThresholdsConfig,
};
use crate::{ConfigError, FloeResult};
use crate::yaml_decode::{hash_get, load_yaml, yaml_array, yaml_hash, yaml_number, yaml_string};

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
    let version = get_string(root, "version", "root")?;

    let metadata = match hash_get(root, "metadata") {
        Some(value) => Some(parse_project_metadata(value)?),
        None => None,
    };

    let entities_yaml = get_array(root, "entities", "root")?;
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
        project: get_string(hash, "project", "metadata")?,
        description: opt_string(hash, "description", "metadata")?,
        owner: opt_string(hash, "owner", "metadata")?,
        tags: opt_vec_string(hash, "tags", "metadata")?,
    })
}

fn parse_entity(value: &Yaml) -> FloeResult<EntityConfig> {
    let hash = yaml_hash(value, "entity")?;
    let name = get_string(hash, "name", "entity")?;

    let metadata = match hash_get(hash, "metadata") {
        Some(value) => Some(parse_entity_metadata(value)?),
        None => None,
    };

    let source = parse_source(get_value(hash, "source", "entity")?)?;
    let sink = parse_sink(get_value(hash, "sink", "entity")?)?;
    let policy = parse_policy(get_value(hash, "policy", "entity")?)?;
    let schema = parse_schema(get_value(hash, "schema", "entity")?)?;

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
        data_product: opt_string(hash, "data_product", "entity.metadata")?,
        domain: opt_string(hash, "domain", "entity.metadata")?,
        owner: opt_string(hash, "owner", "entity.metadata")?,
        description: opt_string(hash, "description", "entity.metadata")?,
        tags: opt_vec_string(hash, "tags", "entity.metadata")?,
    })
}

fn parse_source(value: &Yaml) -> FloeResult<SourceConfig> {
    let hash = yaml_hash(value, "source")?;
    let options = match hash_get(hash, "options") {
        Some(value) => Some(parse_source_options(value)?),
        None => Some(SourceOptions::default()),
    };

    Ok(SourceConfig {
        format: get_string(hash, "format", "source")?,
        path: get_string(hash, "path", "source")?,
        options,
        cast_mode: opt_string(hash, "cast_mode", "source")?,
    })
}

fn parse_source_options(value: &Yaml) -> FloeResult<SourceOptions> {
    let hash = yaml_hash(value, "source.options")?;
    let defaults = SourceOptions::default();
    Ok(SourceOptions {
        header: opt_bool(hash, "header", "source.options")?.or(defaults.header),
        separator: opt_string(hash, "separator", "source.options")?.or(defaults.separator),
        encoding: opt_string(hash, "encoding", "source.options")?.or(defaults.encoding),
        null_values: opt_vec_string(hash, "null_values", "source.options")?.or(defaults.null_values),
    })
}

fn parse_sink(value: &Yaml) -> FloeResult<SinkConfig> {
    let hash = yaml_hash(value, "sink")?;
    let rejected = match hash_get(hash, "rejected") {
        Some(value) => Some(parse_sink_target(value, "sink.rejected")?),
        None => None,
    };

    Ok(SinkConfig {
        accepted: parse_sink_target(get_value(hash, "accepted", "sink")?, "sink.accepted")?,
        rejected,
        report: parse_report_target(get_value(hash, "report", "sink")?)?,
    })
}

fn parse_sink_target(value: &Yaml, ctx: &str) -> FloeResult<SinkTarget> {
    let hash = yaml_hash(value, ctx)?;
    Ok(SinkTarget {
        format: get_string(hash, "format", ctx)?,
        path: get_string(hash, "path", ctx)?,
    })
}

fn parse_report_target(value: &Yaml) -> FloeResult<ReportTarget> {
    let hash = yaml_hash(value, "sink.report")?;
    Ok(ReportTarget {
        path: get_string(hash, "path", "sink.report")?,
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
        severity: get_string(hash, "severity", "policy")?,
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
        mode: opt_string(hash, "mode", "policy.quarantine")?,
        add_reason_columns: opt_bool(hash, "add_reason_columns", "policy.quarantine")?,
        reason_columns,
    })
}

fn parse_reason_columns(value: &Yaml) -> FloeResult<ReasonColumns> {
    let hash = yaml_hash(value, "policy.quarantine.reason_columns")?;
    Ok(ReasonColumns {
        rule: opt_string(hash, "rule", "policy.quarantine.reason_columns")?,
        column: opt_string(hash, "column", "policy.quarantine.reason_columns")?,
        message: opt_string(hash, "message", "policy.quarantine.reason_columns")?,
    })
}

fn parse_thresholds(value: &Yaml) -> FloeResult<ThresholdsConfig> {
    let hash = yaml_hash(value, "policy.thresholds")?;
    Ok(ThresholdsConfig {
        max_reject_rate: opt_f64(hash, "max_reject_rate", "policy.thresholds")?,
        max_reject_count: opt_u64(hash, "max_reject_count", "policy.thresholds")?,
    })
}

fn parse_schema(value: &Yaml) -> FloeResult<SchemaConfig> {
    let hash = yaml_hash(value, "schema")?;
    let normalize_columns = match hash_get(hash, "normalize_columns") {
        Some(value) => Some(parse_normalize_columns(value)?),
        None => None,
    };
    let columns_yaml = get_array(hash, "columns", "schema")?;

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
        enabled: opt_bool(hash, "enabled", "schema.normalize_columns")?,
        strategy: opt_string(hash, "strategy", "schema.normalize_columns")?,
    })
}

fn parse_column(value: &Yaml) -> FloeResult<ColumnConfig> {
    let hash = yaml_hash(value, "schema.columns")?;
    Ok(ColumnConfig {
        name: get_string(hash, "name", "schema.columns")?,
        column_type: get_string(hash, "type", "schema.columns")?,
        nullable: opt_bool(hash, "nullable", "schema.columns")?,
        unique: opt_bool(hash, "unique", "schema.columns")?,
        unique_strategy: opt_string(hash, "unique_strategy", "schema.columns")?,
    })
}

fn get_value<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Yaml> {
    hash_get(hash, key).ok_or_else(|| {
        Box::new(ConfigError(format!("missing required field {ctx}.{key}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

fn get_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<String> {
    let value = get_value(hash, key, ctx)?;
    yaml_string(value, &format!("{ctx}.{key}"))
}

fn get_array<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Vec<Yaml>> {
    let value = get_value(hash, key, ctx)?;
    yaml_array(value, &format!("{ctx}.{key}"))
}

fn opt_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<String>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => Ok(Some(yaml_string(value, &format!("{ctx}.{key}"))?)),
    }
}

fn opt_vec_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<Vec<String>>> {
    let value = match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => return Ok(None),
        Some(value) => value,
    };
    let list = yaml_array(value, &format!("{ctx}.{key}"))?;
    let mut values = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let item_ctx = format!("{ctx}.{key}[{index}]");
        values.push(yaml_string(item, &item_ctx)?);
    }
    Ok(Some(values))
}

fn opt_bool(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<bool>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Boolean(value) => Ok(Some(*value)),
            _ => Err(Box::new(ConfigError(format!(
                "expected boolean at {ctx}.{key}"
            )))),
        },
    }
}

fn opt_f64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<f64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => yaml_number(value)
            .map(Some)
            .map_err(|_| Box::new(ConfigError(format!("expected number at {ctx}.{key}"))) as _),
    }
}

fn opt_u64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<u64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Integer(raw) if *raw >= 0 => Ok(Some(*raw as u64)),
            _ => Err(Box::new(ConfigError(format!(
                "expected positive integer at {ctx}.{key}"
            )))),
        },
    }
}
