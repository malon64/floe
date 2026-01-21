use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::config::yaml_decode::{
    hash_get, load_yaml, validate_known_keys, yaml_array, yaml_hash, yaml_string,
};
use crate::config::{
    ArchiveTarget, ColumnConfig, EntityConfig, EntityMetadata, NormalizeColumnsConfig,
    PolicyConfig, ProjectMetadata, ReportConfig, RootConfig, SchemaConfig, SchemaMismatchConfig,
    SinkConfig, SinkTarget, SourceConfig, SourceOptions,
};
use crate::{ConfigError, FloeResult};

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
    validate_known_keys(root, "root", &["version", "metadata", "report", "entities"])?;
    let version = get_string(root, "version", "root")?;

    let metadata = match hash_get(root, "metadata") {
        Some(value) => Some(parse_project_metadata(value)?),
        None => None,
    };

    let report = parse_report_config(get_value(root, "report", "root")?)?;
    let entities_yaml = get_array(root, "entities", "root")?;
    let mut entities = Vec::with_capacity(entities_yaml.len());
    for (index, entity_yaml) in entities_yaml.iter().enumerate() {
        let name_hint = entity_name_hint(entity_yaml);
        let entity = parse_entity(entity_yaml).map_err(|err| {
            Box::new(ConfigError(format_entity_error(
                index,
                name_hint,
                err.as_ref(),
            )))
        })?;
        entities.push(entity);
    }

    Ok(RootConfig {
        version,
        metadata,
        report,
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
    validate_known_keys(
        hash,
        "entity",
        &["name", "metadata", "source", "sink", "policy", "schema"],
    )?;
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

fn entity_name_hint(value: &Yaml) -> Option<String> {
    let hash = value.as_hash()?;
    let name = hash_get(hash, "name")?;
    match name {
        Yaml::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn format_entity_error(index: usize, name: Option<String>, err: &dyn std::error::Error) -> String {
    match name {
        Some(name) => format!("entities[{index}] (entity.name={name}): {err}"),
        None => format!("entities[{index}]: {err}"),
    }
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
    validate_known_keys(hash, "source", &["format", "path", "options", "cast_mode"])?;
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
    validate_known_keys(
        hash,
        "source.options",
        &["header", "separator", "encoding", "null_values"],
    )?;
    let defaults = SourceOptions::default();
    Ok(SourceOptions {
        header: opt_bool(hash, "header", "source.options")?.or(defaults.header),
        separator: opt_string(hash, "separator", "source.options")?.or(defaults.separator),
        encoding: opt_string(hash, "encoding", "source.options")?.or(defaults.encoding),
        null_values: opt_vec_string(hash, "null_values", "source.options")?
            .or(defaults.null_values),
    })
}

fn parse_sink(value: &Yaml) -> FloeResult<SinkConfig> {
    let hash = yaml_hash(value, "sink")?;
    validate_known_keys(hash, "sink", &["accepted", "rejected", "archive"])?;
    let rejected = match hash_get(hash, "rejected") {
        Some(value) => Some(parse_sink_target(value, "sink.rejected")?),
        None => None,
    };
    let archive = match hash_get(hash, "archive") {
        Some(value) => Some(parse_archive_target(value)?),
        None => None,
    };

    Ok(SinkConfig {
        accepted: parse_sink_target(get_value(hash, "accepted", "sink")?, "sink.accepted")?,
        rejected,
        archive,
    })
}

fn parse_sink_target(value: &Yaml, ctx: &str) -> FloeResult<SinkTarget> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(hash, ctx, &["format", "path"])?;
    Ok(SinkTarget {
        format: get_string(hash, "format", ctx)?,
        path: get_string(hash, "path", ctx)?,
    })
}

fn parse_report_config(value: &Yaml) -> FloeResult<ReportConfig> {
    let hash = yaml_hash(value, "report")?;
    validate_known_keys(hash, "report", &["path"])?;
    Ok(ReportConfig {
        path: get_string(hash, "path", "report")?,
    })
}

fn parse_archive_target(value: &Yaml) -> FloeResult<ArchiveTarget> {
    let hash = yaml_hash(value, "sink.archive")?;
    validate_known_keys(hash, "sink.archive", &["path"])?;
    Ok(ArchiveTarget {
        path: get_string(hash, "path", "sink.archive")?,
    })
}

fn parse_policy(value: &Yaml) -> FloeResult<PolicyConfig> {
    let hash = yaml_hash(value, "policy")?;
    validate_known_keys(hash, "policy", &["severity"])?;
    Ok(PolicyConfig {
        severity: get_string(hash, "severity", "policy")?,
    })
}

fn parse_schema(value: &Yaml) -> FloeResult<SchemaConfig> {
    let hash = yaml_hash(value, "schema")?;
    validate_known_keys(
        hash,
        "schema",
        &["normalize_columns", "mismatch", "columns"],
    )?;
    let normalize_columns = match hash_get(hash, "normalize_columns") {
        Some(value) => Some(parse_normalize_columns(value)?),
        None => None,
    };
    let mismatch = match hash_get(hash, "mismatch") {
        Some(value) => Some(parse_mismatch(value)?),
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
        mismatch,
        columns,
    })
}

fn parse_normalize_columns(value: &Yaml) -> FloeResult<NormalizeColumnsConfig> {
    let hash = yaml_hash(value, "schema.normalize_columns")?;
    validate_known_keys(hash, "schema.normalize_columns", &["enabled", "strategy"])?;
    Ok(NormalizeColumnsConfig {
        enabled: opt_bool(hash, "enabled", "schema.normalize_columns")?,
        strategy: opt_string(hash, "strategy", "schema.normalize_columns")?,
    })
}

fn parse_mismatch(value: &Yaml) -> FloeResult<SchemaMismatchConfig> {
    let hash = yaml_hash(value, "schema.mismatch")?;
    validate_known_keys(
        hash,
        "schema.mismatch",
        &["missing_columns", "extra_columns"],
    )?;
    Ok(SchemaMismatchConfig {
        missing_columns: opt_string(hash, "missing_columns", "schema.mismatch")?,
        extra_columns: opt_string(hash, "extra_columns", "schema.mismatch")?,
    })
}

fn parse_column(value: &Yaml) -> FloeResult<ColumnConfig> {
    let hash = yaml_hash(value, "schema.columns")?;
    validate_known_keys(
        hash,
        "schema.columns",
        &["name", "type", "nullable", "unique"],
    )?;
    Ok(ColumnConfig {
        name: get_string(hash, "name", "schema.columns")?,
        column_type: get_string(hash, "type", "schema.columns")?,
        nullable: opt_bool(hash, "nullable", "schema.columns")?,
        unique: opt_bool(hash, "unique", "schema.columns")?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_config(contents: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        path.push(format!("floe-config-{nanos}.yml"));
        fs::write(&path, contents).expect("write temp config");
        path
    }

    #[test]
    fn parse_config_loads_report_and_defaults() {
        let yaml = r#"
version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
"#;
        let path = write_temp_config(yaml);
        let config = parse_config(&path).expect("parse config");

        assert_eq!(config.report.path, "/tmp/reports");
        assert_eq!(config.entities.len(), 1);
        let entity = &config.entities[0];
        let options = entity.source.options.as_ref().expect("options");
        assert_eq!(options.header, Some(true));
        assert_eq!(options.separator.as_deref(), Some(";"));
        assert_eq!(options.encoding.as_deref(), Some("UTF8"));
    }
}
