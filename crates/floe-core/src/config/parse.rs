use std::collections::HashMap;
use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::config::apply_templates;
use crate::config::yaml_decode::{
    hash_get, load_yaml, validate_known_keys, yaml_array, yaml_hash, yaml_string,
};
use crate::config::{
    ArchiveTarget, ColumnConfig, DomainConfig, EntityConfig, EntityMetadata, EnvConfig,
    NormalizeColumnsConfig, PolicyConfig, ProjectMetadata, ReportConfig, RootConfig, SchemaConfig,
    SchemaMismatchConfig, SinkConfig, SinkOptions, SinkTarget, SourceConfig, SourceOptions,
    StorageDefinition, StoragesConfig,
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
    let mut config = parse_root(&docs[0])?;
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    apply_templates(&mut config, config_dir)?;
    Ok(config)
}

fn parse_root(doc: &Yaml) -> FloeResult<RootConfig> {
    let root = yaml_hash(doc, "root")?;
    validate_known_keys(
        root,
        "root",
        &[
            "version",
            "metadata",
            "storages",
            "filesystems",
            "env",
            "domains",
            "report",
            "entities",
        ],
    )?;
    let version = get_string(root, "version", "root")?;

    let metadata = match hash_get(root, "metadata") {
        Some(value) => Some(parse_project_metadata(value)?),
        None => None,
    };

    let storages = match (hash_get(root, "storages"), hash_get(root, "filesystems")) {
        (Some(_), Some(_)) => {
            return Err(Box::new(ConfigError(
                "root.storages and root.filesystems are mutually exclusive".to_string(),
            )))
        }
        (Some(value), None) => Some(parse_storages(value)?),
        (None, Some(value)) => Some(parse_storages(value)?),
        (None, None) => None,
    };

    let env = match hash_get(root, "env") {
        Some(value) => Some(parse_env(value)?),
        None => None,
    };

    let domains = match hash_get(root, "domains") {
        Some(value) => parse_domains(value)?,
        None => Vec::new(),
    };

    let report = match hash_get(root, "report") {
        Some(value) => Some(parse_report_config(value)?),
        None => Some(ReportConfig {
            path: "report".to_string(),
            formatter: None,
            storage: None,
        }),
    };
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
        storages,
        env,
        domains,
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
        &[
            "name", "metadata", "domain", "source", "sink", "policy", "schema",
        ],
    )?;
    let name = get_string(hash, "name", "entity")?;

    let metadata = match hash_get(hash, "metadata") {
        Some(value) => Some(parse_entity_metadata(value)?),
        None => None,
    };
    let domain = opt_string(hash, "domain", "entity")?;

    let source = parse_source(get_value(hash, "source", "entity")?)?;
    let sink = parse_sink(get_value(hash, "sink", "entity")?)?;
    let policy = parse_policy(get_value(hash, "policy", "entity")?)?;
    let schema = parse_schema(get_value(hash, "schema", "entity")?)?;

    Ok(EntityConfig {
        name,
        metadata,
        domain,
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

fn parse_env(value: &Yaml) -> FloeResult<EnvConfig> {
    let hash = yaml_hash(value, "env")?;
    validate_known_keys(hash, "env", &["file", "vars"])?;
    let file = opt_string(hash, "file", "env")?;
    let vars = match hash_get(hash, "vars") {
        Some(value) => parse_string_map(value, "env.vars")?,
        None => HashMap::new(),
    };
    Ok(EnvConfig { file, vars })
}

fn parse_domains(value: &Yaml) -> FloeResult<Vec<DomainConfig>> {
    let array = yaml_array(value, "domains")?;
    let mut domains = Vec::with_capacity(array.len());
    for item in array.iter() {
        let hash = yaml_hash(item, "domains")?;
        validate_known_keys(hash, "domains", &["name", "incoming_dir"])?;
        let name = get_string(hash, "name", "domains")?;
        let incoming_dir = get_string(hash, "incoming_dir", "domains")?;
        domains.push(DomainConfig {
            name,
            incoming_dir,
            resolved_incoming_dir: None,
        });
    }
    Ok(domains)
}

fn parse_string_map(value: &Yaml, context: &str) -> FloeResult<HashMap<String, String>> {
    let hash = yaml_hash(value, context)?;
    let mut map = HashMap::new();
    for (key, val) in hash {
        let key_str = yaml_string(key, context)?;
        let value_str = yaml_string(val, context)?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}

fn parse_source(value: &Yaml) -> FloeResult<SourceConfig> {
    let hash = yaml_hash(value, "source")?;
    validate_known_keys(
        hash,
        "source",
        &[
            "format",
            "path",
            "storage",
            "filesystem",
            "options",
            "cast_mode",
        ],
    )?;
    let options = match hash_get(hash, "options") {
        Some(value) => Some(parse_source_options(value)?),
        None => Some(SourceOptions::default()),
    };

    let storage = opt_string(hash, "storage", "source")?;
    let filesystem = opt_string(hash, "filesystem", "source")?;
    if storage.is_some() && filesystem.is_some() {
        return Err(Box::new(ConfigError(
            "source.storage and source.storage are mutually exclusive".to_string(),
        )));
    }

    Ok(SourceConfig {
        format: get_string(hash, "format", "source")?,
        path: get_string(hash, "path", "source")?,
        storage: storage.or(filesystem),
        options,
        cast_mode: opt_string(hash, "cast_mode", "source")?,
    })
}

fn parse_source_options(value: &Yaml) -> FloeResult<SourceOptions> {
    let hash = yaml_hash(value, "source.options")?;
    validate_known_keys(
        hash,
        "source.options",
        &[
            "header",
            "separator",
            "encoding",
            "null_values",
            "recursive",
            "glob",
            "json_mode",
        ],
    )?;
    let defaults = SourceOptions::default();
    Ok(SourceOptions {
        header: opt_bool(hash, "header", "source.options")?.or(defaults.header),
        separator: opt_string(hash, "separator", "source.options")?.or(defaults.separator),
        encoding: opt_string(hash, "encoding", "source.options")?.or(defaults.encoding),
        null_values: opt_vec_string(hash, "null_values", "source.options")?
            .or(defaults.null_values),
        recursive: opt_bool(hash, "recursive", "source.options")?.or(defaults.recursive),
        glob: opt_string(hash, "glob", "source.options")?.or(defaults.glob),
        json_mode: opt_string(hash, "json_mode", "source.options")?.or(defaults.json_mode),
    })
}

fn parse_sink(value: &Yaml) -> FloeResult<SinkConfig> {
    let hash = yaml_hash(value, "sink")?;
    validate_known_keys(hash, "sink", &["accepted", "rejected", "archive"])?;
    let rejected = match hash_get(hash, "rejected") {
        Some(value) => Some(parse_sink_target(value, "sink.rejected", false)?),
        None => None,
    };
    let archive = match hash_get(hash, "archive") {
        Some(value) => Some(parse_archive_target(value)?),
        None => None,
    };

    Ok(SinkConfig {
        accepted: parse_sink_target(get_value(hash, "accepted", "sink")?, "sink.accepted", true)?,
        rejected,
        archive,
    })
}

fn parse_sink_target(value: &Yaml, ctx: &str, allow_options: bool) -> FloeResult<SinkTarget> {
    let hash = yaml_hash(value, ctx)?;
    let mut allowed = vec!["format", "path", "storage", "filesystem"];
    if allow_options {
        allowed.push("options");
    }
    validate_known_keys(hash, ctx, &allowed)?;
    let storage = opt_string(hash, "storage", ctx)?;
    let filesystem = opt_string(hash, "filesystem", ctx)?;
    if storage.is_some() && filesystem.is_some() {
        return Err(Box::new(ConfigError(format!(
            "{ctx}.storage and {ctx}.storage are mutually exclusive"
        ))));
    }
    let options = if allow_options {
        match hash_get(hash, "options") {
            Some(value) => Some(parse_sink_options(value, &format!("{ctx}.options"))?),
            None => None,
        }
    } else {
        None
    };
    Ok(SinkTarget {
        format: get_string(hash, "format", ctx)?,
        path: get_string(hash, "path", ctx)?,
        storage: storage.or(filesystem),
        options,
    })
}

fn parse_sink_options(value: &Yaml, ctx: &str) -> FloeResult<SinkOptions> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(
        hash,
        ctx,
        &["compression", "row_group_size", "max_size_per_file"],
    )?;
    Ok(SinkOptions {
        compression: opt_string(hash, "compression", ctx)?,
        row_group_size: opt_u64(hash, "row_group_size", ctx)?,
        max_size_per_file: opt_u64(hash, "max_size_per_file", ctx)?,
    })
}

fn parse_report_config(value: &Yaml) -> FloeResult<ReportConfig> {
    let hash = yaml_hash(value, "report")?;
    validate_known_keys(hash, "report", &["path", "formatter", "storage"])?;
    let path = opt_string(hash, "path", "report")?.unwrap_or_else(|| "report".to_string());
    Ok(ReportConfig {
        path,
        formatter: opt_string(hash, "formatter", "report")?,
        storage: opt_string(hash, "storage", "report")?,
    })
}

fn parse_storages(value: &Yaml) -> FloeResult<StoragesConfig> {
    let hash = yaml_hash(value, "storages")?;
    validate_known_keys(hash, "storages", &["default", "definitions"])?;
    let definitions_yaml = match hash_get(hash, "definitions") {
        Some(value) => yaml_array(value, "storages.definitions")?,
        None => {
            return Err(Box::new(ConfigError(
                "missing required field storages.definitions".to_string(),
            )))
        }
    };
    let mut definitions = Vec::with_capacity(definitions_yaml.len());
    for (index, item) in definitions_yaml.iter().enumerate() {
        let definition = parse_storage_definition(item).map_err(|err| {
            Box::new(ConfigError(format!("storages.definitions[{index}]: {err}")))
        })?;
        definitions.push(definition);
    }
    Ok(StoragesConfig {
        default: opt_string(hash, "default", "storages")?,
        definitions,
    })
}

fn parse_storage_definition(value: &Yaml) -> FloeResult<StorageDefinition> {
    let hash = yaml_hash(value, "storages.definitions")?;
    validate_known_keys(
        hash,
        "storages.definitions",
        &[
            "name",
            "type",
            "bucket",
            "region",
            "account",
            "container",
            "prefix",
        ],
    )?;
    Ok(StorageDefinition {
        name: get_string(hash, "name", "storages.definitions")?,
        fs_type: get_string(hash, "type", "storages.definitions")?,
        bucket: opt_string(hash, "bucket", "storages.definitions")?,
        region: opt_string(hash, "region", "storages.definitions")?,
        account: opt_string(hash, "account", "storages.definitions")?,
        container: opt_string(hash, "container", "storages.definitions")?,
        prefix: opt_string(hash, "prefix", "storages.definitions")?,
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

fn opt_u64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<u64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Integer(raw) => {
                if *raw < 0 {
                    return Err(Box::new(ConfigError(format!(
                        "expected positive integer at {ctx}.{key}"
                    ))));
                }
                Ok(Some(*raw as u64))
            }
            _ => Err(Box::new(ConfigError(format!(
                "expected integer at {ctx}.{key}"
            )))),
        },
    }
}
