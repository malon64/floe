use std::collections::HashSet;

use crate::config::{
    CatalogDefinition, EntityConfig, RootConfig, SourceOptions, StorageDefinition,
};
use crate::io::format;
use crate::io::read::json_selector::parse_selector;
use crate::io::read::xml_selector;
use crate::{warnings, ConfigError, FloeResult};

const ALLOWED_COLUMN_TYPES: &[&str] = &["string", "number", "boolean", "datetime", "date", "time"];
const ALLOWED_CAST_MODES: &[&str] = &["strict", "coerce"];
const ALLOWED_NORMALIZE_STRATEGIES: &[&str] = &["snake_case", "lower", "camel_case", "none"];
const ALLOWED_POLICY_SEVERITIES: &[&str] = &["warn", "reject", "abort"];
const ALLOWED_MISSING_POLICIES: &[&str] = &["reject_file", "fill_nulls"];
const ALLOWED_EXTRA_POLICIES: &[&str] = &["reject_file", "ignore"];
const ALLOWED_STORAGE_TYPES: &[&str] = &["local", "s3", "adls", "gcs"];
const ALLOWED_CATALOG_TYPES: &[&str] = &["glue"];
const ALLOWED_ICEBERG_PARTITION_TRANSFORMS: &[&str] = &["identity", "year", "month", "day", "hour"];
const ALLOWED_CONFIG_VERSIONS: &[&str] = &["0.1", "0.2"];
const MAX_JSON_COLUMNS: usize = 1024;

pub(crate) fn validate_config(config: &RootConfig) -> FloeResult<()> {
    validate_version(config)?;

    if config.entities.is_empty() {
        return Err(Box::new(ConfigError(
            "entities list is empty (at least one entity is required)".to_string(),
        )));
    }

    let storage_registry = StorageRegistry::new(config)?;
    let catalog_registry = CatalogRegistry::new(config, &storage_registry)?;
    if let Some(report) = &config.report {
        validate_report(report, &storage_registry)?;
    }

    let mut names = HashSet::new();
    for entity in &config.entities {
        validate_entity(
            entity,
            &config.version,
            &storage_registry,
            &catalog_registry,
        )?;
        if !names.insert(entity.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} is duplicated in config",
                entity.name
            ))));
        }
    }

    Ok(())
}

fn validate_version(config: &RootConfig) -> FloeResult<()> {
    if !ALLOWED_CONFIG_VERSIONS.contains(&config.version.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "root.version={} is unsupported (allowed: {})",
            config.version,
            ALLOWED_CONFIG_VERSIONS.join(", ")
        ))));
    }
    Ok(())
}

fn validate_report(
    report: &crate::config::ReportConfig,
    storages: &StorageRegistry,
) -> FloeResult<()> {
    let storage_name = storages.resolve_report_name(report.storage.as_deref())?;
    storages.validate_report_reference("report.storage", &storage_name)?;
    Ok(())
}

fn validate_entity(
    entity: &EntityConfig,
    config_version: &str,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
) -> FloeResult<()> {
    validate_source(entity, storages)?;
    validate_policy(entity)?;
    validate_sink(entity, storages, catalogs)?;
    validate_schema(entity, config_version)?;
    Ok(())
}

fn validate_source(entity: &EntityConfig, storages: &StorageRegistry) -> FloeResult<()> {
    format::ensure_input_format(&entity.name, entity.source.format.as_str())?;

    if let Some(cast_mode) = &entity.source.cast_mode {
        if !ALLOWED_CAST_MODES.contains(&cast_mode.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.cast_mode={} is unsupported (allowed: {})",
                entity.name,
                cast_mode,
                ALLOWED_CAST_MODES.join(", ")
            ))));
        }
    }

    let storage_name =
        storages.resolve_name(entity, "source.storage", entity.source.storage.as_deref())?;
    storages.validate_reference(entity, "source.storage", &storage_name)?;
    if entity.source.format == "json" {
        let options = entity.source.options.as_ref();
        let mode = options
            .and_then(|options| options.json_mode.as_deref())
            .unwrap_or("array");
        match mode {
            "array" | "ndjson" => {}
            _ => {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.options.json_mode={} is unsupported (allowed: array, ndjson)",
                    entity.name, mode
                ))));
            }
        }
    }
    if entity.source.format == "xlsx" {
        let default_options = SourceOptions::default();
        let options = entity.source.options.as_ref().unwrap_or(&default_options);
        if let Some(sheet) = options.sheet.as_ref() {
            if sheet.trim().is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.options.sheet must not be empty",
                    entity.name
                ))));
            }
        }
        let header_row = options.header_row.unwrap_or(1);
        if header_row == 0 {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.options.header_row must be greater than 0",
                entity.name
            ))));
        }
        let data_row = options.data_row.unwrap_or(header_row + 1);
        if data_row <= header_row {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.options.data_row must be greater than source.options.header_row",
                entity.name
            ))));
        }
    }
    if entity.source.format == "xml" {
        let options = entity.source.options.as_ref();
        let row_tag = options
            .and_then(|options| options.row_tag.as_deref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());
        if row_tag.is_none() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.options.row_tag is required for xml input",
                entity.name
            ))));
        }
        if let Some(namespace) = options
            .and_then(|options| options.namespace.as_deref())
            .map(|value| value.trim())
        {
            if namespace.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.options.namespace must not be empty",
                    entity.name
                ))));
            }
        }
        if let Some(value_tag) = options
            .and_then(|options| options.value_tag.as_deref())
            .map(|value| value.trim())
        {
            if value_tag.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.options.value_tag must not be empty",
                    entity.name
                ))));
            }
        }
    }

    let _ = storages.definition_type(&storage_name);

    Ok(())
}

fn validate_sink(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
) -> FloeResult<()> {
    format::ensure_accepted_sink_format(&entity.name, entity.sink.accepted.format.as_str())?;
    format::validate_sink_options(
        &entity.name,
        entity.sink.accepted.format.as_str(),
        entity.sink.accepted.options.as_ref(),
    )?;

    if entity.policy.severity == "reject" && entity.sink.rejected.is_none() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected is required when policy.severity=reject",
            entity.name
        ))));
    }

    if let Some(rejected) = &entity.sink.rejected {
        format::ensure_rejected_sink_format(&entity.name, rejected.format.as_str())?;
    }

    let source_storage =
        storages.resolve_name(entity, "source.storage", entity.source.storage.as_deref())?;
    let accepted_storage = storages.resolve_name(
        entity,
        "sink.accepted.storage",
        entity.sink.accepted.storage.as_deref(),
    )?;
    storages.validate_reference(entity, "sink.accepted.storage", &accepted_storage)?;
    if entity.sink.accepted.format == "delta" {
        if let Some(storage_type) = storages.definition_type(&accepted_storage) {
            if storage_type != "local"
                && storage_type != "s3"
                && storage_type != "adls"
                && storage_type != "gcs"
            {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.format=delta is only supported on local, s3, adls, or gcs storage (got {})",
                    entity.name, storage_type
                ))));
            }
        }
    }
    if entity.sink.accepted.format == "iceberg" {
        if let Some(storage_type) = storages.definition_type(&accepted_storage) {
            if storage_type != "local" && storage_type != "s3" && storage_type != "gcs" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.format=iceberg is only supported on local, s3, or gcs storage for now (got {})",
                    entity.name, storage_type
                ))));
            }
        }
    }
    validate_iceberg_catalog_binding(entity, storages, catalogs, &accepted_storage)?;

    let _ = storages.definition_type(&accepted_storage);

    validate_sink_partitioning(entity)?;
    validate_sink_write_mode(entity)?;

    if let Some(rejected) = &entity.sink.rejected {
        let rejected_storage =
            storages.resolve_name(entity, "sink.rejected.storage", rejected.storage.as_deref())?;
        storages.validate_reference(entity, "sink.rejected.storage", &rejected_storage)?;
    }

    if let Some(archive) = &entity.sink.archive {
        let archive_storage = archive.storage.as_deref().unwrap_or(&source_storage);
        if let Some(storage) = archive.storage.as_deref() {
            if storage != source_storage {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.archive.storage must match source.storage ({})",
                    entity.name, source_storage
                ))));
            }
        }
        storages.validate_reference(entity, "sink.archive.storage", archive_storage)?;
    }

    Ok(())
}

fn validate_sink_write_mode(entity: &EntityConfig) -> FloeResult<()> {
    let write_mode = entity.sink.resolved_write_mode();
    let is_merge_mode = matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    );
    if is_merge_mode {
        let mode_name = write_mode.as_str();
        if entity.sink.accepted.format != "delta" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.write_mode={} requires sink.accepted.format=delta",
                entity.name, mode_name
            ))));
        }

        let primary_key = entity.schema.primary_key.as_ref().ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} sink.write_mode={} requires schema.primary_key",
                entity.name, mode_name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        if primary_key.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.write_mode={} requires non-empty schema.primary_key",
                entity.name, mode_name
            ))));
        }
    }

    validate_merge_options(entity, write_mode)
}

fn validate_merge_options(
    entity: &EntityConfig,
    write_mode: crate::config::WriteMode,
) -> FloeResult<()> {
    let Some(merge) = entity.sink.accepted.merge.as_ref() else {
        return Ok(());
    };

    if entity.sink.accepted.format != "delta" {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.merge is only supported when sink.accepted.format=delta",
            entity.name
        ))));
    }
    if !matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    ) {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.merge is only supported with sink.write_mode=merge_scd1 or merge_scd2",
            entity.name
        ))));
    }

    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let normalize_strategy = if entity
        .schema
        .normalize_columns
        .as_ref()
        .and_then(|normalize| normalize.enabled)
        .unwrap_or(false)
    {
        entity
            .schema
            .normalize_columns
            .as_ref()
            .and_then(|normalize| normalize.strategy.as_deref())
            .or(Some("snake_case"))
    } else {
        None
    };
    let resolved_output_columns = crate::checks::normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize_strategy,
    );
    let resolved_output_column_names = resolved_output_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let primary_key_columns = entity
        .schema
        .primary_key
        .as_ref()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.trim().to_string())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    if let Some(ignore_columns) = merge.ignore_columns.as_ref() {
        validate_merge_column_list(
            entity,
            "sink.accepted.merge.ignore_columns",
            ignore_columns,
            &schema_columns,
        )?;
        for (index, column_name) in ignore_columns.iter().enumerate() {
            let value = column_name.trim();
            if primary_key_columns.contains(value) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.merge.ignore_columns[{}]={} cannot reference schema.primary_key column",
                    entity.name, index, value
                ))));
            }
        }
    }

    if let Some(compare_columns) = merge.compare_columns.as_ref() {
        validate_merge_column_list(
            entity,
            "sink.accepted.merge.compare_columns",
            compare_columns,
            &schema_columns,
        )?;
        for (index, column_name) in compare_columns.iter().enumerate() {
            let value = column_name.trim();
            if primary_key_columns.contains(value) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.merge.compare_columns[{}]={} cannot reference schema.primary_key column",
                    entity.name, index, value
                ))));
            }
        }
    }

    if let Some(scd2) = merge.scd2.as_ref() {
        if write_mode != crate::config::WriteMode::MergeScd2 {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_scd2",
                entity.name
            ))));
        }
        let current_flag_column = scd2
            .current_flag_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_CURRENT_FLAG_COLUMN)
            .trim();
        let valid_from_column = scd2
            .valid_from_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_VALID_FROM_COLUMN)
            .trim();
        let valid_to_column = scd2
            .valid_to_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_VALID_TO_COLUMN)
            .trim();
        let resolved_columns = [
            ("current_flag_column", current_flag_column),
            ("valid_from_column", valid_from_column),
            ("valid_to_column", valid_to_column),
        ];
        for (field, value) in resolved_columns {
            if value.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.merge.scd2.{} must not be empty",
                    entity.name, field
                ))));
            }
            if resolved_output_column_names.contains(value) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.merge.scd2.{}={} collides with schema column name",
                    entity.name, field, value
                ))));
            }
        }
        let unique_columns = resolved_columns
            .iter()
            .map(|(_, value)| *value)
            .collect::<HashSet<_>>();
        if unique_columns.len() != resolved_columns.len() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.merge.scd2 column names must be unique",
                entity.name
            ))));
        }
    }

    Ok(())
}

fn validate_merge_column_list(
    entity: &EntityConfig,
    field: &str,
    values: &[String],
    schema_columns: &HashSet<&str>,
) -> FloeResult<()> {
    let mut seen = HashSet::new();
    for (index, value) in values.iter().enumerate() {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} {}[{}] must not be empty",
                entity.name, field, index
            ))));
        }
        if !schema_columns.contains(trimmed) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} {}[{}]={} references unknown schema column",
                entity.name, field, index, trimmed
            ))));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} {} has duplicate column {}",
                entity.name, field, trimmed
            ))));
        }
    }
    Ok(())
}

fn validate_iceberg_catalog_binding(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
    accepted_storage: &str,
) -> FloeResult<()> {
    let accepted = &entity.sink.accepted;
    if accepted.format != "iceberg" {
        if accepted.iceberg.is_some() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.iceberg is only supported for sink.accepted.format=iceberg",
                entity.name
            ))));
        }
        return Ok(());
    }

    let Some(iceberg_cfg) = accepted.iceberg.as_ref() else {
        return Ok(());
    };

    let accepted_storage_type = storages
        .definition_type(accepted_storage)
        .unwrap_or("local");
    if accepted_storage_type != "s3" {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.iceberg.catalog requires sink.accepted storage type s3 (got {})",
            entity.name, accepted_storage_type
        ))));
    }

    let catalog_name = if let Some(name) = iceberg_cfg.catalog.as_deref() {
        name.to_string()
    } else if let Some(name) = catalogs.default_name() {
        name.to_string()
    } else {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.iceberg.catalog is required (or set catalogs.default)",
            entity.name
        ))));
    };

    let definition = catalogs.definition(&catalog_name).ok_or_else(|| {
        Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.iceberg.catalog references unknown catalog {}",
            entity.name, catalog_name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if definition.catalog_type != "glue" {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.iceberg.catalog={} uses unsupported catalog type {} (allowed: glue)",
            entity.name, catalog_name, definition.catalog_type
        ))));
    }
    if let Some(storage_name) = definition.warehouse_storage.as_deref() {
        let storage_type = storages.definition_type(storage_name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "catalogs.definitions name={} warehouse_storage references unknown storage {}",
                definition.name, storage_name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        if storage_type != "s3" {
            return Err(Box::new(ConfigError(format!(
                "catalogs.definitions name={} warehouse_storage must reference s3 storage for glue catalog (got {})",
                definition.name, storage_type
            ))));
        }
    }

    Ok(())
}

fn validate_sink_partitioning(entity: &EntityConfig) -> FloeResult<()> {
    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let accepted = &entity.sink.accepted;

    if accepted.partition_by.is_some() && accepted.partition_spec.is_some() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.partition_by and sink.accepted.partition_spec are mutually exclusive",
            entity.name
        ))));
    }

    if let Some(partition_by) = &accepted.partition_by {
        if accepted.format != "delta" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.partition_by is only supported for sink.accepted.format=delta",
                entity.name
            ))));
        }
        if partition_by.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.partition_by must not be empty",
                entity.name
            ))));
        }
        let mut seen = HashSet::new();
        for (index, column) in partition_by.iter().enumerate() {
            let value = column.trim();
            if value.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_by[{}] must not be empty",
                    entity.name, index
                ))));
            }
            if !seen.insert(value.to_string()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_by has duplicate column {}",
                    entity.name, value
                ))));
            }
            if !schema_columns.contains(value) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_by[{}]={} references unknown schema column",
                    entity.name, index, value
                ))));
            }
        }
    }

    if let Some(partition_spec) = &accepted.partition_spec {
        if accepted.format != "iceberg" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.partition_spec is only supported for sink.accepted.format=iceberg",
                entity.name
            ))));
        }
        if partition_spec.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.partition_spec must not be empty",
                entity.name
            ))));
        }
        let mut seen = HashSet::new();
        for (index, field) in partition_spec.iter().enumerate() {
            let column = field.column.trim();
            if column.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].column must not be empty",
                    entity.name, index
                ))));
            }
            if !schema_columns.contains(column) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].column={} references unknown schema column",
                    entity.name, index, column
                ))));
            }
            let transform = field.transform.trim().to_ascii_lowercase();
            if !ALLOWED_ICEBERG_PARTITION_TRANSFORMS.contains(&transform.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].transform={} is unsupported (allowed: {})",
                    entity.name,
                    index,
                    field.transform,
                    ALLOWED_ICEBERG_PARTITION_TRANSFORMS.join(", ")
                ))));
            }
            if !seen.insert((column.to_string(), transform.clone())) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.partition_spec has duplicate entry ({}, {})",
                    entity.name, column, transform
                ))));
            }
        }
    }

    Ok(())
}

fn validate_policy(entity: &EntityConfig) -> FloeResult<()> {
    if !ALLOWED_POLICY_SEVERITIES.contains(&entity.policy.severity.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} policy.severity={} is unsupported (allowed: {})",
            entity.name,
            entity.policy.severity,
            ALLOWED_POLICY_SEVERITIES.join(", ")
        ))));
    }
    Ok(())
}

fn validate_schema(entity: &EntityConfig, config_version: &str) -> FloeResult<()> {
    if entity.source.format == "json" && entity.schema.columns.len() > MAX_JSON_COLUMNS {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} schema.columns has {} entries which exceeds the JSON selector limit of {}",
            entity.name,
            entity.schema.columns.len(),
            MAX_JSON_COLUMNS
        ))));
    }
    if entity.source.format == "fixed" {
        for (index, column) in entity.schema.columns.iter().enumerate() {
            let width = column.width.ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} schema.columns[{}].width is required for source.format=fixed",
                    entity.name, index
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            if width == 0 {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.columns[{}].width must be greater than 0",
                    entity.name, index
                ))));
            }
        }
    }
    if let Some(normalize) = &entity.schema.normalize_columns {
        if let Some(strategy) = &normalize.strategy {
            if !is_allowed_normalize_strategy(strategy) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.normalize_columns.strategy={} is unsupported (allowed: {})",
                    entity.name,
                    strategy,
                    ALLOWED_NORMALIZE_STRATEGIES.join(", ")
                ))));
            }
        }
    }

    if let Some(mismatch) = &entity.schema.mismatch {
        if let Some(policy) = &mismatch.missing_columns {
            if !ALLOWED_MISSING_POLICIES.contains(&policy.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.mismatch.missing_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_MISSING_POLICIES.join(", ")
                ))));
            }
        }
        if let Some(policy) = &mismatch.extra_columns {
            if !ALLOWED_EXTRA_POLICIES.contains(&policy.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.mismatch.extra_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_EXTRA_POLICIES.join(", ")
                ))));
            }
        }
    }

    for (index, column) in entity.schema.columns.iter().enumerate() {
        if canonical_column_type(&column.column_type).is_none() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.columns[{}].type={} is unsupported for column {} (allowed: {})",
                entity.name,
                index,
                column.column_type,
                column.name,
                ALLOWED_COLUMN_TYPES.join(", ")
            ))));
        }
        if entity.source.format == "json" {
            let selector = column.source_or_name();
            if let Err(err) = parse_selector(selector) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.columns[{}].source={} is invalid: {}",
                    entity.name, index, selector, err.message
                ))));
            }
        }
        if entity.source.format == "xml" {
            let selector = column.source_or_name();
            if let Err(err) = xml_selector::parse_selector(selector) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.columns[{}].source={} is invalid: {}",
                    entity.name, index, selector, err.message
                ))));
            }
        }
    }

    validate_schema_primary_key(entity)?;
    validate_schema_unique_keys(entity)?;
    validate_schema_evolution(entity, config_version)?;

    Ok(())
}

fn validate_schema_evolution(entity: &EntityConfig, config_version: &str) -> FloeResult<()> {
    let Some(schema_evolution) = entity.schema.schema_evolution else {
        return Ok(());
    };

    if config_version != "0.2" {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} schema.schema_evolution requires root.version=\"0.2\"",
            entity.name
        ))));
    }

    if entity.sink.accepted.format != "delta"
        && schema_evolution.mode == crate::config::SchemaEvolutionMode::AddColumns
    {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} schema.schema_evolution.mode={} requires sink.accepted.format=delta",
            entity.name,
            schema_evolution.mode.as_str()
        ))));
    }

    Ok(())
}

fn validate_schema_primary_key(entity: &EntityConfig) -> FloeResult<()> {
    let Some(primary_key) = entity.schema.primary_key.as_ref() else {
        return Ok(());
    };
    if primary_key.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} schema.primary_key must not be empty",
            entity.name
        ))));
    }
    let mut seen = HashSet::new();
    for (index, column_name) in primary_key.iter().enumerate() {
        let value = column_name.trim();
        if value.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.primary_key[{}] must not be empty",
                entity.name, index
            ))));
        }
        if !seen.insert(value.to_string()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.primary_key has duplicate column {}",
                entity.name, value
            ))));
        }
        let column = entity
            .schema
            .columns
            .iter()
            .find(|column| column.name == value)
            .ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} schema.primary_key[{}]={} references unknown schema column",
                    entity.name, index, value
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        if column.nullable == Some(true) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.primary_key column {} cannot set nullable=true",
                entity.name, value
            ))));
        }
    }
    Ok(())
}

fn validate_schema_unique_keys(entity: &EntityConfig) -> FloeResult<()> {
    let Some(unique_keys) = entity.schema.unique_keys.as_ref() else {
        return Ok(());
    };
    if unique_keys.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} schema.unique_keys must not be empty",
            entity.name
        ))));
    }

    let has_legacy_unique = entity
        .schema
        .columns
        .iter()
        .any(|column| column.unique == Some(true));
    if has_legacy_unique {
        warnings::emit(
            "validate",
            Some(&entity.name),
            None,
            Some("schema_unique_keys_override"),
            &format!(
                "entity.name={} schema.unique_keys is set; schema.columns[].unique is ignored",
                entity.name
            ),
        );
    }

    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let mut seen_constraints = HashSet::new();
    let mut duplicate_constraints = 0_u64;
    for (index, key) in unique_keys.iter().enumerate() {
        if key.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.unique_keys[{}] must not be empty",
                entity.name, index
            ))));
        }
        let mut seen_columns = HashSet::new();
        let mut signature_parts = Vec::with_capacity(key.len());
        for (column_index, column_name) in key.iter().enumerate() {
            let value = column_name.trim();
            if value.is_empty() {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.unique_keys[{}][{}] must not be empty",
                    entity.name, index, column_index
                ))));
            }
            if !schema_columns.contains(value) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.unique_keys[{}][{}]={} references unknown schema column",
                    entity.name, index, column_index, value
                ))));
            }
            if !seen_columns.insert(value.to_string()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.unique_keys[{}] has duplicate column {}",
                    entity.name, index, value
                ))));
            }
            signature_parts.push(value.to_string());
        }
        let signature = signature_parts.join("\u{1f}");
        if !seen_constraints.insert(signature) {
            duplicate_constraints += 1;
        }
    }

    if duplicate_constraints > 0 {
        warnings::emit(
            "validate",
            Some(&entity.name),
            None,
            Some("schema_unique_keys_dedup"),
            &format!(
                "entity.name={} schema.unique_keys contains {} duplicated constraint(s); duplicates are ignored",
                entity.name, duplicate_constraints
            ),
        );
    }

    Ok(())
}

fn is_allowed_normalize_strategy(value: &str) -> bool {
    let normalized = normalize_value(value);
    ALLOWED_NORMALIZE_STRATEGIES
        .iter()
        .any(|allowed| normalize_value(allowed).as_str() == normalized)
}

fn canonical_column_type(value: &str) -> Option<&'static str> {
    let normalized = normalize_value(value);
    match normalized.as_str() {
        "string" | "str" | "text" => Some("string"),
        "number" | "float" | "float32" | "float64" | "double" | "decimal" | "int" | "int8"
        | "int16" | "int32" | "int64" | "integer" | "long" | "uint8" | "uint16" | "uint32"
        | "uint64" => Some("number"),
        "boolean" | "bool" => Some("boolean"),
        "datetime" | "timestamp" => Some("datetime"),
        "date" => Some("date"),
        "time" => Some("time"),
        _ => None,
    }
}

fn normalize_value(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', '_'], "")
}

struct StorageRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, StorageDefinition>,
}

struct CatalogRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, CatalogDefinition>,
}

impl CatalogRegistry {
    fn new(config: &RootConfig, storages: &StorageRegistry) -> FloeResult<Self> {
        let Some(catalogs) = &config.catalogs else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if catalogs.definitions.is_empty() {
            return Err(Box::new(ConfigError(
                "catalogs.definitions must not be empty".to_string(),
            )));
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &catalogs.definitions {
            if !ALLOWED_CATALOG_TYPES.contains(&definition.catalog_type.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "catalogs.definitions name={} type={} is unsupported (allowed: {})",
                    definition.name,
                    definition.catalog_type,
                    ALLOWED_CATALOG_TYPES.join(", ")
                ))));
            }
            if definition.catalog_type == "glue" {
                if definition.region.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "catalogs.definitions name={} requires region for type glue",
                        definition.name
                    ))));
                }
                if definition.database.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "catalogs.definitions name={} requires database for type glue",
                        definition.name
                    ))));
                }
                if let Some(storage_name) = definition.warehouse_storage.as_deref() {
                    let storage_type = storages.definition_type(storage_name).ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "catalogs.definitions name={} warehouse_storage references unknown storage {}",
                            definition.name, storage_name
                        ))) as Box<dyn std::error::Error + Send + Sync>
                    })?;
                    if storage_type != "s3" {
                        return Err(Box::new(ConfigError(format!(
                            "catalogs.definitions name={} warehouse_storage must reference s3 storage for glue catalog (got {})",
                            definition.name, storage_type
                        ))));
                    }
                }
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "catalogs.definitions name={} is duplicated",
                    definition.name
                ))));
            }
        }

        if let Some(default_name) = &catalogs.default {
            if !definitions.contains_key(default_name) {
                return Err(Box::new(ConfigError(format!(
                    "catalogs.default={} does not match any definition",
                    default_name
                ))));
            }
        }

        Ok(Self {
            has_config: true,
            default_name: catalogs.default.clone(),
            definitions,
        })
    }

    fn definition(&self, name: &str) -> Option<&CatalogDefinition> {
        if !self.has_config {
            return None;
        }
        self.definitions.get(name)
    }

    fn default_name(&self) -> Option<&str> {
        self.default_name.as_deref()
    }
}

impl StorageRegistry {
    fn new(config: &RootConfig) -> FloeResult<Self> {
        let Some(storages) = &config.storages else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if storages.definitions.is_empty() {
            return Err(Box::new(ConfigError(
                "storages.definitions must not be empty".to_string(),
            )));
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &storages.definitions {
            if !ALLOWED_STORAGE_TYPES.contains(&definition.fs_type.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "storages.definitions name={} type={} is unsupported (allowed: {})",
                    definition.name,
                    definition.fs_type,
                    ALLOWED_STORAGE_TYPES.join(", ")
                ))));
            }
            if definition.fs_type == "s3" {
                if definition.bucket.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires bucket for type s3",
                        definition.name
                    ))));
                }
                if definition.region.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires region for type s3",
                        definition.name
                    ))));
                }
            }
            if definition.fs_type == "adls" {
                if definition.account.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires account for type adls",
                        definition.name
                    ))));
                }
                if definition.container.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires container for type adls",
                        definition.name
                    ))));
                }
            }
            if definition.fs_type == "gcs" && definition.bucket.is_none() {
                return Err(Box::new(ConfigError(format!(
                    "storages.definitions name={} requires bucket for type gcs",
                    definition.name
                ))));
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "storages.definitions name={} is duplicated",
                    definition.name
                ))));
            }
        }

        if let Some(default_name) = &storages.default {
            if !definitions.contains_key(default_name) {
                return Err(Box::new(ConfigError(format!(
                    "storages.default={} does not match any definition",
                    default_name
                ))));
            }
        } else {
            return Err(Box::new(ConfigError(
                "storages.default is required when storages is set".to_string(),
            )));
        }

        Ok(Self {
            has_config: true,
            default_name: storages.default.clone(),
            definitions,
        })
    }

    fn resolve_name(
        &self,
        entity: &EntityConfig,
        field: &str,
        override_name: Option<&str>,
    ) -> FloeResult<String> {
        if let Some(name) = override_name {
            return Ok(name.to_string());
        }
        if self.has_config {
            let default_name = self.default_name.clone().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} {field} requires storages.default",
                    entity.name
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            return Ok(default_name);
        }
        Ok("local".to_string())
    }

    fn validate_reference(&self, entity: &EntityConfig, field: &str, name: &str) -> FloeResult<()> {
        if !self.has_config {
            if name != "local" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} {field} references unknown storage {} (no storages block)",
                    entity.name, name
                ))));
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} {field} references unknown storage {}",
                entity.name, name
            )))
        })?;

        Ok(())
    }

    fn definition_type(&self, name: &str) -> Option<&str> {
        if !self.has_config {
            if name == "local" {
                return Some("local");
            }
            return None;
        }
        self.definitions
            .get(name)
            .map(|definition| definition.fs_type.as_str())
    }

    fn resolve_report_name(&self, override_name: Option<&str>) -> FloeResult<String> {
        if let Some(name) = override_name {
            return Ok(name.to_string());
        }
        if self.has_config {
            let default_name = self.default_name.clone().ok_or_else(|| {
                Box::new(ConfigError(
                    "report.storage requires storages.default".to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            return Ok(default_name);
        }
        Ok("local".to_string())
    }

    fn validate_report_reference(&self, field: &str, name: &str) -> FloeResult<()> {
        if !self.has_config {
            if name != "local" {
                return Err(Box::new(ConfigError(format!(
                    "{field} references unknown storage {} (no storages block)",
                    name
                ))));
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "{field} references unknown storage {}",
                name
            )))
        })?;

        Ok(())
    }
}
