use clap::ValueEnum;
use floe_core::config::{RootConfig, StorageResolver};
use floe_core::{ConfigLocation, FloeResult};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum ValidateOutputFormat {
    Text,
    Json,
}

#[derive(Serialize)]
struct ValidateJsonOutput {
    schema: &'static str,
    floe_version: &'static str,
    spec_version: Option<String>,
    generated_at_ts_ms: u64,
    config: ConfigInfo,
    valid: bool,
    errors: Vec<PlanIssue>,
    warnings: Vec<PlanIssue>,
    plan: Option<ValidatePlan>,
}

#[derive(Serialize)]
struct ConfigInfo {
    uri: String,
    resolved_uri: Option<String>,
    checksum: Option<String>,
}

#[derive(Serialize)]
struct PlanIssue {
    level: &'static str,
    code: &'static str,
    message: String,
    path: Option<String>,
    entity: Option<String>,
    hint: Option<String>,
}

#[derive(Serialize)]
struct ValidatePlan {
    report: ReportPlan,
    domains: Vec<DomainPlan>,
    entities: Vec<EntityPlan>,
}

#[derive(Serialize)]
struct ReportPlan {
    base_uri: String,
    storage: String,
    resolved: bool,
}

#[derive(Serialize)]
struct DomainPlan {
    name: String,
    incoming_dir: String,
    resolved_incoming_dir: Option<String>,
}

#[derive(Serialize)]
struct EntityPlan {
    name: String,
    domain: Option<String>,
    metadata: Option<EntityMetadataPlan>,
    key: Vec<String>,
    group_name: Option<String>,
    policy: PolicyPlan,
    source: SourcePlan,
    sinks: SinksPlan,
    schema: SchemaPlan,
}

#[derive(Serialize)]
struct EntityMetadataPlan {
    data_product: Option<String>,
    domain: Option<String>,
    owner: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
}

#[derive(Serialize)]
struct PolicyPlan {
    severity: String,
}

#[derive(Serialize)]
struct SourcePlan {
    format: String,
    storage: String,
    path: String,
    uri: String,
    resolved: bool,
    cast_mode: Option<String>,
    options: Option<SourceOptionsPlan>,
}

#[derive(Serialize)]
struct SourceOptionsPlan {
    header: Option<bool>,
    separator: Option<String>,
    encoding: Option<String>,
    null_values: Option<Vec<String>>,
    recursive: Option<bool>,
    glob: Option<String>,
    json_mode: Option<String>,
}

#[derive(Serialize)]
struct SinksPlan {
    accepted: SinkPlan,
    rejected: Option<SinkPlan>,
    archive: Option<ArchivePlan>,
}

#[derive(Serialize)]
struct SinkPlan {
    format: String,
    storage: String,
    path: String,
    uri: String,
    resolved: bool,
    options: Option<SinkOptionsPlan>,
}

#[derive(Serialize)]
struct SinkOptionsPlan {
    compression: Option<String>,
    row_group_size: Option<u64>,
    max_size_per_file: Option<u64>,
}

#[derive(Serialize)]
struct ArchivePlan {
    storage: String,
    path: String,
    uri: String,
    resolved: bool,
}

#[derive(Serialize)]
struct SchemaPlan {
    normalize_columns: Option<NormalizeColumnsPlan>,
    mismatch: Option<MismatchPlan>,
    columns: Vec<ColumnPlan>,
}

#[derive(Serialize)]
struct NormalizeColumnsPlan {
    enabled: Option<bool>,
    strategy: Option<String>,
}

#[derive(Serialize)]
struct MismatchPlan {
    missing_columns: Option<String>,
    extra_columns: Option<String>,
}

#[derive(Serialize)]
struct ColumnPlan {
    name: String,
    source: String,
    column_type: String,
    nullable: Option<bool>,
    unique: Option<bool>,
}

pub fn build_valid_json(
    config_location: &ConfigLocation,
    config: &RootConfig,
    selected_entities: &[String],
) -> FloeResult<String> {
    let now_ms = now_ts_ms();
    let resolver = StorageResolver::new(config, config_location.base.clone())?;

    let report = config
        .report
        .as_ref()
        .expect("report has a default value during parsing");
    let report_resolved = resolver.resolve_report_path(report.storage.as_deref(), &report.path);
    let report_plan = match report_resolved {
        Ok(resolved) => ReportPlan {
            base_uri: resolved.uri,
            storage: resolved.storage,
            resolved: true,
        },
        Err(_) => ReportPlan {
            base_uri: report.path.clone(),
            storage: report
                .storage
                .clone()
                .unwrap_or_else(|| resolver.default_storage_name().to_string()),
            resolved: false,
        },
    };

    let entities: Vec<_> = if selected_entities.is_empty() {
        config.entities.iter().collect()
    } else {
        config
            .entities
            .iter()
            .filter(|entity| selected_entities.iter().any(|name| name == &entity.name))
            .collect()
    };

    let mut entity_plans = Vec::with_capacity(entities.len());
    for entity in entities {
        let source_path = resolve_or_raw(
            &resolver,
            &entity.name,
            "source.path",
            entity.source.storage.as_deref(),
            &entity.source.path,
        );
        let accepted_path = resolve_or_raw(
            &resolver,
            &entity.name,
            "sink.accepted.path",
            entity.sink.accepted.storage.as_deref(),
            &entity.sink.accepted.path,
        );
        let rejected_plan = entity.sink.rejected.as_ref().map(|rejected| {
            let rejected_path = resolve_or_raw(
                &resolver,
                &entity.name,
                "sink.rejected.path",
                rejected.storage.as_deref(),
                &rejected.path,
            );
            SinkPlan {
                format: rejected.format.clone(),
                storage: rejected_path.storage,
                path: rejected.path.clone(),
                uri: rejected_path.uri,
                resolved: rejected_path.resolved,
                options: rejected.options.as_ref().map(|options| SinkOptionsPlan {
                    compression: options.compression.clone(),
                    row_group_size: options.row_group_size,
                    max_size_per_file: options.max_size_per_file,
                }),
            }
        });

        let archive_plan = entity.sink.archive.as_ref().map(|archive| {
            let storage_override = archive
                .storage
                .as_deref()
                .or(entity.source.storage.as_deref());
            let archive_path = resolve_or_raw(
                &resolver,
                &entity.name,
                "sink.archive.path",
                storage_override,
                &archive.path,
            );
            ArchivePlan {
                storage: archive_path.storage,
                path: archive.path.clone(),
                uri: archive_path.uri,
                resolved: archive_path.resolved,
            }
        });

        let metadata = entity.metadata.as_ref().map(|metadata| EntityMetadataPlan {
            data_product: metadata.data_product.clone(),
            domain: metadata.domain.clone(),
            owner: metadata.owner.clone(),
            description: metadata.description.clone(),
            tags: metadata.tags.clone(),
        });

        let (key, group_name) = entity
            .domain
            .as_ref()
            .map(|domain| {
                (
                    vec![domain.clone(), entity.name.clone()],
                    Some(domain.clone()),
                )
            })
            .unwrap_or_else(|| (vec![entity.name.clone()], Some("floe".to_string())));

        entity_plans.push(EntityPlan {
            name: entity.name.clone(),
            domain: entity.domain.clone(),
            metadata,
            key,
            group_name,
            policy: PolicyPlan {
                severity: entity.policy.severity.clone(),
            },
            source: SourcePlan {
                format: entity.source.format.clone(),
                storage: source_path.storage,
                path: entity.source.path.clone(),
                uri: source_path.uri,
                resolved: source_path.resolved,
                cast_mode: entity.source.cast_mode.clone(),
                options: entity
                    .source
                    .options
                    .as_ref()
                    .map(|options| SourceOptionsPlan {
                        header: options.header,
                        separator: options.separator.clone(),
                        encoding: options.encoding.clone(),
                        null_values: options.null_values.clone(),
                        recursive: options.recursive,
                        glob: options.glob.clone(),
                        json_mode: options.json_mode.clone(),
                    }),
            },
            sinks: SinksPlan {
                accepted: SinkPlan {
                    format: entity.sink.accepted.format.clone(),
                    storage: accepted_path.storage,
                    path: entity.sink.accepted.path.clone(),
                    uri: accepted_path.uri,
                    resolved: accepted_path.resolved,
                    options: entity
                        .sink
                        .accepted
                        .options
                        .as_ref()
                        .map(|options| SinkOptionsPlan {
                            compression: options.compression.clone(),
                            row_group_size: options.row_group_size,
                            max_size_per_file: options.max_size_per_file,
                        }),
                },
                rejected: rejected_plan,
                archive: archive_plan,
            },
            schema: SchemaPlan {
                normalize_columns: entity.schema.normalize_columns.as_ref().map(|normalize| {
                    NormalizeColumnsPlan {
                        enabled: normalize.enabled,
                        strategy: normalize.strategy.clone(),
                    }
                }),
                mismatch: entity
                    .schema
                    .mismatch
                    .as_ref()
                    .map(|mismatch| MismatchPlan {
                        missing_columns: mismatch.missing_columns.clone(),
                        extra_columns: mismatch.extra_columns.clone(),
                    }),
                columns: entity
                    .schema
                    .columns
                    .iter()
                    .map(|column| ColumnPlan {
                        name: column.name.clone(),
                        source: column.source_or_name().to_string(),
                        column_type: column.column_type.clone(),
                        nullable: column.nullable,
                        unique: column.unique,
                    })
                    .collect(),
            },
        });
    }

    let output = ValidateJsonOutput {
        schema: "floe.plan.v1",
        floe_version: env!("FLOE_VERSION"),
        spec_version: Some(config.version.clone()),
        generated_at_ts_ms: now_ms,
        config: ConfigInfo {
            uri: config_location.display.clone(),
            resolved_uri: Some(config_location.display.clone()),
            checksum: None,
        },
        valid: true,
        errors: Vec::new(),
        warnings: Vec::new(),
        plan: Some(ValidatePlan {
            report: report_plan,
            domains: config
                .domains
                .iter()
                .map(|domain| DomainPlan {
                    name: domain.name.clone(),
                    incoming_dir: domain.incoming_dir.clone(),
                    resolved_incoming_dir: domain.resolved_incoming_dir.clone(),
                })
                .collect(),
            entities: entity_plans,
        }),
    };

    Ok(serde_json::to_string(&output)?)
}

pub fn build_invalid_json(
    config_uri: &str,
    spec_version: Option<&str>,
    err: &dyn std::error::Error,
) -> String {
    let message = err.to_string();
    let output = ValidateJsonOutput {
        schema: "floe.plan.v1",
        floe_version: env!("FLOE_VERSION"),
        spec_version: spec_version.map(|value| value.to_string()),
        generated_at_ts_ms: now_ts_ms(),
        config: ConfigInfo {
            uri: config_uri.to_string(),
            resolved_uri: Some(config_uri.to_string()),
            checksum: None,
        },
        valid: false,
        errors: vec![PlanIssue {
            level: "error",
            code: "CONFIG_INVALID",
            message: message.clone(),
            path: best_effort_path(&message),
            entity: best_effort_entity(&message),
            hint: None,
        }],
        warnings: Vec::new(),
        plan: None,
    };

    serde_json::to_string(&output).unwrap_or_else(|_| {
        format!(
            "{{\"schema\":\"floe.plan.v1\",\"valid\":false,\"errors\":[{{\"level\":\"error\",\"code\":\"CONFIG_INVALID\",\"message\":{}}}]}}",
            serde_json::to_string(&message).unwrap_or_else(|_| "\"config invalid\"".to_string())
        )
    })
}

struct ResolvedOrRaw {
    storage: String,
    uri: String,
    resolved: bool,
}

fn resolve_or_raw(
    resolver: &StorageResolver,
    entity_name: &str,
    field: &str,
    storage_name: Option<&str>,
    raw_path: &str,
) -> ResolvedOrRaw {
    match resolver.resolve_path(entity_name, field, storage_name, raw_path) {
        Ok(resolved) => ResolvedOrRaw {
            storage: resolved.storage,
            uri: resolved.uri,
            resolved: true,
        },
        Err(_) => ResolvedOrRaw {
            storage: storage_name
                .map(|value| value.to_string())
                .unwrap_or_else(|| resolver.default_storage_name().to_string()),
            uri: raw_path.to_string(),
            resolved: false,
        },
    }
}

fn best_effort_entity(message: &str) -> Option<String> {
    let marker = "entity.name=";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == ',' || ch == ')')
        .unwrap_or(rest.len());
    let value = rest[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn best_effort_path(message: &str) -> Option<String> {
    if let Some(pos) = message.find("unknown field ") {
        return Some(message[pos + "unknown field ".len()..].trim().to_string());
    }
    if message.starts_with("entities list is empty") {
        return Some("root.entities".to_string());
    }
    None
}

fn now_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
