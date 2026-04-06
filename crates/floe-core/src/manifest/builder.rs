use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{ConfigLocation, RootConfig, SourceOptions, StorageResolver};
use crate::manifest::model::{
    CommonManifest, ManifestArchiveTarget, ManifestDomain, ManifestEntity, ManifestExecution,
    ManifestExecutionDefaults, ManifestResultContract, ManifestRunnerDefinition, ManifestRunners,
    ManifestSinkTarget, ManifestSinks, ManifestSource,
};
use crate::profile::ProfileConfig;
use crate::FloeResult;

#[derive(Debug)]
struct ResolvedOrRaw {
    storage: String,
    uri: String,
    resolved: bool,
}

pub fn build_common_manifest_json(
    config_location: &ConfigLocation,
    config: &RootConfig,
    selected_entities: &[String],
    profile: Option<&ProfileConfig>,
) -> FloeResult<String> {
    let resolver = StorageResolver::new(config, config_location.base.clone())?;
    let manifest = build_common_manifest(
        config_location,
        config,
        selected_entities,
        &resolver,
        profile,
    );
    Ok(serde_json::to_string_pretty(&manifest)?)
}

fn build_common_manifest(
    config_location: &ConfigLocation,
    config: &RootConfig,
    selected_entities: &[String],
    resolver: &StorageResolver,
    profile: Option<&ProfileConfig>,
) -> CommonManifest {
    let mut entities: Vec<_> = if selected_entities.is_empty() {
        config.entities.iter().collect()
    } else {
        config
            .entities
            .iter()
            .filter(|entity| selected_entities.iter().any(|name| name == &entity.name))
            .collect()
    };
    entities.sort_by(|left, right| left.name.cmp(&right.name));

    let report_path = config
        .report
        .as_ref()
        .map(|report| report.path.as_str())
        .unwrap_or("report");
    let report_storage = config
        .report
        .as_ref()
        .and_then(|report| report.storage.as_deref());
    let report_base = resolve_or_raw(
        resolver,
        "__manifest__",
        "report.path",
        report_storage,
        report_path,
    );

    let mut manifest_entities = Vec::with_capacity(entities.len());
    for entity in entities {
        let source = resolve_or_raw(
            resolver,
            &entity.name,
            "source.path",
            entity.source.storage.as_deref(),
            &entity.source.path,
        );
        let accepted = resolve_or_raw(
            resolver,
            &entity.name,
            "sink.accepted.path",
            entity.sink.accepted.storage.as_deref(),
            &entity.sink.accepted.path,
        );
        let rejected = entity.sink.rejected.as_ref().map(|target| {
            resolve_or_raw(
                resolver,
                &entity.name,
                "sink.rejected.path",
                target.storage.as_deref(),
                &target.path,
            )
        });
        let archive = entity.sink.archive.as_ref().map(|target| {
            resolve_or_raw(
                resolver,
                &entity.name,
                "sink.archive.path",
                target.storage.as_deref(),
                &target.path,
            )
        });

        let (asset_key, group_name) = if let Some(domain) = &entity.domain {
            (vec![domain.clone(), entity.name.clone()], domain.clone())
        } else {
            (
                vec!["default".to_string(), entity.name.clone()],
                "default".to_string(),
            )
        };

        let mut tags = HashMap::new();
        if let Some(metadata) = &entity.metadata {
            if let Some(owner) = &metadata.owner {
                tags.insert("owner".to_string(), owner.clone());
            }
            if let Some(product) = &metadata.data_product {
                tags.insert("data_product".to_string(), product.clone());
            }
            if let Some(domain_tag) = &metadata.domain {
                tags.insert("domain".to_string(), domain_tag.clone());
            }
        }
        let tags = if tags.is_empty() { None } else { Some(tags) };

        manifest_entities.push(ManifestEntity {
            name: entity.name.clone(),
            domain: entity.domain.clone(),
            group_name,
            asset_key,
            source_format: entity.source.format.clone(),
            accepted_sink_uri: accepted.uri.clone(),
            rejected_sink_uri: rejected.as_ref().map(|value| value.uri.clone()),
            tags,
            source: ManifestSource {
                format: entity.source.format.clone(),
                storage: source.storage,
                uri: source.uri,
                path: entity.source.path.clone(),
                resolved: source.resolved,
                cast_mode: entity.source.cast_mode.clone(),
                options: map_source_options(entity.source.options.as_ref()),
            },
            sinks: ManifestSinks {
                accepted: ManifestSinkTarget {
                    format: entity.sink.accepted.format.clone(),
                    storage: accepted.storage,
                    uri: accepted.uri,
                    path: entity.sink.accepted.path.clone(),
                    resolved: accepted.resolved,
                },
                rejected: rejected.map(|value| ManifestSinkTarget {
                    format: entity
                        .sink
                        .rejected
                        .as_ref()
                        .map(|target| target.format.clone())
                        .unwrap_or_else(|| "csv".to_string()),
                    storage: value.storage,
                    uri: value.uri,
                    path: entity
                        .sink
                        .rejected
                        .as_ref()
                        .map(|target| target.path.clone())
                        .unwrap_or_default(),
                    resolved: value.resolved,
                }),
                archive: archive.map(|value| ManifestArchiveTarget {
                    storage: value.storage,
                    uri: value.uri,
                    path: entity
                        .sink
                        .archive
                        .as_ref()
                        .map(|target| target.path.clone())
                        .unwrap_or_default(),
                    resolved: value.resolved,
                }),
            },
            runner: None,
        });
    }

    let config_uri = canonical_config_uri(&config_location.display);
    let config_checksum = None;

    CommonManifest {
        schema: "floe.manifest.v1",
        generated_at_ts_ms: now_ts_ms(),
        floe_version: env!("CARGO_PKG_VERSION"),
        spec_version: config.version.clone(),
        manifest_id: build_manifest_id(&config_uri, config_checksum),
        config_uri,
        config_checksum: config_checksum.map(ToString::to_string),
        report_base_uri: report_base.uri,
        domains: config
            .domains
            .iter()
            .map(|domain| ManifestDomain {
                name: domain.name.clone(),
                incoming_dir: domain
                    .resolved_incoming_dir
                    .clone()
                    .unwrap_or_else(|| domain.incoming_dir.clone()),
            })
            .collect(),
        execution: default_execution_contract(),
        runners: runners_contract(profile),
        entities: manifest_entities,
    }
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
            storage: storage_name.unwrap_or("local").to_string(),
            uri: raw_path.to_string(),
            resolved: false,
        },
    }
}

fn canonical_config_uri(display: &str) -> String {
    if display.contains("://") {
        display.to_string()
    } else {
        format!("local://{}", display)
    }
}

fn build_manifest_id(config_uri: &str, config_checksum: Option<&str>) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv1a_update(hash, config_uri.as_bytes(), FNV_PRIME);
    hash = fnv1a_update(hash, &[0], FNV_PRIME);
    hash = fnv1a_update(hash, config_checksum.unwrap_or("").as_bytes(), FNV_PRIME);

    format!("mfv1-{hash:016x}")
}

fn fnv1a_update(mut hash: u64, bytes: &[u8], prime: u64) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(prime);
    }
    hash
}

fn map_source_options(options: Option<&SourceOptions>) -> Option<serde_json::Value> {
    let options = options?;
    let mut map = serde_json::Map::new();
    map.insert("header".to_string(), serde_json::json!(options.header));
    map.insert(
        "separator".to_string(),
        serde_json::json!(options.separator),
    );
    map.insert("encoding".to_string(), serde_json::json!(options.encoding));
    map.insert(
        "null_values".to_string(),
        serde_json::json!(options.null_values),
    );
    map.insert(
        "recursive".to_string(),
        serde_json::json!(options.recursive),
    );
    map.insert("glob".to_string(), serde_json::json!(options.glob));
    map.insert(
        "json_mode".to_string(),
        serde_json::json!(options.json_mode),
    );
    map.insert("sheet".to_string(), serde_json::json!(options.sheet));
    map.insert(
        "header_row".to_string(),
        serde_json::json!(options.header_row),
    );
    map.insert("data_row".to_string(), serde_json::json!(options.data_row));
    map.insert("row_tag".to_string(), serde_json::json!(options.row_tag));
    map.insert(
        "namespace".to_string(),
        serde_json::json!(options.namespace),
    );
    map.insert(
        "value_tag".to_string(),
        serde_json::json!(options.value_tag),
    );
    Some(serde_json::Value::Object(map))
}

fn default_execution_contract() -> ManifestExecution {
    let mut exit_codes = HashMap::new();
    exit_codes.insert("0", "success_or_rejected");
    exit_codes.insert("1", "technical_failure");
    exit_codes.insert("2", "aborted");

    ManifestExecution {
        entrypoint: "floe",
        base_args: vec![
            "run",
            "-c",
            "{config_uri}",
            "--log-format",
            "json",
            "--quiet",
        ],
        per_entity_args: vec!["--entities", "{entity_name}"],
        log_format: "json",
        result_contract: ManifestResultContract {
            run_finished_event: true,
            summary_uri_field: "summary_uri",
            exit_codes,
        },
        defaults: ManifestExecutionDefaults {
            env: HashMap::new(),
            workdir: None,
        },
    }
}

fn runners_contract(profile: Option<&ProfileConfig>) -> ManifestRunners {
    let profile_runner_type = profile
        .and_then(|p| p.execution.as_ref())
        .map(|e| e.runner.runner_type.as_str());

    match profile_runner_type {
        Some("kubernetes_job") => {
            let profile_runner = profile
                .and_then(|p| p.execution.as_ref())
                .map(|e| &e.runner);
            let mut definitions = HashMap::new();
            definitions.insert(
                "default",
                ManifestRunnerDefinition {
                    runner_type: "kubernetes_job",
                    command: profile_runner.and_then(|r| r.command.clone()),
                    args: profile_runner.and_then(|r| r.args.clone()),
                    timeout_seconds: profile_runner.and_then(|r| r.timeout_seconds),
                    ttl_seconds_after_finished: profile_runner
                        .and_then(|r| r.ttl_seconds_after_finished),
                    poll_interval_seconds: profile_runner.and_then(|r| r.poll_interval_seconds),
                    secrets: profile_runner.and_then(|r| r.secrets.clone()),
                    image: None,
                    namespace: None,
                    service_account: None,
                    resources: None,
                    env: None,
                },
            );
            ManifestRunners {
                default: "default",
                definitions,
            }
        }
        // "local" or absent → local_process (backward-compatible default)
        _ => {
            let mut definitions = HashMap::new();
            definitions.insert(
                "local",
                ManifestRunnerDefinition {
                    runner_type: "local_process",
                    command: None,
                    args: None,
                    timeout_seconds: None,
                    ttl_seconds_after_finished: None,
                    poll_interval_seconds: None,
                    secrets: None,
                    image: None,
                    namespace: None,
                    service_account: None,
                    resources: None,
                    env: None,
                },
            );
            ManifestRunners {
                default: "local",
                definitions,
            }
        }
    }
}

fn now_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
