use clap::ValueEnum;
use floe_core::config::{RootConfig, StorageResolver};
use floe_core::{ConfigLocation, FloeResult};
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestTarget {
    Airflow,
    Dagster,
}

impl ManifestTarget {
    fn schema(self) -> &'static str {
        match self {
            ManifestTarget::Airflow => "floe.airflow.manifest.v1",
            ManifestTarget::Dagster => "floe.dagster.manifest.v1",
        }
    }
}

#[derive(Serialize)]
struct ManifestOutput {
    schema: &'static str,
    generated_at_ts_ms: u64,
    floe_version: &'static str,
    config_uri: String,
    config_checksum: Option<String>,
    entities: Vec<ManifestEntity>,
}

#[derive(Serialize)]
struct ManifestEntity {
    name: String,
    domain: Option<String>,
    group_name: String,
    asset_key: Vec<String>,
    source_format: String,
    accepted_sink_uri: String,
    rejected_sink_uri: Option<String>,
}

struct ResolvedOrRaw {
    uri: String,
}

pub fn build_manifest_json(
    config_location: &ConfigLocation,
    config: &RootConfig,
    target: ManifestTarget,
    selected_entities: &[String],
) -> FloeResult<String> {
    let resolver = StorageResolver::new(config, config_location.base.clone())?;

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

    let mut manifest_entities = Vec::with_capacity(entities.len());
    for entity in entities {
        let accepted = resolve_or_raw(
            &resolver,
            &entity.name,
            "sink.accepted.path",
            entity.sink.accepted.storage.as_deref(),
            &entity.sink.accepted.path,
        );

        let rejected = entity.sink.rejected.as_ref().map(|target| {
            resolve_or_raw(
                &resolver,
                &entity.name,
                "sink.rejected.path",
                target.storage.as_deref(),
                &target.path,
            )
        });

        let (asset_key, group_name) = if let Some(domain) = &entity.domain {
            (vec![domain.clone(), entity.name.clone()], domain.clone())
        } else {
            (vec![entity.name.clone()], "floe".to_string())
        };

        manifest_entities.push(ManifestEntity {
            name: entity.name.clone(),
            domain: entity.domain.clone(),
            group_name,
            asset_key,
            source_format: entity.source.format.clone(),
            accepted_sink_uri: accepted.uri,
            rejected_sink_uri: rejected.map(|value| value.uri),
        });
    }

    let output = ManifestOutput {
        schema: target.schema(),
        generated_at_ts_ms: now_ts_ms(),
        floe_version: env!("FLOE_VERSION"),
        config_uri: config_location.display.clone(),
        config_checksum: None,
        entities: manifest_entities,
    };

    Ok(serde_json::to_string_pretty(&output)?)
}

pub fn write_manifest(output_path: &str, payload: &str) -> FloeResult<()> {
    if output_path == "-" {
        let mut out = std::io::stdout().lock();
        writeln!(out, "{payload}")?;
        out.flush()?;
        return Ok(());
    }

    write_atomic(Path::new(output_path), payload.as_bytes())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> FloeResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp_path = temp_path(path);
    fs::write(&tmp_path, bytes)?;

    if let Err(rename_err) = fs::rename(&tmp_path, path) {
        if path.exists() {
            fs::remove_file(path)?;
            fs::rename(&tmp_path, path)?;
        } else {
            let _ = fs::remove_file(&tmp_path);
            return Err(Box::new(rename_err));
        }
    }

    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or(0);
    let filename = path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "manifest.json".to_string());
    path.with_file_name(format!(".{filename}.{stamp}.tmp"))
}

fn resolve_or_raw(
    resolver: &StorageResolver,
    entity_name: &str,
    field: &str,
    storage_name: Option<&str>,
    raw_path: &str,
) -> ResolvedOrRaw {
    match resolver.resolve_path(entity_name, field, storage_name, raw_path) {
        Ok(resolved) => ResolvedOrRaw { uri: resolved.uri },
        Err(_) => ResolvedOrRaw {
            uri: raw_path.to_string(),
        },
    }
}

fn now_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
