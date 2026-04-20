use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::config::{EntityConfig, ResolvedPath, StorageResolver};
use crate::io::storage::extensions;
use crate::{ConfigError, FloeResult};

pub const ENTITY_STATE_SCHEMA_V1: &str = "floe.state.file-ingest.v1";
pub const ENTITY_STATE_FILENAME: &str = "state.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityState {
    pub schema: String,
    pub entity: String,
    pub updated_at: Option<String>,
    #[serde(default)]
    pub files: BTreeMap<String, EntityFileState>,
}

impl EntityState {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            schema: ENTITY_STATE_SCHEMA_V1.to_string(),
            entity: entity.into(),
            updated_at: None,
            files: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityFileState {
    pub processed_at: String,
    pub size: Option<u64>,
    pub mtime: Option<String>,
}

pub fn resolve_entity_state_path(
    resolver: &StorageResolver,
    entity: &EntityConfig,
) -> FloeResult<ResolvedPath> {
    if let Some(state) = &entity.state {
        if let Some(path) = state.path.as_deref() {
            return resolver.resolve_path(
                &entity.name,
                "entity.state.path",
                entity.source.storage.as_deref(),
                path,
            );
        }
    }

    let source_root = derive_source_root(&entity.source.path, &entity.source.format);
    let default_path = join_state_path(&source_root, &entity.name);
    resolver.resolve_path(
        &entity.name,
        "entity.state.path",
        entity.source.storage.as_deref(),
        &default_path,
    )
}

pub fn read_entity_state(path: &Path) -> FloeResult<Option<EntityState>> {
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read_to_string(path)?;
    let state: EntityState = serde_json::from_str(&payload)?;
    Ok(Some(state))
}

pub fn write_entity_state_atomic(path: &Path, state: &EntityState) -> FloeResult<()> {
    let parent = path.parent().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "state path has no parent directory: {}",
            path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    fs::create_dir_all(parent)?;

    let mut temp = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(temp.as_file_mut(), state)?;
    temp.as_file_mut().sync_all()?;
    temp.persist(path).map_err(|err| err.error)?;
    Ok(())
}

fn join_state_path(source_root: &str, entity_name: &str) -> String {
    if source_root.is_empty() || source_root == "." {
        format!(".floe/state/{entity_name}/{ENTITY_STATE_FILENAME}")
    } else {
        format!(
            "{}/.floe/state/{entity_name}/{ENTITY_STATE_FILENAME}",
            source_root.trim_end_matches('/')
        )
    }
}

fn derive_source_root(raw_path: &str, source_format: &str) -> String {
    let trimmed = raw_path.trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    if let Some(prefix) = prefix_before_first_glob(trimmed) {
        if prefix.is_empty() {
            return String::new();
        }
        if prefix.ends_with('/') {
            return prefix.trim_end_matches('/').to_string();
        }
        return parent_like(prefix)
            .unwrap_or(prefix)
            .trim_end_matches('/')
            .to_string();
    }

    if matches_source_file_suffix(trimmed, source_format) {
        return parent_like(trimmed)
            .unwrap_or(trimmed)
            .trim_end_matches('/')
            .to_string();
    }

    trimmed.to_string()
}

fn prefix_before_first_glob(value: &str) -> Option<&str> {
    let index = value.find(['*', '?', '['])?;
    Some(&value[..index])
}

fn matches_source_file_suffix(value: &str, source_format: &str) -> bool {
    let Some(segment) = value.rsplit('/').next() else {
        return false;
    };

    extensions::suffixes_for_format(source_format)
        .map(|suffixes| suffixes.iter().any(|suffix| segment.ends_with(suffix)))
        .unwrap_or(false)
}

fn parent_like(value: &str) -> Option<&str> {
    value
        .rfind('/')
        .map(|index| if index == 0 { "/" } else { &value[..index] })
}
