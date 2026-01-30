use std::path::{Path, PathBuf};

use glob::glob;

use crate::errors::{RunError, StorageError};
use crate::{config, ConfigError, FloeResult};

use super::{planner, ObjectRef, StorageClient};

pub struct LocalClient;

impl LocalClient {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageClient for LocalClient {
    fn list(&self, prefix: &str) -> FloeResult<Vec<ObjectRef>> {
        let path = Path::new(prefix);
        if path.is_file() {
            let uri = self.resolve_uri(prefix)?;
            return Ok(vec![ObjectRef {
                uri,
                key: prefix.to_string(),
                last_modified: None,
                size: None,
            }]);
        }
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut refs = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let key = path.display().to_string();
                let uri = self.resolve_uri(&key)?;
                refs.push(ObjectRef {
                    uri,
                    key,
                    last_modified: None,
                    size: None,
                });
            }
        }
        refs = planner::stable_sort_refs(refs);
        Ok(refs)
    }

    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
        let src = PathBuf::from(uri.trim_start_matches("local://"));
        let dest = temp_dir.join(
            src.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("object"),
        );
        planner::ensure_parent_dir(&dest)?;
        std::fs::copy(&src, &dest).map_err(|err| {
            Box::new(StorageError(format!(
                "local download failed from {}: {err}",
                src.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(dest)
    }

    fn upload_from_path(&self, local_path: &Path, uri: &str) -> FloeResult<()> {
        let dest = PathBuf::from(uri.trim_start_matches("local://"));
        planner::ensure_parent_dir(&dest)?;
        std::fs::copy(local_path, &dest).map_err(|err| {
            Box::new(StorageError(format!(
                "local upload failed to {}: {err}",
                dest.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(())
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        let path = Path::new(path);
        if path.is_absolute() {
            Ok(format!("local://{}", path.display()))
        } else {
            let abs = std::env::current_dir()?.join(path);
            Ok(format!("local://{}", abs.display()))
        }
    }

    fn delete(&self, uri: &str) -> FloeResult<()> {
        let path = Path::new(uri.trim_start_matches("local://"));
        if path.exists() {
            std::fs::remove_file(path).map_err(|err| {
                Box::new(StorageError(format!(
                    "local delete failed for {}: {err}",
                    path.display()
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalInputMode {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct ResolvedLocalInputs {
    pub files: Vec<PathBuf>,
    pub mode: LocalInputMode,
}

pub fn resolve_local_inputs(
    config_dir: &Path,
    entity_name: &str,
    source: &config::SourceConfig,
    storage: &str,
    default_globs: &[String],
) -> FloeResult<ResolvedLocalInputs> {
    let default_options = config::SourceOptions::default();
    let options = source.options.as_ref().unwrap_or(&default_options);
    let recursive = options.recursive.unwrap_or(false);
    let glob_override = options.glob.as_deref();
    let raw_path = source.path.as_str();

    if is_glob_pattern(raw_path) {
        let pattern_path = resolve_glob_pattern(config_dir, raw_path);
        let pattern = pattern_path.to_string_lossy().to_string();
        let files = collect_glob_files(&pattern)?;
        if files.is_empty() {
            let (base_path, glob_used) = split_glob_details(&pattern_path, raw_path);
            return Err(Box::new(RunError(no_match_message(
                entity_name,
                storage,
                &base_path,
                &glob_used,
                recursive,
            ))));
        }
        return Ok(ResolvedLocalInputs {
            files,
            mode: LocalInputMode::Directory,
        });
    }

    let base_path = config::resolve_local_path(config_dir, raw_path);
    if base_path.is_file() {
        return Ok(ResolvedLocalInputs {
            files: vec![base_path],
            mode: LocalInputMode::File,
        });
    }

    let glob_used = if let Some(glob_override) = glob_override {
        vec![glob_override.to_string()]
    } else {
        default_globs.to_vec()
    };
    if !base_path.is_dir() {
        return Err(Box::new(RunError(no_match_message(
            entity_name,
            storage,
            &base_path.display().to_string(),
            &glob_used.join(","),
            recursive,
        ))));
    }

    let pattern_paths = if recursive {
        glob_used
            .iter()
            .map(|glob| base_path.join("**").join(glob))
            .collect::<Vec<_>>()
    } else {
        glob_used
            .iter()
            .map(|glob| base_path.join(glob))
            .collect::<Vec<_>>()
    };
    let files = collect_glob_files_multi(&pattern_paths)?;
    if files.is_empty() {
        return Err(Box::new(RunError(no_match_message(
            entity_name,
            storage,
            &base_path.display().to_string(),
            &glob_used.join(","),
            recursive,
        ))));
    }

    Ok(ResolvedLocalInputs {
        files,
        mode: LocalInputMode::Directory,
    })
}

fn is_glob_pattern(value: &str) -> bool {
    value.contains('*') || value.contains('?') || value.contains('[')
}

fn resolve_glob_pattern(config_dir: &Path, raw_path: &str) -> PathBuf {
    let path = Path::new(raw_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(raw_path)
    }
}

fn split_glob_details(pattern_path: &Path, raw_pattern: &str) -> (String, String) {
    let base = pattern_path
        .parent()
        .unwrap_or(pattern_path)
        .display()
        .to_string();
    let glob_used = pattern_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| raw_pattern.to_string());
    (base, glob_used)
}

fn collect_glob_files(pattern: &str) -> FloeResult<Vec<PathBuf>> {
    let mut files = Vec::new();
    let entries = glob(pattern).map_err(|err| {
        Box::new(ConfigError(format!(
            "invalid glob pattern {pattern:?}: {err}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    for entry in entries {
        let path = entry.map_err(|err| {
            Box::new(ConfigError(format!(
                "glob match failed for {pattern:?}: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        if path.is_file() {
            files.push(path);
        }
    }
    files.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    Ok(files)
}

fn collect_glob_files_multi(patterns: &[PathBuf]) -> FloeResult<Vec<PathBuf>> {
    let mut files = Vec::new();
    for pattern_path in patterns {
        let pattern = pattern_path.to_string_lossy().to_string();
        files.extend(collect_glob_files(&pattern)?);
    }
    files.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    files.dedup_by(|a, b| a.to_string_lossy() == b.to_string_lossy());
    Ok(files)
}

fn no_match_message(
    entity_name: &str,
    storage: &str,
    base_path: &str,
    glob_used: &str,
    recursive: bool,
) -> String {
    format!(
        "entity.name={} source.storage={} no input files matched (base_path={}, glob={}, recursive={})",
        entity_name, storage, base_path, glob_used, recursive
    )
}
