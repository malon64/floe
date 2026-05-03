use std::path::{Path, PathBuf};
use std::time::SystemTime;

use glob::{MatchOptions, Pattern};

use crate::errors::RunError;
use crate::io::storage::{planner, Target};
use crate::{config, io, report, FloeResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveInputsMode {
    Download,
    ListOnly,
}

#[derive(Debug, Clone)]
pub struct ResolvedInputs {
    pub files: Vec<io::format::InputFile>,
    pub listed: Vec<String>,
    pub mode: report::ResolvedInputMode,
}

pub fn resolve_inputs(
    config_dir: &Path,
    entity: &config::EntityConfig,
    adapter: &dyn io::format::InputAdapter,
    target: &Target,
    resolution_mode: ResolveInputsMode,
    temp_dir: Option<&Path>,
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> FloeResult<ResolvedInputs> {
    // Storage-specific resolution: list + download for cloud, direct paths for local.
    match target {
        Target::S3 { storage, .. } => {
            let client = require_storage_client(storage_client, "s3")?;
            let (bucket, key) = target.s3_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            resolve_cloud_inputs_for_prefix(
                client,
                key,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Gcs { storage, .. } => {
            let client = require_storage_client(storage_client, "gcs")?;
            let (bucket, key) = target.gcs_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "gcs target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            resolve_cloud_inputs_for_prefix(
                client,
                key,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Adls { storage, .. } => {
            let client = require_storage_client(storage_client, "adls")?;
            let (container, account, base_path) = target.adls_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "adls target missing container".to_string(),
                ))
            })?;
            let location = format!("container={}, account={}", container, account);
            resolve_cloud_inputs_for_prefix(
                client,
                base_path,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Local { storage, .. } => {
            let resolved =
                adapter.resolve_local_inputs(config_dir, &entity.name, &entity.source, storage)?;
            let listed = build_local_listing(&resolved.files, storage_client);
            // Download mode lists files for processing; ListOnly produces only URIs for dry-run.
            let files = match resolution_mode {
                ResolveInputsMode::Download => {
                    build_local_inputs(&resolved.files, entity, storage_client)
                }
                ResolveInputsMode::ListOnly => Vec::new(),
            };
            let mode = match resolved.mode {
                io::storage::local::LocalInputMode::File => report::ResolvedInputMode::File,
                io::storage::local::LocalInputMode::Directory => {
                    report::ResolvedInputMode::Directory
                }
            };
            Ok(ResolvedInputs {
                files,
                listed,
                mode,
            })
        }
    }
}

fn resolve_cloud_inputs_for_prefix(
    client: &dyn crate::io::storage::StorageClient,
    prefix: &str,
    adapter: &dyn io::format::InputAdapter,
    entity: &config::EntityConfig,
    storage: &str,
    location: &str,
    resolution_mode: ResolveInputsMode,
    _temp_dir: Option<&Path>,
) -> FloeResult<ResolvedInputs> {
    let objects = list_cloud_objects(client, prefix, adapter, entity, storage, location)?;
    let listed = objects.iter().map(|obj| obj.uri.clone()).collect();
    // Download mode now lists metadata only; actual downloads happen JIT in the entity run.
    let files = match resolution_mode {
        ResolveInputsMode::Download => list_cloud_inputs(&objects, entity),
        ResolveInputsMode::ListOnly => Vec::new(),
    };
    Ok(ResolvedInputs {
        files,
        listed,
        mode: report::ResolvedInputMode::Directory,
    })
}

fn require_storage_client<'a>(
    storage_client: Option<&'a dyn crate::io::storage::StorageClient>,
    label: &str,
) -> FloeResult<&'a dyn crate::io::storage::StorageClient> {
    storage_client.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(crate::errors::RunError(format!(
            "{} storage client missing",
            label
        )))
    })
}

fn list_cloud_objects(
    client: &dyn crate::io::storage::StorageClient,
    source_path: &str,
    adapter: &dyn io::format::InputAdapter,
    entity: &config::EntityConfig,
    storage: &str,
    location: &str,
) -> FloeResult<Vec<io::storage::planner::ObjectRef>> {
    let source_match = CloudSourceMatch::new(source_path).map_err(|err| {
        Box::new(crate::errors::RunError(format!(
            "entity.name={} source.storage={} invalid cloud source path ({}, path={}): {}",
            entity.name, storage, location, source_path, err
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let suffixes = adapter.suffixes()?;
    let list_refs = client.list(source_match.list_prefix())?;
    let filtered = filter_cloud_list_refs(list_refs, &source_match, &suffixes);
    if filtered.is_empty() {
        let match_desc = source_match.match_description();
        return Err(Box::new(crate::errors::RunError(format!(
            "entity.name={} source.storage={} no input objects matched ({}, prefix={}, {}, suffixes={})",
            entity.name,
            storage,
            location,
            source_match.list_prefix(),
            match_desc,
            suffixes.join(",")
        ))));
    }
    Ok(filtered)
}

/// Builds `InputFile` metadata from cloud object refs without downloading.
/// Downloads happen later, JIT per file, via [`localize_input`].
fn list_cloud_inputs(
    objects: &[io::storage::planner::ObjectRef],
    entity: &config::EntityConfig,
) -> Vec<io::format::InputFile> {
    objects
        .iter()
        .map(|object| {
            let source_name =
                planner::file_name_from_key(&object.key).unwrap_or_else(|| entity.name.clone());
            let source_stem =
                planner::file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
            io::format::InputFile {
                source_uri: object.uri.clone(),
                source_name,
                source_stem,
                source_size: object.size,
                source_mtime: object.last_modified.clone(),
            }
        })
        .collect()
}

fn build_local_inputs(
    files: &[PathBuf],
    entity: &config::EntityConfig,
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> Vec<io::format::InputFile> {
    files
        .iter()
        .map(|path| {
            let normalized_path = crate::io::storage::paths::normalize_local_path(path);
            let metadata = std::fs::metadata(&normalized_path).ok();
            let source_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            let source_stem = Path::new(&source_name)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            let uri = storage_client
                .and_then(|client| {
                    client
                        .resolve_uri(&normalized_path.display().to_string())
                        .ok()
                })
                .unwrap_or_else(|| normalized_path.display().to_string());
            io::format::InputFile {
                source_uri: uri,
                source_name,
                source_stem,
                source_size: metadata.as_ref().map(|metadata| metadata.len()),
                source_mtime: metadata
                    .as_ref()
                    .and_then(|metadata| metadata.modified().ok())
                    .and_then(system_time_to_rfc3339),
            }
        })
        .collect()
}

/// Resolves an `InputFile` to a `LocalInputFile` with a guaranteed local path.
///
/// For cloud URIs (s3://, gs://, abfs://) the file is downloaded to `temp_dir`
/// and `is_ephemeral` is set to `true` so callers can delete it after use.
/// For local URIs the source path is used directly with no copy.
pub fn localize_input(
    input_file: io::format::InputFile,
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
    temp_dir: Option<&Path>,
) -> FloeResult<io::format::LocalInputFile> {
    if is_cloud_uri(&input_file.source_uri) {
        let client = storage_client.ok_or_else(|| {
            Box::new(RunError(format!(
                "storage client required to download {}",
                input_file.source_uri
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let temp = temp_dir.ok_or_else(|| {
            Box::new(RunError(format!(
                "temp_dir required to download {}",
                input_file.source_uri
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let local_path = client.download_to_temp(&input_file.source_uri, temp)?;
        Ok(io::format::LocalInputFile {
            file: input_file,
            local_path,
            is_ephemeral: true,
        })
    } else {
        // Local URI: strip the "local://" scheme if present; the rest is the path.
        let path_str = input_file.source_uri.trim_start_matches("local://");
        Ok(io::format::LocalInputFile {
            local_path: PathBuf::from(path_str),
            file: input_file,
            is_ephemeral: false,
        })
    }
}

fn is_cloud_uri(uri: &str) -> bool {
    uri.starts_with("s3://")
        || uri.starts_with("gs://")
        || uri.starts_with("abfs://")
        || uri.starts_with("az://")
        || uri.starts_with("gcs://")
}

fn system_time_to_rfc3339(value: SystemTime) -> Option<String> {
    time::OffsetDateTime::from(value)
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

fn build_local_listing(
    files: &[PathBuf],
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> Vec<String> {
    files
        .iter()
        .map(|path| {
            let normalized_path = crate::io::storage::paths::normalize_local_path(path);
            storage_client
                .and_then(|client| {
                    client
                        .resolve_uri(&normalized_path.display().to_string())
                        .ok()
                })
                .unwrap_or_else(|| normalized_path.display().to_string())
        })
        .collect()
}

#[derive(Debug)]
struct CloudSourceMatch {
    list_prefix: String,
    glob_pattern: Option<String>,
    matcher: Option<Pattern>,
}

impl CloudSourceMatch {
    fn new(source_path: &str) -> FloeResult<Self> {
        if !contains_glob_metachar(source_path) {
            return Ok(Self {
                list_prefix: source_path.to_string(),
                glob_pattern: None,
                matcher: None,
            });
        }

        let list_prefix = prefix_before_first_glob(source_path);
        if list_prefix.trim_matches('/').is_empty() {
            return Err(Box::new(crate::errors::RunError(
                "glob patterns for cloud sources must include a non-empty literal prefix before the first wildcard"
                    .to_string(),
            )));
        }

        let matcher = Pattern::new(source_path).map_err(|err| {
            Box::new(crate::errors::RunError(format!(
                "invalid cloud glob pattern {:?}: {err}",
                source_path
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Self {
            list_prefix: list_prefix.to_string(),
            glob_pattern: Some(source_path.to_string()),
            matcher: Some(matcher),
        })
    }

    fn list_prefix(&self) -> &str {
        self.list_prefix.as_str()
    }

    fn matches_key(&self, key: &str) -> bool {
        match &self.matcher {
            Some(matcher) => matcher.matches_with(key, cloud_glob_match_options()),
            None => true,
        }
    }

    fn match_description(&self) -> String {
        match &self.glob_pattern {
            Some(pattern) => format!("glob={pattern}"),
            None => "glob=<none>".to_string(),
        }
    }
}

fn filter_cloud_list_refs(
    list_refs: Vec<io::storage::planner::ObjectRef>,
    source_match: &CloudSourceMatch,
    suffixes: &[String],
) -> Vec<io::storage::planner::ObjectRef> {
    let filtered = list_refs
        .into_iter()
        .filter(|obj| source_match.matches_key(&obj.key))
        .collect::<Vec<_>>();
    let filtered = planner::filter_by_suffixes(filtered, suffixes);
    planner::stable_sort_refs(filtered)
}

fn contains_glob_metachar(value: &str) -> bool {
    first_glob_metachar_index(value).is_some()
}

fn prefix_before_first_glob(value: &str) -> &str {
    match first_glob_metachar_index(value) {
        Some(index) => &value[..index],
        None => value,
    }
}

fn first_glob_metachar_index(value: &str) -> Option<usize> {
    let mut escaped = false;
    for (idx, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if matches!(ch, '*' | '?') {
            return Some(idx);
        }
    }
    None
}

fn cloud_glob_match_options() -> MatchOptions {
    MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    }
}
