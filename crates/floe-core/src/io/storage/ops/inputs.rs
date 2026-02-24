use std::path::{Path, PathBuf};

use glob::{MatchOptions, Pattern};

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
    temp_dir: Option<&Path>,
) -> FloeResult<ResolvedInputs> {
    let objects = list_cloud_objects(client, prefix, adapter, entity, storage, location)?;
    let listed = objects.iter().map(|obj| obj.uri.clone()).collect();
    let files = match resolution_mode {
        ResolveInputsMode::Download => {
            let temp_dir = require_temp_dir(temp_dir, storage)?;
            build_cloud_inputs(client, &objects, temp_dir, entity)?
        }
        ResolveInputsMode::ListOnly => Vec::new(),
    };
    Ok(ResolvedInputs {
        files,
        listed,
        mode: report::ResolvedInputMode::Directory,
    })
}

fn require_temp_dir<'a>(temp_dir: Option<&'a Path>, label: &str) -> FloeResult<&'a Path> {
    temp_dir.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(crate::errors::RunError(format!(
            "{} tempdir missing",
            label
        )))
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

fn build_cloud_inputs(
    client: &dyn crate::io::storage::StorageClient,
    objects: &[io::storage::planner::ObjectRef],
    temp_dir: &Path,
    entity: &config::EntityConfig,
) -> FloeResult<Vec<io::format::InputFile>> {
    let mut inputs = Vec::with_capacity(objects.len());
    for object in objects {
        let local_path = client.download_to_temp(&object.uri, temp_dir)?;
        let source_name =
            planner::file_name_from_key(&object.key).unwrap_or_else(|| entity.name.clone());
        let source_stem =
            planner::file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
        let source_uri = object.uri.clone();
        inputs.push(io::format::InputFile {
            source_uri,
            source_local_path: local_path,
            source_name,
            source_stem,
        });
    }
    Ok(inputs)
}

fn build_local_inputs(
    files: &[PathBuf],
    entity: &config::EntityConfig,
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> Vec<io::format::InputFile> {
    files
        .iter()
        .map(|path| {
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
                .and_then(|client| client.resolve_uri(&path.display().to_string()).ok())
                .unwrap_or_else(|| path.display().to_string());
            io::format::InputFile {
                source_uri: uri,
                source_local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}

fn build_local_listing(
    files: &[PathBuf],
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> Vec<String> {
    files
        .iter()
        .map(|path| {
            storage_client
                .and_then(|client| client.resolve_uri(&path.display().to_string()).ok())
                .unwrap_or_else(|| path.display().to_string())
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
        if matches!(ch, '*' | '?' | '[') {
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

#[cfg(test)]
mod tests {
    use super::{filter_cloud_list_refs, CloudSourceMatch};
    use crate::io::storage::planner;

    fn object(uri: &str, key: &str) -> planner::ObjectRef {
        planner::object_ref(uri.to_string(), key.to_string(), None, None)
    }

    #[test]
    fn cloud_glob_filters_wildcard_suffix_and_preserves_suffix_filtering() {
        let source_match = CloudSourceMatch::new("data/sales_*.csv").expect("valid glob");
        assert_eq!(source_match.list_prefix(), "data/sales_");

        let refs = vec![
            object("s3://bucket/data/sales_2024.json", "data/sales_2024.json"),
            object("s3://bucket/data/sales_2024.csv", "data/sales_2024.csv"),
            object("s3://bucket/data/sales_q1.csv", "data/sales_q1.csv"),
            object(
                "s3://bucket/data/inventory_2024.csv",
                "data/inventory_2024.csv",
            ),
        ];
        let filtered = filter_cloud_list_refs(refs, &source_match, &[".csv".to_string()]);
        let keys = filtered.into_iter().map(|obj| obj.key).collect::<Vec<_>>();
        assert_eq!(keys, vec!["data/sales_2024.csv", "data/sales_q1.csv"]);
    }

    #[test]
    fn cloud_glob_filters_nested_path_segment_wildcard() {
        let source_match = CloudSourceMatch::new("data/*/sales.csv").expect("valid glob");
        assert_eq!(source_match.list_prefix(), "data/");

        let refs = vec![
            object("gs://bucket/data/us/sales.csv", "data/us/sales.csv"),
            object("gs://bucket/data/eu/sales.csv", "data/eu/sales.csv"),
            object(
                "gs://bucket/data/us/archive/sales.csv",
                "data/us/archive/sales.csv",
            ),
            object("gs://bucket/data/sales.csv", "data/sales.csv"),
        ];
        let filtered = filter_cloud_list_refs(refs, &source_match, &[".csv".to_string()]);
        let keys = filtered.into_iter().map(|obj| obj.key).collect::<Vec<_>>();
        assert_eq!(keys, vec!["data/eu/sales.csv", "data/us/sales.csv"]);
    }

    #[test]
    fn cloud_glob_rejects_patterns_without_literal_listing_prefix() {
        let err = CloudSourceMatch::new("*.csv").expect_err("missing prefix should error");
        let msg = err.to_string();
        assert!(msg.contains("non-empty literal prefix"));
    }

    #[test]
    fn cloud_glob_rejects_invalid_patterns() {
        let err = CloudSourceMatch::new("data/[sales.csv").expect_err("invalid glob");
        let msg = err.to_string();
        assert!(msg.contains("invalid cloud glob pattern"));
    }

    #[test]
    fn cloud_glob_results_are_stably_sorted() {
        let source_match = CloudSourceMatch::new("root/*.csv").expect("valid glob");
        let refs = vec![
            object(
                "abfs://container@acct.dfs.core.windows.net/root/b.csv",
                "root/b.csv",
            ),
            object(
                "abfs://container@acct.dfs.core.windows.net/root/a.csv",
                "root/a.csv",
            ),
            object(
                "abfs://container@acct.dfs.core.windows.net/root/c.csv",
                "root/c.csv",
            ),
        ];
        let filtered = filter_cloud_list_refs(refs, &source_match, &[".csv".to_string()]);
        let uris = filtered.into_iter().map(|obj| obj.uri).collect::<Vec<_>>();
        assert_eq!(
            uris,
            vec![
                "abfs://container@acct.dfs.core.windows.net/root/a.csv",
                "abfs://container@acct.dfs.core.windows.net/root/b.csv",
                "abfs://container@acct.dfs.core.windows.net/root/c.csv",
            ]
        );
    }
}
