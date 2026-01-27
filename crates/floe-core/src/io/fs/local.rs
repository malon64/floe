use std::path::{Path, PathBuf};

use glob::glob;

use crate::{config, ConfigError, FloeResult};

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
            return Err(Box::new(ConfigError(no_match_message(
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
        return Err(Box::new(ConfigError(no_match_message(
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
        return Err(Box::new(ConfigError(no_match_message(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        path.push(format!("{prefix}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(path, contents).expect("write file");
    }

    fn default_globs(format: &str) -> Vec<String> {
        crate::io::fs::extensions::glob_patterns_for_format(format).expect("default globs")
    }

    fn source_config(
        format: &str,
        path: &Path,
        options: Option<config::SourceOptions>,
    ) -> config::SourceConfig {
        config::SourceConfig {
            format: format.to_string(),
            path: path.display().to_string(),
            storage: None,
            options,
            cast_mode: None,
        }
    }

    #[test]
    fn default_glob_filters_by_format() {
        let root = temp_dir("floe-resolve-default-glob");
        write_file(&root.join("a.csv"), "id\n1\n");
        write_file(&root.join("B.CSV"), "id\n2\n");
        write_file(&root.join("b.txt"), "skip\n");
        let source = source_config("csv", &root, None);

        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");

        let names = resolved
            .files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["B.CSV", "a.csv"]);
        assert_eq!(resolved.mode, LocalInputMode::Directory);
    }

    #[test]
    fn glob_override_is_used() {
        let root = temp_dir("floe-resolve-override-glob");
        write_file(&root.join("a.csv"), "id\n1\n");
        write_file(&root.join("b.data"), "id\n2\n");
        let options = config::SourceOptions {
            glob: Some("*.data".to_string()),
            ..config::SourceOptions::default()
        };
        let source = source_config("csv", &root, Some(options));

        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");
        let names = resolved
            .files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["b.data"]);
    }

    #[test]
    fn recursive_flag_controls_descent() {
        let root = temp_dir("floe-resolve-recursive");
        write_file(&root.join("root.csv"), "id\n1\n");
        write_file(&root.join("nested/child.csv"), "id\n2\n");
        let source = source_config("csv", &root, None);

        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");
        let names = resolved
            .files
            .iter()
            .map(|path| {
                path.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["root.csv"]);

        let options = config::SourceOptions {
            recursive: Some(true),
            ..config::SourceOptions::default()
        };
        let source = source_config("csv", &root, Some(options));
        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");
        let names = resolved
            .files
            .iter()
            .map(|path| {
                path.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["nested/child.csv", "root.csv"]);
    }

    #[test]
    fn glob_path_input_is_supported() {
        let root = temp_dir("floe-resolve-glob-path");
        write_file(&root.join("a.csv"), "id\n1\n");
        write_file(&root.join("b.csv"), "id\n2\n");
        let source = source_config("csv", &root.join("*.csv"), None);

        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");
        assert_eq!(resolved.files.len(), 2);
    }

    #[test]
    fn sorting_is_stable_by_full_path() {
        let root = temp_dir("floe-resolve-sort");
        write_file(&root.join("b.csv"), "id\n1\n");
        write_file(&root.join("a.csv"), "id\n2\n");
        let source = source_config("csv", &root, None);

        let resolved =
            resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
                .expect("resolve inputs");
        let names = resolved
            .files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.csv", "b.csv"]);
    }

    #[test]
    fn no_matches_error_includes_context() {
        let root = temp_dir("floe-resolve-empty");
        let options = config::SourceOptions {
            recursive: Some(true),
            ..config::SourceOptions::default()
        };
        let source = source_config("csv", &root, Some(options));

        let err = resolve_local_inputs(&root, "customer", &source, "local", &default_globs("csv"))
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("entity.name=customer"));
        assert!(message.contains("source.storage=local"));
        assert!(message.contains("base_path="));
        assert!(message.contains("glob="));
        assert!(message.contains("recursive=true"));
    }
}
