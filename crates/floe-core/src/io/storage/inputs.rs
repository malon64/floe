use std::path::{Path, PathBuf};

use crate::{config, io, report, FloeResult};

use super::Target;

#[derive(Debug, Clone)]
pub struct ResolvedInputs {
    pub files: Vec<io::format::InputFile>,
    pub mode: report::ResolvedInputMode,
}

pub fn resolve_inputs(
    config_dir: &Path,
    entity: &config::EntityConfig,
    adapter: &dyn io::format::InputAdapter,
    target: &Target,
    temp_dir: Option<&Path>,
    storage_client: Option<&dyn super::StorageClient>,
) -> FloeResult<ResolvedInputs> {
    match target {
        Target::S3 { storage, .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(crate::errors::RunError("s3 tempdir missing".to_string()))
            })?;
            let client = storage_client.ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 storage client missing".to_string(),
                ))
            })?;
            let (bucket, key) = target.s3_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 target missing bucket".to_string(),
                ))
            })?;
            let files = io::storage::s3::build_input_files(
                client, bucket, key, adapter, temp_dir, entity, storage,
            )?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Local { storage, .. } => {
            let resolved =
                adapter.resolve_local_inputs(config_dir, &entity.name, &entity.source, storage)?;
            let files = build_local_inputs(&resolved.files, entity);
            let mode = match resolved.mode {
                io::storage::local::LocalInputMode::File => report::ResolvedInputMode::File,
                io::storage::local::LocalInputMode::Directory => {
                    report::ResolvedInputMode::Directory
                }
            };
            Ok(ResolvedInputs { files, mode })
        }
    }
}

fn build_local_inputs(
    files: &[PathBuf],
    entity: &config::EntityConfig,
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
            io::format::InputFile {
                source_uri: path.display().to_string(),
                source_local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::storage::StorageClient;
    use std::collections::HashMap;

    struct MockStorageClient {
        keys: Vec<String>,
        contents: HashMap<String, String>,
    }

    impl MockStorageClient {
        fn new(keys: Vec<String>) -> Self {
            let mut contents = HashMap::new();
            for key in &keys {
                contents.insert(key.clone(), format!("data for {key}"));
            }
            Self { keys, contents }
        }
    }

    impl StorageClient for MockStorageClient {
        fn list(&self, _prefix: &str) -> FloeResult<Vec<String>> {
            Ok(self.keys.clone())
        }

        fn download(&self, key: &str, dest: &Path) -> FloeResult<()> {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let content = self
                .contents
                .get(key)
                .cloned()
                .unwrap_or_else(|| "missing".to_string());
            std::fs::write(dest, content)?;
            Ok(())
        }

        fn upload(&self, _key: &str, _path: &Path) -> FloeResult<()> {
            Ok(())
        }
    }

    struct MockAdapter;

    impl io::format::InputAdapter for MockAdapter {
        fn format(&self) -> &'static str {
            "csv"
        }

        fn suffixes(&self) -> FloeResult<Vec<String>> {
            Ok(vec![".csv".to_string()])
        }

        fn read_input_columns(
            &self,
            _entity: &config::EntityConfig,
            _input_file: &io::format::InputFile,
            _columns: &[config::ColumnConfig],
        ) -> Result<Vec<String>, io::format::FileReadError> {
            Ok(vec!["id".to_string()])
        }

        fn read_inputs(
            &self,
            _entity: &config::EntityConfig,
            _files: &[io::format::InputFile],
            _columns: &[config::ColumnConfig],
            _normalize_strategy: Option<&str>,
            _collect_raw: bool,
        ) -> FloeResult<Vec<io::format::ReadInput>> {
            Ok(Vec::new())
        }
    }

    fn mock_entity(name: &str) -> config::EntityConfig {
        config::EntityConfig {
            name: name.to_string(),
            metadata: None,
            source: config::SourceConfig {
                format: "csv".to_string(),
                path: "unused".to_string(),
                storage: None,
                options: None,
                cast_mode: None,
            },
            sink: config::SinkConfig {
                accepted: config::SinkTarget {
                    format: "csv".to_string(),
                    path: "out".to_string(),
                    storage: None,
                    options: None,
                },
                rejected: None,
                archive: None,
            },
            policy: config::PolicyConfig {
                severity: "warn".to_string(),
            },
            schema: config::SchemaConfig {
                normalize_columns: None,
                mismatch: None,
                columns: Vec::new(),
            },
        }
    }

    #[test]
    fn resolve_inputs_s3_filters_and_downloads() -> FloeResult<()> {
        let temp_dir = tempfile::TempDir::new()?;
        let keys = vec![
            "data/b.csv".to_string(),
            "data/a.csv".to_string(),
            "data/ignore.txt".to_string(),
        ];
        let client = MockStorageClient::new(keys);
        let adapter = MockAdapter;
        let entity = mock_entity("orders");
        let target = Target::S3 {
            storage: "s3_raw".to_string(),
            uri: "s3://bucket/data".to_string(),
            bucket: "bucket".to_string(),
            base_key: "data".to_string(),
        };

        let resolved = resolve_inputs(
            Path::new("."),
            &entity,
            &adapter,
            &target,
            Some(temp_dir.path()),
            Some(&client),
        )?;

        assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
        assert_eq!(resolved.files.len(), 2);
        assert!(resolved.files[0].source_uri.ends_with("data/a.csv"));
        assert!(resolved.files[1].source_uri.ends_with("data/b.csv"));
        for input in resolved.files {
            assert!(input.source_local_path.exists());
        }
        Ok(())
    }
}
