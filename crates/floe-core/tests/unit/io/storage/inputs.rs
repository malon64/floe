use std::collections::HashMap;
use std::path::{Path, PathBuf};

use floe_core::io::storage::inputs::{resolve_inputs, ResolveInputsMode};
use floe_core::io::storage::{ObjectRef, StorageClient, Target};
use floe_core::{config, io, report, FloeResult};

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
    fn list(&self, _prefix: &str) -> FloeResult<Vec<ObjectRef>> {
        Ok(self
            .keys
            .iter()
            .map(|key| ObjectRef {
                uri: format!("s3://bucket/{key}"),
                key: key.clone(),
                last_modified: None,
                size: None,
            })
            .collect())
    }

    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
        let key = uri.strip_prefix("s3://bucket/").unwrap_or(uri).to_string();
        let dest = temp_dir.join(key.replace('/', "_"));
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = self
            .contents
            .get(&key)
            .cloned()
            .unwrap_or_else(|| "missing".to_string());
        std::fs::write(&dest, content)?;
        Ok(dest)
    }

    fn upload_from_path(&self, _local_path: &Path, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(format!("s3://bucket/{path}"))
    }

    fn copy_object(&self, _src_uri: &str, _dst_uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn delete_object(&self, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn exists(&self, _uri: &str) -> FloeResult<bool> {
        Ok(false)
    }
}

struct ListOnlyStorageClient {
    keys: Vec<String>,
}

impl ListOnlyStorageClient {
    fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl StorageClient for ListOnlyStorageClient {
    fn list(&self, _prefix: &str) -> FloeResult<Vec<ObjectRef>> {
        Ok(self
            .keys
            .iter()
            .map(|key| ObjectRef {
                uri: format!("s3://bucket/{key}"),
                key: key.clone(),
                last_modified: None,
                size: None,
            })
            .collect())
    }

    fn download_to_temp(&self, _uri: &str, _temp_dir: &Path) -> FloeResult<PathBuf> {
        panic!("download_to_temp should not be called in list-only mode");
    }

    fn upload_from_path(&self, _local_path: &Path, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(format!("s3://bucket/{path}"))
    }

    fn copy_object(&self, _src_uri: &str, _dst_uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn delete_object(&self, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn exists(&self, _uri: &str) -> FloeResult<bool> {
        Ok(false)
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

struct MockParquetAdapter;

impl io::format::InputAdapter for MockParquetAdapter {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn suffixes(&self) -> FloeResult<Vec<String>> {
        Ok(vec![".parquet".to_string()])
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
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "unused".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "csv".to_string(),
                path: "out".to_string(),
                storage: None,
                options: None,
                write_mode: config::WriteMode::Overwrite,
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
        ResolveInputsMode::Download,
        Some(temp_dir.path()),
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 2);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.files[0].source_uri.ends_with("data/a.csv"));
    assert!(resolved.files[1].source_uri.ends_with("data/b.csv"));
    for input in resolved.files {
        assert!(input.source_local_path.exists());
    }
    Ok(())
}

#[test]
fn resolve_inputs_s3_filters_parquet_objects() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let keys = vec![
        "data/b.parquet".to_string(),
        "data/a.parquet".to_string(),
        "data/ignore.csv".to_string(),
    ];
    let client = MockStorageClient::new(keys);
    let adapter = MockParquetAdapter;
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
        ResolveInputsMode::Download,
        Some(temp_dir.path()),
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 2);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.files[0].source_uri.ends_with("data/a.parquet"));
    assert!(resolved.files[1].source_uri.ends_with("data/b.parquet"));
    Ok(())
}

#[test]
fn resolve_inputs_s3_list_only_does_not_download() -> FloeResult<()> {
    let keys = vec![
        "data/b.csv".to_string(),
        "data/a.csv".to_string(),
        "data/ignore.txt".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
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
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 0);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.listed[0].ends_with("data/a.csv"));
    assert!(resolved.listed[1].ends_with("data/b.csv"));
    Ok(())
}
