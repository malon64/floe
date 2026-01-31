use floe_core::{
    config, io::storage::object_store::delta_store_config, io::storage::Target, FloeResult,
};

fn sample_config() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("s3_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "s3_raw".to_string(),
                fs_type: "s3".to_string(),
                bucket: Some("my-bucket".to_string()),
                region: Some("eu-west-1".to_string()),
                account: None,
                container: None,
                prefix: Some("data".to_string()),
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    }
}

#[test]
fn delta_store_config_builds_s3_url_and_options() -> FloeResult<()> {
    let config = sample_config();
    let resolver = config::StorageResolver::new(&config, std::path::Path::new("."))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        metadata: None,
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
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
    };

    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(store.table_url.as_str(), "s3://my-bucket/data/delta/orders");
    assert_eq!(
        store.storage_options.get("region").map(String::as_str),
        Some("eu-west-1")
    );
    assert_eq!(
        store.storage_options.get("bucket").map(String::as_str),
        Some("my-bucket")
    );
    Ok(())
}

#[test]
fn delta_store_config_builds_local_url() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    };
    let temp_dir = tempfile::TempDir::new()?;
    let resolver = config::StorageResolver::new(&config, temp_dir.path())?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        metadata: None,
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
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
    };
    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(store.storage_options.len(), 0);
    assert_eq!(store.table_url.scheme(), "file");
    Ok(())
}

#[test]
fn delta_store_config_builds_adls_url_and_options() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("adls_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "adls_raw".to_string(),
                fs_type: "adls".to_string(),
                bucket: None,
                region: None,
                account: Some("account".to_string()),
                container: Some("container".to_string()),
                prefix: Some("data".to_string()),
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    };
    let resolver = config::StorageResolver::new(&config, std::path::Path::new("."))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        metadata: None,
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
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
    };

    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(
        store.table_url.as_str(),
        "abfs://container@account.dfs.core.windows.net/data/delta/orders"
    );
    assert_eq!(
        store
            .storage_options
            .get("azure_storage_account_name")
            .map(String::as_str),
        Some("account")
    );
    assert_eq!(
        store
            .storage_options
            .get("azure_container_name")
            .map(String::as_str),
        Some("container")
    );
    Ok(())
}
