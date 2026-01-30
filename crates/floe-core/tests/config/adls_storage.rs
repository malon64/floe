use floe_core::config;
use floe_core::config::StorageResolver;
use floe_core::FloeResult;

fn base_root() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    }
}

#[test]
fn adls_uri_with_prefix_is_built() -> FloeResult<()> {
    let mut config = base_root();
    config.storages = Some(config::StoragesConfig {
        default: Some("adls".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "adls".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some("acct".to_string()),
            container: Some("cont".to_string()),
            prefix: Some("prefix".to_string()),
        }],
    });

    let resolver = StorageResolver::new(&config, std::path::Path::new("."))?;
    let resolved = resolver.resolve_path("entity", "source.storage", None, "data/file.csv")?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/prefix/data/file.csv"
    );
    Ok(())
}

#[test]
fn adls_uri_without_prefix_is_built() -> FloeResult<()> {
    let mut config = base_root();
    config.storages = Some(config::StoragesConfig {
        default: Some("adls".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "adls".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some("acct".to_string()),
            container: Some("cont".to_string()),
            prefix: None,
        }],
    });

    let resolver = StorageResolver::new(&config, std::path::Path::new("."))?;
    let resolved = resolver.resolve_path("entity", "source.storage", None, "data/file.csv")?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/data/file.csv"
    );
    Ok(())
}
