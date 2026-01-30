use floe_core::config;

fn base_entity() -> config::EntityConfig {
    config::EntityConfig {
        name: "customer".to_string(),
        metadata: None,
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in.csv".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            accepted: config::SinkTarget {
                format: "csv".to_string(),
                path: "out.csv".to_string(),
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
fn adls_missing_required_fields_errors() {
    let mut config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("adls".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "adls".to_string(),
                fs_type: "adls".to_string(),
                bucket: None,
                region: None,
                account: None,
                container: None,
                prefix: None,
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        entities: vec![base_entity()],
    };

    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    assert!(err.to_string().contains("requires account"));

    config.storages.as_mut().unwrap().definitions[0].account = Some("acct".to_string());
    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    assert!(err.to_string().contains("requires container"));
}

#[test]
fn adls_referenced_errors_until_implemented() {
    let mut config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
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
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        entities: vec![base_entity()],
    };

    config.entities[0].source.storage = Some("adls".to_string());
    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    assert!(err
        .to_string()
        .contains("source.storage=adls is not implemented"));

    config.entities[0].source.storage = Some("local".to_string());
    config.entities[0].sink.accepted.storage = Some("adls".to_string());
    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    let message = err.to_string();
    assert!(!message.is_empty());
}
