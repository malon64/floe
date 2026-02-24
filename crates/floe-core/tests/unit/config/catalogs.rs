use floe_core::config;

fn base_root() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("s3_out".to_string()),
            definitions: vec![
                config::StorageDefinition {
                    name: "s3_out".to_string(),
                    fs_type: "s3".to_string(),
                    bucket: Some("data-bucket".to_string()),
                    region: Some("us-east-1".to_string()),
                    account: None,
                    container: None,
                    prefix: Some("accepted".to_string()),
                },
                config::StorageDefinition {
                    name: "s3_wh".to_string(),
                    fs_type: "s3".to_string(),
                    bucket: Some("warehouse-bucket".to_string()),
                    region: Some("us-east-1".to_string()),
                    account: None,
                    container: None,
                    prefix: Some("warehouse".to_string()),
                },
            ],
        }),
        catalogs: Some(config::CatalogsConfig {
            default: Some("glue_main".to_string()),
            definitions: vec![config::CatalogDefinition {
                name: "glue_main".to_string(),
                catalog_type: "glue".to_string(),
                region: Some("us-east-1".to_string()),
                database: Some("lakehouse".to_string()),
                warehouse_storage: Some("s3_wh".to_string()),
                warehouse_prefix: Some("iceberg".to_string()),
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    }
}

fn entity() -> config::EntityConfig {
    config::EntityConfig {
        name: "Customer Orders".to_string(),
        metadata: None,
        domain: Some("Sales Ops".to_string()),
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Append,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "customer/orders".to_string(),
                storage: Some("s3_out".to_string()),
                options: None,
                iceberg: Some(config::IcebergSinkTargetConfig {
                    catalog: Some("glue_main".to_string()),
                    namespace: None,
                    table: None,
                    location: None,
                }),
                partition_by: None,
                partition_spec: None,
                write_mode: config::WriteMode::Append,
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
fn catalog_resolver_derives_glue_identity_and_warehouse_location() {
    let root = base_root();
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let entity = entity();

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("glue target");

    assert_eq!(resolved.catalog_name, "glue_main");
    assert_eq!(resolved.catalog_type, "glue");
    assert_eq!(resolved.database, "lakehouse");
    assert_eq!(resolved.namespace, "sales_ops");
    assert_eq!(resolved.table, "customer_orders");
    assert_eq!(
        resolved.table_location.uri,
        "s3://warehouse-bucket/warehouse/iceberg/sales_ops/customer_orders"
    );
}

#[test]
fn catalog_resolver_honors_explicit_location_override() {
    let root = base_root();
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let mut entity = entity();
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("glue_main".to_string()),
        namespace: Some("billing".to_string()),
        table: Some("invoice_events".to_string()),
        location: Some("custom/location".to_string()),
    });

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("glue target");

    assert_eq!(resolved.namespace, "billing");
    assert_eq!(resolved.table, "invoice_events");
    assert_eq!(
        resolved.table_location.uri,
        "s3://warehouse-bucket/warehouse/custom/location"
    );
}
