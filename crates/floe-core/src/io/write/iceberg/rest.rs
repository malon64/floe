use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use iceberg::io::{LocalFsStorageFactory, StorageFactory};
use iceberg::spec::UnboundPartitionSpec;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use crate::config::CatalogTypeConfig;
use crate::errors::RunError;
use crate::io::format::CatalogRegistration;
use crate::{config, FloeResult};

use super::data_files::write_data_files;
use super::metadata::parse_metadata_version_from_location;
use super::{map_iceberg_err, IcebergWriteResult, PreparedIcebergWrite};

/// Build a REST catalog from the given config and file I/O props.
///
/// Credential routing:
///  - `"token:<value>"` → `"token"` property (bearer PAT: Unity Catalog, Nessie)
///  - `"client_credentials:<id>:<secret>"` → strip prefix → `"credential"` property (OAuth2: Polaris / Snowflake)
///  - anything else → raw `"credential"` property
pub(crate) async fn build_rest_catalog(
    rest_cfg: &RestIcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    table_location: &str,
) -> FloeResult<iceberg_catalog_rest::RestCatalog> {
    let mut props: HashMap<String, String> = file_io_props;

    props.insert(REST_CATALOG_PROP_URI.to_string(), rest_cfg.uri.clone());

    if let Some(warehouse) = rest_cfg.warehouse.as_deref() {
        props.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse.to_string(),
        );
        // When warehouse is not configured, omit the property so the REST server
        // uses its own default warehouse rather than receiving an arbitrary path.
    }

    if let Some(credential) = rest_cfg.credential.as_deref() {
        let credential = expand_env_refs(credential, &rest_cfg.catalog_name)?;
        if let Some(token_value) = credential.strip_prefix("token:") {
            // Bearer PAT (Unity Catalog / Nessie)
            props.insert("token".to_string(), token_value.to_string());
        } else if let Some(rest) = credential.strip_prefix("client_credentials:") {
            // OAuth2 client credentials — store as "credential" with id:secret format.
            props.insert("credential".to_string(), rest.to_string());
        } else {
            // Raw credential fallthrough.
            props.insert("credential".to_string(), credential.to_string());
        }
    }

    if let Some(oauth2_uri) = rest_cfg.oauth2_server_uri.as_deref() {
        props.insert("oauth2-server-uri".to_string(), oauth2_uri.to_string());
    }

    if let Some(scope) = rest_cfg.scope.as_deref() {
        props.insert("scope".to_string(), scope.to_string());
    }

    // Prefer the concrete table location for storage factory dispatch; fall back to the
    // warehouse field for cases where the caller only has catalog-level config (e.g. seeding).
    let effective_uri = if !table_location.is_empty() {
        table_location
    } else {
        rest_cfg.warehouse.as_deref().unwrap_or("")
    };
    let storage_factory: Arc<dyn StorageFactory> =
        if effective_uri.starts_with("s3://") || effective_uri.starts_with("s3a://") {
            let scheme = effective_uri
                .split("://")
                .next()
                .unwrap_or("s3")
                .to_string();
            Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: scheme,
                customized_credential_load: None,
            })
        } else if effective_uri.starts_with("gs://") {
            Arc::new(OpenDalStorageFactory::Gcs)
        } else {
            Arc::new(LocalFsStorageFactory)
        };

    RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("floe_rest", props)
        .await
        .map_err(map_iceberg_err("rest catalog init failed"))
}

/// Config struct for REST Iceberg catalog operations.
#[derive(Debug, Clone)]
pub(crate) struct RestIcebergCatalogConfig {
    pub(crate) catalog_name: String,
    pub(crate) uri: String,
    pub(crate) credential: Option<String>,
    pub(crate) warehouse: Option<String>,
    pub(crate) oauth2_server_uri: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

impl RestIcebergCatalogConfig {
    pub(crate) fn from_type_config(
        catalog_name: String,
        type_config: &CatalogTypeConfig,
        namespace: String,
        table: String,
    ) -> FloeResult<Self> {
        match type_config {
            CatalogTypeConfig::Rest {
                uri,
                credential,
                warehouse,
                oauth2_server_uri,
                scope,
            } => Ok(Self {
                catalog_name,
                uri: uri.clone(),
                credential: credential.clone(),
                warehouse: warehouse.clone(),
                oauth2_server_uri: oauth2_server_uri.clone(),
                scope: scope.clone(),
                namespace,
                table,
            }),
            _ => Err(Box::new(RunError(
                "RestIcebergCatalogConfig::from_type_config called on non-REST catalog".to_string(),
            ))),
        }
    }
}

fn expand_env_refs(value: &str, catalog_name: &str) -> FloeResult<String> {
    if !value.contains("${") {
        return Ok(value.to_string());
    }

    let mut parts = Vec::new();
    for part in value.split(':') {
        parts.push(expand_env_ref_part(part, catalog_name)?);
    }
    Ok(parts.join(":"))
}

fn expand_env_ref_part(part: &str, catalog_name: &str) -> FloeResult<String> {
    let Some(inner) = part.strip_prefix("${") else {
        return Ok(part.to_string());
    };
    let Some(name) = inner.strip_suffix('}') else {
        return Err(Box::new(RunError(format!(
            "rest iceberg catalog {catalog_name} credential has unclosed env placeholder"
        ))));
    };
    if name.is_empty() || name.contains('{') || name.contains('}') {
        return Err(Box::new(RunError(format!(
            "rest iceberg catalog {catalog_name} credential has invalid env placeholder"
        ))));
    }
    std::env::var(name).map_err(|_| {
        Box::new(RunError(format!(
            "rest iceberg catalog {catalog_name} credential references env var {name} which is not set"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

pub(crate) async fn write_via_rest_catalog(
    rest_cfg: &RestIcebergCatalogConfig,
    table_root_uri: String,
    file_io_props: HashMap<String, String>,
    prepared: PreparedIcebergWrite,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    small_file_threshold_bytes: u64,
) -> FloeResult<IcebergWriteResult> {
    let catalog = build_rest_catalog(rest_cfg, file_io_props, &table_root_uri).await?;

    let namespace_name = rest_cfg.namespace.clone();
    let namespace = NamespaceIdent::new(namespace_name);
    ensure_rest_namespace(&catalog, &namespace).await?;

    let table_ident = TableIdent::new(namespace.clone(), rest_cfg.table.clone());

    let existing_table = if catalog
        .table_exists(&table_ident)
        .await
        .map_err(map_iceberg_err("rest catalog table_exists check failed"))?
    {
        Some(
            catalog
                .load_table(&table_ident)
                .await
                .map_err(map_iceberg_err("rest catalog load_table failed"))?,
        )
    } else {
        None
    };

    if let Some(existing) = existing_table.as_ref() {
        super::schema::ensure_schema_matches(
            existing.metadata().current_schema(),
            &prepared.iceberg_schema,
            entity,
        )?;
        super::schema::ensure_partition_spec_matches(
            existing.metadata().default_partition_spec(),
            prepared.partition_spec.as_ref(),
            &prepared.iceberg_schema,
            entity,
        )?;
    }

    let table = match mode {
        config::WriteMode::Append => match existing_table {
            Some(table) => table,
            None => {
                create_rest_table(
                    &catalog,
                    &namespace,
                    &table_ident,
                    table_root_uri.clone(),
                    &prepared.iceberg_schema,
                    prepared.partition_spec.clone(),
                )
                .await?
            }
        },
        config::WriteMode::Overwrite => {
            if existing_table.is_some() {
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(map_iceberg_err("rest catalog drop_table failed"))?;
            }
            create_rest_table(
                &catalog,
                &namespace,
                &table_ident,
                table_root_uri.clone(),
                &prepared.iceberg_schema,
                prepared.partition_spec.clone(),
            )
            .await?
        }
        config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2 => {
            return Err(Box::new(RunError(format!(
                "entity.name={} sink.write_mode={} is only supported for delta accepted sinks",
                entity.name,
                mode.as_str()
            ))));
        }
    };

    let mut file_paths = Vec::new();
    let mut file_sizes = Vec::new();
    let mut files_written = 0_u64;
    let mut table_after_write = table;
    let mut perf = crate::io::format::AcceptedWritePerfBreakdown::default();

    if prepared.batch.num_rows() > 0 {
        let data_write_start = Instant::now();
        let data_files = write_data_files(&table_after_write, prepared.batch).await?;
        perf.data_write_ms = Some(data_write_start.elapsed().as_millis() as u64);
        files_written = data_files.len() as u64;
        file_sizes = data_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .collect();
        file_paths = data_files
            .iter()
            .map(|file| {
                let file_path = file.file_path().to_string();
                std::path::Path::new(file_path.as_str())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or(file_path)
            })
            .take(50)
            .collect();

        let tx = Transaction::new(&table_after_write);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).map_err(map_iceberg_err(
            "rest iceberg append transaction apply failed",
        ))?;
        let commit_start = Instant::now();
        table_after_write = tx
            .commit(&catalog)
            .await
            .map_err(map_iceberg_err("rest iceberg commit failed"))?;
        perf.commit_ms = Some(commit_start.elapsed().as_millis() as u64);
    }

    let snapshot_id = table_after_write
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let metadata_version = table_after_write
        .metadata_location()
        .and_then(parse_metadata_version_from_location);

    let metrics_start = Instant::now();
    let metrics = super::super::metrics::summarize_written_file_sizes(
        &file_sizes,
        files_written,
        small_file_threshold_bytes,
    );
    perf.metrics_read_ms = Some(metrics_start.elapsed().as_millis() as u64);

    Ok(IcebergWriteResult {
        files_written,
        snapshot_id,
        metadata_version,
        file_paths,
        metrics,
        table_root_uri,
        catalog: Some(CatalogRegistration::IcebergRest {
            catalog_name: rest_cfg.catalog_name.clone(),
            namespace: rest_cfg.namespace.clone(),
            table: rest_cfg.table.clone(),
        }),
        perf,
    })
}

async fn ensure_rest_namespace(
    catalog: &iceberg_catalog_rest::RestCatalog,
    namespace: &NamespaceIdent,
) -> FloeResult<()> {
    let exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(map_iceberg_err(
            "rest catalog namespace_exists check failed",
        ))?;
    if !exists {
        catalog
            .create_namespace(namespace, HashMap::new())
            .await
            .map_err(map_iceberg_err("rest catalog namespace create failed"))?;
    }
    Ok(())
}

async fn create_rest_table(
    catalog: &iceberg_catalog_rest::RestCatalog,
    namespace: &NamespaceIdent,
    table_ident: &TableIdent,
    table_root: String,
    schema: &iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
) -> FloeResult<iceberg::table::Table> {
    use iceberg::TableCreation;

    let creation_builder = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location(table_root)
        .schema(schema.clone());
    let creation = match partition_spec {
        Some(partition_spec) => creation_builder.partition_spec(partition_spec).build(),
        None => creation_builder.build(),
    };
    catalog
        .create_table(namespace, creation)
        .await
        .map_err(map_iceberg_err("rest catalog create_table failed"))
}

#[cfg(test)]
mod tests {
    use super::expand_env_refs;

    #[test]
    fn expands_partial_env_refs_in_client_credentials() {
        std::env::set_var("FLOE_TEST_REST_CLIENT_ID", "client-id");
        std::env::set_var("FLOE_TEST_REST_CLIENT_SECRET", "client-secret");

        let expanded = expand_env_refs(
            "client_credentials:${FLOE_TEST_REST_CLIENT_ID}:${FLOE_TEST_REST_CLIENT_SECRET}",
            "polaris",
        )
        .expect("expand credential");

        assert_eq!(expanded, "client_credentials:client-id:client-secret");
        std::env::remove_var("FLOE_TEST_REST_CLIENT_ID");
        std::env::remove_var("FLOE_TEST_REST_CLIENT_SECRET");
    }

    #[test]
    fn expands_exact_env_ref_in_token_credential() {
        std::env::set_var("FLOE_TEST_REST_TOKEN", "pat-token");

        let expanded =
            expand_env_refs("token:${FLOE_TEST_REST_TOKEN}", "nessie").expect("expand token");

        assert_eq!(expanded, "token:pat-token");
        std::env::remove_var("FLOE_TEST_REST_TOKEN");
    }

    #[test]
    fn preserves_literal_credential_text_that_contains_env_ref_syntax() {
        let expanded =
            expand_env_refs("token:abc${def}ghi", "nessie").expect("preserve literal credential");

        assert_eq!(expanded, "token:abc${def}ghi");
    }

    #[test]
    fn errors_when_env_ref_is_missing() {
        std::env::remove_var("FLOE_TEST_REST_MISSING");

        let err = expand_env_refs(
            "client_credentials:${FLOE_TEST_REST_MISSING}:secret",
            "polaris",
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "rest iceberg catalog polaris credential references env var FLOE_TEST_REST_MISSING which is not set"
        );
    }

    #[test]
    fn errors_on_malformed_env_ref() {
        std::env::set_var("ID", "client-id");

        let err = expand_env_refs(
            "client_credentials:${ID}:literal-secret:${UNCLOSED",
            "polaris",
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "rest iceberg catalog polaris credential has unclosed env placeholder"
        );
        assert!(!err.to_string().contains("literal-secret"));
        std::env::remove_var("ID");
    }
}
