#[cfg(any(feature = "delta", feature = "iceberg"))]
use crate::errors::FloeError;
use std::collections::HashMap;

#[cfg(feature = "iceberg")]
use iceberg::io::{
    ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_SAS_TOKEN, CLIENT_REGION, S3_ACCESS_KEY_ID,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
#[cfg(feature = "delta")]
use url::Url;

#[cfg(any(feature = "delta", feature = "iceberg"))]
use crate::{config, FloeResult};

#[cfg(any(feature = "delta", feature = "iceberg"))]
use super::Target;

#[cfg(feature = "delta")]
pub struct DeltaStoreConfig {
    pub table_url: Url,
    pub storage_options: HashMap<String, String>,
}

#[cfg(feature = "iceberg")]
#[derive(Debug)]
pub struct IcebergStoreConfig {
    pub warehouse_location: String,
    pub file_io_props: HashMap<String, String>,
}

#[cfg(feature = "delta")]
pub fn delta_store_config(
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<DeltaStoreConfig> {
    match target {
        Target::Local { base_path, .. } => {
            let url = Url::from_directory_path(base_path).map_err(|_| {
                FloeError::config(format!(
                    "entity.name={} delta table path is not a valid url: {}",
                    entity.name, base_path
                ))
            })?;
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options: HashMap::new(),
            })
        }
        Target::S3 {
            storage,
            uri,
            bucket,
            ..
        } => {
            let url = Url::parse(uri).map_err(|err| {
                FloeError::config(format!(
                    "entity.name={} delta s3 path is invalid: {} ({err})",
                    entity.name, uri
                ))
            })?;
            let mut storage_options = HashMap::new();
            if let Some(definition) = resolver.definition(storage) {
                if let Some(region) = definition.region {
                    storage_options.insert("region".to_string(), region);
                }
            }
            storage_options.insert("bucket".to_string(), bucket.to_string());
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options,
            })
        }
        Target::Adls {
            uri,
            account,
            container,
            ..
        } => {
            let url = Url::parse(uri).map_err(|err| {
                FloeError::config(format!(
                    "entity.name={} delta adls path is invalid: {} ({err})",
                    entity.name, uri
                ))
            })?;
            let mut storage_options = HashMap::new();
            storage_options.insert(
                "azure_storage_account_name".to_string(),
                account.to_string(),
            );
            storage_options.insert("azure_container_name".to_string(), container.to_string());
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options,
            })
        }
        Target::Gcs { uri, .. } => {
            let url = Url::parse(uri).map_err(|err| {
                FloeError::config(format!(
                    "entity.name={} delta gcs path is invalid: {} ({err})",
                    entity.name, uri
                ))
            })?;
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options: HashMap::new(),
            })
        }
    }
}

#[cfg(feature = "iceberg")]
pub fn iceberg_store_config(
    target: &Target,
    resolver: &config::StorageResolver,
    _entity: &config::EntityConfig,
) -> FloeResult<IcebergStoreConfig> {
    match target {
        Target::Local { base_path, .. } => Ok(IcebergStoreConfig {
            warehouse_location: base_path.to_string(),
            file_io_props: HashMap::new(),
        }),
        Target::S3 { storage, uri, .. } => {
            let mut file_io_props = HashMap::new();
            if let Some(definition) = resolver.definition(storage) {
                if let Some(region) = &definition.region {
                    file_io_props.insert(S3_REGION.to_string(), region.clone());
                    file_io_props.insert(CLIENT_REGION.to_string(), region.clone());
                }
                if let Some(endpoint) = &definition.endpoint {
                    file_io_props.insert("s3.endpoint".to_string(), endpoint.clone());
                }
                if let Some(path_style) = definition.path_style_access {
                    file_io_props
                        .insert("s3.path-style-access".to_string(), path_style.to_string());
                }
            }
            Ok(IcebergStoreConfig {
                warehouse_location: uri.to_string(),
                file_io_props,
            })
        }
        Target::Gcs { uri, .. } => Ok(IcebergStoreConfig {
            warehouse_location: uri.to_string(),
            file_io_props: HashMap::new(),
        }),
        Target::Adls { uri, account, .. } => {
            let mut file_io_props = HashMap::new();
            let warehouse_location = uri
                .strip_prefix("abfs://")
                .map(|rest| format!("abfss://{rest}"))
                .unwrap_or_else(|| uri.to_string());
            file_io_props.insert(ADLS_ACCOUNT_NAME.to_string(), account.to_string());
            if let Ok(key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
                file_io_props.insert(ADLS_ACCOUNT_KEY.to_string(), key);
            }
            if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
                file_io_props.insert(ADLS_SAS_TOKEN.to_string(), sas);
            }
            Ok(IcebergStoreConfig {
                warehouse_location,
                file_io_props,
            })
        }
    }
}

/// Resolve AWS credentials through the AWS SDK default provider chain and inject them as
/// static `s3.*` properties for the opendal-backed Iceberg S3 writer.
///
/// The Iceberg write path builds its S3 operator through opendal + reqsign, whose default
/// credential chain does **not** understand EKS Pod Identity / ECS container credentials
/// (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + token file) and falls back to EC2 IMDS — which
/// fails inside EKS Pod Identity pods (#426). The AWS SDK chain (`aws-config`, the same one
/// the Glue catalog and S3 reads already use successfully) does resolve those credentials, so
/// we resolve them here and hand opendal explicit static credentials.
///
/// Best-effort and non-regressive: if no provider is configured or resolution fails, the props
/// are left untouched so existing static-env / IMDS setups keep working. Caller-supplied
/// credential props are never overwritten.
#[cfg(feature = "iceberg")]
pub async fn inject_aws_static_credentials(
    props: &mut HashMap<String, String>,
    region: Option<&str>,
) {
    use aws_credential_types::provider::ProvideCredentials;
    use aws_sdk_s3::config::Region as S3Region;

    // Respect explicitly-configured credentials; skip the resolver entirely.
    if props.contains_key(S3_ACCESS_KEY_ID) {
        return;
    }

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(S3Region::new(region.to_string()));
    }
    let sdk_config = loader.load().await;

    let Some(provider) = sdk_config.credentials_provider() else {
        return;
    };
    let Ok(creds) = provider.provide_credentials().await else {
        return;
    };

    apply_aws_credentials_to_props(props, &creds);
}

/// Map resolved AWS credentials onto the `s3.*` properties consumed by the opendal-backed
/// Iceberg S3 writer. Existing credential props are left untouched.
#[cfg(feature = "iceberg")]
fn apply_aws_credentials_to_props(
    props: &mut HashMap<String, String>,
    creds: &aws_credential_types::Credentials,
) {
    if props.contains_key(S3_ACCESS_KEY_ID) {
        return;
    }
    props.insert(
        S3_ACCESS_KEY_ID.to_string(),
        creds.access_key_id().to_string(),
    );
    props.insert(
        S3_SECRET_ACCESS_KEY.to_string(),
        creds.secret_access_key().to_string(),
    );
    if let Some(token) = creds.session_token() {
        props.insert(S3_SESSION_TOKEN.to_string(), token.to_string());
    }
}

#[cfg(all(test, feature = "iceberg"))]
mod credential_tests {
    use super::*;
    use aws_credential_types::Credentials;

    #[test]
    fn maps_temporary_credentials_to_s3_props() {
        let creds = Credentials::new(
            "AKIDEXAMPLE",
            "SECRET",
            Some("SESSION".into()),
            None,
            "test",
        );
        let mut props = HashMap::new();
        apply_aws_credentials_to_props(&mut props, &creds);

        assert_eq!(
            props.get(S3_ACCESS_KEY_ID).map(String::as_str),
            Some("AKIDEXAMPLE")
        );
        assert_eq!(
            props.get(S3_SECRET_ACCESS_KEY).map(String::as_str),
            Some("SECRET")
        );
        assert_eq!(
            props.get(S3_SESSION_TOKEN).map(String::as_str),
            Some("SESSION")
        );
    }

    #[test]
    fn omits_session_token_for_long_lived_credentials() {
        let creds = Credentials::new("AKIDEXAMPLE", "SECRET", None, None, "test");
        let mut props = HashMap::new();
        apply_aws_credentials_to_props(&mut props, &creds);

        assert_eq!(
            props.get(S3_ACCESS_KEY_ID).map(String::as_str),
            Some("AKIDEXAMPLE")
        );
        assert!(!props.contains_key(S3_SESSION_TOKEN));
    }

    #[test]
    fn does_not_overwrite_caller_supplied_credentials() {
        let creds = Credentials::new("RESOLVED", "RESOLVED_SECRET", None, None, "test");
        let mut props = HashMap::new();
        props.insert(S3_ACCESS_KEY_ID.to_string(), "USER_KEY".to_string());
        apply_aws_credentials_to_props(&mut props, &creds);

        assert_eq!(
            props.get(S3_ACCESS_KEY_ID).map(String::as_str),
            Some("USER_KEY")
        );
        assert!(!props.contains_key(S3_SECRET_ACCESS_KEY));
    }
}
