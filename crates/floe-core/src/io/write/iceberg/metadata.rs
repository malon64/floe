use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{errors::StorageError, FloeResult};

pub(crate) fn latest_local_metadata_location(table_root: &Path) -> FloeResult<Option<String>> {
    let metadata_dir = table_root.join("metadata");
    if !metadata_dir.exists() {
        return Ok(None);
    }

    let mut best: Option<(i64, PathBuf)> = None;
    for entry in fs::read_dir(&metadata_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_path)) => {
                version > *best_version || (version == *best_version && path > *best_path)
            }
        };
        if replace {
            best = Some((version, path));
        }
    }

    Ok(best.map(|(_, path)| path.display().to_string()))
}

pub(crate) fn latest_s3_metadata_location(
    client: &mut dyn crate::io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

pub(crate) fn latest_gcs_metadata_location(
    client: &mut dyn crate::io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

/// List the Iceberg metadata directory on ADLS using an OpenDAL Azdls operator
/// built from the same `file_io_props` used for the write — avoiding the
/// OAuth-only floe blob client which fails when the CLI identity lacks
/// Storage Blob Data plane RBAC.
///
/// `warehouse_uri` is the fully-qualified `abfs[s]://container@account.dfs.core.windows.net/path`
/// table-root URI from `IcebergStoreConfig`.
#[cfg(feature = "iceberg")]
pub(crate) fn latest_adls_metadata_location_via_opendal(
    file_io_props: &HashMap<String, String>,
    warehouse_uri: &str,
) -> FloeResult<Option<String>> {
    use opendal::services::AzdlsConfig;
    use opendal::{Configurator, Operator};

    let url = url::Url::parse(warehouse_uri).map_err(|e| {
        Box::new(StorageError(format!(
            "adls iceberg warehouse uri invalid ({warehouse_uri}): {e}"
        )))
    })?;

    let filesystem = url.username().to_string();
    let host = url.host_str().unwrap_or("").to_string();
    let http_scheme = if warehouse_uri.starts_with("abfss://") {
        "https"
    } else {
        "http"
    };
    let endpoint = format!("{http_scheme}://{host}");

    // Container-relative path for the table root; metadata lives one level below.
    let table_path = url.path().trim_start_matches('/').trim_end_matches('/');
    let metadata_list_path = if table_path.is_empty() {
        "metadata/".to_string()
    } else {
        format!("{table_path}/metadata/")
    };

    let config = AzdlsConfig {
        filesystem: filesystem.clone(),
        endpoint: Some(endpoint),
        account_name: file_io_props.get("adls.account-name").cloned(),
        account_key: file_io_props.get("adls.account-key").cloned(),
        sas_token: file_io_props.get("adls.sas-token").cloned(),
        tenant_id: file_io_props.get("adls.tenant-id").cloned(),
        client_id: file_io_props.get("adls.client-id").cloned(),
        client_secret: file_io_props.get("adls.client-secret").cloned(),
        ..Default::default()
    };

    let op = Operator::new(config.into_builder())
        .map_err(|e| {
            Box::new(StorageError(format!(
                "adls iceberg opendal operator init failed: {e}"
            )))
        })?
        .finish();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            Box::new(StorageError(format!(
                "adls iceberg listing runtime init failed: {e}"
            )))
        })?;

    let entries = runtime
        .block_on(op.list(&metadata_list_path))
        .map_err(|e| {
            Box::new(StorageError(format!("adls list failed: {e}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;

    let adls_scheme = if warehouse_uri.starts_with("abfss://") {
        "abfss"
    } else {
        "abfs"
    };
    let uri_base = format!("{adls_scheme}://{filesystem}@{host}");

    let mut best: Option<(i64, String)> = None;
    for entry in &entries {
        let path = entry.path();
        let file_name = path.rsplit('/').next().unwrap_or("");
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(file_name) else {
            continue;
        };
        let take = match &best {
            None => true,
            Some((bv, bpath)) => version > *bv || (version == *bv && path > bpath.as_str()),
        };
        if take {
            let full_uri = format!("{uri_base}/{}", path.trim_start_matches('/'));
            best = Some((version, full_uri));
        }
    }

    Ok(best.map(|(_, uri)| uri))
}

fn latest_metadata_location_from_objects(
    objects: Vec<crate::io::storage::ObjectRef>,
) -> FloeResult<Option<String>> {
    let mut best: Option<(i64, String, String)> = None;
    for object in objects {
        let file_name = object
            .key
            .rsplit('/')
            .next()
            .unwrap_or(object.key.as_str())
            .to_string();
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(&file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_key, _)) => {
                version > *best_version || (version == *best_version && object.key > *best_key)
            }
        };
        if replace {
            best = Some((version, object.key.clone(), object.uri.clone()));
        }
    }
    Ok(best.map(|(_, _, uri)| uri))
}

pub(super) fn parse_metadata_version_from_location(location: &str) -> Option<i64> {
    let file_name = Path::new(location).file_name()?.to_str()?;
    parse_metadata_version_from_filename(file_name)
}

pub(super) fn parse_metadata_version_from_filename(file_name: &str) -> Option<i64> {
    let prefix = file_name.split_once('-')?.0;
    prefix.parse::<i64>().ok()
}
