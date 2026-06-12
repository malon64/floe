use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
use azure_core::error::ErrorKind;
use azure_core::http::{Etag, RequestContent, StatusCode, Url};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientUploadOptions, BlobContainerClientListBlobsOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobServiceClient};
use futures::{StreamExt, TryStreamExt};
use tokio::runtime::Runtime;

use crate::errors::StorageError;
use crate::io::storage::{
    planner, uri, validation, ConditionalWrite, ObjectRef, StorageClient, StoredObject,
};
use crate::{config, FloeResult};

pub struct AdlsClient {
    account: String,
    container: String,
    prefix: String,
    runtime: Runtime,
    container_client: BlobContainerClient,
}

/// Ordered chain of token credentials, mirroring the retired
/// `DefaultAzureCredential` from the pre-official SDK: each source is probed at
/// token-acquisition time and the first one that succeeds is cached for
/// subsequent calls. The official `azure_identity` crate ships no equivalent
/// chain for production credentials, only the dev-time `DeveloperToolsCredential`.
#[derive(Debug)]
struct ChainedCredential {
    sources: Vec<Arc<dyn TokenCredential>>,
    cached_index: AtomicUsize,
}

#[async_trait::async_trait]
impl TokenCredential for ChainedCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let cached = self.cached_index.load(Ordering::Relaxed);
        if let Some(source) = self.sources.get(cached) {
            return source.get_token(scopes, options).await;
        }
        let mut errors = Vec::new();
        for (index, source) in self.sources.iter().enumerate() {
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => {
                    self.cached_index.store(index, Ordering::Relaxed);
                    return Ok(token);
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(azure_core::Error::with_message(
            ErrorKind::Credential,
            format!(
                "no Azure credential in the chain could authenticate: {}",
                errors.join("; ")
            ),
        ))
    }
}

/// Build the ADLS credential with the same resolution order users relied on
/// under the old `DefaultAzureCredential`: explicit service-principal env vars,
/// then workload identity, then managed identity, then developer tools (az CLI).
fn build_credential() -> FloeResult<Arc<dyn TokenCredential>> {
    if let (Ok(tenant), Ok(client), Ok(secret)) = (
        std::env::var("AZURE_TENANT_ID"),
        std::env::var("AZURE_CLIENT_ID"),
        std::env::var("AZURE_CLIENT_SECRET"),
    ) {
        let credential = ClientSecretCredential::new(&tenant, client, secret.into(), None)
            .map_err(|err| {
                Box::new(StorageError(format!(
                    "adls service-principal credential init failed: {err}"
                )))
            })?;
        return Ok(credential);
    }
    if std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok() {
        let credential = WorkloadIdentityCredential::new(None).map_err(|err| {
            Box::new(StorageError(format!(
                "adls workload-identity credential init failed: {err}"
            )))
        })?;
        return Ok(credential);
    }

    let mut sources: Vec<Arc<dyn TokenCredential>> = Vec::new();
    if let Ok(managed) = ManagedIdentityCredential::new(None) {
        sources.push(managed);
    }
    if let Ok(developer) = DeveloperToolsCredential::new(None) {
        sources.push(developer);
    }
    if sources.is_empty() {
        return Err(Box::new(StorageError(
            "adls credential init failed: no Azure credential source is available \
             (set AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET, run on a host \
             with managed identity, or sign in with the Azure CLI)"
                .to_string(),
        )));
    }
    Ok(Arc::new(ChainedCredential {
        sources,
        cached_index: AtomicUsize::new(usize::MAX),
    }))
}

impl AdlsClient {
    pub fn new(definition: &config::StorageDefinition) -> FloeResult<Self> {
        let account =
            validation::require_field(definition, definition.account.as_ref(), "account", "adls")?;
        let container = validation::require_field(
            definition,
            definition.container.as_ref(),
            "container",
            "adls",
        )?;
        let prefix = definition.prefix.clone().unwrap_or_default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Box::new(StorageError(format!("adls runtime init failed: {err}"))))?;
        let credential = build_credential()?;
        let service_url = Url::parse(&format!("https://{account}.blob.core.windows.net/"))
            .map_err(|err| {
                Box::new(StorageError(format!(
                    "adls service url for account {account} is invalid: {err}"
                )))
            })?;
        let service_client = BlobServiceClient::new(service_url, Some(credential), None)
            .map_err(|err| Box::new(StorageError(format!("adls client init failed: {err}"))))?;
        let container_client = service_client.blob_container_client(&container);
        Ok(Self {
            account,
            container,
            prefix,
            runtime,
            container_client,
        })
    }

    fn base_prefix(&self) -> String {
        planner::normalize_separators(&self.prefix)
    }

    fn full_path(&self, path: &str) -> String {
        let prefix = self.base_prefix();
        let joined = planner::join_prefix(&prefix, &planner::normalize_separators(path));
        joined.trim_start_matches('/').to_string()
    }

    fn format_abfs(&self, path: &str) -> String {
        format_abfs_uri(&self.container, &self.account, path)
    }
}

impl StorageClient for AdlsClient {
    fn list(&self, prefix_or_path: &str) -> FloeResult<Vec<ObjectRef>> {
        let prefix = self.full_path(prefix_or_path);
        self.runtime.block_on(async {
            let options = BlobContainerClientListBlobsOptions {
                prefix: Some(prefix),
                ..Default::default()
            };
            let mut pages = self
                .container_client
                .list_blobs(Some(options))
                .map_err(|err| {
                    Box::new(StorageError(format!("adls list failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?
                .into_pages();
            let mut refs = Vec::new();
            while let Some(page) = pages.try_next().await.map_err(|err| {
                Box::new(StorageError(format!("adls list failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })? {
                let segment = page.into_model().map_err(|err| {
                    Box::new(StorageError(format!("adls list decode failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                for blob in segment.blob_items {
                    let Some(key) = blob.name else { continue };
                    let uri = if key.is_empty() {
                        format!(
                            "abfs://{}@{}.dfs.core.windows.net",
                            self.container, self.account
                        )
                    } else {
                        format!(
                            "abfs://{}@{}.dfs.core.windows.net/{}",
                            self.container, self.account, key
                        )
                    };
                    let properties = blob.properties;
                    refs.push(planner::object_ref(
                        uri,
                        key,
                        properties
                            .as_ref()
                            .and_then(|props| props.last_modified)
                            .map(|modified| modified.to_string()),
                        properties.as_ref().and_then(|props| props.content_length),
                    ));
                }
            }
            Ok(planner::stable_sort_refs(refs))
        })
    }

    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
        let key = uri
            .split_once(".dfs.core.windows.net/")
            .map(|(_, tail)| tail)
            .unwrap_or("")
            .trim_start_matches('/')
            .to_string();
        if key.is_empty() {
            return Err(Box::new(StorageError(
                "adls download requires a blob path".to_string(),
            )));
        }
        let dest = planner::temp_path_for_key(temp_dir, &key);
        self.runtime.block_on(async {
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let response = self
                .container_client
                .blob_client(&key)
                .download(None)
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("adls download failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            let mut body = response.body;
            let mut file = tokio::fs::File::create(&dest).await?;
            while let Some(chunk) = body.next().await {
                let bytes = chunk.map_err(|err| {
                    Box::new(StorageError(format!("adls download read failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
            }
            tokio::io::AsyncWriteExt::flush(&mut file).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(dest)
    }

    fn upload_from_path(&self, local_path: &Path, uri: &str) -> FloeResult<()> {
        let key = uri
            .split_once(".dfs.core.windows.net/")
            .map(|(_, tail)| tail)
            .unwrap_or("")
            .trim_start_matches('/')
            .to_string();
        if key.is_empty() {
            return Err(Box::new(StorageError(
                "adls upload requires a blob path".to_string(),
            )));
        }
        self.runtime.block_on(async {
            let data = tokio::fs::read(local_path).await?;
            let options = BlobClientUploadOptions {
                blob_content_type: Some("application/octet-stream".to_string()),
                ..Default::default()
            };
            self.container_client
                .blob_client(&key)
                .upload(RequestContent::from(data), Some(options))
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("adls upload failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(self.format_abfs(&self.full_path(path)))
    }

    fn copy_object(&self, src_uri: &str, dst_uri: &str) -> FloeResult<()> {
        planner::copy_via_temp(self, src_uri, dst_uri)
    }

    fn delete_object(&self, uri: &str) -> FloeResult<()> {
        let key = uri
            .split_once(".dfs.core.windows.net/")
            .map(|(_, tail)| tail)
            .unwrap_or("")
            .trim_start_matches('/')
            .to_string();
        if key.is_empty() {
            return Ok(());
        }
        self.runtime.block_on(async {
            self.container_client
                .blob_client(&key)
                .delete(None)
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("adls delete failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }

    fn exists(&self, uri: &str) -> FloeResult<bool> {
        let key = uri
            .split_once(".dfs.core.windows.net/")
            .map(|(_, tail)| tail)
            .unwrap_or("")
            .trim_start_matches('/')
            .to_string();
        planner::exists_by_key(self, &key)
    }

    fn read_object(&self, uri: &str) -> FloeResult<Option<StoredObject>> {
        let key = adls_key_from_uri(uri)?;
        self.runtime.block_on(async {
            let response = match self.container_client.blob_client(&key).download(None).await {
                Ok(response) => response,
                Err(err) if is_not_found(&err) => return Ok(None),
                Err(err) => {
                    return Err(
                        Box::new(StorageError(format!("adls download failed: {err}")))
                            as Box<dyn std::error::Error + Send + Sync>,
                    )
                }
            };
            let version = response.properties.etag.clone().map(String::from);
            let body = response.body.collect().await.map_err(|err| {
                Box::new(StorageError(format!("adls download read failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;
            let Some(version) = version else {
                return Err(Box::new(StorageError(format!(
                    "adls download response for {key} is missing an etag"
                )))
                    as Box<dyn std::error::Error + Send + Sync>);
            };
            Ok(Some(StoredObject {
                body: body.to_vec(),
                version,
            }))
        })
    }

    fn write_object_conditional(
        &self,
        uri: &str,
        expected_version: Option<&str>,
        body: &[u8],
    ) -> FloeResult<ConditionalWrite> {
        let key = adls_key_from_uri(uri)?;
        let body = body.to_vec();
        self.runtime.block_on(async {
            let options = BlobClientUploadOptions {
                blob_content_type: Some("application/json".to_string()),
                if_match: expected_version.map(Etag::from),
                if_none_match: expected_version.is_none().then(|| Etag::from("*")),
                ..Default::default()
            };
            match self
                .container_client
                .blob_client(&key)
                .upload(RequestContent::from(body), Some(options))
                .await
            {
                Ok(result) => {
                    let Some(etag) = result.etag else {
                        return Err(Box::new(StorageError(format!(
                            "adls upload response for {key} is missing an etag"
                        )))
                            as Box<dyn std::error::Error + Send + Sync>);
                    };
                    Ok(ConditionalWrite::Written {
                        version: String::from(etag),
                    })
                }
                Err(err)
                    if is_precondition_failed(&err)
                        || (expected_version.is_none() && is_create_race(&err)) =>
                {
                    Ok(ConditionalWrite::Conflict)
                }
                Err(err) => Err(Box::new(StorageError(format!("adls upload failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }

    fn delete_object_conditional(
        &self,
        uri: &str,
        expected_version: Option<&str>,
    ) -> FloeResult<ConditionalWrite> {
        let Some(expected_version) = expected_version else {
            return Ok(ConditionalWrite::Written {
                version: "deleted".to_string(),
            });
        };
        let key = adls_key_from_uri(uri)?;
        self.runtime.block_on(async {
            let options = BlobClientDeleteOptions {
                if_match: Some(Etag::from(expected_version)),
                ..Default::default()
            };
            match self
                .container_client
                .blob_client(&key)
                .delete(Some(options))
                .await
            {
                Ok(_) => Ok(ConditionalWrite::Written {
                    version: "deleted".to_string(),
                }),
                Err(err) if is_precondition_failed(&err) => Ok(ConditionalWrite::Conflict),
                Err(err) if is_not_found(&err) => Ok(ConditionalWrite::Written {
                    version: "deleted".to_string(),
                }),
                Err(err) => Err(Box::new(StorageError(format!("adls delete failed: {err}")))
                    as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }
}

fn adls_key_from_uri(uri: &str) -> FloeResult<String> {
    let key = uri
        .split_once(".dfs.core.windows.net/")
        .map(|(_, tail)| tail)
        .unwrap_or("")
        .trim_start_matches('/')
        .to_string();
    if key.is_empty() {
        return Err(Box::new(StorageError(
            "adls state operation requires a blob path".to_string(),
        )));
    }
    Ok(key)
}

fn http_failure(err: &azure_core::Error) -> Option<(StatusCode, Option<&str>)> {
    match err.kind() {
        ErrorKind::HttpResponse {
            status, error_code, ..
        } => Some((*status, error_code.as_deref())),
        _ => None,
    }
}

fn is_not_found(err: &azure_core::Error) -> bool {
    matches!(http_failure(err), Some((StatusCode::NotFound, _)))
}

/// `If-Match` mismatch: another writer changed the blob since our read.
fn is_precondition_failed(err: &azure_core::Error) -> bool {
    matches!(http_failure(err), Some((StatusCode::PreconditionFailed, _)))
}

/// Create-only `If-None-Match: *` losing the race: Azure reports it as 409
/// with error code `BlobAlreadyExists`, not 412. Scoped to that exact code —
/// Azure uses 409 for many non-race failures (`BlobImmutableDueToPolicy`,
/// `BlobArchived`, `SnapshotsPresent`, lease errors, ...) that must surface as
/// real storage errors instead of triggering CAS retries.
fn is_create_race(err: &azure_core::Error) -> bool {
    matches!(
        http_failure(err),
        Some((StatusCode::Conflict, Some(code))) if code.eq_ignore_ascii_case("BlobAlreadyExists")
    )
}

pub fn parse_adls_uri(uri: &str) -> FloeResult<AdlsLocation> {
    uri::parse_abfs_uri(uri)
}

pub fn format_abfs_uri(container: &str, account: &str, path: &str) -> String {
    uri::format_abfs_uri(container, account, path)
}

pub type AdlsLocation = uri::AdlsLocation;

#[cfg(test)]
mod tests {
    use super::*;

    fn http_error(status: StatusCode, error_code: Option<&str>) -> azure_core::Error {
        azure_core::Error::with_message(
            ErrorKind::HttpResponse {
                status,
                error_code: error_code.map(str::to_string),
                raw_response: None,
            },
            "test error",
        )
    }

    #[test]
    fn if_match_conflict_is_412_only() {
        assert!(is_precondition_failed(&http_error(
            StatusCode::PreconditionFailed,
            Some("ConditionNotMet")
        )));
        assert!(!is_precondition_failed(&http_error(
            StatusCode::Conflict,
            Some("BlobAlreadyExists")
        )));
    }

    #[test]
    fn create_race_requires_blob_already_exists_code() {
        assert!(is_create_race(&http_error(
            StatusCode::Conflict,
            Some("BlobAlreadyExists")
        )));
        // Other 409 codes are real storage failures, not CAS races: mapping
        // them to Conflict would make state operations retry unrecoverable
        // conditions (immutability policies, legal holds, snapshots, leases).
        assert!(!is_create_race(&http_error(
            StatusCode::Conflict,
            Some("BlobImmutableDueToPolicy")
        )));
        assert!(!is_create_race(&http_error(
            StatusCode::Conflict,
            Some("SnapshotsPresent")
        )));
        assert!(!is_create_race(&http_error(StatusCode::Conflict, None)));
        assert!(!is_create_race(&http_error(
            StatusCode::PreconditionFailed,
            Some("ConditionNotMet")
        )));
    }

    #[test]
    fn not_found_matches_404_only() {
        assert!(is_not_found(&http_error(
            StatusCode::NotFound,
            Some("BlobNotFound")
        )));
        assert!(!is_not_found(&http_error(StatusCode::Conflict, None)));
        assert!(!is_not_found(&azure_core::Error::with_message(
            ErrorKind::Other,
            "not http"
        )));
    }
}
