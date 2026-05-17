use std::path::{Path, PathBuf};
use std::sync::Arc;

use azure_core::prelude::IfMatchCondition;
use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ContainerClient};
use futures::StreamExt;
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
    container_client: ContainerClient,
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
        let credential = DefaultAzureCredential::create(TokenCredentialOptions::default())
            .map_err(|err| Box::new(StorageError(format!("adls credential init failed: {err}"))))?;
        let storage_credentials = StorageCredentials::token_credential(Arc::new(credential));
        let service_client = BlobServiceClient::new(account.clone(), storage_credentials);
        let container_client = service_client.container_client(container.clone());
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
        let container = self.container.clone();
        let account = self.account.clone();
        let client = self.container_client.clone();
        self.runtime.block_on(async move {
            let mut refs = Vec::new();
            let mut stream = client.list_blobs().prefix(prefix.clone()).into_stream();
            while let Some(resp) = stream.next().await {
                let resp = resp.map_err(|err| {
                    Box::new(StorageError(format!("adls list failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                for blob in resp.blobs.blobs() {
                    let key = blob.name.clone();
                    let uri = if key.is_empty() {
                        format!("abfs://{}@{}.dfs.core.windows.net", container, account)
                    } else {
                        format!(
                            "abfs://{}@{}.dfs.core.windows.net/{}",
                            container, account, key
                        )
                    };
                    refs.push(planner::object_ref(
                        uri,
                        key,
                        Some(blob.properties.last_modified.to_string()),
                        Some(blob.properties.content_length),
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
        let key = if key.is_empty() {
            return Err(Box::new(StorageError(
                "adls download requires a blob path".to_string(),
            )));
        } else {
            key
        };
        let dest = planner::temp_path_for_key(temp_dir, &key);
        let dest_clone = dest.clone();
        let client = self.container_client.clone();
        let key_clone = key.clone();
        self.runtime.block_on(async move {
            if let Some(parent) = dest_clone.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let blob = client.blob_client(key_clone);
            let mut stream = blob.get().into_stream();
            let mut file = tokio::fs::File::create(&dest_clone).await?;
            while let Some(chunk) = stream.next().await {
                let resp = chunk.map_err(|err| {
                    Box::new(StorageError(format!("adls download failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                let bytes = resp.data.collect().await.map_err(|err| {
                    Box::new(StorageError(format!("adls download read failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
            }
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
        let client = self.container_client.clone();
        let path = local_path.to_path_buf();
        self.runtime.block_on(async move {
            let data = tokio::fs::read(path).await?;
            let blob = client.blob_client(key);
            blob.put_block_blob(data)
                .content_type("application/octet-stream")
                .into_future()
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
        let client = self.container_client.clone();
        self.runtime.block_on(async move {
            let blob = client.blob_client(key);
            blob.delete().into_future().await.map_err(|err| {
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
        let client = self.container_client.clone();
        self.runtime.block_on(async move {
            let blob = client.blob_client(key);
            let mut stream = blob.get().into_stream();
            let mut body = Vec::new();
            let mut version = None;
            while let Some(chunk) = stream.next().await {
                let resp = match chunk {
                    Ok(resp) => resp,
                    Err(err) if is_not_found(&err) => return Ok(None),
                    Err(err) => {
                        return Err(
                            Box::new(StorageError(format!("adls download failed: {err}")))
                                as Box<dyn std::error::Error + Send + Sync>,
                        )
                    }
                };
                if version.is_none() {
                    version = Some(resp.blob.properties.etag.to_string());
                }
                let bytes = resp.data.collect().await.map_err(|err| {
                    Box::new(StorageError(format!("adls download read failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
                body.extend_from_slice(&bytes);
            }
            Ok(version.map(|version| StoredObject { body, version }))
        })
    }

    fn write_object_conditional(
        &self,
        uri: &str,
        expected_version: Option<&str>,
        body: &[u8],
    ) -> FloeResult<ConditionalWrite> {
        let key = adls_key_from_uri(uri)?;
        let client = self.container_client.clone();
        let body = body.to_vec();
        self.runtime.block_on(async move {
            let condition = expected_version
                .map(|version| IfMatchCondition::Match(version.to_string()))
                .unwrap_or_else(|| IfMatchCondition::NotMatch("*".to_string()));
            match client
                .blob_client(key)
                .put_block_blob(body)
                .if_match(condition)
                .content_type("application/json")
                .into_future()
                .await
            {
                Ok(resp) => Ok(ConditionalWrite::Written { version: resp.etag }),
                Err(err) if is_precondition(&err) => Ok(ConditionalWrite::Conflict),
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
        let client = self.container_client.clone();
        self.runtime.block_on(async move {
            let request = client
                .blob_client(key)
                .delete()
                .if_match(IfMatchCondition::Match(expected_version.to_string()));
            match request.into_future().await {
                Ok(_) => Ok(ConditionalWrite::Written {
                    version: "deleted".to_string(),
                }),
                Err(err) if is_precondition(&err) => Ok(ConditionalWrite::Conflict),
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

fn is_not_found<E: std::fmt::Display>(err: &E) -> bool {
    let text = err.to_string();
    text.contains("404") || text.contains("NotFound")
}

fn is_precondition<E: std::fmt::Display>(err: &E) -> bool {
    let text = err.to_string();
    text.contains("412") || text.contains("condition") || text.contains("Condition")
}

pub fn parse_adls_uri(uri: &str) -> FloeResult<AdlsLocation> {
    uri::parse_abfs_uri(uri)
}

pub fn format_abfs_uri(container: &str, account: &str, path: &str) -> String {
    uri::format_abfs_uri(container, account, path)
}

pub type AdlsLocation = uri::AdlsLocation;
