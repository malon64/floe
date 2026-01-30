use std::path::{Path, PathBuf};
use std::sync::Arc;

use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ContainerClient};
use futures::StreamExt;
use tokio::runtime::Runtime;

use crate::errors::StorageError;
use crate::{config, FloeResult};

use super::{planner, ObjectRef, StorageClient};

pub struct AdlsClient {
    account: String,
    container: String,
    prefix: String,
    runtime: Runtime,
    container_client: ContainerClient,
}

impl AdlsClient {
    pub fn new(definition: &config::StorageDefinition) -> FloeResult<Self> {
        let account = definition.account.clone().ok_or_else(|| {
            Box::new(StorageError(format!(
                "storage {} requires account for type adls",
                definition.name
            )))
        })?;
        let container = definition.container.clone().ok_or_else(|| {
            Box::new(StorageError(format!(
                "storage {} requires container for type adls",
                definition.name
            )))
        })?;
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
        let trimmed = path.trim_start_matches('/');
        if trimmed.is_empty() {
            format!(
                "abfs://{}@{}.dfs.core.windows.net",
                self.container, self.account
            )
        } else {
            format!(
                "abfs://{}@{}.dfs.core.windows.net/{}",
                self.container, self.account, trimmed
            )
        }
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
                    refs.push(ObjectRef {
                        uri,
                        key,
                        last_modified: Some(blob.properties.last_modified.to_string()),
                        size: Some(blob.properties.content_length),
                    });
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
        let dest = temp_dir.join(
            Path::new(&key)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("object"),
        );
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

    fn delete(&self, uri: &str) -> FloeResult<()> {
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
}
