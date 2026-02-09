use std::path::{Path, PathBuf};

use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use tokio::runtime::Runtime;

use crate::errors::StorageError;
use crate::FloeResult;

use super::uri::{format_bucket_uri, parse_bucket_uri, BucketLocation};
use super::{planner, ObjectRef, StorageClient};

pub struct GcsClient {
    bucket: String,
    client: Client,
    runtime: Runtime,
}

impl GcsClient {
    pub fn new(bucket: String) -> FloeResult<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Box::new(StorageError(format!("gcs runtime init failed: {err}"))))?;
        let client = runtime.block_on(async {
            let config = ClientConfig::default()
                .with_auth()
                .await
                .map_err(|err| Box::new(StorageError(format!("gcs auth init failed: {err}"))))?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Client::new(config))
        })?;
        Ok(Self {
            bucket,
            client,
            runtime,
        })
    }

    fn bucket(&self) -> &str {
        self.bucket.as_str()
    }
}

impl StorageClient for GcsClient {
    fn list(&self, prefix_or_path: &str) -> FloeResult<Vec<ObjectRef>> {
        let bucket = self.bucket().to_string();
        let prefix = prefix_or_path.trim_start_matches('/').to_string();
        let client = self.client.clone();
        self.runtime.block_on(async move {
            let mut refs = Vec::new();
            let mut page_token = None;
            loop {
                let request = ListObjectsRequest {
                    bucket: bucket.clone(),
                    prefix: if prefix.is_empty() {
                        None
                    } else {
                        Some(prefix.clone())
                    },
                    page_token,
                    ..Default::default()
                };
                let response = client.list_objects(&request).await.map_err(|err| {
                    Box::new(StorageError(format!(
                        "gcs list objects failed for bucket {}: {err}",
                        bucket
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
                if let Some(items) = response.items {
                    for object in items {
                        let key = object.name.clone();
                        let uri = format_gcs_uri(&bucket, &key);
                        refs.push(ObjectRef {
                            uri,
                            key,
                            last_modified: object.updated.map(|value| value.to_string()),
                            size: Some(object.size as u64),
                        });
                    }
                }
                match response.next_page_token {
                    Some(token) if !token.is_empty() => {
                        page_token = Some(token);
                    }
                    _ => break,
                }
            }
            Ok(planner::stable_sort_refs(refs))
        })
    }

    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
        let location = parse_gcs_uri(uri)?;
        let bucket = location.bucket;
        let key = location.key;
        let dest = planner::temp_path_for_key(temp_dir, &key);
        let dest_clone = dest.clone();
        let client = self.client.clone();
        self.runtime.block_on(async move {
            let data = client
                .download_object(
                    &GetObjectRequest {
                        bucket,
                        object: key,
                        ..Default::default()
                    },
                    &Range::default(),
                )
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("gcs download failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            if let Some(parent) = dest_clone.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&dest_clone, data).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(dest)
    }

    fn upload_from_path(&self, local_path: &Path, uri: &str) -> FloeResult<()> {
        let location = parse_gcs_uri(uri)?;
        let path = local_path.to_path_buf();
        let client = self.client.clone();
        self.runtime.block_on(async move {
            let data = tokio::fs::read(path).await?;
            let upload_type = UploadType::Simple(Media::new(location.key.clone()));
            let request = UploadObjectRequest {
                bucket: location.bucket,
                ..Default::default()
            };
            client
                .upload_object(&request, data, &upload_type)
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("gcs upload failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(format_gcs_uri(self.bucket(), path.trim_start_matches('/')))
    }

    fn copy_object(&self, src_uri: &str, dst_uri: &str) -> FloeResult<()> {
        planner::copy_via_temp(self, src_uri, dst_uri)
    }

    fn delete_object(&self, uri: &str) -> FloeResult<()> {
        let location = parse_gcs_uri(uri)?;
        let client = self.client.clone();
        self.runtime.block_on(async move {
            client
                .delete_object(&DeleteObjectRequest {
                    bucket: location.bucket,
                    object: location.key,
                    ..Default::default()
                })
                .await
                .map_err(|err| {
                    Box::new(StorageError(format!("gcs delete failed: {err}")))
                        as Box<dyn std::error::Error + Send + Sync>
                })?;
            Ok(())
        })
    }

    fn exists(&self, uri: &str) -> FloeResult<bool> {
        let location = parse_gcs_uri(uri)?;
        planner::exists_by_key(self, &location.key)
    }
}

pub fn parse_gcs_uri(uri: &str) -> FloeResult<GcsLocation> {
    parse_bucket_uri("gs", uri)
}

pub fn format_gcs_uri(bucket: &str, key: &str) -> String {
    format_bucket_uri("gs", bucket, key)
}

pub type GcsLocation = BucketLocation;
