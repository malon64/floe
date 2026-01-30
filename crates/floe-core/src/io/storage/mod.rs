use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{config, ConfigError, FloeResult};

pub mod extensions;
pub mod inputs;
pub mod local;
pub mod object_store;
pub mod output;
pub mod paths;
pub mod planner;
pub mod s3;
pub mod target;

pub use planner::{filter_by_suffixes, join_prefix, normalize_separators, stable_sort_refs};
pub use target::Target;

pub use planner::ObjectRef;

pub trait StorageClient: Send + Sync {
    fn list(&self, prefix_or_path: &str) -> FloeResult<Vec<ObjectRef>>;
    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf>;
    fn upload_from_path(&self, local_path: &Path, uri: &str) -> FloeResult<()>;
    fn resolve_uri(&self, path: &str) -> FloeResult<String>;
    fn delete(&self, uri: &str) -> FloeResult<()>;
}

pub struct CloudClient {
    clients: HashMap<String, Box<dyn StorageClient>>,
}

impl CloudClient {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    pub fn client_for<'a>(
        &'a mut self,
        resolver: &config::StorageResolver,
        storage: &str,
        entity: &config::EntityConfig,
    ) -> FloeResult<&'a mut dyn StorageClient> {
        if !self.clients.contains_key(storage) {
            let definition = resolver.definition(storage).ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} storage {} is not defined",
                    entity.name, storage
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let client: Box<dyn StorageClient> = match definition.fs_type.as_str() {
                "local" => Box::new(local::LocalClient::new()),
                "s3" => {
                    let bucket = definition.bucket.clone().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "storage {} requires bucket for type s3",
                            definition.name
                        ))) as Box<dyn std::error::Error + Send + Sync>
                    })?;
                    Box::new(s3::S3Client::new(bucket, definition.region.as_deref())?)
                }
                other => {
                    return Err(Box::new(ConfigError(format!(
                        "storage type {} is unsupported",
                        other
                    ))))
                }
            };
            self.clients.insert(storage.to_string(), client);
        }
        Ok(self
            .clients
            .get_mut(storage)
            .expect("storage client inserted")
            .as_mut())
    }
}

impl Default for CloudClient {
    fn default() -> Self {
        Self::new()
    }
}
