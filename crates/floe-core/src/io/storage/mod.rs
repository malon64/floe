use std::collections::HashMap;
use std::path::Path;

use crate::{config, ConfigError, FloeResult};

pub mod extensions;
pub mod inputs;
pub mod local;
pub mod output;
pub mod paths;
pub mod s3;
pub mod target;

pub use target::Target;

pub trait StorageClient: Send + Sync {
    fn list(&self, prefix: &str) -> FloeResult<Vec<String>>;
    fn download(&self, key: &str, dest: &Path) -> FloeResult<()>;
    fn upload(&self, key: &str, path: &Path) -> FloeResult<()>;
    fn delete(&self, key: &str) -> FloeResult<()>;
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
