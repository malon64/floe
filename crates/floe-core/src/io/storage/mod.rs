use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{config, ConfigError, FloeResult};

pub mod core;
pub mod object_store;
pub mod ops;
pub mod providers;
pub mod target;

pub use core::{extensions, paths, placement, planner, uri, validation};
pub use ops::{archive, inputs, output};
pub use placement::OutputPlacement;
pub use planner::{
    filter_by_suffixes, join_prefix, normalize_separators, stable_sort_refs, temp_path_for_key,
};
pub use providers::{adls, gcs, local, s3};
pub use target::Target;

pub use planner::ObjectRef;

pub trait StorageClient: Send + Sync {
    fn list(&self, prefix_or_path: &str) -> FloeResult<Vec<ObjectRef>>;
    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf>;
    fn upload_from_path(&self, local_path: &Path, uri: &str) -> FloeResult<()>;
    fn resolve_uri(&self, path: &str) -> FloeResult<String>;
    fn copy_object(&self, src_uri: &str, dst_uri: &str) -> FloeResult<()>;
    fn delete_object(&self, uri: &str) -> FloeResult<()>;
    fn exists(&self, uri: &str) -> FloeResult<bool>;
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
        let context = format!("entity.name={}", entity.name);
        self.client_for_context(resolver, storage, &context)
    }

    pub fn client_for_context<'a>(
        &'a mut self,
        resolver: &config::StorageResolver,
        storage: &str,
        context: &str,
    ) -> FloeResult<&'a mut dyn StorageClient> {
        if !self.clients.contains_key(storage) {
            let definition = resolver.definition(storage).ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "{} storage {} is not defined",
                    context, storage
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let client = build_client(&definition)?;
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

fn build_client(definition: &config::StorageDefinition) -> FloeResult<Box<dyn StorageClient>> {
    let client: Box<dyn StorageClient> = match definition.fs_type.as_str() {
        "local" => Box::new(local::LocalClient::new()),
        "s3" => {
            let bucket =
                validation::require_field(definition, definition.bucket.as_ref(), "bucket", "s3")?;
            Box::new(s3::S3Client::new(bucket, definition.region.as_deref())?)
        }
        "adls" => Box::new(adls::AdlsClient::new(definition)?),
        "gcs" => {
            let bucket =
                validation::require_field(definition, definition.bucket.as_ref(), "bucket", "gcs")?;
            Box::new(gcs::GcsClient::new(bucket)?)
        }
        other => {
            return Err(Box::new(ConfigError(format!(
                "storage type {} is unsupported",
                other
            ))))
        }
    };
    Ok(client)
}
