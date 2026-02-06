use std::path::Path;

use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

use super::parts;

mod append;
mod overwrite;

#[derive(Debug, Clone, Copy)]
pub enum PartScope {
    Accepted { format: &'static str },
    Rejected { format: &'static str },
}

#[derive(Debug, Clone, Copy)]
pub struct PartSpec {
    pub extension: &'static str,
    pub min_width: usize,
    pub scope: PartScope,
}

pub struct WriteContext<'a> {
    pub target: &'a Target,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub entity: &'a config::EntityConfig,
}

pub trait ModeStrategy {
    fn mode(&self) -> config::WriteMode;
    fn part_allocator(
        &self,
        ctx: &mut WriteContext<'_>,
        spec: PartSpec,
    ) -> FloeResult<parts::PartNameAllocator>;
}

#[derive(Debug, Clone)]
enum CloudProvider {
    S3,
    Gcs { bucket: String },
    Adls { container: String, account: String },
}

struct CloudObjects {
    storage: String,
    list_prefix: String,
    objects: Vec<io::storage::ObjectRef>,
}

pub fn strategy_for(mode: config::WriteMode) -> &'static dyn ModeStrategy {
    match mode {
        config::WriteMode::Overwrite => &overwrite::OVERWRITE_STRATEGY,
        config::WriteMode::Append => &append::APPEND_STRATEGY,
    }
}

pub fn ensure_mode_supported(mode: config::WriteMode) -> FloeResult<()> {
    match mode {
        config::WriteMode::Overwrite => Ok(()),
        config::WriteMode::Append => Ok(()),
    }
}

pub fn accepted_parquet_spec() -> PartSpec {
    PartSpec {
        extension: "parquet",
        min_width: 1,
        scope: PartScope::Accepted { format: "parquet" },
    }
}

pub fn rejected_csv_spec() -> PartSpec {
    PartSpec {
        extension: "csv",
        min_width: 5,
        scope: PartScope::Rejected { format: "csv" },
    }
}

pub fn append_part_allocator(
    ctx: &mut WriteContext<'_>,
    spec: PartSpec,
) -> FloeResult<parts::PartNameAllocator> {
    match ctx.target {
        Target::Local { base_path, .. } => {
            parts::PartNameAllocator::from_local_path(Path::new(base_path), spec.extension)
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            let next_index = next_cloud_part_index(ctx, spec)?;
            Ok(parts::PartNameAllocator::from_next_index(
                next_index,
                spec.extension,
            ))
        }
    }
}

pub fn overwrite_part_allocator(
    ctx: &mut WriteContext<'_>,
    spec: PartSpec,
) -> FloeResult<parts::PartNameAllocator> {
    match ctx.target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let _ = parts::clear_local_part_files(base_path, spec.extension)?;
            parts::PartNameAllocator::from_local_path(base_path, spec.extension)
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            clear_cloud_parts(ctx, spec)?;
            Ok(parts::PartNameAllocator::from_next_index(0, spec.extension))
        }
    }
}

fn next_cloud_part_index(ctx: &mut WriteContext<'_>, spec: PartSpec) -> FloeResult<usize> {
    let cloud_objects = list_part_objects(ctx, spec)?;
    let indexes = cloud_objects.objects.into_iter().filter_map(|obj| {
        if obj.key.starts_with(&cloud_objects.list_prefix) {
            parts::parse_part_index_from_key(&obj.key, spec.extension, spec.min_width)
        } else {
            None
        }
    });
    match indexes.max() {
        Some(index) => index.checked_add(1).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "{} part index overflow while preparing append write",
                spec.extension
            ))) as Box<dyn std::error::Error + Send + Sync>
        }),
        None => Ok(0),
    }
}

fn clear_cloud_parts(ctx: &mut WriteContext<'_>, spec: PartSpec) -> FloeResult<()> {
    let cloud_objects = list_part_objects(ctx, spec)?;
    let client = ctx
        .cloud
        .client_for(ctx.resolver, cloud_objects.storage.as_str(), ctx.entity)?;
    for object in cloud_objects
        .objects
        .into_iter()
        .filter(|obj| obj.key.starts_with(&cloud_objects.list_prefix))
        .filter(|obj| {
            parts::parse_part_index_from_key(&obj.key, spec.extension, spec.min_width).is_some()
        })
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

fn list_part_objects(ctx: &mut WriteContext<'_>, spec: PartSpec) -> FloeResult<CloudObjects> {
    match ctx.target {
        Target::S3 {
            storage, base_key, ..
        } => {
            let provider = CloudProvider::S3;
            let list_prefix = list_prefix(ctx.entity, base_key, &provider, spec)?;
            let client = ctx.cloud.client_for(ctx.resolver, storage, ctx.entity)?;
            let objects = client.list(&list_prefix)?;
            Ok(CloudObjects {
                storage: storage.clone(),
                list_prefix,
                objects,
            })
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let provider = CloudProvider::Gcs {
                bucket: bucket.clone(),
            };
            let list_prefix = list_prefix(ctx.entity, base_key, &provider, spec)?;
            let client = ctx.cloud.client_for(ctx.resolver, storage, ctx.entity)?;
            let objects = client.list(&list_prefix)?;
            Ok(CloudObjects {
                storage: storage.clone(),
                list_prefix,
                objects,
            })
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            let provider = CloudProvider::Adls {
                container: container.clone(),
                account: account.clone(),
            };
            let list_prefix = list_prefix(ctx.entity, base_path, &provider, spec)?;
            let client = ctx.cloud.client_for(ctx.resolver, storage, ctx.entity)?;
            let objects = client.list(&list_prefix)?;
            Ok(CloudObjects {
                storage: storage.clone(),
                list_prefix,
                objects,
            })
        }
        Target::Local { .. } => Err(Box::new(ConfigError(
            "cloud part listing requested for local target".to_string(),
        ))),
    }
}

fn list_prefix(
    entity: &config::EntityConfig,
    base_path: &str,
    provider: &CloudProvider,
    spec: PartSpec,
) -> FloeResult<String> {
    let prefix = base_path.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(prefix_error(entity, provider, spec)));
    }
    Ok(format!("{prefix}/"))
}

fn prefix_error(
    entity: &config::EntityConfig,
    provider: &CloudProvider,
    spec: PartSpec,
) -> ConfigError {
    match (&spec.scope, provider) {
        (PartScope::Accepted { format }, CloudProvider::S3) => ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for s3 {format} outputs",
            entity.name
        )),
        (PartScope::Accepted { format }, CloudProvider::Gcs { bucket }) => ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for gcs {format} outputs (bucket={})",
            entity.name, bucket
        )),
        (PartScope::Accepted { format }, CloudProvider::Adls { container, account }) => {
            ConfigError(format!(
                "entity.name={} sink.accepted.path must not be container root for adls {format} outputs (container={}, account={})",
                entity.name, container, account
            ))
        }
        (PartScope::Rejected { .. }, CloudProvider::S3) => ConfigError(format!(
            "entity.name={} sink.rejected.path must not be bucket root for s3 outputs",
            entity.name
        )),
        (PartScope::Rejected { .. }, CloudProvider::Gcs { .. }) => ConfigError(format!(
            "entity.name={} sink.rejected.path must not be bucket root for gcs outputs",
            entity.name
        )),
        (PartScope::Rejected { .. }, CloudProvider::Adls { .. }) => ConfigError(format!(
            "entity.name={} sink.rejected.path must not be container root for adls outputs",
            entity.name
        )),
    }
}
