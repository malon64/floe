use std::collections::HashMap;
use std::path::Path;

use polars::prelude::DataFrame;

use crate::io::format::{AcceptedSinkAdapter, StorageTarget};
use crate::{config, io, ConfigError, FloeResult};

struct IcebergAcceptedAdapter;

static ICEBERG_ACCEPTED_ADAPTER: IcebergAcceptedAdapter = IcebergAcceptedAdapter;

pub(crate) fn iceberg_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &ICEBERG_ACCEPTED_ADAPTER
}

impl AcceptedSinkAdapter for IcebergAcceptedAdapter {
    fn write_accepted(
        &self,
        _target: &StorageTarget,
        _df: &mut DataFrame,
        _source_stem: &str,
        _temp_dir: Option<&Path>,
        _s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        _resolver: &config::FilesystemResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.format=iceberg is not supported yet (filesystem catalog writer not implemented)",
            entity.name
        ))))
    }
}
