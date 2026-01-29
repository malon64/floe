use std::path::Path;

use polars::prelude::DataFrame;

use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

struct IcebergAcceptedAdapter;

static ICEBERG_ACCEPTED_ADAPTER: IcebergAcceptedAdapter = IcebergAcceptedAdapter;

pub(crate) fn iceberg_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &ICEBERG_ACCEPTED_ADAPTER
}

impl AcceptedSinkAdapter for IcebergAcceptedAdapter {
    fn write_accepted(
        &self,
        _target: &Target,
        _df: &mut DataFrame,
        _output_stem: &str,
        _temp_dir: Option<&Path>,
        _cloud: &mut io::storage::CloudClient,
        _resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.format=iceberg is not supported yet (storage catalog writer not implemented)",
            entity.name
        ))))
    }
}
