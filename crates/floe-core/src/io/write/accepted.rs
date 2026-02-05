use std::path::Path;

use polars::prelude::DataFrame;

use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::Target;
use crate::{config, io, FloeResult};

use super::modes;

pub struct AcceptedWriteRequest<'a> {
    pub target: &'a Target,
    pub df: &'a mut DataFrame,
    pub output_stem: &'a str,
    pub temp_dir: Option<&'a Path>,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub entity: &'a config::EntityConfig,
    pub mode: config::WriteMode,
}

pub trait AcceptedWriter: Send + Sync {
    fn write(&self, request: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput>;
}

pub struct AdapterAcceptedWriter<'a> {
    adapter: &'a dyn AcceptedSinkAdapter,
}

impl<'a> AdapterAcceptedWriter<'a> {
    pub fn new(adapter: &'a dyn AcceptedSinkAdapter) -> Self {
        Self { adapter }
    }
}

impl AcceptedWriter for AdapterAcceptedWriter<'_> {
    fn write(&self, request: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput> {
        modes::ensure_mode_supported(request.mode)?;
        self.adapter.write_accepted(
            request.target,
            request.df,
            request.output_stem,
            request.temp_dir,
            request.cloud,
            request.resolver,
            request.entity,
        )
    }
}

pub fn write_with_adapter(
    adapter: &dyn AcceptedSinkAdapter,
    request: AcceptedWriteRequest<'_>,
) -> FloeResult<AcceptedWriteOutput> {
    AdapterAcceptedWriter::new(adapter).write(request)
}
