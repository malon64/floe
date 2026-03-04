use polars::prelude::DataFrame;

use crate::io::format::AcceptedMergeMetrics;
use crate::io::storage::Target;
use crate::{config, FloeResult};

pub(crate) mod scd1;
pub(crate) mod scd2;
pub(crate) mod shared;

pub(crate) struct MergeExecutionContext<'a> {
    pub(crate) runtime: &'a tokio::runtime::Runtime,
    pub(crate) target: &'a Target,
    pub(crate) resolver: &'a config::StorageResolver,
    pub(crate) entity: &'a config::EntityConfig,
    pub(crate) partition_by: Option<Vec<String>>,
    pub(crate) target_file_size_bytes: Option<usize>,
}

/// MergeBackend separates format-specific merge primitives (Delta today, others later)
/// from merge strategy orchestration so new table formats can plug in without duplicating
/// key resolution, validation, and reporting flow.
pub(crate) trait MergeBackend {
    fn execute_scd1(
        &self,
        source_df: &mut DataFrame,
        ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(i64, AcceptedMergeMetrics)>;
}
