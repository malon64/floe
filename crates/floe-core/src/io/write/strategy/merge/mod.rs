#[cfg(feature = "delta")]
use polars::prelude::DataFrame;

#[cfg(feature = "delta")]
use crate::errors::RunError;
#[cfg(feature = "delta")]
use crate::io::format::AcceptedMergeMetrics;
#[cfg(feature = "delta")]
use crate::io::storage::Target;
#[cfg(feature = "delta")]
use crate::{config, FloeResult};

// `keys` holds deltalake-free merge-key/SCD helpers shared by the DuckDB sink and
// the run pipeline, so it stays compiled regardless of the delta feature. The
// Delta merge backend (`shared`/`scd1`/`scd2` and the `MergeBackend` trait) is
// gated since it links deltalake.
pub(crate) mod keys;
#[cfg(feature = "delta")]
pub(crate) mod scd1;
#[cfg(feature = "delta")]
pub(crate) mod scd2;
#[cfg(feature = "delta")]
pub(crate) mod shared;

#[cfg(feature = "delta")]
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
#[cfg(feature = "delta")]
pub(crate) trait MergeBackend {
    fn execute_scd1(
        &self,
        source_df: &mut DataFrame,
        ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(
        i64,
        AcceptedMergeMetrics,
        crate::io::format::AcceptedSchemaEvolution,
        shared::DeltaMergePerfBreakdown,
    )>;

    fn execute_scd2(
        &self,
        _source_df: &mut DataFrame,
        _ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(
        i64,
        AcceptedMergeMetrics,
        crate::io::format::AcceptedSchemaEvolution,
        shared::DeltaMergePerfBreakdown,
    )> {
        Err(Box::new(RunError(
            "write_mode=merge_scd2 is not implemented for this backend".to_string(),
        )))
    }
}
