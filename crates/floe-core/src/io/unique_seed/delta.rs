use std::collections::HashMap;

use deltalake::table::builder::DeltaTableBuilder;

use crate::errors::RunError;
use crate::io::storage::{object_store, Target};
use crate::{check, config, FloeResult};

use super::{seed_from_batches, FormatSeeder};

pub(super) struct DeltaSeeder<'a> {
    pub(super) target: &'a Target,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) entity: &'a config::EntityConfig,
}

impl FormatSeeder for DeltaSeeder<'_> {
    fn seed(
        &mut self,
        unique_tracker: &mut check::UniqueTracker,
        scan_cols: &[String],
        rename_back: &HashMap<String, String>,
    ) -> FloeResult<()> {
        let store = object_store::delta_store_config(self.target, self.resolver, self.entity)?;
        let builder = DeltaTableBuilder::from_url(store.table_url)
            .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
            .with_storage_options(store.storage_options);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
        let table = runtime.block_on(async move { builder.load().await });
        let table = match table {
            Ok(table) => table,
            Err(deltalake::DeltaTableError::NotATable(_)) => return Ok(()),
            Err(err) => return Err(Box::new(RunError(format!("delta load failed: {err}")))),
        };
        let scan_cols = scan_cols.to_vec();
        let batches = runtime
            .block_on(async {
                let (_t, stream) = table.scan_table().with_columns(scan_cols).await?;
                deltalake::operations::collect_sendable_stream(stream).await
            })
            .map_err(|err| Box::new(RunError(format!("delta scan failed: {err}"))))?;
        seed_from_batches(unique_tracker, batches, rename_back)
    }
}
