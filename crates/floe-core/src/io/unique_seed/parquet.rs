use std::collections::HashMap;
use std::path::Path;

use crate::checks::normalize::rename_output_columns;
use crate::errors::StorageError;
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::Target;
use crate::io::write::{parts, strategy};
use crate::{check, config, io, FloeResult};

use super::FormatSeeder;

pub(super) struct ParquetSeeder<'a> {
    pub(super) target: &'a Target,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) cloud: &'a mut io::storage::CloudClient,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) entity: &'a config::EntityConfig,
}

impl FormatSeeder for ParquetSeeder<'_> {
    fn seed(
        &mut self,
        unique_tracker: &mut check::UniqueTracker,
        scan_cols: &[String],
        rename_back: &HashMap<String, String>,
    ) -> FloeResult<()> {
        match self.target {
            Target::Local { base_path, .. } => {
                let base_path = Path::new(base_path);
                let part_files = parts::list_local_part_paths(base_path, "parquet")?;
                for part_path in part_files {
                    seed_from_parquet_path(unique_tracker, &part_path, scan_cols, rename_back)?;
                }
            }
            Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
                let temp_dir = self.temp_dir.ok_or_else(|| {
                    Box::new(StorageError(format!(
                        "entity.name={} missing temp dir for parquet read",
                        self.entity.name
                    )))
                })?;
                let spec = strategy::accepted_parquet_spec();
                let (list_prefix, objects) = {
                    let mut ctx = strategy::WriteContext {
                        target: self.target,
                        cloud: self.cloud,
                        resolver: self.resolver,
                        entity: self.entity,
                    };
                    strategy::list_part_objects(&mut ctx, spec)?
                };
                let client =
                    self.cloud
                        .client_for(self.resolver, self.target.storage(), self.entity)?;
                for object in objects
                    .into_iter()
                    .filter(|obj| obj.key.starts_with(&list_prefix))
                    .filter(|obj| parts::is_part_key(&obj.key, spec.extension))
                {
                    let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                    seed_from_parquet_path(unique_tracker, &local_path, scan_cols, rename_back)?;
                }
            }
        }
        Ok(())
    }
}

fn seed_from_parquet_path(
    unique_tracker: &mut check::UniqueTracker,
    path: &Path,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    let mut df = read_parquet_lazy(path, Some(scan_cols))?;
    rename_output_columns(&mut df, rename_back)?;
    unique_tracker.seed_from_df(&df)?;
    Ok(())
}
