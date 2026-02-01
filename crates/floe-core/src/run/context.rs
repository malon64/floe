use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::io::storage::Target;
use crate::{config, report, FloeResult, RunOptions};

pub struct RunContext {
    pub config: config::RootConfig,
    pub config_path: PathBuf,
    pub config_dir: PathBuf,
    pub storage_resolver: config::StorageResolver,
    pub report_base_path: Option<String>,
    pub report_target: Option<Target>,
    pub run_id: String,
    pub started_at: String,
    pub run_timer: Instant,
}

impl RunContext {
    pub fn new(
        config_path: &Path,
        config_base: config::ConfigBase,
        options: &RunOptions,
    ) -> FloeResult<Self> {
        let config = config::parse_config(config_path)?;
        let storage_resolver = config::StorageResolver::new(&config, config_base)?;
        let config_dir = storage_resolver.config_dir().to_path_buf();
        let (report_target, report_base_path) = match config.report.as_ref() {
            Some(report) => {
                let resolved = storage_resolver
                    .resolve_report_path(report.storage.as_deref(), &report.path)?;
                let target = Target::from_resolved(&resolved)?;
                let base_path = match resolved.local_path.as_ref() {
                    Some(path) => path.display().to_string(),
                    None => resolved.uri.clone(),
                };
                (Some(target), Some(base_path))
            }
            None => (None, None),
        };
        let started_at = report::now_rfc3339();
        let run_id = options
            .run_id
            .clone()
            .unwrap_or_else(|| report::run_id_from_timestamp(&started_at));

        Ok(Self {
            config,
            config_path: config_path.to_path_buf(),
            config_dir,
            storage_resolver,
            report_base_path,
            report_target,
            run_id,
            started_at,
            run_timer: Instant::now(),
        })
    }
}
