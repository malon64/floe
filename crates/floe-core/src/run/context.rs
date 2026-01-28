use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{config, report, FloeResult, RunOptions};

pub struct RunContext {
    pub config: config::RootConfig,
    pub config_path: PathBuf,
    pub config_dir: PathBuf,
    pub storage_resolver: config::StorageResolver,
    pub report_dir: Option<PathBuf>,
    pub report_base_path: Option<String>,
    pub run_id: String,
    pub started_at: String,
    pub run_timer: Instant,
}

impl RunContext {
    pub fn new(config_path: &Path, options: &RunOptions) -> FloeResult<Self> {
        let config = config::parse_config(config_path)?;
        let storage_resolver = config::StorageResolver::new(&config, config_path)?;
        let config_dir = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let report_dir = config
            .report
            .as_ref()
            .map(|report| config::resolve_local_path(&config_dir, &report.path));
        let report_base_path = report_dir.as_ref().map(|path| path.display().to_string());
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
            report_dir,
            report_base_path,
            run_id,
            started_at,
            run_timer: Instant::now(),
        })
    }
}
