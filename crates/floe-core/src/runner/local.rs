use std::path::Path;

use crate::config::ConfigBase;
use crate::run::RunOutcome;
use crate::runner::{RunnerAdapter, RunnerKind, RunnerMeta};
use crate::{FloeResult, RunOptions};

/// Runner adapter that executes Floe runs in the current process using the
/// local filesystem.
///
/// This is the only adapter available today; it delegates directly to the
/// existing [`crate::run::run_with_base`] execution path and preserves all
/// current behaviour unchanged.
pub struct LocalRunnerAdapter;

impl RunnerAdapter for LocalRunnerAdapter {
    fn execute(
        &self,
        config_path: &Path,
        config_base: ConfigBase,
        options: RunOptions,
    ) -> FloeResult<RunOutcome> {
        crate::run::run_with_base(config_path, config_base, options)
    }

    fn meta(&self, config_path: &Path) -> RunnerMeta {
        RunnerMeta {
            kind: RunnerKind::Local,
            config_path: config_path.display().to_string(),
            backend: None,
        }
    }
}
