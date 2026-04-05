mod local;
mod outcome;

pub use local::LocalRunnerAdapter;
pub use outcome::{parse_run_status_from_logs, ConnectorRunStatus};

use std::path::Path;

use crate::config::ConfigBase;
use crate::run::RunOutcome;
use crate::{ConfigError, FloeResult, RunOptions};

// ---------------------------------------------------------------------------
// RunnerKind
// ---------------------------------------------------------------------------

/// Identifies which execution adapter to use for a run.
///
/// The value is derived from `execution.runner.type` in an environment
/// profile, or defaults to [`RunnerKind::Local`] when no profile is active.
///
/// `Kubernetes` is a recognized kind; its adapter lives in the connector
/// layer (T5/T6) and is not implemented inside floe-core.  Calling
/// [`select_runner`] with `Kubernetes` returns an error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunnerKind {
    /// Execute the run in the current process on the local filesystem.
    Local,
    /// Execute the run as a Kubernetes Job (connector-owned; not implemented
    /// in floe-core).
    Kubernetes,
}

impl RunnerKind {
    /// Parse a `runner.type` string from a profile or CLI flag.
    ///
    /// # Errors
    /// Returns an error for unknown runner names.
    pub fn from_profile_str(s: &str) -> FloeResult<Self> {
        match s {
            "local" => Ok(RunnerKind::Local),
            "kubernetes" => Ok(RunnerKind::Kubernetes),
            other => Err(Box::new(ConfigError(format!(
                "unknown runner kind \"{other}\"; supported runners: local, kubernetes"
            )))),
        }
    }

    /// String representation, matching `execution.runner.type` in profiles.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunnerKind::Local => "local",
            RunnerKind::Kubernetes => "kubernetes",
        }
    }
}

impl Default for RunnerKind {
    /// The default runner when no profile specifies one.
    fn default() -> Self {
        RunnerKind::Local
    }
}

// ---------------------------------------------------------------------------
// RunnerMeta — normalized execution metadata scaffold
// ---------------------------------------------------------------------------

/// Normalized metadata describing the execution environment for a run.
///
/// This struct is a forward-compatible scaffold: callers should treat
/// unrecognised fields as opaque.  Future adapters (Kubernetes, Databricks,
/// …) will extend it with adapter-specific sub-structs rather than adding
/// required fields here.
#[derive(Debug, Clone)]
pub struct RunnerMeta {
    /// Which adapter was selected.
    pub kind: RunnerKind,
    /// Absolute or URI path to the config file used for this run.
    pub config_path: String,
}

// ---------------------------------------------------------------------------
// RunnerAdapter — the execution strategy trait
// ---------------------------------------------------------------------------

/// Strategy for executing a Floe run.
///
/// Implement this trait to add new execution backends (Kubernetes, Databricks,
/// etc.).  The local path is always available via [`LocalRunnerAdapter`].
pub trait RunnerAdapter {
    /// Execute the run described by `config_path` / `config_base` /
    /// `options`, returning the outcome.
    fn execute(
        &self,
        config_path: &Path,
        config_base: ConfigBase,
        options: RunOptions,
    ) -> FloeResult<RunOutcome>;

    /// Return normalized metadata about this runner for observability /
    /// manifest generation.
    fn meta(&self, config_path: &Path) -> RunnerMeta;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Build the appropriate [`RunnerAdapter`] for the given [`RunnerKind`].
///
/// Returns an error for runner kinds whose adapters live outside floe-core
/// (e.g. `Kubernetes`).  Connector layers should handle those kinds directly
/// and never call this function with them.
///
/// # Errors
/// Returns an error if `kind` does not have a built-in adapter in floe-core.
pub fn select_runner(kind: RunnerKind) -> FloeResult<Box<dyn RunnerAdapter>> {
    match kind {
        RunnerKind::Local => Ok(Box::new(LocalRunnerAdapter)),
        RunnerKind::Kubernetes => Err(Box::new(ConfigError(
            "the kubernetes runner is connector-owned and cannot be dispatched from floe-core; \
             use a connector adapter (T5/T6) to execute Kubernetes runs"
                .to_string(),
        ))),
        // Keep exhaustive — new variants added to RunnerKind must be handled
        // explicitly here.
    }
}
