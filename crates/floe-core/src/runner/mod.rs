mod local;

pub use local::LocalRunnerAdapter;

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
/// Future variants (e.g. `Kubernetes`, `Databricks`) will be added here
/// without breaking existing callers; always match exhaustively using a
/// wildcard arm or upgrade the match when adding a variant.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunnerKind {
    /// Execute the run in the current process on the local filesystem.
    Local,
}

impl RunnerKind {
    /// Parse a `runner.type` string from a profile or CLI flag.
    ///
    /// # Errors
    /// Returns an error for unknown runner names.
    pub fn from_profile_str(s: &str) -> FloeResult<Self> {
        match s {
            "local" => Ok(RunnerKind::Local),
            other => Err(Box::new(ConfigError(format!(
                "unknown runner kind \"{other}\"; supported runners: local"
            )))),
        }
    }

    /// String representation, matching `execution.runner.type` in profiles.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunnerKind::Local => "local",
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
/// This is the single dispatch point.  Add new arms here as new adapters
/// are implemented.
pub fn select_runner(kind: RunnerKind) -> Box<dyn RunnerAdapter> {
    match kind {
        RunnerKind::Local => Box::new(LocalRunnerAdapter),
    }
}
