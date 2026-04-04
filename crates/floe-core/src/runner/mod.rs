mod kubernetes;
mod local;

pub use kubernetes::{
    kubernetes_runner_meta, K8sJobPhase, KubernetesConfig, KubernetesRunStatus,
    KubernetesRunnerAdapter,
};
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunnerKind {
    /// Execute the run in the current process on the local filesystem.
    Local,
    /// Submit the run as a Kubernetes Job and poll for completion.
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
// BackendMeta — normalized backend-specific execution metadata
// ---------------------------------------------------------------------------

/// Normalized metadata from the execution backend, populated after a run
/// completes.  Fields are optional because not all backends expose all data,
/// and the local runner does not produce backend metadata.
#[derive(Debug, Clone)]
pub struct BackendMeta {
    /// Identifies the backend type (e.g. `"kubernetes"`, `"databricks"`).
    pub backend_type: String,
    /// Backend-assigned run identifier (e.g. Kubernetes Job name).
    pub backend_run_id: Option<String>,
    /// When the job was submitted to the backend (RFC 3339).
    pub submitted_at: Option<String>,
    /// When the job reached a terminal state on the backend (RFC 3339).
    pub finished_at: Option<String>,
    /// Final status string from the backend (e.g. `"succeeded"`, `"timeout"`).
    pub final_status: Option<String>,
    /// Optional link to a monitoring dashboard for this job.
    pub url: Option<String>,
}

// ---------------------------------------------------------------------------
// RunnerMeta — normalized execution metadata scaffold
// ---------------------------------------------------------------------------

/// Normalized metadata describing the execution environment for a run.
///
/// `backend` is populated by non-local adapters after the run completes.
/// The local adapter always sets `backend = None`.
#[derive(Debug, Clone)]
pub struct RunnerMeta {
    /// Which adapter was selected.
    pub kind: RunnerKind,
    /// Absolute or URI path to the config file used for this run.
    pub config_path: String,
    /// Backend-specific metadata; `None` for local runs.
    pub backend: Option<BackendMeta>,
}

// ---------------------------------------------------------------------------
// RunnerAdapter — the execution strategy trait
// ---------------------------------------------------------------------------

/// Strategy for executing a Floe run.
///
/// Implement this trait to add new execution backends.  The local path is
/// always available via [`LocalRunnerAdapter`].
pub trait RunnerAdapter {
    /// Execute the run and return the outcome.
    fn execute(
        &self,
        config_path: &Path,
        config_base: ConfigBase,
        options: RunOptions,
    ) -> FloeResult<RunOutcome>;

    /// Return normalized metadata about this runner.
    fn meta(&self, config_path: &Path) -> RunnerMeta;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Build the appropriate [`RunnerAdapter`] for [`RunnerKind::Local`].
///
/// For [`RunnerKind::Kubernetes`] use [`select_kubernetes_runner`] instead,
/// which requires a [`KubernetesConfig`].
///
/// # Errors
/// Returns an error if called with `RunnerKind::Kubernetes` — use
/// [`select_kubernetes_runner`] with a [`KubernetesConfig`] instead.
pub fn select_runner(kind: RunnerKind) -> FloeResult<Box<dyn RunnerAdapter>> {
    match kind {
        RunnerKind::Local => Ok(Box::new(LocalRunnerAdapter)),
        // #[non_exhaustive] only affects external crates; match is exhaustive here.
        RunnerKind::Kubernetes => Err(Box::new(ConfigError(
            "select_runner: use select_kubernetes_runner(config) for the Kubernetes runner"
                .to_string(),
        ))),
    }
}

/// Build a [`KubernetesRunnerAdapter`] after validating `config`.
///
/// # Errors
/// Propagates any validation error from [`KubernetesConfig::validate`].
pub fn select_kubernetes_runner(config: KubernetesConfig) -> FloeResult<Box<dyn RunnerAdapter>> {
    KubernetesRunnerAdapter::new(config).map(|a| Box::new(a) as Box<dyn RunnerAdapter>)
}
