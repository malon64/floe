use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::ConfigBase;
use crate::report::{self, RunStatus};
use crate::run::RunOutcome;
use crate::runner::{BackendMeta, RunnerAdapter, RunnerKind, RunnerMeta};
use crate::{ConfigError, FloeResult, RunOptions};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Required configuration for the Kubernetes runner adapter.
///
/// All fields are validated at construction time; `new()` fails fast if any
/// required value is missing or invalid.
#[derive(Debug, Clone)]
pub struct KubernetesConfig {
    /// Kubernetes namespace to submit the Job into.
    pub namespace: String,
    /// Container image that includes the `floe` binary (e.g. `my-registry/floe:1.0.0`).
    pub image: String,
    /// Prefix for generated Job names (e.g. `floe-run`).  The run-id is appended.
    pub job_name_prefix: String,
    /// Maximum time in seconds to wait for the Job to complete before timing out.
    pub timeout_secs: u64,
    /// How often (in seconds) to poll for Job status while waiting.
    pub poll_interval_secs: u64,
    /// Optional Kubernetes ServiceAccount to attach to the Job Pod.
    pub service_account: Option<String>,
    /// Optional dashboard URL pattern (e.g. `https://k8s.internal/jobs/{job}`).
    /// The literal `{job}` token is replaced with the actual job name.
    pub dashboard_url_template: Option<String>,
    /// Remote URI pointing to the Floe config file that the pod will fetch
    /// (e.g. `s3://my-bucket/floe/config.yml`).  Must contain `://`.
    ///
    /// The host-local `config_path` passed to `execute()` is **not** available
    /// inside the pod; callers must stage the config to object storage and
    /// supply the resulting URI here.
    pub config_uri: String,
}

impl KubernetesConfig {
    /// Validate all required fields.  Returns an actionable error for the
    /// first invalid field found.
    pub fn validate(&self) -> FloeResult<()> {
        if self.namespace.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "kubernetes runner: namespace must not be empty".to_string(),
            )));
        }
        if self.image.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "kubernetes runner: image must not be empty".to_string(),
            )));
        }
        if self.job_name_prefix.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "kubernetes runner: job_name_prefix must not be empty".to_string(),
            )));
        }
        if self.timeout_secs == 0 {
            return Err(Box::new(ConfigError(
                "kubernetes runner: timeout_secs must be greater than zero".to_string(),
            )));
        }
        if self.poll_interval_secs == 0 {
            return Err(Box::new(ConfigError(
                "kubernetes runner: poll_interval_secs must be greater than zero".to_string(),
            )));
        }
        if self.config_uri.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "kubernetes runner: config_uri must not be empty".to_string(),
            )));
        }
        if !self.config_uri.contains("://") {
            return Err(Box::new(ConfigError(format!(
                "kubernetes runner: config_uri must be a remote URI (e.g. s3://…), got \"{}\"",
                self.config_uri
            ))));
        }
        Ok(())
    }

    fn resolve_dashboard_url(&self, job_name: &str) -> Option<String> {
        self.dashboard_url_template
            .as_ref()
            .map(|tmpl| tmpl.replace("{job}", job_name))
    }
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            image: String::new(),
            job_name_prefix: "floe-run".to_string(),
            timeout_secs: 3600,
            poll_interval_secs: 10,
            service_account: None,
            dashboard_url_template: None,
            config_uri: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Job phase and run status
// ---------------------------------------------------------------------------

/// Observed phase of a Kubernetes Job, as reported by the k8s API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K8sJobPhase {
    /// The Job is still running (has active pods).
    Active,
    /// All pods completed successfully (`.status.succeeded >= 1`).
    Succeeded,
    /// At least one pod failed and the backoff limit was reached.
    Failed,
    /// An unrecognised or intermediate phase (carries the raw string).
    Unknown(String),
}

/// Deterministic final status produced by the Kubernetes adapter after the
/// Job reaches a terminal phase or the timeout elapses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubernetesRunStatus {
    Succeeded,
    Failed,
    Timeout,
}

impl KubernetesRunStatus {
    /// Map a terminal [`K8sJobPhase`] to a [`KubernetesRunStatus`].
    /// `Unknown` phases are treated as failures.
    pub fn from_phase(phase: &K8sJobPhase) -> Self {
        match phase {
            K8sJobPhase::Succeeded => Self::Succeeded,
            K8sJobPhase::Failed | K8sJobPhase::Unknown(_) => Self::Failed,
            K8sJobPhase::Active => Self::Failed, // should not reach here
        }
    }

    /// Map to a Floe [`RunStatus`] for the summary report.
    pub fn to_run_status(&self) -> RunStatus {
        match self {
            Self::Succeeded => RunStatus::Success,
            Self::Failed | Self::Timeout => RunStatus::Failed,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
        }
    }
}

// ---------------------------------------------------------------------------
// KubernetesClient trait — injectable for testability
// ---------------------------------------------------------------------------

/// Abstraction over the Kubernetes API for testability.
///
/// Production code uses [`KubectlClient`]; tests inject a mock.
pub(crate) trait KubernetesClient: Send + Sync {
    /// Submit a Job manifest and return the actual Job name.
    fn submit_job(&self, namespace: &str, spec: &serde_json::Value) -> FloeResult<String>;
    /// Return the current phase of the named Job.
    fn get_job_phase(&self, namespace: &str, job_name: &str) -> FloeResult<K8sJobPhase>;
}

// ---------------------------------------------------------------------------
// KubectlClient — subprocess-based implementation
// ---------------------------------------------------------------------------

/// Kubernetes client that shells out to `kubectl`.
///
/// Requires `kubectl` in `PATH` and a valid kubeconfig / in-cluster
/// service-account token.
pub(crate) struct KubectlClient;

impl KubernetesClient for KubectlClient {
    fn submit_job(&self, namespace: &str, spec: &serde_json::Value) -> FloeResult<String> {
        let spec_json = serde_json::to_string(spec)
            .map_err(|e| Box::new(ConfigError(format!("k8s spec serialization failed: {e}"))))?;

        let job_name = spec["metadata"]["name"]
            .as_str()
            .ok_or_else(|| {
                Box::new(ConfigError(
                    "k8s job spec missing metadata.name".to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?
            .to_string();

        let output = std::process::Command::new("kubectl")
            .args(["apply", "-n", namespace, "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                if let Some(stdin) = child.stdin.as_mut() {
                    stdin.write_all(spec_json.as_bytes())?;
                }
                child.wait_with_output()
            })
            .map_err(|e| {
                Box::new(ConfigError(format!("kubectl apply failed: {e}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Box::new(ConfigError(format!(
                "kubectl apply failed (exit {}): {stderr}",
                output.status.code().unwrap_or(-1)
            ))));
        }

        Ok(job_name)
    }

    fn get_job_phase(&self, namespace: &str, job_name: &str) -> FloeResult<K8sJobPhase> {
        let output = std::process::Command::new("kubectl")
            .args([
                "get",
                "job",
                job_name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.active},{.status.succeeded},{.status.failed}",
            ])
            .output()
            .map_err(|e| {
                Box::new(ConfigError(format!("kubectl get job failed: {e}")))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Box::new(ConfigError(format!(
                "kubectl get job failed (exit {}): {stderr}",
                output.status.code().unwrap_or(-1)
            ))));
        }

        let raw = String::from_utf8_lossy(&output.stdout);
        Ok(parse_kubectl_status(&raw))
    }
}

/// Parse the kubectl jsonpath output `{active},{succeeded},{failed}`.
fn parse_kubectl_status(raw: &str) -> K8sJobPhase {
    let parts: Vec<&str> = raw.trim().splitn(3, ',').collect();
    let active: u64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let succeeded: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let failed: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    if succeeded > 0 {
        K8sJobPhase::Succeeded
    } else if failed > 0 {
        K8sJobPhase::Failed
    } else if active > 0 {
        K8sJobPhase::Active
    } else {
        K8sJobPhase::Unknown(raw.trim().to_string())
    }
}

// ---------------------------------------------------------------------------
// Job spec builder
// ---------------------------------------------------------------------------

fn build_job_spec(
    config: &KubernetesConfig,
    job_name: &str,
    options: &RunOptions,
) -> serde_json::Value {
    let mut cmd = vec!["floe", "run", "--config"];
    cmd.push(&config.config_uri);

    let mut extra_args: Vec<String> = Vec::new();
    for entity in &options.entities {
        extra_args.push("--entity".to_string());
        extra_args.push(entity.clone());
    }
    if options.dry_run {
        extra_args.push("--dry-run".to_string());
    }

    let full_cmd: Vec<serde_json::Value> = cmd
        .iter()
        .map(|s| json!(s))
        .chain(extra_args.iter().map(|s| json!(s)))
        .collect();

    let mut pod_spec = json!({
        "containers": [{
            "name": "floe",
            "image": config.image,
            "command": full_cmd,
        }],
        "restartPolicy": "Never",
    });

    if let Some(sa) = &config.service_account {
        pod_spec["serviceAccountName"] = json!(sa);
    }

    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": config.namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "floe",
                "floe/run-id": job_name,
            }
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "spec": pod_spec,
            }
        }
    })
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// Runner adapter that submits a Kubernetes Job and polls until completion.
///
/// Construct via [`KubernetesRunnerAdapter::new`] (uses `kubectl`) or
/// [`KubernetesRunnerAdapter::with_client`] (injects a custom client, for
/// tests).
pub struct KubernetesRunnerAdapter {
    config: KubernetesConfig,
    client: Box<dyn KubernetesClient>,
}

impl KubernetesRunnerAdapter {
    /// Create a new adapter using the `kubectl` CLI.
    ///
    /// # Errors
    /// Returns an error if `config` fails validation.
    pub fn new(config: KubernetesConfig) -> FloeResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            client: Box::new(KubectlClient),
        })
    }

    /// Create a new adapter with an injected client (in-crate tests only).
    #[cfg(test)]
    pub(crate) fn with_client(
        config: KubernetesConfig,
        client: Box<dyn KubernetesClient>,
    ) -> FloeResult<Self> {
        config.validate()?;
        Ok(Self { config, client })
    }

    /// Execute the submit → poll → status-map flow and return a [`RunOutcome`]
    /// populated with the information available from the k8s control plane.
    fn run_job(
        &self,
        options: &RunOptions,
    ) -> FloeResult<(String, KubernetesRunStatus, String, String)> {
        let started_at = report::now_rfc3339();
        let run_id = options
            .run_id
            .clone()
            .unwrap_or_else(|| report::run_id_from_timestamp(&started_at));

        // Sanitise for k8s naming rules (lowercase, no underscores).
        let safe_id = run_id
            .to_lowercase()
            .replace([':', '_', '.'], "-")
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
            .take(52)
            .collect::<String>();
        let job_name = format!("{}-{safe_id}", self.config.job_name_prefix);

        // 1. Submit.
        let spec = build_job_spec(&self.config, &job_name, options);
        let actual_job_name = self.client.submit_job(&self.config.namespace, &spec)?;

        // 2. Poll until terminal or timeout.
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_secs);
        let final_phase = loop {
            let phase = self
                .client
                .get_job_phase(&self.config.namespace, &actual_job_name)?;

            match &phase {
                K8sJobPhase::Succeeded | K8sJobPhase::Failed => break phase,
                K8sJobPhase::Active | K8sJobPhase::Unknown(_) => {
                    if Instant::now() >= deadline {
                        break K8sJobPhase::Unknown("timeout".to_string());
                    }
                    std::thread::sleep(Duration::from_secs(self.config.poll_interval_secs));
                }
            }
        };

        // 3. Map phase → status.
        let k8s_status = if matches!(final_phase, K8sJobPhase::Unknown(ref s) if s == "timeout") {
            KubernetesRunStatus::Timeout
        } else {
            KubernetesRunStatus::from_phase(&final_phase)
        };

        let finished_at = report::now_rfc3339();
        Ok((actual_job_name, k8s_status, started_at, finished_at))
    }
}

impl RunnerAdapter for KubernetesRunnerAdapter {
    fn execute(
        &self,
        config_path: &Path,
        _config_base: ConfigBase,
        options: RunOptions,
    ) -> FloeResult<RunOutcome> {
        let (job_name, k8s_status, started_at, finished_at) = self.run_job(&options)?;

        let run_status = k8s_status.to_run_status();
        let exit_code = match run_status {
            RunStatus::Success | RunStatus::SuccessWithWarnings => 0,
            _ => 1,
        };

        // Minimal summary — detailed row counts require reading the remote
        // run report, which is out of scope for the MVP adapter.
        let summary = report::RunSummaryReport {
            spec_version: "0.1".to_string(),
            tool: report::ToolInfo {
                name: "floe".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                git: None,
            },
            run: report::RunInfo {
                run_id: job_name.clone(),
                started_at: started_at.clone(),
                finished_at: finished_at.clone(),
                duration_ms: 0, // not available from control plane
                status: run_status,
                exit_code,
            },
            config: report::ConfigEcho {
                path: config_path.display().to_string(),
                version: "unknown".to_string(),
                metadata: None,
            },
            report: report::ReportEcho {
                path: "remote".to_string(),
                report_file: "remote".to_string(),
            },
            results: report::ResultsTotals {
                files_total: 0,
                rows_total: 0,
                accepted_total: 0,
                rejected_total: 0,
                warnings_total: 0,
                errors_total: 0,
            },
            entities: Vec::new(),
        };

        if run_status == RunStatus::Failed {
            return Err(Box::new(ConfigError(format!(
                "kubernetes job \"{job_name}\" finished with status: {}",
                k8s_status.as_str()
            ))));
        }

        Ok(RunOutcome {
            run_id: job_name,
            report_base_path: None,
            entity_outcomes: Vec::new(),
            summary,
            dry_run_previews: None,
        })
    }

    fn meta(&self, config_path: &Path) -> RunnerMeta {
        RunnerMeta {
            kind: RunnerKind::Kubernetes,
            config_path: config_path.display().to_string(),
            backend: Some(BackendMeta {
                backend_type: "kubernetes".to_string(),
                backend_run_id: None, // populated after submission
                submitted_at: None,
                finished_at: None,
                final_status: None,
                url: None,
            }),
        }
    }
}

/// Build a pre-populated [`RunnerMeta`] for a completed k8s run.
pub fn kubernetes_runner_meta(
    config_path: &Path,
    config: &KubernetesConfig,
    job_name: &str,
    submitted_at: &str,
    finished_at: &str,
    status: &KubernetesRunStatus,
) -> RunnerMeta {
    RunnerMeta {
        kind: RunnerKind::Kubernetes,
        config_path: config_path.display().to_string(),
        backend: Some(BackendMeta {
            backend_type: "kubernetes".to_string(),
            backend_run_id: Some(job_name.to_string()),
            submitted_at: Some(submitted_at.to_string()),
            finished_at: Some(finished_at.to_string()),
            final_status: Some(status.as_str().to_string()),
            url: config.resolve_dashboard_url(job_name),
        }),
    }
}

// ---------------------------------------------------------------------------
// In-crate tests — use pub(crate) APIs including with_client + mock client
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    // ---- Mock client -------------------------------------------------------

    struct MockK8sClient {
        job_name: String,
        phases: Mutex<VecDeque<K8sJobPhase>>,
    }

    impl MockK8sClient {
        fn new(job_name: &str, phases: Vec<K8sJobPhase>) -> Self {
            Self {
                job_name: job_name.to_string(),
                phases: Mutex::new(phases.into()),
            }
        }
    }

    impl KubernetesClient for MockK8sClient {
        fn submit_job(&self, _namespace: &str, _spec: &serde_json::Value) -> FloeResult<String> {
            Ok(self.job_name.clone())
        }

        fn get_job_phase(&self, _namespace: &str, _job_name: &str) -> FloeResult<K8sJobPhase> {
            let mut q = self.phases.lock().unwrap();
            Ok(q.pop_front()
                .unwrap_or(K8sJobPhase::Unknown("empty".to_string())))
        }
    }

    fn test_config() -> KubernetesConfig {
        KubernetesConfig {
            namespace: "test-ns".to_string(),
            image: "floe:test".to_string(),
            job_name_prefix: "floe-test".to_string(),
            timeout_secs: 30,
            poll_interval_secs: 1, // minimum valid; mock responds instantly
            service_account: None,
            dashboard_url_template: None,
            config_uri: "s3://my-bucket/floe/config.yml".to_string(),
        }
    }

    // ---- succeed path -------------------------------------------------------

    #[test]
    fn mock_run_succeeds_when_job_succeeds() {
        let client = Box::new(MockK8sClient::new(
            "floe-test-abc",
            vec![K8sJobPhase::Active, K8sJobPhase::Succeeded],
        ));
        let adapter =
            KubernetesRunnerAdapter::with_client(test_config(), client).expect("construct");
        let (job_name, status, _, _) = adapter
            .run_job(&Default::default())
            .expect("run_job");
        assert_eq!(job_name, "floe-test-abc");
        assert_eq!(status, KubernetesRunStatus::Succeeded);
    }

    // ---- fail path ----------------------------------------------------------

    #[test]
    fn mock_run_fails_when_job_fails() {
        let client = Box::new(MockK8sClient::new(
            "floe-test-xyz",
            vec![K8sJobPhase::Active, K8sJobPhase::Failed],
        ));
        let adapter =
            KubernetesRunnerAdapter::with_client(test_config(), client).expect("construct");
        let (_, status, _, _) = adapter
            .run_job(&Default::default())
            .expect("run_job");
        assert_eq!(status, KubernetesRunStatus::Failed);
    }

    // ---- timeout path -------------------------------------------------------

    #[test]
    fn mock_run_times_out_when_job_stays_active() {
        // All polls return Active → deadline will be reached (1 s timeout).
        let phases: Vec<K8sJobPhase> = (0..5).map(|_| K8sJobPhase::Active).collect();
        let client = Box::new(MockK8sClient::new("floe-test-timeout", phases));
        let mut cfg = test_config();
        cfg.timeout_secs = 1;
        cfg.poll_interval_secs = 1;
        let adapter = KubernetesRunnerAdapter::with_client(cfg, client).expect("construct");
        let (_, status, _, _) = adapter
            .run_job(&Default::default())
            .expect("run_job");
        assert_eq!(status, KubernetesRunStatus::Timeout);
    }

    // ---- submit error -------------------------------------------------------

    #[test]
    fn mock_run_propagates_submit_error() {
        struct FailingClient;
        impl KubernetesClient for FailingClient {
            fn submit_job(
                &self,
                _namespace: &str,
                _spec: &serde_json::Value,
            ) -> FloeResult<String> {
                Err(Box::new(crate::ConfigError("submit failed".to_string())))
            }
            fn get_job_phase(&self, _namespace: &str, _job_name: &str) -> FloeResult<K8sJobPhase> {
                unreachable!()
            }
        }
        let adapter = KubernetesRunnerAdapter::with_client(test_config(), Box::new(FailingClient))
            .expect("construct");
        let err = adapter
            .run_job(&Default::default())
            .unwrap_err();
        assert!(err.to_string().contains("submit failed"), "got: {err}");
    }

    // ---- parse_kubectl_status -----------------------------------------------

    #[test]
    fn parse_kubectl_status_succeeded() {
        assert_eq!(parse_kubectl_status("0,1,0"), K8sJobPhase::Succeeded);
    }

    #[test]
    fn parse_kubectl_status_failed() {
        assert_eq!(parse_kubectl_status("0,0,1"), K8sJobPhase::Failed);
    }

    #[test]
    fn parse_kubectl_status_active() {
        assert_eq!(parse_kubectl_status("1,0,0"), K8sJobPhase::Active);
    }

    #[test]
    fn parse_kubectl_status_unknown() {
        assert!(matches!(
            parse_kubectl_status("0,0,0"),
            K8sJobPhase::Unknown(_)
        ));
    }
}
