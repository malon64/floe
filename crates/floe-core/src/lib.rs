use std::path::Path;

mod add_entity;
pub mod checks;
pub mod config;
pub mod errors;
pub mod io;
pub mod manifest;
pub mod profile;
pub mod report;
pub mod run;
pub mod runner;
pub mod runtime;
pub mod vars;
pub mod warnings;

pub use add_entity::{add_entity_to_config, AddEntityOptions, AddEntityOutcome};
pub use checks as check;
pub use config::{resolve_config_location, ConfigLocation};
pub use errors::ConfigError;
pub use manifest::build_common_manifest_json;
pub use profile::{
    detect_unresolved_placeholders, parse_profile, parse_profile_from_str, validate_merged_vars,
    validate_profile, ProfileConfig,
};
pub use run::events::{set_observer, RunEvent, RunObserver};
pub use run::{run, run_with_base, DryRunEntityPreview, EntityOutcome, RunOutcome};
pub use runner::{
    kubernetes_runner_meta, select_kubernetes_runner, select_runner, BackendMeta, K8sJobPhase,
    KubernetesConfig, KubernetesRunStatus, KubernetesRunnerAdapter, LocalRunnerAdapter,
    RunnerAdapter, RunnerKind, RunnerMeta,
};
pub use runtime::{DefaultRuntime, Runtime};
pub use vars::{resolve_vars, VarSources};

pub type FloeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub entities: Vec<String>,
}

#[derive(Debug, Default)]
pub struct RunOptions {
    pub run_id: Option<String>,
    pub entities: Vec<String>,
    pub dry_run: bool,
}

pub fn validate(config_path: &Path, options: ValidateOptions) -> FloeResult<()> {
    let config_base = config::ConfigBase::local_from_path(config_path);
    validate_with_base(config_path, config_base, options)
}

pub fn validate_with_base(
    config_path: &Path,
    _config_base: config::ConfigBase,
    options: ValidateOptions,
) -> FloeResult<()> {
    let config = config::parse_config(config_path)?;
    config::validate_config(&config)?;

    if !options.entities.is_empty() {
        run::validate_entities(&config, &options.entities)?;
    }

    Ok(())
}

pub fn load_config(config_path: &Path) -> FloeResult<config::RootConfig> {
    config::parse_config(config_path)
}

pub fn validate_config_for_tests(config: &config::RootConfig) -> FloeResult<()> {
    config::validate_config(config)
}
