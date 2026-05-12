use std::path::Path;

mod add_entity;
pub mod checks;
pub mod config;
pub mod errors;
pub mod io;
pub(crate) mod log;
pub mod manifest;
pub mod profile;
pub mod report;
pub mod run;
pub mod runner;
pub mod runtime;
pub mod state;
pub mod vars;
pub mod warnings;

pub use crate::state::{inspect_entity_state_with_base, reset_entity_state_with_base};
pub use add_entity::{add_entity_to_config, AddEntityOptions, AddEntityOutcome};
pub use checks as check;
pub use config::{resolve_config_location, ConfigLocation};
pub use errors::ConfigError;
pub use manifest::build_common_manifest_json;
pub use profile::{
    detect_malformed_placeholder, detect_unresolved_placeholders, parse_profile,
    parse_profile_from_str, validate_merged_vars, validate_profile, ProfileConfig,
};
pub use run::events::{set_observer, RunEvent, RunObserver};
pub use run::{run, run_with_base, DryRunEntityPreview, EntityOutcome, RunOutcome};
pub use runner::{parse_run_status_from_logs, ConnectorRunStatus};
pub use runtime::{DefaultRuntime, Runtime};
pub use vars::{resolve_vars, VarSources};

pub type FloeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub entities: Vec<String>,
    pub profile_vars: std::collections::HashMap<String, String>,
    pub profile_catalogs: Option<config::CatalogsConfig>,
}

#[derive(Debug, Default)]
pub struct RunOptions {
    pub run_id: Option<String>,
    pub entities: Vec<String>,
    pub dry_run: bool,
    pub profile: Option<ProfileConfig>,
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
    let mut config = config::parse_config_with_vars(config_path, &options.profile_vars)?;
    apply_profile_catalogs(&mut config, options.profile_catalogs.as_ref());
    config::validate_config(&config)?;

    if !options.entities.is_empty() {
        run::validate_entities(&config, &options.entities)?;
    }

    Ok(())
}

pub fn load_config(config_path: &Path) -> FloeResult<config::RootConfig> {
    config::parse_config(config_path)
}

pub fn load_config_with_profile_overrides(
    config_path: &Path,
    profile_vars: &std::collections::HashMap<String, String>,
    profile_catalogs: Option<&config::CatalogsConfig>,
) -> FloeResult<config::RootConfig> {
    let mut config = config::parse_config_with_vars(config_path, profile_vars)?;
    apply_profile_catalogs(&mut config, profile_catalogs);
    Ok(config)
}

pub fn validate_profile_file(profile_path: &Path) -> FloeResult<ProfileConfig> {
    let profile = parse_profile(profile_path)?;
    validate_profile(&profile)?;
    Ok(profile)
}

pub(crate) fn apply_profile_catalogs(
    config: &mut config::RootConfig,
    profile_catalogs: Option<&config::CatalogsConfig>,
) {
    if let Some(catalogs) = profile_catalogs {
        config.catalogs = Some(catalogs.clone());
    }
}

pub fn extract_config_env_vars(
    config_path: &Path,
) -> FloeResult<std::collections::HashMap<String, String>> {
    Ok(config::extract_raw_env_vars(config_path).unwrap_or_default())
}

pub fn validate_config_for_tests(config: &config::RootConfig) -> FloeResult<()> {
    config::validate_config(config)
}
