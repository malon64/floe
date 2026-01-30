use std::path::Path;

pub mod checks;
pub mod config;
pub mod errors;
pub mod io;
pub mod report;
pub mod run;
pub mod warnings;

pub use checks as check;
pub use run::{run, EntityOutcome, RunOutcome};

pub type FloeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
pub(crate) struct ConfigError(pub(crate) String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub entities: Vec<String>,
}

#[derive(Debug, Default)]
pub struct RunOptions {
    pub run_id: Option<String>,
    pub entities: Vec<String>,
}

pub fn validate(config_path: &Path, options: ValidateOptions) -> FloeResult<()> {
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
