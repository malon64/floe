use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub mod config;
mod parse;
mod required;
mod yaml_decode;

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
    pub entity: Option<String>,
}

#[derive(Debug, Default)]
pub struct RunOptions {
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub run_id: Option<String>,
    pub entity: Option<String>,
}

pub fn validate(config_path: &Path, options: ValidateOptions) -> FloeResult<()> {
    let config = parse::parse_config(config_path)?;
    if config.version.trim().is_empty() {
        return Err(Box::new(ConfigError("version is required".to_string())));
    }

    if config.entities.is_empty() {
        return Err(Box::new(ConfigError(
            "at least one entity is required".to_string(),
        )));
    }

    let mut seen = HashSet::new();
    for entity in &config.entities {
        if entity.name.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "entity name cannot be empty".to_string(),
            )));
        }
        if !seen.insert(entity.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "duplicate entity name: {}",
                entity.name
            ))));
        }
    }

    if let Some(name) = &options.entity {
        if !config.entities.iter().any(|entity| entity.name == *name) {
            return Err(Box::new(ConfigError(format!(
                "entity not found: {}",
                name
            ))));
        }
    }
    Ok(())
}

pub fn run(config_path: &Path, options: RunOptions) -> FloeResult<()> {
    let validate_options = ValidateOptions {
        entity: options.entity.clone(),
    };
    validate(config_path, validate_options)?;
    let _ = options;
    // TODO: execute ingestion once config parsing + validation exists.
    Ok(())
}
