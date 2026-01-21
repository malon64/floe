use std::path::Path;
use yaml_schema::{Engine, RootSchema};

pub mod checks;
pub mod config;
mod io;
pub mod report;
pub mod run;

pub use checks as check;
pub use run::{run, EntityOutcome, RunOutcome};

pub type FloeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SCHEMA_YAML: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/config.schema.yaml"));

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

    validate_schema(config_path, &config)?;

    if !options.entities.is_empty() {
        run::validate_entities(&config, &options.entities)?;
    }

    Ok(())
}

pub fn load_config(config_path: &Path) -> FloeResult<config::RootConfig> {
    config::parse_config(config_path)
}

fn validate_schema(config_path: &Path, config: &config::RootConfig) -> FloeResult<()> {
    let root_schema = RootSchema::load_from_str(SCHEMA_YAML).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to load embedded schema: {err}"
        )))
    })?;

    let yaml_contents = std::fs::read_to_string(config_path)?;
    let context = Engine::evaluate(&root_schema, &yaml_contents, false)
        .map_err(|err| Box::new(ConfigError(format!("schema validation failed: {err}"))))?;

    if context.has_errors() {
        let errors = context.errors.borrow();
        let message = errors
            .iter()
            .map(|error| annotate_schema_error(&error.to_string(), config))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(Box::new(ConfigError(message)));
    }

    Ok(())
}

fn annotate_schema_error(message: &str, config: &config::RootConfig) -> String {
    let marker = "entities[";
    let Some(start) = message.find(marker) else {
        return message.to_string();
    };
    let idx_start = start + marker.len();
    let bytes = message.as_bytes();
    let mut idx_end = idx_start;
    while idx_end < bytes.len() && bytes[idx_end].is_ascii_digit() {
        idx_end += 1;
    }
    if idx_end >= bytes.len() || bytes[idx_end] != b']' {
        return message.to_string();
    }
    let Ok(index) = message[idx_start..idx_end].parse::<usize>() else {
        return message.to_string();
    };
    let Some(entity) = config.entities.get(index) else {
        return message.to_string();
    };
    let insert_at = idx_end + 1;
    let mut out = String::new();
    out.push_str(&message[..insert_at]);
    out.push_str(&format!(" (entity.name={})", entity.name));
    out.push_str(&message[insert_at..]);
    out
}
