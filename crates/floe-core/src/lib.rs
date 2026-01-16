use std::path::Path;

use yaml_schema::{Engine, RootSchema};

pub mod config;
mod check;
mod parse;
mod read;
mod write;
mod yaml_decode;

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
    let root_schema =
        RootSchema::load_from_str(SCHEMA_YAML).map_err(|err| {
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
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        return Err(Box::new(ConfigError(message)));
    }
    let config = parse::parse_config(config_path)?;
    if !options.entities.is_empty() {
        validate_entities(&config, &options.entities)?;
    }

    Ok(())
}

pub fn run(config_path: &Path, options: RunOptions) -> FloeResult<()> {
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
    };
    validate(config_path, validate_options)?;
    let config = parse::parse_config(config_path)?;
    if !options.entities.is_empty() {
        validate_entities(&config, &options.entities)?;
    }
    let _ = options;
    let version = &config.version;
    println!("Config file version : {}", version);

    for entity in &config.entities {
        let input = &entity.source;
        let input_path = Path::new(&input.path);
        let required_cols: Vec<String> = entity
            .schema
            .columns
            .iter()
            .filter(|col| col.nullable == Some(false))
            .map(|col| col.name.clone())
            .collect();

        let mut df = match input.format.as_str() {
            "csv" => {
                let default_options = config::SourceOptions::default();
                let source_options = input.options.as_ref().unwrap_or(&default_options);
                let schema = entity.schema.to_polars_schema()?;
                read::read_csv(input_path, source_options, schema)?
            }
            format => {
                return Err(Box::new(ConfigError(format!(
                    "unsupported source format for now: {format}"
                ))))
            }
        };

        let (accept_rows, errors_per_row) = check::not_null_results(&df, &required_cols)?;
        let has_errors = errors_per_row.iter().any(|err| err.is_some());

        match entity.policy.severity.as_str() {
            "warn" => {
                if has_errors {
                    let count = errors_per_row.iter().filter(|err| err.is_some()).count();
                    eprintln!(
                        "warn: {count} row(s) failed not_null checks for entity {}",
                        entity.name
                    );
                }
                match entity.sink.accepted.format.as_str() {
                    "parquet" => {
                        write::write_parquet(&mut df, &entity.sink.accepted.path, &entity.name)?
                    }
                    format => {
                        return Err(Box::new(ConfigError(format!(
                            "unsupported sink format for now: {format}"
                        ))))
                    }
                }
            }
            "reject" => {
                if has_errors {
                    let rejected_target = entity.sink.rejected.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(
                            "sink.rejected is required for reject severity".to_string(),
                        ))
                    })?;
                    match rejected_target.format.as_str() {
                        "csv" => {}
                        format => {
                            return Err(Box::new(ConfigError(format!(
                                "unsupported rejected sink format for now: {format}"
                            ))))
                        }
                    }

                    let (accept_mask, reject_mask) = check::build_row_masks(&accept_rows);
                    let mut accepted_df = df.filter(&accept_mask).map_err(|err| {
                        Box::new(ConfigError(format!("failed to filter accepted rows: {err}")))
                    })?;
                    let mut rejected_df = df.filter(&reject_mask).map_err(|err| {
                        Box::new(ConfigError(format!("failed to filter rejected rows: {err}")))
                    })?;
                    let (row_index, errors) =
                        check::rejected_error_columns(&errors_per_row, false);
                    rejected_df.with_column(row_index).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to add __floe_row_index: {err}"
                        )))
                    })?;
                    rejected_df.with_column(errors).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to add __floe_errors: {err}"
                        )))
                    })?;

                    match entity.sink.accepted.format.as_str() {
                        "parquet" => write::write_parquet(
                            &mut accepted_df,
                            &entity.sink.accepted.path,
                            &entity.name,
                        )?,
                        format => {
                            return Err(Box::new(ConfigError(format!(
                                "unsupported sink format for now: {format}"
                            ))))
                        }
                    }
                    write::write_rejected_csv(
                        &mut rejected_df,
                        &rejected_target.path,
                        &entity.name,
                    )?;
                } else {
                    match entity.sink.accepted.format.as_str() {
                        "parquet" => {
                            write::write_parquet(&mut df, &entity.sink.accepted.path, &entity.name)?
                        }
                        format => {
                            return Err(Box::new(ConfigError(format!(
                                "unsupported sink format for now: {format}"
                            ))))
                        }
                    }
                }
            }
            "abort" => {
                if has_errors {
                    let rejected_target = entity.sink.rejected.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(
                            "sink.rejected is required for abort severity".to_string(),
                        ))
                    })?;
                    match rejected_target.format.as_str() {
                        "csv" => {}
                        format => {
                            return Err(Box::new(ConfigError(format!(
                                "unsupported rejected sink format for now: {format}"
                            ))))
                        }
                    }
                    let (row_index, errors) =
                        check::rejected_error_columns(&errors_per_row, true);
                    df.with_column(row_index).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to add __floe_row_index: {err}"
                        )))
                    })?;
                    df.with_column(errors).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to add __floe_errors: {err}"
                        )))
                    })?;
                    write::write_rejected_csv(&mut df, &rejected_target.path, &entity.name)?;
                    write::write_error_report(
                        &rejected_target.path,
                        &entity.name,
                        &errors_per_row,
                    )?;
                } else {
                    match entity.sink.accepted.format.as_str() {
                        "parquet" => {
                            write::write_parquet(&mut df, &entity.sink.accepted.path, &entity.name)?
                        }
                        format => {
                            return Err(Box::new(ConfigError(format!(
                                "unsupported sink format for now: {format}"
                            ))))
                        }
                    }
                }
            }
            severity => {
                return Err(Box::new(ConfigError(format!(
                    "unsupported policy severity: {severity}"
                ))))
            }
        }
    }

    Ok(())
}

fn validate_entities(config: &config::RootConfig, selected: &[String]) -> FloeResult<()> {
    let missing: Vec<String> = selected
        .iter()
        .filter(|name| !config.entities.iter().any(|entity| &entity.name == *name))
        .cloned()
        .collect();

    if !missing.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entities not found: {}",
            missing.join(", ")
        ))));
    }
    Ok(())
}
