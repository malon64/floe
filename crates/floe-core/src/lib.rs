use std::path::{Path, PathBuf};

use polars::prelude::DataFrame;
use yaml_schema::{Engine, RootSchema};

pub mod config;
pub mod check;
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

pub fn load_config(config_path: &Path) -> FloeResult<config::RootConfig> {
    parse::parse_config(config_path)
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
        let required_cols = required_columns(entity);

        let inputs = read_inputs(entity, input_path)?;

        let track_cast_errors = !matches!(input.cast_mode.as_deref(), Some("coerce"));

        for (source_path, raw_df, mut df) in inputs {
            let source_stem = source_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(entity.name.as_str());
            let (accept_rows, errors_per_row) = collect_errors(
                &raw_df,
                &df,
                &required_cols,
                &entity.schema.columns,
                track_cast_errors,
            )?;
            let has_errors = errors_per_row.iter().any(|err| err.is_some());

            match entity.policy.severity.as_str() {
                "warn" => {
                    if has_errors {
                        let count = errors_per_row.iter().filter(|err| err.is_some()).count();
                        eprintln!(
                            "warn: {count} row(s) failed not_null checks for entity {} in {}",
                            entity.name,
                            source_path.display()
                        );
                    }
                    write_accepted_output(entity, &mut df, source_stem)?;
                    // TODO: re-enable archiving once behavior is finalized.
                    // let archived_path = write::archive_input(&source_path, &archive_dir)?;
                    // log_output(&entity.name, "archived", &archived_path);
                }
                "reject" => {
                    if has_errors {
                        let rejected_target = validate_rejected_target(entity, "reject")?;

                        let (accept_mask, reject_mask) = check::build_row_masks(&accept_rows);
                        let mut accepted_df = df.filter(&accept_mask).map_err(|err| {
                            Box::new(ConfigError(format!("failed to filter accepted rows: {err}")))
                        })?;
                        let mut rejected_df = df.filter(&reject_mask).map_err(|err| {
                            Box::new(ConfigError(format!("failed to filter rejected rows: {err}")))
                        })?;
                        append_rejection_columns(&mut rejected_df, &errors_per_row, false)?;

                        write_accepted_output(entity, &mut accepted_df, source_stem)?;
                        let rejected_path = write::write_rejected_csv(
                            &mut rejected_df,
                            &rejected_target.path,
                            source_stem,
                        )?;
                        log_output(&entity.name, "rejected", &rejected_path);
                    } else {
                        write_accepted_output(entity, &mut df, source_stem)?;
                    }
                    // TODO: re-enable archiving once behavior is finalized.
                    // let archived_path = write::archive_input(&source_path, &archive_dir)?;
                    // log_output(&entity.name, "archived", &archived_path);
                }
                "abort" => {
                    if has_errors {
                        let rejected_target = validate_rejected_target(entity, "abort")?;
                        let rejected_path =
                            write::write_rejected_raw(&source_path, &rejected_target.path)?;
                        let report_path = write::write_error_report(
                            &rejected_target.path,
                            source_stem,
                            &errors_per_row,
                        )?;
                        log_output(&entity.name, "rejected", &rejected_path);
                        log_output(&entity.name, "reject report", &report_path);
                    } else {
                        write_accepted_output(entity, &mut df, source_stem)?;
                    }
                    // TODO: re-enable archiving once behavior is finalized.
                    // let archived_path = write::archive_input(&source_path, &archive_dir)?;
                    // log_output(&entity.name, "archived", &archived_path);
                }
                severity => {
                    return Err(Box::new(ConfigError(format!(
                        "unsupported policy severity: {severity}"
                    ))))
                }
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

fn log_output(entity_name: &str, label: &str, path: &Path) {
    println!("entity {}: {} -> {}", entity_name, label, path.display());
}

fn required_columns(entity: &config::EntityConfig) -> Vec<String> {
    entity
        .schema
        .columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}

fn read_inputs(
    entity: &config::EntityConfig,
    input_path: &Path,
) -> FloeResult<Vec<(PathBuf, DataFrame, DataFrame)>> {
    let input = &entity.source;
    match input.format.as_str() {
        "csv" => {
            let default_options = config::SourceOptions::default();
            let source_options = input.options.as_ref().unwrap_or(&default_options);
            let typed_schema = entity.schema.to_polars_schema()?;
            let raw_schema = entity.schema.to_polars_string_schema()?;
            let files = read::list_csv_files(input_path)?;
            let mut inputs = Vec::with_capacity(files.len());
            let raw_plan = read::CsvReadPlan::strict(raw_schema);
            let typed_plan = read::CsvReadPlan::permissive(typed_schema);
            for path in files {
                let raw_df = read::read_csv_file(&path, source_options, &raw_plan)?;
                let typed_df = read::read_csv_file(&path, source_options, &typed_plan)?;
                inputs.push((path, raw_df, typed_df));
            }
            Ok(inputs)
        }
        format => Err(Box::new(ConfigError(format!(
            "unsupported source format for now: {format}"
        )))),
    }
}

fn collect_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
) -> FloeResult<(Vec<bool>, Vec<Option<String>>)> {
    let mut error_lists = check::not_null_errors(typed_df, required_cols)?;
    if track_cast_errors {
        let cast_errors = check::cast_error_errors(raw_df, typed_df, columns)?;
        for (errors, cast) in error_lists.iter_mut().zip(cast_errors) {
            errors.extend(cast);
        }
    }
    Ok(check::build_error_state(error_lists))
}

fn write_accepted_output(
    entity: &config::EntityConfig,
    df: &mut DataFrame,
    source_stem: &str,
) -> FloeResult<()> {
    match entity.sink.accepted.format.as_str() {
        "parquet" => {
            let output_path =
                write::write_parquet(df, &entity.sink.accepted.path, source_stem)?;
            log_output(&entity.name, "accepted", &output_path);
            Ok(())
        }
        format => Err(Box::new(ConfigError(format!(
            "unsupported sink format for now: {format}"
        )))),
    }
}

fn validate_rejected_target<'a>(
    entity: &'a config::EntityConfig,
    severity: &str,
) -> FloeResult<&'a config::SinkTarget> {
    let rejected_target = entity.sink.rejected.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "sink.rejected is required for {severity} severity"
        )))
    })?;
    match rejected_target.format.as_str() {
        "csv" => Ok(rejected_target),
        format => Err(Box::new(ConfigError(format!(
            "unsupported rejected sink format for now: {format}"
        )))),
    }
}

fn append_rejection_columns(
    df: &mut DataFrame,
    errors_per_row: &[Option<String>],
    include_all_rows: bool,
) -> FloeResult<()> {
    let (row_index, errors) = check::rejected_error_columns(errors_per_row, include_all_rows);
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
    Ok(())
}
