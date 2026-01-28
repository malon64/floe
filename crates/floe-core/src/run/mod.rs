use std::path::Path;

use crate::io::storage::CloudClient;
use crate::{config, ConfigError, FloeResult, RunOptions, ValidateOptions};

mod context;
pub(crate) mod entity;
mod file;
pub(crate) mod normalize;
mod output;
mod reporting;

use context::RunContext;
use entity::{run_entity, EntityRunResult};

pub(super) const MAX_RESOLVED_INPUTS: usize = 50;

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: String,
    pub report_base_path: Option<String>,
    pub entity_outcomes: Vec<EntityOutcome>,
}

#[derive(Debug, Clone)]
pub struct EntityOutcome {
    pub report: crate::report::RunReport,
    pub file_timings_ms: Vec<Option<u64>>,
}

pub(crate) fn validate_entities(
    config: &config::RootConfig,
    selected: &[String],
) -> FloeResult<()> {
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

pub fn run(config_path: &Path, options: RunOptions) -> FloeResult<RunOutcome> {
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
    };
    crate::validate(config_path, validate_options)?;

    let context = RunContext::new(config_path, &options)?;
    if !options.entities.is_empty() {
        validate_entities(&context.config, &options.entities)?;
    }

    let mut entity_outcomes = Vec::new();
    let mut abort_run = false;
    let mut cloud = CloudClient::new();
    for entity in &context.config.entities {
        let EntityRunResult {
            outcome,
            abort_run: aborted,
        } = run_entity(&context, &mut cloud, entity)?;
        entity_outcomes.push(outcome);
        abort_run = abort_run || aborted;
        if abort_run {
            break;
        }
    }

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes,
    })
}
