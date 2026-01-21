use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use polars::prelude::{DataFrame, DataType, Schema, Series};
use serde_json::{Map, Value};

use crate::{check, config, io, report, ConfigError, FloeResult, RunOptions, ValidateOptions};

mod normalize;
use normalize::{
    normalize_dataframe_columns, normalize_name, normalize_schema_columns,
    resolve_normalize_strategy,
};

const MAX_EXAMPLES_PER_RULE: u64 = 3;
const MAX_MISMATCH_COLUMNS: usize = 50;
const RULE_COUNT: usize = 4;
const CAST_ERROR_INDEX: usize = 1;

type ValidationCollect = (Vec<bool>, Vec<Option<String>>, Vec<Vec<check::RowError>>);

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: String,
    pub report_base_path: String,
    pub entity_outcomes: Vec<EntityOutcome>,
}

#[derive(Debug, Clone)]
pub struct EntityOutcome {
    pub report: report::RunReport,
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
    let config = config::parse_config(config_path)?;
    if !options.entities.is_empty() {
        validate_entities(&config, &options.entities)?;
    }
    let report_base_path = config.report.path.clone();
    let started_at = report::now_rfc3339();
    let run_id = options
        .run_id
        .clone()
        .unwrap_or_else(|| report::run_id_from_timestamp(&started_at));
    let run_timer = Instant::now();
    let mut entity_outcomes = Vec::new();

    let mut abort_run = false;
    for entity in &config.entities {
        let input = &entity.source;
        let input_path = Path::new(&input.path);
        let normalize_strategy = resolve_normalize_strategy(entity)?;
        let normalized_columns = if let Some(strategy) = normalize_strategy.as_deref() {
            normalize_schema_columns(&entity.schema.columns, strategy)?
        } else {
            entity.schema.columns.clone()
        };
        let required_cols = required_columns(&normalized_columns);

        let inputs = read_inputs(
            entity,
            input_path,
            &normalized_columns,
            normalize_strategy.as_deref(),
        )?;
        let resolved_files = inputs
            .iter()
            .map(|(path, _, _)| path.display().to_string())
            .collect::<Vec<_>>();
        let resolved_mode = if input_path.is_dir() {
            report::ResolvedInputMode::Directory
        } else {
            report::ResolvedInputMode::File
        };
        let severity = match entity.policy.severity.as_str() {
            "warn" => report::Severity::Warn,
            "reject" => report::Severity::Reject,
            "abort" => report::Severity::Abort,
            severity => {
                return Err(Box::new(ConfigError(format!(
                    "unsupported policy severity: {severity}"
                ))))
            }
        };

        let track_cast_errors = !matches!(input.cast_mode.as_deref(), Some("coerce"));
        let mut file_reports = Vec::with_capacity(inputs.len());
        let mut file_statuses = Vec::with_capacity(inputs.len());
        let mut totals = report::ResultsTotals {
            files_total: 0,
            rows_total: 0,
            accepted_total: 0,
            rejected_total: 0,
            warnings_total: 0,
            errors_total: 0,
        };
        let archive_enabled = entity.sink.archive.is_some();
        let archive_dir = entity
            .sink
            .archive
            .as_ref()
            .map(|archive| PathBuf::from(&archive.path));

        let mut file_timings_ms = Vec::with_capacity(inputs.len());
        for (source_path, mut raw_df, mut df) in inputs {
            let file_timer = Instant::now();
            let source_stem = source_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(entity.name.as_str());
            let mismatch =
                apply_schema_mismatch(entity, &normalized_columns, &mut raw_df, &mut df)?;
            let row_count = raw_df.height() as u64;

            if mismatch.rejected || mismatch.aborted {
                let accepted_path = None;
                let errors_path = None;

                let rejected_target = validate_rejected_target(
                    entity,
                    if mismatch.aborted { "abort" } else { "reject" },
                )?;
                let rejected_path_buf =
                    io::write::write_rejected_raw(&source_path, &rejected_target.path)?;
                let rejected_path = Some(rejected_path_buf.display().to_string());

                let archived_path = if archive_enabled {
                    if let Some(dir) = &archive_dir {
                        let archived_path_buf = io::write::archive_input(&source_path, dir)?;
                        Some(archived_path_buf.display().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                let status = if mismatch.aborted {
                    report::FileStatus::Aborted
                } else {
                    report::FileStatus::Rejected
                };
                let accepted_count = 0;
                let rejected_count = row_count;
                let errors = mismatch.errors;
                let warnings = mismatch.warnings;

                let file_report = report::FileReport {
                    input_file: source_path.display().to_string(),
                    status,
                    row_count,
                    accepted_count,
                    rejected_count,
                    mismatch: mismatch.report,
                    output: report::FileOutput {
                        accepted_path,
                        rejected_path,
                        errors_path,
                        archived_path,
                    },
                    validation: report::FileValidation {
                        errors,
                        warnings,
                        rules: Vec::new(),
                        examples: report::ExampleSummary {
                            max_examples_per_rule: MAX_EXAMPLES_PER_RULE,
                            items: Vec::new(),
                        },
                    },
                };

                totals.rows_total += row_count;
                totals.accepted_total += accepted_count;
                totals.rejected_total += rejected_count;
                totals.errors_total += errors;
                totals.warnings_total += warnings;
                file_statuses.push(status);
                file_reports.push(file_report);
                file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

                if mismatch.aborted {
                    abort_run = true;
                    break;
                }
                continue;
            }

            let (accept_rows, errors_json, error_lists) = collect_errors(
                &raw_df,
                &df,
                &required_cols,
                &normalized_columns,
                track_cast_errors,
            )?;
            let row_error_count = error_lists
                .iter()
                .filter(|errors| !errors.is_empty())
                .count() as u64;
            let violation_count = error_lists
                .iter()
                .map(|errors| errors.len() as u64)
                .sum::<u64>();
            let accept_count = accept_rows.iter().filter(|accepted| **accepted).count() as u64;
            let reject_count = row_count.saturating_sub(accept_count);
            let has_errors = row_error_count > 0;
            let mut accepted_path = None;
            let mut rejected_path = None;
            let mut errors_path = None;
            let mut archived_path = None;
            let (rules, examples) =
                summarize_validation(&error_lists, &normalized_columns, severity);

            match entity.policy.severity.as_str() {
                "warn" => {
                    let output_path = write_accepted_output(entity, &mut df, source_stem)?;
                    accepted_path = Some(output_path.display().to_string());
                }
                "reject" => {
                    if has_errors {
                        let rejected_target = validate_rejected_target(entity, "reject")?;

                        let (accept_mask, reject_mask) = check::build_row_masks(&accept_rows);
                        let mut accepted_df = df.filter(&accept_mask).map_err(|err| {
                            Box::new(ConfigError(format!(
                                "failed to filter accepted rows: {err}"
                            )))
                        })?;
                        let mut rejected_df = df.filter(&reject_mask).map_err(|err| {
                            Box::new(ConfigError(format!(
                                "failed to filter rejected rows: {err}"
                            )))
                        })?;
                        append_rejection_columns(&mut rejected_df, &errors_json, false)?;

                        let output_path =
                            write_accepted_output(entity, &mut accepted_df, source_stem)?;
                        accepted_path = Some(output_path.display().to_string());
                        let rejected_path_buf = io::write::write_rejected_csv(
                            &mut rejected_df,
                            &rejected_target.path,
                            source_stem,
                        )?;
                        rejected_path = Some(rejected_path_buf.display().to_string());
                    } else {
                        let output_path = write_accepted_output(entity, &mut df, source_stem)?;
                        accepted_path = Some(output_path.display().to_string());
                    }
                }
                "abort" => {
                    if has_errors {
                        let rejected_target = validate_rejected_target(entity, "abort")?;
                        let rejected_path_buf =
                            io::write::write_rejected_raw(&source_path, &rejected_target.path)?;
                        let report_path = io::write::write_error_report(
                            &rejected_target.path,
                            source_stem,
                            &errors_json,
                        )?;
                        rejected_path = Some(rejected_path_buf.display().to_string());
                        errors_path = Some(report_path.display().to_string());
                    } else {
                        let output_path = write_accepted_output(entity, &mut df, source_stem)?;
                        accepted_path = Some(output_path.display().to_string());
                    }
                }
                severity => {
                    return Err(Box::new(ConfigError(format!(
                        "unsupported policy severity: {severity}"
                    ))))
                }
            }

            if archive_enabled {
                if let Some(dir) = &archive_dir {
                    let archived_path_buf = io::write::archive_input(&source_path, dir)?;
                    archived_path = Some(archived_path_buf.display().to_string());
                }
            }

            let (status, accepted_count, rejected_count, errors, warnings) =
                match entity.policy.severity.as_str() {
                    "warn" => (
                        report::FileStatus::Success,
                        row_count,
                        0,
                        0,
                        violation_count,
                    ),
                    "reject" => {
                        if has_errors {
                            (
                                report::FileStatus::Rejected,
                                accept_count,
                                reject_count,
                                violation_count,
                                0,
                            )
                        } else {
                            (report::FileStatus::Success, row_count, 0, 0, 0)
                        }
                    }
                    "abort" => {
                        if has_errors {
                            (
                                report::FileStatus::Aborted,
                                0,
                                row_count,
                                violation_count,
                                0,
                            )
                        } else {
                            (report::FileStatus::Success, row_count, 0, 0, 0)
                        }
                    }
                    _ => unreachable!("severity validated earlier"),
                };
            let errors = errors + mismatch.errors;
            let warnings = warnings + mismatch.warnings;

            let file_report = report::FileReport {
                input_file: source_path.display().to_string(),
                status,
                row_count,
                accepted_count,
                rejected_count,
                mismatch: mismatch.report,
                output: report::FileOutput {
                    accepted_path,
                    rejected_path,
                    errors_path,
                    archived_path,
                },
                validation: report::FileValidation {
                    errors,
                    warnings,
                    rules,
                    examples,
                },
            };

            totals.rows_total += row_count;
            totals.accepted_total += accepted_count;
            totals.rejected_total += rejected_count;
            totals.errors_total += errors;
            totals.warnings_total += warnings;
            file_statuses.push(status);
            file_reports.push(file_report);
            file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));
        }

        totals.files_total = file_reports.len() as u64;

        let (mut run_status, exit_code) = report::compute_run_outcome(&file_statuses);
        if run_status == report::RunStatus::Success && totals.warnings_total > 0 {
            run_status = report::RunStatus::SuccessWithWarnings;
        }

        let report_dir = Path::new(&config.report.path);
        let report_path = report::ReportWriter::report_path(report_dir, &run_id, &entity.name);
        let finished_at = report::now_rfc3339();
        let duration_ms = run_timer.elapsed().as_millis() as u64;
        let run_report =
            report::RunReport {
                spec_version: config.version.clone(),
                tool: report::ToolInfo {
                    name: "floe".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    git: None,
                },
                run: report::RunInfo {
                    run_id: run_id.clone(),
                    started_at: started_at.clone(),
                    finished_at,
                    duration_ms,
                    status: run_status,
                    exit_code,
                },
                config: report::ConfigEcho {
                    path: config_path.display().to_string(),
                    version: config.version.clone(),
                    metadata: config.metadata.as_ref().map(project_metadata_json),
                },
                entity: report::EntityEcho {
                    name: entity.name.clone(),
                    metadata: entity.metadata.as_ref().map(entity_metadata_json),
                },
                source: report::SourceEcho {
                    format: input.format.clone(),
                    path: input.path.clone(),
                    options: input.options.as_ref().map(source_options_json),
                    cast_mode: input.cast_mode.clone(),
                    read_plan: report::SourceReadPlan::RawAndTyped,
                    resolved_inputs: report::ResolvedInputs {
                        mode: resolved_mode,
                        file_count: resolved_files.len() as u64,
                        files: resolved_files,
                    },
                },
                sink: report::SinkEcho {
                    accepted: report::SinkTargetEcho {
                        format: entity.sink.accepted.format.clone(),
                        path: entity.sink.accepted.path.clone(),
                    },
                    rejected: entity.sink.rejected.as_ref().map(|rejected| {
                        report::SinkTargetEcho {
                            format: rejected.format.clone(),
                            path: rejected.path.clone(),
                        }
                    }),
                    archive: report::SinkArchiveEcho {
                        enabled: entity.sink.archive.is_some(),
                        path: entity
                            .sink
                            .archive
                            .as_ref()
                            .map(|archive| archive.path.clone()),
                    },
                },
                report: report::ReportEcho {
                    path: config.report.path.clone(),
                    report_file: report_path.display().to_string(),
                },
                policy: report::PolicyEcho { severity },
                results: totals,
                files: file_reports,
            };
        report::ReportWriter::write_report(report_dir, &run_id, &entity.name, &run_report)?;
        entity_outcomes.push(EntityOutcome {
            report: run_report,
            file_timings_ms,
        });
        if abort_run {
            break;
        }
    }

    Ok(RunOutcome {
        run_id,
        report_base_path,
        entity_outcomes,
    })
}

fn required_columns(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}

fn read_inputs(
    entity: &config::EntityConfig,
    input_path: &Path,
    columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Vec<(PathBuf, DataFrame, DataFrame)>> {
    let input = &entity.source;
    match input.format.as_str() {
        "csv" => {
            let default_options = config::SourceOptions::default();
            let source_options = input.options.as_ref().unwrap_or(&default_options);
            let files = io::read_csv::list_csv_files(input_path)?;
            let mut inputs = Vec::with_capacity(files.len());
            for path in files {
                let input_columns = resolve_input_columns(&path, source_options, columns)?;
                let raw_schema = build_raw_schema(&input_columns);
                let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
                let raw_plan = io::read_csv::CsvReadPlan::strict(raw_schema);
                let typed_plan = io::read_csv::CsvReadPlan::permissive(typed_schema);
                let mut raw_df = io::read_csv::read_csv_file(&path, source_options, &raw_plan)?;
                let mut typed_df = io::read_csv::read_csv_file(&path, source_options, &typed_plan)?;
                if let Some(strategy) = normalize_strategy {
                    normalize_dataframe_columns(&mut raw_df, strategy)?;
                    normalize_dataframe_columns(&mut typed_df, strategy)?;
                }
                inputs.push((path, raw_df, typed_df));
            }
            Ok(inputs)
        }
        format => Err(Box::new(ConfigError(format!(
            "unsupported source format for now: {format}"
        )))),
    }
}

fn resolve_input_columns(
    path: &Path,
    source_options: &config::SourceOptions,
    declared_columns: &[config::ColumnConfig],
) -> FloeResult<Vec<String>> {
    let header = source_options.header.unwrap_or(true);
    let input_columns = io::read_csv::read_csv_header(path, source_options)?;
    if header {
        return Ok(input_columns);
    }

    let declared_names = declared_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    Ok(headless_columns(&declared_names, input_columns.len()))
}

fn headless_columns(declared_names: &[String], input_count: usize) -> Vec<String> {
    let mut names = declared_names
        .iter()
        .take(input_count)
        .cloned()
        .collect::<Vec<_>>();
    if input_count > declared_names.len() {
        for index in declared_names.len()..input_count {
            names.push(format!("extra_column_{}", index + 1));
        }
    }
    names
}

fn build_raw_schema(columns: &[String]) -> Schema {
    let mut schema = Schema::with_capacity(columns.len());
    for name in columns {
        schema.insert(name.as_str().into(), DataType::String);
    }
    schema
}

fn build_typed_schema(
    input_columns: &[String],
    declared_columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Schema> {
    let mut declared_types = HashMap::new();
    for column in declared_columns {
        declared_types.insert(
            column.name.as_str(),
            config::parse_data_type(&column.column_type)?,
        );
    }

    let mut schema = Schema::with_capacity(input_columns.len());
    for name in input_columns {
        let normalized = if let Some(strategy) = normalize_strategy {
            normalize_name(name, strategy)
        } else {
            name.to_string()
        };
        let dtype = declared_types
            .get(normalized.as_str())
            .cloned()
            .unwrap_or(DataType::String);
        schema.insert(name.as_str().into(), dtype);
    }
    Ok(schema)
}

fn collect_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
) -> FloeResult<ValidationCollect> {
    let mut error_lists = check::not_null_errors(typed_df, required_cols)?;
    if track_cast_errors {
        let cast_errors = check::cast_mismatch_errors(raw_df, typed_df, columns)?;
        for (errors, cast) in error_lists.iter_mut().zip(cast_errors) {
            errors.extend(cast);
        }
    }
    let unique_errors = check::unique_errors(typed_df, columns)?;
    for (errors, unique) in error_lists.iter_mut().zip(unique_errors) {
        errors.extend(unique);
    }
    let (accept_rows, errors_json) = check::build_error_state(&error_lists);
    Ok((accept_rows, errors_json, error_lists))
}

struct MismatchOutcome {
    report: report::FileMismatch,
    rejected: bool,
    aborted: bool,
    warnings: u64,
    errors: u64,
}

fn apply_schema_mismatch(
    entity: &config::EntityConfig,
    declared_columns: &[config::ColumnConfig],
    raw_df: &mut DataFrame,
    typed_df: &mut DataFrame,
) -> FloeResult<MismatchOutcome> {
    let declared_names = declared_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let input_names = raw_df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    let declared_set = declared_names
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let input_set = input_names
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();

    let mut missing = declared_names
        .iter()
        .filter(|name| !input_set.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    let mut extra = input_names
        .iter()
        .filter(|name| !declared_set.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    missing.sort();
    extra.sort();

    let mismatch_config = entity.schema.mismatch.as_ref();
    let missing_policy = mismatch_config
        .and_then(|mismatch| mismatch.missing_columns.as_deref())
        .unwrap_or("fill_nulls");
    let extra_policy = mismatch_config
        .and_then(|mismatch| mismatch.extra_columns.as_deref())
        .unwrap_or("ignore");

    let mut effective_missing = missing_policy;
    let mut effective_extra = extra_policy;
    let mut warning = None;
    let rejection_requested = (effective_missing == "reject_file" && !missing.is_empty())
        || (effective_extra == "reject_file" && !extra.is_empty());
    if rejection_requested && entity.policy.severity == "warn" {
        warning = Some(format!(
            "entity.name={} schema mismatch requested reject_file but policy.severity=warn; continuing",
            entity.name
        ));
        effective_missing = "fill_nulls";
        effective_extra = "ignore";
        eprintln!(
            "warn: {}",
            warning.as_deref().unwrap_or("schema mismatch override")
        );
    }

    let mut rejected = false;
    let mut aborted = false;
    let mut action = report::MismatchAction::None;
    if (effective_missing == "reject_file" && !missing.is_empty())
        || (effective_extra == "reject_file" && !extra.is_empty())
    {
        if entity.policy.severity == "abort" {
            aborted = true;
            action = report::MismatchAction::Aborted;
        } else if entity.policy.severity == "reject" {
            rejected = true;
            action = report::MismatchAction::RejectedFile;
        }
    }

    let mut errors = 0;
    if rejected || aborted {
        errors = 1;
    } else {
        let mut filled = false;
        let mut ignored = false;
        if effective_missing == "fill_nulls" && !missing.is_empty() {
            add_missing_columns(raw_df, typed_df, declared_columns, &missing)?;
            filled = true;
        }
        if effective_extra == "ignore" && !extra.is_empty() {
            drop_extra_columns(raw_df, &extra)?;
            drop_extra_columns(typed_df, &extra)?;
            ignored = true;
        }
        if filled {
            action = report::MismatchAction::FilledNulls;
        } else if ignored {
            action = report::MismatchAction::IgnoredExtras;
        }
    }

    let warnings = if warning.is_some() { 1 } else { 0 };
    let error = if rejected || aborted {
        Some(report::MismatchIssue {
            rule: "schema_mismatch".to_string(),
            message: format!(
                "entity.name={} schema mismatch: missing={} extra={}",
                entity.name,
                missing.len(),
                extra.len()
            ),
        })
    } else {
        None
    };

    let mismatch_report = report::FileMismatch {
        declared_columns_count: declared_names.len() as u64,
        input_columns_count: input_names.len() as u64,
        missing_columns: missing.iter().take(MAX_MISMATCH_COLUMNS).cloned().collect(),
        extra_columns: extra.iter().take(MAX_MISMATCH_COLUMNS).cloned().collect(),
        mismatch_action: action,
        error,
        warning,
    };

    Ok(MismatchOutcome {
        report: mismatch_report,
        rejected,
        aborted,
        warnings,
        errors,
    })
}

fn add_missing_columns(
    raw_df: &mut DataFrame,
    typed_df: &mut DataFrame,
    declared_columns: &[config::ColumnConfig],
    missing: &[String],
) -> FloeResult<()> {
    let mut types = HashMap::new();
    for column in declared_columns {
        types.insert(
            column.name.as_str(),
            config::parse_data_type(&column.column_type)?,
        );
    }

    let height = raw_df.height();
    for name in missing {
        let raw_series = Series::full_null(name.as_str().into(), height, &DataType::String);
        raw_df.with_column(raw_series).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to add missing column {}: {err}",
                name
            )))
        })?;

        let dtype = types
            .get(name.as_str())
            .cloned()
            .unwrap_or(DataType::String);
        let typed_series = Series::full_null(name.as_str().into(), height, &dtype);
        typed_df.with_column(typed_series).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to add missing column {}: {err}",
                name
            )))
        })?;
    }
    Ok(())
}

fn drop_extra_columns(df: &mut DataFrame, extra: &[String]) -> FloeResult<()> {
    for name in extra {
        df.drop_in_place(name).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to drop extra column {}: {err}",
                name
            )))
        })?;
    }
    Ok(())
}

fn write_accepted_output(
    entity: &config::EntityConfig,
    df: &mut DataFrame,
    source_stem: &str,
) -> FloeResult<PathBuf> {
    match entity.sink.accepted.format.as_str() {
        "parquet" => {
            let output_path =
                io::write::write_parquet(df, &entity.sink.accepted.path, source_stem)?;
            Ok(output_path)
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
    df.with_column(errors)
        .map_err(|err| Box::new(ConfigError(format!("failed to add __floe_errors: {err}"))))?;
    Ok(())
}

fn summarize_validation(
    errors_per_row: &[Vec<check::RowError>],
    columns: &[config::ColumnConfig],
    severity: report::Severity,
) -> (Vec<report::RuleSummary>, report::ExampleSummary) {
    if errors_per_row.iter().all(|errors| errors.is_empty()) {
        return (
            Vec::new(),
            report::ExampleSummary {
                max_examples_per_rule: MAX_EXAMPLES_PER_RULE,
                items: Vec::new(),
            },
        );
    }

    let mut column_types = HashMap::new();
    for column in columns {
        column_types.insert(column.name.clone(), column.column_type.clone());
    }

    let mut accumulators = vec![RuleAccumulator::default(); RULE_COUNT];
    let mut examples: Vec<Vec<report::ValidationExample>> = vec![Vec::new(); RULE_COUNT];

    for (row_idx, errors) in errors_per_row.iter().enumerate() {
        for error in errors {
            let idx = rule_index(&error.rule);
            let accumulator = &mut accumulators[idx];
            accumulator.violations += 1;
            let target_type = if idx == CAST_ERROR_INDEX {
                column_types.get(&error.column).cloned()
            } else {
                None
            };
            let entry = accumulator
                .columns
                .entry(error.column.clone())
                .or_insert_with(|| ColumnAccumulator {
                    violations: 0,
                    target_type,
                });
            entry.violations += 1;

            if examples[idx].len() < MAX_EXAMPLES_PER_RULE as usize {
                examples[idx].push(report::ValidationExample {
                    rule: rule_from_index(idx),
                    column: error.column.clone(),
                    row_index: row_idx as u64,
                    message: error.message.clone(),
                });
            }
        }
    }

    let mut rules = Vec::new();
    for (idx, accumulator) in accumulators.iter().enumerate() {
        if accumulator.violations == 0 {
            continue;
        }
        let mut columns = Vec::with_capacity(accumulator.columns.len());
        for (name, column_acc) in &accumulator.columns {
            columns.push(report::ColumnSummary {
                column: name.clone(),
                violations: column_acc.violations,
                target_type: column_acc.target_type.clone(),
            });
        }
        rules.push(report::RuleSummary {
            rule: rule_from_index(idx),
            severity,
            violations: accumulator.violations,
            columns,
        });
    }

    let mut items = Vec::new();
    for example_list in &examples {
        items.extend(example_list.iter().cloned());
    }

    (
        rules,
        report::ExampleSummary {
            max_examples_per_rule: MAX_EXAMPLES_PER_RULE,
            items,
        },
    )
}

#[derive(Debug, Default, Clone)]
struct RuleAccumulator {
    violations: u64,
    columns: BTreeMap<String, ColumnAccumulator>,
}

#[derive(Debug, Default, Clone)]
struct ColumnAccumulator {
    violations: u64,
    target_type: Option<String>,
}

fn rule_index(rule: &str) -> usize {
    match rule {
        "not_null" => 0,
        "cast_error" => 1,
        "unique" => 2,
        "schema_error" => 3,
        _ => 3,
    }
}

fn rule_from_index(idx: usize) -> report::RuleName {
    match idx {
        0 => report::RuleName::NotNull,
        1 => report::RuleName::CastError,
        2 => report::RuleName::Unique,
        _ => report::RuleName::SchemaError,
    }
}

fn project_metadata_json(meta: &config::ProjectMetadata) -> Value {
    let mut map = Map::new();
    map.insert("project".to_string(), Value::String(meta.project.clone()));
    if let Some(description) = &meta.description {
        map.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(owner) = &meta.owner {
        map.insert("owner".to_string(), Value::String(owner.clone()));
    }
    if let Some(tags) = &meta.tags {
        map.insert("tags".to_string(), string_array(tags));
    }
    Value::Object(map)
}

fn entity_metadata_json(meta: &config::EntityMetadata) -> Value {
    let mut map = Map::new();
    if let Some(data_product) = &meta.data_product {
        map.insert(
            "data_product".to_string(),
            Value::String(data_product.clone()),
        );
    }
    if let Some(domain) = &meta.domain {
        map.insert("domain".to_string(), Value::String(domain.clone()));
    }
    if let Some(owner) = &meta.owner {
        map.insert("owner".to_string(), Value::String(owner.clone()));
    }
    if let Some(description) = &meta.description {
        map.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(tags) = &meta.tags {
        map.insert("tags".to_string(), string_array(tags));
    }
    Value::Object(map)
}

fn source_options_json(options: &config::SourceOptions) -> Value {
    let mut map = Map::new();
    if let Some(header) = options.header {
        map.insert("header".to_string(), Value::Bool(header));
    }
    if let Some(separator) = &options.separator {
        map.insert("separator".to_string(), Value::String(separator.clone()));
    }
    if let Some(encoding) = &options.encoding {
        map.insert("encoding".to_string(), Value::String(encoding.clone()));
    }
    if let Some(null_values) = &options.null_values {
        map.insert("null_values".to_string(), string_array(null_values));
    }
    Value::Object(map)
}

fn string_array(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}
