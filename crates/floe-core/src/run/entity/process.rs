use std::path::{Path, PathBuf};

use crate::{check, config, io, report, ConfigError, FloeResult};

use super::WarnOutcome;
use crate::run::output::write_accepted_output;
use io::format::InputFile;
use io::storage::Target;

pub(super) struct WarnContext<'a> {
    pub entity: &'a config::EntityConfig,
    pub input_file: &'a InputFile,
    pub row_count: u64,
    pub raw_df: Option<&'a polars::prelude::DataFrame>,
    pub df: &'a mut polars::prelude::DataFrame,
    pub required_cols: &'a [String],
    pub normalized_columns: &'a [config::ColumnConfig],
    pub track_cast_errors: bool,
    pub severity: report::Severity,
    pub accepted_target: &'a Target,
    pub sink_options_warning: &'a Option<String>,
    pub sink_options_warned: &'a mut bool,
    pub archive_enabled: bool,
    pub archive_dir: &'a Option<PathBuf>,
    pub mismatch: &'a check::MismatchOutcome,
    pub source_stem: &'a str,
    pub temp_dir: Option<&'a Path>,
    pub cloud: &'a mut io::storage::CloudClient,
    pub resolver: &'a config::StorageResolver,
    pub elapsed_ms: u64,
}

pub(super) fn try_warn_counts(ctx: &mut WarnContext<'_>) -> FloeResult<Option<WarnOutcome>> {
    if ctx.entity.policy.severity != "warn" {
        return Ok(None);
    }

    let cast_counts = if ctx.track_cast_errors {
        let raw_df = ctx.raw_df.ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} raw dataframe unavailable for cast checks",
                ctx.entity.name
            )))
        })?;
        check::cast_mismatch_counts(raw_df, ctx.df, ctx.normalized_columns)?
    } else {
        Vec::new()
    };
    let not_null_counts = check::not_null_counts(ctx.df, ctx.required_cols)?;
    let unique_counts = check::unique_counts(ctx.df, ctx.normalized_columns)?;
    let cast_total = cast_counts.iter().map(|(_, count, _)| *count).sum::<u64>();
    let not_null_total = not_null_counts.iter().map(|(_, count)| *count).sum::<u64>();
    let unique_total = unique_counts.iter().map(|(_, count)| *count).sum::<u64>();
    let violation_count = cast_total + not_null_total + unique_total;

    let mut rules = Vec::new();
    if cast_total > 0 {
        let columns = cast_counts
            .iter()
            .map(|(name, count, target_type)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: Some(target_type.clone()),
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::CastError,
            severity: ctx.severity,
            violations: cast_total,
            columns,
        });
    }
    if not_null_total > 0 {
        let columns = not_null_counts
            .iter()
            .map(|(name, count)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: None,
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::NotNull,
            severity: ctx.severity,
            violations: not_null_total,
            columns,
        });
    }
    if unique_total > 0 {
        let columns = unique_counts
            .iter()
            .map(|(name, count)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: None,
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::Unique,
            severity: ctx.severity,
            violations: unique_total,
            columns,
        });
    }

    let mut examples = report::ExampleSummary {
        max_examples_per_rule: 3,
        items: Vec::new(),
    };
    let mut archived_path = None;
    let mut sink_options_warnings = 0;
    if let Some(message) = ctx.sink_options_warning.as_deref() {
        sink_options_warnings = 1;
        if !*ctx.sink_options_warned {
            eprintln!("warn: {message}");
            *ctx.sink_options_warned = true;
        }
        append_sink_options_warning(&mut rules, &mut examples, message);
    }

    let output_path = write_accepted_output(
        ctx.entity.sink.accepted.format.as_str(),
        ctx.accepted_target,
        ctx.df,
        ctx.source_stem,
        ctx.temp_dir,
        ctx.cloud,
        ctx.resolver,
        ctx.entity,
    )?;
    let accepted_path = Some(output_path);

    if ctx.archive_enabled {
        if let Some(dir) = ctx.archive_dir {
            let archived_path_buf = io::write::archive_input(&ctx.input_file.local_path, dir)?;
            archived_path = Some(archived_path_buf.display().to_string());
        }
    }

    let errors = ctx.mismatch.errors;
    let warnings = violation_count + ctx.mismatch.warnings + sink_options_warnings;
    let status = report::FileStatus::Success;

    let file_report = report::FileReport {
        input_file: ctx.input_file.source_uri.clone(),
        status,
        row_count: ctx.row_count,
        accepted_count: ctx.row_count,
        rejected_count: 0,
        mismatch: ctx.mismatch.report.clone(),
        output: report::FileOutput {
            accepted_path,
            rejected_path: None,
            errors_path: None,
            archived_path,
        },
        validation: report::FileValidation {
            errors,
            warnings,
            rules,
            examples,
        },
    };

    Ok(Some(WarnOutcome {
        file_report,
        status,
        elapsed_ms: ctx.elapsed_ms,
    }))
}

pub(super) fn sink_options_warning(entity: &config::EntityConfig) -> Option<String> {
    let options = entity.sink.accepted.options.as_ref()?;
    if entity.sink.accepted.format == "parquet" {
        return None;
    }
    let mut keys = Vec::new();
    if options.compression.is_some() {
        keys.push("compression");
    }
    if options.row_group_size.is_some() {
        keys.push("row_group_size");
    }
    let detail = if keys.is_empty() {
        "options".to_string()
    } else {
        keys.join(", ")
    };
    Some(format!(
        "entity.name={} sink.accepted.options ({detail}) ignored for format={}",
        entity.name, entity.sink.accepted.format
    ))
}

pub(super) fn append_sink_options_warning(
    rules: &mut Vec<report::RuleSummary>,
    examples: &mut report::ExampleSummary,
    message: &str,
) {
    let column = "sink.accepted.options".to_string();
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.rule == report::RuleName::SchemaError)
    {
        rule.violations += 1;
        rule.severity = report::Severity::Warn;
        if let Some(entry) = rule.columns.iter_mut().find(|entry| entry.column == column) {
            entry.violations += 1;
        } else {
            rule.columns.push(report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            });
        }
    } else {
        rules.push(report::RuleSummary {
            rule: report::RuleName::SchemaError,
            severity: report::Severity::Warn,
            violations: 1,
            columns: vec![report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            }],
        });
    }

    let existing = examples
        .items
        .iter()
        .filter(|item| item.rule == report::RuleName::SchemaError)
        .count() as u64;
    if existing < examples.max_examples_per_rule {
        examples.items.push(report::ValidationExample {
            rule: report::RuleName::SchemaError,
            column,
            row_index: 0,
            message: message.to_string(),
        });
    }
}
