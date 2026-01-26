use std::collections::HashMap;

use polars::prelude::{DataFrame, DataType, Series};

use crate::{config, report, ConfigError, FloeResult};

const MAX_MISMATCH_COLUMNS: usize = 50;

#[derive(Debug, Clone)]
pub struct MismatchOutcome {
    pub report: report::FileMismatch,
    pub rejected: bool,
    pub aborted: bool,
    pub warnings: u64,
    pub errors: u64,
}

pub fn apply_schema_mismatch(
    entity: &config::EntityConfig,
    declared_columns: &[config::ColumnConfig],
    mut raw_df: Option<&mut DataFrame>,
    typed_df: &mut DataFrame,
) -> FloeResult<MismatchOutcome> {
    let declared_names = declared_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let input_names = match raw_df.as_mut() {
        Some(df) => df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
        None => typed_df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
    };

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
            if let Some(raw_df) = raw_df.as_mut() {
                add_missing_columns(raw_df, typed_df, declared_columns, &missing)?;
            } else {
                add_missing_columns_typed(typed_df, declared_columns, &missing)?;
            }
            filled = true;
        }
        if effective_extra == "ignore" && !extra.is_empty() {
            if let Some(raw_df) = raw_df.as_mut() {
                drop_extra_columns(raw_df, typed_df, &extra)?;
            } else {
                drop_extra_columns_typed(typed_df, &extra)?;
            }
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

fn add_missing_columns_typed(
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

    let height = typed_df.height();
    for name in missing {
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

fn drop_extra_columns(
    raw_df: &mut DataFrame,
    typed_df: &mut DataFrame,
    extra: &[String],
) -> FloeResult<()> {
    for name in extra {
        raw_df.drop_in_place(name).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to drop extra column {}: {err}",
                name
            )))
        })?;
    }

    for name in extra {
        typed_df.drop_in_place(name).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to drop extra column {}: {err}",
                name
            )))
        })?;
    }
    Ok(())
}

fn drop_extra_columns_typed(typed_df: &mut DataFrame, extra: &[String]) -> FloeResult<()> {
    for name in extra {
        typed_df.drop_in_place(name).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to drop extra column {}: {err}",
                name
            )))
        })?;
    }
    Ok(())
}
