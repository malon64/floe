use std::collections::HashMap;

use polars::prelude::*;
use sha2::{Digest, Sha256};

use crate::config::{extract_first_n, extract_last_n, PiiColumnConfig, PiiConfig, PiiStrategy};
use crate::FloeResult;

/// Apply PII masking to all configured columns.
///
/// `schema_to_runtime` maps schema column names to post-rename runtime names
/// (e.g. `"Credit Card"` → `"credit_card"` when normalize_columns is active).
/// Only entries where the names differ are present; columns whose schema name
/// already equals the runtime name need no entry.
pub fn apply_pii_masking(
    df: &mut DataFrame,
    pii: &PiiConfig,
    schema_to_runtime: &HashMap<String, String>,
) -> FloeResult<()> {
    for col_cfg in &pii.columns {
        apply_pii_column(df, col_cfg, schema_to_runtime)?;
    }
    Ok(())
}

fn apply_pii_column(
    df: &mut DataFrame,
    col_cfg: &PiiColumnConfig,
    schema_to_runtime: &HashMap<String, String>,
) -> FloeResult<()> {
    let declared_name = col_cfg.name.as_str();
    // When normalize_columns is active the schema column name may differ from
    // the runtime name in the DataFrame (e.g. "Credit Card" → "credit_card").
    // schema_to_runtime maps schema names to their post-rename runtime names.
    let runtime_name = schema_to_runtime
        .get(declared_name)
        .map(|s| s.as_str())
        .unwrap_or(declared_name);

    // If column was removed by a prior `drop` or doesn't exist, skip silently.
    if df.column(runtime_name).is_err() {
        return Ok(());
    }

    match col_cfg.strategy {
        PiiStrategy::Hash => {
            let col = df.column(runtime_name)?;
            let new_series = hash_column(col, runtime_name)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII hash: failed to replace column {runtime_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Drop => {
            df.drop_in_place(runtime_name).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII drop: failed to drop column {runtime_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Nullify => {
            let col = df.column(runtime_name)?;
            let len = col.len();
            let dtype = col.dtype().clone();
            let null_series = Series::full_null(runtime_name.into(), len, &dtype);
            df.with_column(null_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII nullify: failed to replace column {runtime_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Redact => {
            let redact_value = col_cfg.redact_value.as_deref().unwrap_or("[REDACTED]");
            let col = df.column(runtime_name)?;
            let new_series = redact_column(col, runtime_name, redact_value)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII redact: failed to replace column {runtime_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Mask => {
            let pattern = col_cfg.mask_pattern.as_deref().unwrap_or("");
            let first_n = extract_first_n(pattern);
            let last_n = extract_last_n(pattern);
            let col = df.column(runtime_name)?;
            let new_series = mask_column(col, runtime_name, pattern, first_n, last_n)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII mask: failed to replace column {runtime_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Tokenize => {
            // Rejected at config validation; never reached at runtime.
        }
    }
    Ok(())
}

fn to_string_chunked(col: &Column, col_name: &str) -> FloeResult<StringChunked> {
    let str_col = col.cast(&DataType::String).map_err(|e| {
        Box::new(crate::errors::RunError(format!(
            "PII: failed to cast column {col_name} to string: {e}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let series = str_col.as_materialized_series().clone();
    let ca = series.str().map_err(|e| {
        Box::new(crate::errors::RunError(format!(
            "PII: failed to access string chunked array for column {col_name}: {e}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(ca.clone())
}

fn hash_column(col: &Column, col_name: &str) -> FloeResult<Series> {
    let ca = to_string_chunked(col, col_name)?;
    let hashed: StringChunked = ca.apply(|opt| {
        opt.map(|v| {
            let mut hasher = Sha256::new();
            hasher.update(v.as_bytes());
            hex::encode(hasher.finalize()).into()
        })
    });
    let mut s = hashed.into_series();
    s.rename(col_name.into());
    Ok(s)
}

fn redact_column(col: &Column, col_name: &str, redact_value: &str) -> FloeResult<Series> {
    let ca = to_string_chunked(col, col_name)?;
    let redacted: StringChunked = ca.apply(|opt| opt.map(|_| redact_value.into()));
    let mut s = redacted.into_series();
    s.rename(col_name.into());
    Ok(s)
}

fn mask_column(
    col: &Column,
    col_name: &str,
    pattern: &str,
    first_n: Option<usize>,
    last_n: Option<usize>,
) -> FloeResult<Series> {
    let ca = to_string_chunked(col, col_name)?;
    let masked: StringChunked =
        ca.apply(|opt| opt.map(|v| apply_mask(v, pattern, first_n, last_n).into()));
    let mut s = masked.into_series();
    s.rename(col_name.into());
    Ok(s)
}

pub(crate) fn apply_mask(
    value: &str,
    pattern: &str,
    first_n: Option<usize>,
    last_n: Option<usize>,
) -> String {
    let fn_val = first_n.unwrap_or(0);
    let ln_val = last_n.unwrap_or(0);

    // Count Unicode scalar values, not bytes, to avoid splitting multi-byte chars.
    let char_count = value.chars().count();

    // Clamp revealed chars so they never overlap; suffix takes priority.
    // Never return the raw value unchanged — always apply the pattern's literal mask chars.
    let actual_ln = ln_val.min(char_count);
    let actual_fn = fn_val.min(char_count.saturating_sub(actual_ln));

    let prefix: String = value.chars().take(actual_fn).collect();
    let suffix: String = value.chars().skip(char_count - actual_ln).collect();

    let mut result = pattern.to_string();
    if fn_val > 0 {
        result = result.replace(&format!("{{first{fn_val}}}"), &prefix);
    }
    if ln_val > 0 {
        result = result.replace(&format!("{{last{ln_val}}}"), &suffix);
    }
    result
}
