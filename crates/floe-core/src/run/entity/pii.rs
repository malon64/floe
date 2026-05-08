use polars::prelude::*;
use sha2::{Digest, Sha256};

use crate::config::{extract_first_n, extract_last_n, PiiColumnConfig, PiiConfig, PiiStrategy};
use crate::FloeResult;

pub fn apply_pii_masking(df: &mut DataFrame, pii: &PiiConfig) -> FloeResult<()> {
    for col_cfg in &pii.columns {
        apply_pii_column(df, col_cfg)?;
    }
    Ok(())
}

fn apply_pii_column(df: &mut DataFrame, col_cfg: &PiiColumnConfig) -> FloeResult<()> {
    let col_name = col_cfg.name.as_str();

    // If column was removed by a prior `drop` or doesn't exist, skip silently.
    if df.column(col_name).is_err() {
        return Ok(());
    }

    match col_cfg.strategy {
        PiiStrategy::Hash => {
            let col = df.column(col_name)?;
            let new_series = hash_column(col, col_name)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII hash: failed to replace column {col_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Drop => {
            df.drop_in_place(col_name).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII drop: failed to drop column {col_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Nullify => {
            let col = df.column(col_name)?;
            let len = col.len();
            let dtype = col.dtype().clone();
            let null_series = Series::full_null(col_name.into(), len, &dtype);
            df.with_column(null_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII nullify: failed to replace column {col_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Redact => {
            let redact_value = col_cfg.redact_value.as_deref().unwrap_or("[REDACTED]");
            let col = df.column(col_name)?;
            let new_series = redact_column(col, col_name, redact_value)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII redact: failed to replace column {col_name}: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        }
        PiiStrategy::Mask => {
            let pattern = col_cfg.mask_pattern.as_deref().unwrap_or("");
            let first_n = extract_first_n(pattern);
            let last_n = extract_last_n(pattern);
            let col = df.column(col_name)?;
            let new_series = mask_column(col, col_name, pattern, first_n, last_n)?;
            df.with_column(new_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "PII mask: failed to replace column {col_name}: {e}"
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

fn apply_mask(value: &str, pattern: &str, first_n: Option<usize>, last_n: Option<usize>) -> String {
    let fn_val = first_n.unwrap_or(0);
    let ln_val = last_n.unwrap_or(0);

    // If the value is too short to safely extract both prefix and suffix, return unchanged.
    if value.len() < fn_val + ln_val {
        return value.to_string();
    }

    let prefix = &value[..fn_val];
    let suffix = if ln_val > 0 {
        &value[value.len() - ln_val..]
    } else {
        ""
    };

    let mut result = pattern.to_string();
    if fn_val > 0 {
        result = result.replace(&format!("{{first{fn_val}}}"), prefix);
    }
    if ln_val > 0 {
        result = result.replace(&format!("{{last{ln_val}}}"), suffix);
    }
    result
}
