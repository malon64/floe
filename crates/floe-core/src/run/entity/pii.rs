use std::collections::HashMap;

use hmac::{Hmac, Mac};
use polars::prelude::*;
use sha2::{Digest, Sha256};

use crate::config::{extract_first_n, extract_last_n, PiiColumnConfig, PiiConfig, PiiStrategy};
use crate::FloeResult;

type HmacSha256 = Hmac<Sha256>;

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
            let new_series = hash_column(col, runtime_name, col_cfg.key.as_deref())?;
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

fn resolve_pii_key(raw_key: &str, col_name: &str) -> FloeResult<String> {
    if !raw_key.contains("${") {
        return Ok(raw_key.to_string());
    }
    let Some(inner) = raw_key.strip_prefix("${").and_then(|s| s.strip_suffix('}')) else {
        return Err(Box::new(crate::errors::ConfigError(format!(
            "pii column {col_name}: key must be a plain value or a single ${{VAR_NAME}} \
             reference; mixing literal text with ${{...}} is not supported"
        ))));
    };
    if inner.is_empty() || inner.contains('{') || inner.contains('}') {
        return Err(Box::new(crate::errors::ConfigError(format!(
            "pii column {col_name}: key has invalid placeholder syntax"
        ))));
    }
    std::env::var(inner).map_err(|_| {
        Box::new(crate::errors::RunError(format!(
            "pii column {col_name}: key references env var {inner} which is not set"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

fn hash_column(col: &Column, col_name: &str, key: Option<&str>) -> FloeResult<Series> {
    let ca = to_string_chunked(col, col_name)?;
    let hashed: StringChunked = if let Some(raw_key) = key {
        let resolved = resolve_pii_key(raw_key, col_name)?;
        let key_bytes = resolved.as_bytes().to_vec();
        ca.apply(|opt| {
            opt.map(|v| {
                let mut mac =
                    HmacSha256::new_from_slice(&key_bytes).expect("HMAC accepts keys of any size");
                mac.update(v.as_bytes());
                hex::encode(mac.finalize().into_bytes()).into()
            })
        })
    } else {
        ca.apply(|opt| {
            opt.map(|v| {
                let mut hasher = Sha256::new();
                hasher.update(v.as_bytes());
                hex::encode(hasher.finalize()).into()
            })
        })
    };
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::*;

    use super::apply_pii_masking;
    use crate::config::{PiiColumnConfig, PiiConfig, PiiStrategy};

    fn str_series(name: &str, values: &[Option<&str>]) -> Column {
        Series::new(name.into(), values).into()
    }

    fn single_col_df(name: &str, values: &[Option<&str>]) -> DataFrame {
        DataFrame::new(vec![str_series(name, values)]).unwrap()
    }

    fn simple_pii(name: &str, strategy: PiiStrategy) -> PiiConfig {
        PiiConfig {
            columns: vec![PiiColumnConfig {
                name: name.to_string(),
                strategy,
                mask_pattern: None,
                redact_value: None,
                key: None,
            }],
        }
    }

    fn keyed_hash_pii(name: &str, key: &str) -> PiiConfig {
        PiiConfig {
            columns: vec![PiiColumnConfig {
                name: name.to_string(),
                strategy: PiiStrategy::Hash,
                mask_pattern: None,
                redact_value: None,
                key: Some(key.to_string()),
            }],
        }
    }

    fn get_str(df: &DataFrame, col: &str, row: usize) -> Option<String> {
        df.column(col)
            .unwrap()
            .str()
            .unwrap()
            .get(row)
            .map(|s| s.to_string())
    }

    // --- hash ---

    #[test]
    fn hash_produces_64_char_hex() {
        let mut df = single_col_df("email", &[Some("a@b.com")]);
        apply_pii_masking(
            &mut df,
            &simple_pii("email", PiiStrategy::Hash),
            &HashMap::new(),
        )
        .unwrap();
        let v = get_str(&df, "email", 0).unwrap();
        assert_eq!(v.len(), 64, "SHA-256 hex output must be 64 chars");
        assert!(v.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_on_empty_string_produces_sha256() {
        let mut df = single_col_df("email", &[Some("")]);
        apply_pii_masking(
            &mut df,
            &simple_pii("email", PiiStrategy::Hash),
            &HashMap::new(),
        )
        .unwrap();
        // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb924...
        let v = get_str(&df, "email", 0).unwrap();
        assert_eq!(v.len(), 64);
        assert!(v.starts_with("e3b0c442"));
    }

    #[test]
    fn hash_preserves_null() {
        let mut df = single_col_df("email", &[Some("a@b.com"), None]);
        apply_pii_masking(
            &mut df,
            &simple_pii("email", PiiStrategy::Hash),
            &HashMap::new(),
        )
        .unwrap();
        assert!(
            get_str(&df, "email", 1).is_none(),
            "null must stay null after hash"
        );
    }

    // --- redact ---

    #[test]
    fn redact_replaces_with_default_placeholder() {
        let mut df = single_col_df("cc", &[Some("1234-5678-9012-3456")]);
        apply_pii_masking(
            &mut df,
            &simple_pii("cc", PiiStrategy::Redact),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(get_str(&df, "cc", 0).unwrap(), "[REDACTED]");
    }

    #[test]
    fn redact_uses_custom_value() {
        let mut df = single_col_df("cc", &[Some("secret")]);
        let pii = PiiConfig {
            columns: vec![PiiColumnConfig {
                name: "cc".to_string(),
                strategy: PiiStrategy::Redact,
                mask_pattern: None,
                redact_value: Some("[PII]".to_string()),
                key: None,
            }],
        };
        apply_pii_masking(&mut df, &pii, &HashMap::new()).unwrap();
        assert_eq!(get_str(&df, "cc", 0).unwrap(), "[PII]");
    }

    #[test]
    fn redact_on_empty_string_replaces_with_placeholder() {
        let mut df = single_col_df("cc", &[Some("")]);
        apply_pii_masking(
            &mut df,
            &simple_pii("cc", PiiStrategy::Redact),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(get_str(&df, "cc", 0).unwrap(), "[REDACTED]");
    }

    #[test]
    fn redact_preserves_null() {
        let mut df = single_col_df("cc", &[Some("val"), None]);
        apply_pii_masking(
            &mut df,
            &simple_pii("cc", PiiStrategy::Redact),
            &HashMap::new(),
        )
        .unwrap();
        assert!(
            get_str(&df, "cc", 1).is_none(),
            "null must stay null after redact"
        );
    }

    // --- nullify ---

    #[test]
    fn nullify_replaces_all_values_with_null() {
        let mut df = single_col_df("field", &[Some("a"), Some("b"), None]);
        apply_pii_masking(
            &mut df,
            &simple_pii("field", PiiStrategy::Nullify),
            &HashMap::new(),
        )
        .unwrap();
        let col = df.column("field").unwrap();
        assert!(col.get(0).unwrap().is_null());
        assert!(col.get(1).unwrap().is_null());
        assert!(col.get(2).unwrap().is_null());
    }

    // --- drop ---

    #[test]
    fn drop_removes_column_from_dataframe() {
        let mut df = DataFrame::new(vec![
            str_series("keep", &[Some("x")]),
            str_series("pii_col", &[Some("secret")]),
        ])
        .unwrap();
        apply_pii_masking(
            &mut df,
            &simple_pii("pii_col", PiiStrategy::Drop),
            &HashMap::new(),
        )
        .unwrap();
        assert!(
            df.column("pii_col").is_err(),
            "dropped column must not exist"
        );
        assert!(
            df.column("keep").is_ok(),
            "unrelated column must be untouched"
        );
    }

    // --- mask ---

    #[test]
    fn mask_applies_last4_pattern() {
        let mut df = single_col_df("cc", &[Some("1234567890123456")]);
        let pii = PiiConfig {
            columns: vec![PiiColumnConfig {
                name: "cc".to_string(),
                strategy: PiiStrategy::Mask,
                mask_pattern: Some("****{last4}".to_string()),
                redact_value: None,
                key: None,
            }],
        };
        apply_pii_masking(&mut df, &pii, &HashMap::new()).unwrap();
        assert_eq!(get_str(&df, "cc", 0).unwrap(), "****3456");
    }

    #[test]
    fn mask_on_empty_string_keeps_mask_literal() {
        let mut df = single_col_df("cc", &[Some("")]);
        let pii = PiiConfig {
            columns: vec![PiiColumnConfig {
                name: "cc".to_string(),
                strategy: PiiStrategy::Mask,
                mask_pattern: Some("****{last4}".to_string()),
                redact_value: None,
                key: None,
            }],
        };
        apply_pii_masking(&mut df, &pii, &HashMap::new()).unwrap();
        // empty string → no suffix to reveal → {last4} replaced with "" → "****"
        assert_eq!(get_str(&df, "cc", 0).unwrap(), "****");
    }

    #[test]
    fn mask_preserves_null() {
        let mut df = single_col_df("cc", &[Some("1234567890"), None]);
        let pii = PiiConfig {
            columns: vec![PiiColumnConfig {
                name: "cc".to_string(),
                strategy: PiiStrategy::Mask,
                mask_pattern: Some("****{last4}".to_string()),
                redact_value: None,
                key: None,
            }],
        };
        apply_pii_masking(&mut df, &pii, &HashMap::new()).unwrap();
        assert!(
            get_str(&df, "cc", 1).is_none(),
            "null must stay null after mask"
        );
    }

    // --- normalize_columns rename path ---

    #[test]
    fn masking_resolves_schema_to_runtime_name() {
        // Schema declares "Credit Card"; normalize_columns renames it to "credit_card" at runtime.
        let mut df = DataFrame::new(vec![str_series("credit_card", &[Some("secret")])]).unwrap();
        let mut mapping = HashMap::new();
        mapping.insert("Credit Card".to_string(), "credit_card".to_string());
        let pii = PiiConfig {
            columns: vec![PiiColumnConfig {
                name: "Credit Card".to_string(),
                strategy: PiiStrategy::Redact,
                mask_pattern: None,
                redact_value: None,
                key: None,
            }],
        };
        apply_pii_masking(&mut df, &pii, &mapping).unwrap();
        assert_eq!(get_str(&df, "credit_card", 0).unwrap(), "[REDACTED]");
    }

    // --- missing column ---

    #[test]
    fn missing_column_is_skipped_without_error() {
        let mut df = single_col_df("other", &[Some("value")]);
        // "email" is not in the DataFrame (e.g. mismatch=ignore dropped it).
        apply_pii_masking(
            &mut df,
            &simple_pii("email", PiiStrategy::Hash),
            &HashMap::new(),
        )
        .unwrap();
        // Other columns must be untouched.
        assert_eq!(get_str(&df, "other", 0).unwrap(), "value");
    }

    // --- keyed HMAC-SHA256 hash ---

    #[test]
    fn hash_with_literal_key_uses_hmac() {
        let mut df = single_col_df("email", &[Some("alice@example.com")]);
        apply_pii_masking(&mut df, &keyed_hash_pii("email", "secret"), &HashMap::new()).unwrap();
        let v = get_str(&df, "email", 0).unwrap();
        assert_eq!(v.len(), 64, "HMAC-SHA256 hex output must be 64 chars");
        assert!(v.chars().all(|c| c.is_ascii_hexdigit()));
        // Must differ from the plain SHA-256 of the same value.
        let mut df2 = single_col_df("email", &[Some("alice@example.com")]);
        apply_pii_masking(
            &mut df2,
            &simple_pii("email", PiiStrategy::Hash),
            &HashMap::new(),
        )
        .unwrap();
        assert_ne!(
            v,
            get_str(&df2, "email", 0).unwrap(),
            "keyed hash must differ from plain SHA-256"
        );
    }

    #[test]
    fn hash_with_same_key_is_reproducible() {
        let pii = keyed_hash_pii("f", "mykey");
        let mut df1 = single_col_df("f", &[Some("value")]);
        let mut df2 = single_col_df("f", &[Some("value")]);
        apply_pii_masking(&mut df1, &pii, &HashMap::new()).unwrap();
        apply_pii_masking(&mut df2, &pii, &HashMap::new()).unwrap();
        assert_eq!(
            get_str(&df1, "f", 0),
            get_str(&df2, "f", 0),
            "same key and value must produce identical HMAC"
        );
    }

    #[test]
    fn hash_with_different_keys_differ() {
        let mut df1 = single_col_df("f", &[Some("same-value")]);
        let mut df2 = single_col_df("f", &[Some("same-value")]);
        apply_pii_masking(&mut df1, &keyed_hash_pii("f", "key-a"), &HashMap::new()).unwrap();
        apply_pii_masking(&mut df2, &keyed_hash_pii("f", "key-b"), &HashMap::new()).unwrap();
        assert_ne!(
            get_str(&df1, "f", 0),
            get_str(&df2, "f", 0),
            "different keys must produce different HMACs"
        );
    }

    #[test]
    fn hash_with_env_key_resolves_var() {
        std::env::set_var("FLOE_TEST_PII_KEY", "env-secret");
        let mut df1 = single_col_df("f", &[Some("data")]);
        let mut df2 = single_col_df("f", &[Some("data")]);
        apply_pii_masking(
            &mut df1,
            &keyed_hash_pii("f", "${FLOE_TEST_PII_KEY}"),
            &HashMap::new(),
        )
        .unwrap();
        apply_pii_masking(
            &mut df2,
            &keyed_hash_pii("f", "env-secret"),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            get_str(&df1, "f", 0),
            get_str(&df2, "f", 0),
            "${{ENV_VAR}} key must resolve to the same HMAC as the literal key"
        );
        std::env::remove_var("FLOE_TEST_PII_KEY");
    }

    #[test]
    fn resolve_pii_key_rejects_mixed_syntax() {
        let err = super::resolve_pii_key("prefix-${VAR}", "col").unwrap_err();
        assert!(
            err.to_string().contains("single"),
            "error must mention single reference"
        );
    }

    #[test]
    fn resolve_pii_key_rejects_missing_env_var() {
        let err = super::resolve_pii_key("${FLOE_MISSING_PII_KEY_XYZ}", "col").unwrap_err();
        assert!(
            err.to_string().contains("not set"),
            "error must mention not set"
        );
    }

    #[test]
    fn hash_with_key_preserves_null() {
        let mut df = single_col_df("f", &[Some("value"), None]);
        apply_pii_masking(&mut df, &keyed_hash_pii("f", "k"), &HashMap::new()).unwrap();
        assert!(
            get_str(&df, "f", 1).is_none(),
            "null must stay null after keyed hash"
        );
    }
}
