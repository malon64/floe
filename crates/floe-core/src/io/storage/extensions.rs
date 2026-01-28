use crate::{ConfigError, FloeResult};

pub fn suffixes_for_format(format: &str) -> FloeResult<Vec<String>> {
    match format {
        "csv" => Ok(vec![".csv".to_string()]),
        "parquet" => Ok(vec![".parquet".to_string()]),
        "json" => Ok(vec![
            ".json".to_string(),
            ".jsonl".to_string(),
            ".ndjson".to_string(),
            ".djson".to_string(),
        ]),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported source format for input resolution: {format}"
        )))),
    }
}

pub fn glob_patterns_for_format(format: &str) -> FloeResult<Vec<String>> {
    let suffixes = suffixes_for_format(format)?;
    Ok(suffixes
        .iter()
        .map(|suffix| glob_for_suffix(suffix))
        .collect())
}

fn glob_for_suffix(suffix: &str) -> String {
    let mut out = String::from("*");
    for ch in suffix.chars() {
        if ch.is_ascii_alphabetic() {
            out.push('[');
            out.push(ch.to_ascii_lowercase());
            out.push(ch.to_ascii_uppercase());
            out.push(']');
        } else {
            out.push(ch);
        }
    }
    out
}
