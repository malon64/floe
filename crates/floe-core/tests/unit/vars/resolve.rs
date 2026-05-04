use std::collections::HashMap;

use floe_core::{resolve_vars, VarSources};

// Helper: build a map from key=value pairs.
fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn empty() -> HashMap<String, String> {
    HashMap::new()
}

// ---------------------------------------------------------------------------
// Precedence
// ---------------------------------------------------------------------------

#[test]
fn profile_vars_are_used_when_no_override() {
    let profile = vars(&[("CATALOG", "profile_catalog")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["CATALOG"], "profile_catalog");
}

#[test]
fn cli_overrides_profile() {
    let profile = vars(&[("CATALOG", "profile_catalog")]);
    let cli = vars(&[("CATALOG", "cli_catalog")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &cli,
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["CATALOG"], "cli_catalog");
}

#[test]
fn config_overrides_cli_and_profile() {
    let profile = vars(&[("CATALOG", "profile_catalog")]);
    let cli = vars(&[("CATALOG", "cli_catalog")]);
    let config = vars(&[("CATALOG", "config_catalog")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &cli,
        config: &config,
    })
    .expect("resolve");
    assert_eq!(result["CATALOG"], "config_catalog");
}

#[test]
fn precedence_full_chain() {
    // KEY present in all three sources; only config value should survive.
    // EXTRA only in profile; should be present unchanged.
    let profile = vars(&[("KEY", "from_profile"), ("EXTRA", "from_profile_extra")]);
    let cli = vars(&[("KEY", "from_cli")]);
    let config = vars(&[("KEY", "from_config")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &cli,
        config: &config,
    })
    .expect("resolve");
    assert_eq!(result["KEY"], "from_config");
    assert_eq!(result["EXTRA"], "from_profile_extra");
}

// ---------------------------------------------------------------------------
// Nested / chained resolution
// ---------------------------------------------------------------------------

#[test]
fn simple_reference_resolved() {
    // A = ${B}, B = "hello" → A should expand to "hello".
    let profile = vars(&[("A", "${B}"), ("B", "hello")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["A"], "hello");
    assert_eq!(result["B"], "hello");
}

#[test]
fn nested_three_levels() {
    // C = ${B}, B = ${A}, A = "base"
    let profile = vars(&[("A", "base"), ("B", "${A}_mid"), ("C", "${B}_top")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["A"], "base");
    assert_eq!(result["B"], "base_mid");
    assert_eq!(result["C"], "base_mid_top");
}

#[test]
fn reference_in_path_interpolation() {
    // Common pattern: path composed from multiple vars.
    let profile = vars(&[
        ("BASE", "/data"),
        ("ENV", "prod"),
        ("OUTPUT", "${BASE}/${ENV}/output"),
    ]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["OUTPUT"], "/data/prod/output");
}

#[test]
fn cross_source_reference_resolved() {
    // Profile defines BASE; config defines PATH that references BASE from profile.
    let profile = vars(&[("BASE", "/mnt/data")]);
    let config = vars(&[("PATH", "${BASE}/output")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &config,
    })
    .expect("resolve");
    assert_eq!(result["PATH"], "/mnt/data/output");
}

#[test]
fn no_placeholders_passes_through() {
    let profile = vars(&[("CATALOG", "my_catalog"), ("SCHEMA", "bronze")]);
    let result = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert_eq!(result["CATALOG"], "my_catalog");
    assert_eq!(result["SCHEMA"], "bronze");
}

#[test]
fn all_sources_empty_returns_empty_map() {
    let result = resolve_vars(VarSources {
        profile: &empty(),
        cli: &empty(),
        config: &empty(),
    })
    .expect("resolve");
    assert!(result.is_empty());
}

// ---------------------------------------------------------------------------
// Unresolved variable — fail-fast
// ---------------------------------------------------------------------------

#[test]
fn unresolved_reference_fails() {
    let profile = vars(&[("PATH", "${UNDEFINED_VAR}")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("UNDEFINED_VAR"),
        "error should name the missing variable; got: {msg}"
    );
}

#[test]
fn unresolved_in_interpolated_path_fails() {
    let profile = vars(&[("OUT", "/data/${MISSING}/output")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    assert!(err.to_string().contains("MISSING"), "got: {err}");
}

#[test]
fn unclosed_placeholder_fails() {
    let profile = vars(&[("VAR", "${UNCLOSED")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    assert!(err.to_string().contains("unclosed"), "got: {err}");
}

#[test]
fn empty_placeholder_fails() {
    let profile = vars(&[("VAR", "${}")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    assert!(err.to_string().contains("empty"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Circular reference detection
// ---------------------------------------------------------------------------

#[test]
fn direct_cycle_detected() {
    // A = ${A}
    let profile = vars(&[("A", "${A}")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    assert!(
        err.to_string().contains("circular") || err.to_string().contains("cycle"),
        "expected cycle error; got: {err}"
    );
}

#[test]
fn cycle_error_includes_chain() {
    let profile = vars(&[("X", "${Y}"), ("Y", "${X}")]);
    let err = resolve_vars(VarSources {
        profile: &profile,
        cli: &empty(),
        config: &empty(),
    })
    .unwrap_err();
    let msg = err.to_string();
    // The error should name at least one variable involved in the cycle.
    assert!(
        msg.contains('X') || msg.contains('Y'),
        "error should identify cycle participants; got: {msg}"
    );
}
