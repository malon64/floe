use floe_core::resolve_config_location;

fn write_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("config.yml");
    std::fs::write(&path, "version: \"0.1\"\nentities: []\n").expect("write config");
    path
}

#[test]
fn local_config_uri_uses_local_scheme() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let cfg = write_config(tmp.path());
    let loc = resolve_config_location(cfg.to_str().expect("utf8")).expect("resolve");
    assert!(loc.uri.starts_with("local://"), "uri: {}", loc.uri);
    assert!(loc.uri.ends_with("/config.yml"), "uri: {}", loc.uri);
}

// issue #438: the path is preserved lexically, not canonicalized. A `.` is dropped
// and a `..` bounce survives (canonicalize would collapse it), and separators are
// normalized to `/`.
#[test]
fn local_config_uri_is_lexically_normalized() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let cfg_dir = tmp.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    write_config(&cfg_dir);

    let bouncy = format!("{}/./../cfg/config.yml", cfg_dir.display());
    let loc = resolve_config_location(&bouncy).expect("resolve");
    assert!(loc.uri.contains("/../cfg/config.yml"), "uri: {}", loc.uri);
    assert!(!loc.uri.contains("/./"), "uri: {}", loc.uri);
}

// A Windows drive path must yield `local://C:/...`, not `local://C://...` (which the
// `local://` prefix would turn into a scheme-colliding, invalid URI). Windows-only
// because drive prefixes are only parsed on Windows targets.
#[cfg(windows)]
#[test]
fn windows_drive_config_uri_has_single_slash_after_drive() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let cfg = write_config(tmp.path());
    let loc = resolve_config_location(cfg.to_str().expect("utf8")).expect("resolve");
    assert!(loc.uri.starts_with("local://"), "uri: {}", loc.uri);
    assert!(!loc.uri.contains("C://"), "uri: {}", loc.uri);
}
