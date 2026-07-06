use floe_core::{local_uri_for_env, RuntimeEnv};
use std::path::Path;

#[test]
fn cli_records_host_absolute_canonical_path() {
    // `Cli` ignores the (relative) as-typed URI and records the canonicalized
    // host-absolute path, so the manifest is resolvable on the generating host.
    assert_eq!(
        local_uri_for_env(
            "local://domains/orders.yml",
            Path::new("/home/u/repo/domains/orders.yml"),
            RuntimeEnv::Cli,
        ),
        "local:///home/u/repo/domains/orders.yml"
    );
}

#[test]
fn image_absolutizes_relative_local_uri_under_work_root() {
    // Default work-root is `/work` (no FLOE_WORK_ROOT set in the test env). `Image` uses
    // the as-typed relative path and ignores the host-canonical path entirely.
    assert_eq!(
        local_uri_for_env(
            "local://domains/orders.yml",
            Path::new("/home/u/repo/domains/orders.yml"),
            RuntimeEnv::Image,
        ),
        "local:///work/domains/orders.yml"
    );
}

#[test]
fn already_absolute_local_uri_untouched_in_both_modes() {
    for env in [RuntimeEnv::Cli, RuntimeEnv::Image] {
        assert_eq!(
            local_uri_for_env("local:///abs/orders.yml", Path::new("/abs/orders.yml"), env,),
            "local:///abs/orders.yml"
        );
    }
}

#[test]
fn remote_uris_pass_through_in_both_modes() {
    for env in [RuntimeEnv::Cli, RuntimeEnv::Image] {
        assert_eq!(
            local_uri_for_env("s3://bucket/orders.yml", Path::new("/tmp/orders.yml"), env,),
            "s3://bucket/orders.yml"
        );
    }
}

#[test]
fn from_manifest_str_maps_image_case_insensitively_else_cli() {
    assert_eq!(
        RuntimeEnv::from_manifest_str(Some("image")),
        RuntimeEnv::Image
    );
    assert_eq!(
        RuntimeEnv::from_manifest_str(Some("IMAGE")),
        RuntimeEnv::Image
    );
    assert_eq!(RuntimeEnv::from_manifest_str(Some("cli")), RuntimeEnv::Cli);
    assert_eq!(RuntimeEnv::from_manifest_str(None), RuntimeEnv::Cli);
    assert_eq!(
        RuntimeEnv::from_manifest_str(Some("weird")),
        RuntimeEnv::Cli
    );
}

#[test]
fn manifest_work_root_recorded_only_for_image() {
    assert_eq!(
        RuntimeEnv::Image.manifest_work_root().as_deref(),
        Some("/work")
    );
    assert_eq!(RuntimeEnv::Cli.manifest_work_root(), None);
}
