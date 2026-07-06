use std::path::{Path, PathBuf};

/// Default container work-root. Matches `WORKDIR /work` in every Floe Dockerfile
/// (`Dockerfile`, `Dockerfile.duckdb`, `Dockerfile.release`).
pub const DEFAULT_WORK_ROOT: &str = "/work";

/// The runtime environment a generated manifest targets. Controls whether local
/// `config_uri` / `profile_uri` (and the `manifest_id` derived from them) are recorded
/// as absolute paths under a fixed container work-root, or kept relative / as-typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeEnv {
    /// Container image: local paths are recorded absolute under the work-root (`/work`),
    /// which is both portable for remote replay and reproducible across container runs
    /// (every container uses the same `/work` mount).
    Image,
    /// Local CLI / dev checkout: local paths stay relative / as-typed, so the same relative
    /// path produces the same `manifest_id` regardless of where the checkout physically lives.
    #[default]
    Cli,
}

impl RuntimeEnv {
    /// Detect the runtime from the environment when the CLI caller did not pass `--runtime`.
    ///
    /// Precedence:
    /// 1. `FLOE_RUNTIME=image|cli` — explicit override.
    /// 2. Container heuristic: `/.dockerenv` exists, or the current dir is the `/work` mount.
    /// 3. Otherwise `Cli`.
    pub fn detect() -> Self {
        if let Ok(value) = std::env::var("FLOE_RUNTIME") {
            match value.trim().to_ascii_lowercase().as_str() {
                "image" => return RuntimeEnv::Image,
                "cli" => return RuntimeEnv::Cli,
                _ => {}
            }
        }
        if Path::new("/.dockerenv").exists() {
            return RuntimeEnv::Image;
        }
        if std::env::current_dir()
            .ok()
            .is_some_and(|dir| dir == Path::new(DEFAULT_WORK_ROOT))
        {
            return RuntimeEnv::Image;
        }
        RuntimeEnv::Cli
    }

    /// String recorded as `runtime_env` in the manifest.
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeEnv::Image => "image",
            RuntimeEnv::Cli => "cli",
        }
    }

    /// Parse the manifest-recorded `runtime_env` string. Unknown or missing maps to `Cli`,
    /// which is backward-compatible: manifests generated before this field existed carry
    /// relative local URIs and must keep resolving as before.
    pub fn from_manifest_str(value: Option<&str>) -> Self {
        match value {
            Some(v) if v.eq_ignore_ascii_case("image") => RuntimeEnv::Image,
            _ => RuntimeEnv::Cli,
        }
    }

    /// Work-root that local paths are recorded relative to (at generation) and resolved
    /// under (at replay). `Image` → `/work`, overridable via `FLOE_WORK_ROOT`;
    /// `Cli` → the current working directory.
    pub fn work_root(self) -> PathBuf {
        match self {
            RuntimeEnv::Image => std::env::var("FLOE_WORK_ROOT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_WORK_ROOT)),
            RuntimeEnv::Cli => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        }
    }

    /// The `work_root` value recorded in the manifest: `Some("/work")` for `Image`
    /// (so replay resolves against the same root), `None` for `Cli` (paths are relative,
    /// there is no fixed root to record).
    pub fn manifest_work_root(self) -> Option<String> {
        match self {
            RuntimeEnv::Image => Some(self.work_root().to_string_lossy().replace('\\', "/")),
            RuntimeEnv::Cli => None,
        }
    }
}

/// Given the as-typed `local://…` URI recorded for a local config/profile (or a remote URI,
/// passed through unchanged), return the form appropriate for `env`.
///
/// `Image` rewrites a *relative* local path to an absolute one under the work-root
/// (`local://domains/x.yml` → `local:///work/domains/x.yml`), restoring the pre-0.6.6
/// portable form while staying reproducible (every container uses `/work`). An already
/// absolute local path (leading `/`, or a Windows drive/UNC prefix) and any remote URI are
/// returned unchanged.
pub fn local_uri_for_env(uri: &str, env: RuntimeEnv) -> String {
    let Some(rel) = uri.strip_prefix("local://") else {
        // Remote URIs (s3://, gs://, abfs://) are already environment-independent.
        return uri.to_string();
    };
    if env == RuntimeEnv::Cli || rel.starts_with('/') || has_drive_prefix(rel) {
        return uri.to_string();
    }
    let root = env.work_root();
    let root = root.to_string_lossy().replace('\\', "/");
    let root = root.trim_end_matches('/');
    format!("local://{root}/{rel}")
}

/// True for a Windows drive prefix such as `C:` at the start of `path`.
fn has_drive_prefix(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_keeps_relative_local_uri_as_typed() {
        assert_eq!(
            local_uri_for_env("local://domains/orders.yml", RuntimeEnv::Cli),
            "local://domains/orders.yml"
        );
    }

    #[test]
    fn image_absolutizes_relative_local_uri_under_work_root() {
        // Default work-root is `/work` (no FLOE_WORK_ROOT set in the test env).
        assert_eq!(
            local_uri_for_env("local://domains/orders.yml", RuntimeEnv::Image),
            "local:///work/domains/orders.yml"
        );
    }

    #[test]
    fn image_leaves_absolute_local_uri_untouched() {
        assert_eq!(
            local_uri_for_env("local:///abs/orders.yml", RuntimeEnv::Image),
            "local:///abs/orders.yml"
        );
    }

    #[test]
    fn remote_uris_pass_through_in_both_modes() {
        for env in [RuntimeEnv::Cli, RuntimeEnv::Image] {
            assert_eq!(
                local_uri_for_env("s3://bucket/orders.yml", env),
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
}
