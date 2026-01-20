use std::env;
use std::process::Command;

fn main() {
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    let sha = env::var("GITHUB_SHA")
        .or_else(|_| env::var("VERGEN_GIT_SHA"))
        .ok()
        .and_then(trim_sha)
        .or_else(git_sha);

    if let Some(short_sha) = sha {
        println!("cargo:rustc-env=FLOE_VERSION={} ({})", version, short_sha);
    } else {
        println!("cargo:rustc-env=FLOE_VERSION={}", version);
    }
}

fn trim_sha(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(7).collect())
    }
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout);
    trim_sha(sha.to_string())
}
