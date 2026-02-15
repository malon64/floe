use floe_core::FloeResult;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn write_manifest(output_path: &str, payload: &str) -> FloeResult<()> {
    if output_path == "-" {
        let mut out = std::io::stdout().lock();
        writeln!(out, "{payload}")?;
        out.flush()?;
        return Ok(());
    }

    write_atomic(Path::new(output_path), payload.as_bytes())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> FloeResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp_path = temp_path(path);
    fs::write(&tmp_path, bytes)?;

    if let Err(rename_err) = fs::rename(&tmp_path, path) {
        if path.exists() {
            fs::remove_file(path)?;
            fs::rename(&tmp_path, path)?;
        } else {
            let _ = fs::remove_file(&tmp_path);
            return Err(Box::new(rename_err));
        }
    }

    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or(0);
    let filename = path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "manifest.json".to_string());
    path.with_file_name(format!(".{filename}.{stamp}.tmp"))
}
