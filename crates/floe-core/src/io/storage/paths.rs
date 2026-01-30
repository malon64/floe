use std::path::{Path, PathBuf};

pub fn build_output_filename(stem: &str, suffix: &str, extension: &str) -> String {
    let ext = extension.trim_start_matches('.');
    if suffix.is_empty() {
        format!("{stem}.{ext}")
    } else {
        format!("{stem}{suffix}.{ext}")
    }
}

pub fn build_part_stem(index: usize) -> String {
    format!("part-{index:05}")
}

pub fn resolve_output_path(base_path: &str, filename: &str) -> PathBuf {
    let base = Path::new(base_path);
    if base.extension().is_some() {
        base.to_path_buf()
    } else if base.as_os_str().is_empty() {
        PathBuf::from(filename)
    } else {
        base.join(filename)
    }
}

pub fn resolve_output_dir_path(base_path: &str, filename: &str) -> PathBuf {
    let base = Path::new(base_path);
    if base.as_os_str().is_empty() {
        PathBuf::from(filename)
    } else {
        base.join(filename)
    }
}

pub fn resolve_sibling_path(base_path: &str, filename: &str) -> PathBuf {
    let base = Path::new(base_path);
    let dir = if base.extension().is_some() {
        base.parent().unwrap_or(base)
    } else if base.as_os_str().is_empty() {
        Path::new("")
    } else {
        base
    };
    dir.join(filename)
}

pub fn resolve_output_key(base_key: &str, filename: &str) -> String {
    let base = normalize_key(base_key);
    if Path::new(&base).extension().is_some() {
        base
    } else if base.is_empty() {
        filename.to_string()
    } else {
        format!("{base}/{filename}")
    }
}

pub fn resolve_output_dir_key(base_key: &str, filename: &str) -> String {
    let base = normalize_key(base_key);
    if base.is_empty() {
        filename.to_string()
    } else {
        format!("{base}/{filename}")
    }
}

pub fn resolve_sibling_key(base_key: &str, filename: &str) -> String {
    let base = normalize_key(base_key);
    let dir = if Path::new(&base).extension().is_some() {
        parent_key(&base)
    } else {
        base
    };
    if dir.is_empty() {
        filename.to_string()
    } else {
        format!("{dir}/{filename}")
    }
}

fn normalize_key(base_key: &str) -> String {
    base_key.trim_matches('/').to_string()
}

fn parent_key(base: &str) -> String {
    match base.rsplit_once('/') {
        Some((parent, _)) => parent.to_string(),
        None => base.to_string(),
    }
}
