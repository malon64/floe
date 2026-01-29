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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_output_filename_includes_extension() {
        assert_eq!(build_output_filename("file", "", "parquet"), "file.parquet");
        assert_eq!(
            build_output_filename("file", "_rejected", ".csv"),
            "file_rejected.csv"
        );
    }

    #[test]
    fn build_part_stem_zero_pads() {
        assert_eq!(build_part_stem(0), "part-00000");
        assert_eq!(build_part_stem(12), "part-00012");
    }

    #[test]
    fn resolve_output_key_respects_file_base() {
        assert_eq!(
            resolve_output_key("out/file.parquet", "ignored.parquet"),
            "out/file.parquet"
        );
        assert_eq!(
            resolve_output_key("out", "file.parquet"),
            "out/file.parquet"
        );
        assert_eq!(resolve_output_key("", "file.parquet"), "file.parquet");
    }

    #[test]
    fn resolve_output_dir_path_treats_base_as_directory() {
        assert_eq!(
            resolve_output_dir_path("out/file.parquet", "part-00000.parquet"),
            PathBuf::from("out/file.parquet/part-00000.parquet")
        );
        assert_eq!(
            resolve_output_dir_path("out", "part-00000.parquet"),
            PathBuf::from("out/part-00000.parquet")
        );
        assert_eq!(
            resolve_output_dir_path("", "part-00000.parquet"),
            PathBuf::from("part-00000.parquet")
        );
    }

    #[test]
    fn resolve_output_dir_key_treats_base_as_directory() {
        assert_eq!(
            resolve_output_dir_key("out/file.parquet", "part-00000.parquet"),
            "out/file.parquet/part-00000.parquet"
        );
        assert_eq!(
            resolve_output_dir_key("out", "part-00000.parquet"),
            "out/part-00000.parquet"
        );
        assert_eq!(
            resolve_output_dir_key("", "part-00000.parquet"),
            "part-00000.parquet"
        );
    }

    #[test]
    fn resolve_sibling_key_uses_parent_for_file_base() {
        assert_eq!(
            resolve_sibling_key("out/errors.csv", "file_reject_errors.json"),
            "out/file_reject_errors.json"
        );
        assert_eq!(
            resolve_sibling_key("out", "file_reject_errors.json"),
            "out/file_reject_errors.json"
        );
    }
}
