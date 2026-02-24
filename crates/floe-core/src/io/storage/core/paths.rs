use std::path::{Component, Path, PathBuf};

const MAX_FILENAME_COMPONENT_BYTES: usize = 255;
const MAX_ARCHIVE_RUN_COMPONENT_BYTES: usize = 48;

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

pub fn normalize_local_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let can_pop = normalized
                    .components()
                    .next_back()
                    .is_some_and(|tail| matches!(tail, Component::Normal(_)));
                if can_pop {
                    normalized.pop();
                } else if !path.is_absolute() {
                    normalized.push("..");
                }
            }
            Component::Normal(segment) => normalized.push(segment),
        }
    }

    if normalized.as_os_str().is_empty() {
        if path.is_absolute() {
            PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
        } else {
            PathBuf::from(".")
        }
    } else {
        normalized
    }
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

pub fn archive_relative_path(entity: &str, filename: &str) -> String {
    let name = Path::new(filename)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(filename);
    let entity = entity.trim_matches('/');
    if entity.is_empty() {
        name.to_string()
    } else {
        format!("{entity}/{name}")
    }
}

pub fn archive_relative_path_for_run(
    entity: &str,
    filename: &str,
    run_id: &str,
    source_uri: &str,
) -> String {
    let name = archive_filename_for_run(filename, run_id, source_uri);
    let entity = entity.trim_matches('/');
    if entity.is_empty() {
        name
    } else {
        format!("{entity}/{name}")
    }
}

pub fn archive_filename_for_run(filename: &str, run_id: &str, source_uri: &str) -> String {
    let original_name = Path::new(filename)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(filename);
    let path = Path::new(original_name);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(original_name);
    let ext = path.extension().and_then(|value| value.to_str());
    let run_component = compact_archive_run_component(run_id);
    let source_hash = short_stable_hash_hex(source_uri);
    let suffix = format!("__run-{run_component}__src-{source_hash}");
    let extension_suffix = match ext {
        Some(ext) if !ext.is_empty() => format!(".{ext}"),
        _ => String::new(),
    };
    let stem_source = if extension_suffix.is_empty() {
        original_name
    } else {
        stem
    };

    let reserved_bytes = suffix.len() + extension_suffix.len();
    let available_stem_bytes = MAX_FILENAME_COMPONENT_BYTES.saturating_sub(reserved_bytes);
    let stem = truncate_utf8_to_bytes(stem_source, available_stem_bytes);

    format!("{stem}{suffix}{extension_suffix}")
}

pub fn resolve_archive_path(base_path: &str, entity: &str, filename: &str) -> PathBuf {
    let relative = archive_relative_path(entity, filename);
    resolve_output_dir_path(base_path, &relative)
}

pub fn resolve_archive_path_for_run(
    base_path: &str,
    entity: &str,
    filename: &str,
    run_id: &str,
    source_uri: &str,
) -> PathBuf {
    let relative = archive_relative_path_for_run(entity, filename, run_id, source_uri);
    resolve_output_dir_path(base_path, &relative)
}

pub fn resolve_archive_key(base_key: &str, entity: &str, filename: &str) -> String {
    let relative = archive_relative_path(entity, filename);
    resolve_output_dir_key(base_key, &relative)
}

pub fn resolve_archive_key_for_run(
    base_key: &str,
    entity: &str,
    filename: &str,
    run_id: &str,
    source_uri: &str,
) -> String {
    let relative = archive_relative_path_for_run(entity, filename, run_id, source_uri);
    resolve_output_dir_key(base_key, &relative)
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

fn sanitize_archive_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if sanitized.is_empty() {
        "run".to_string()
    } else {
        sanitized
    }
}

fn compact_archive_run_component(run_id: &str) -> String {
    let sanitized = sanitize_archive_component(run_id);
    if sanitized.len() <= MAX_ARCHIVE_RUN_COMPONENT_BYTES {
        return sanitized;
    }
    let truncated = truncate_utf8_to_bytes(&sanitized, MAX_ARCHIVE_RUN_COMPONENT_BYTES);
    let run_hash = &short_stable_hash_hex(run_id)[..8];
    format!("{truncated}_{run_hash}")
}

fn truncate_utf8_to_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    if max_bytes == 0 {
        return String::new();
    }
    let mut end = 0;
    for (idx, ch) in value.char_indices() {
        let next = idx + ch.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    if end == 0 {
        String::new()
    } else {
        value[..end].to_string()
    }
}

fn short_stable_hash_hex(value: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}
