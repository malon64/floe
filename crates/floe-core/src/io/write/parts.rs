use std::path::{Path, PathBuf};

use crate::{io, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartFile {
    pub path: PathBuf,
    pub file_name: String,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct PartNameAllocator {
    next_index: usize,
    extension: String,
}

impl PartNameAllocator {
    pub fn from_local_path(base_path: &Path, extension: &str) -> FloeResult<Self> {
        Ok(Self {
            next_index: next_local_part_index(base_path, extension)?,
            extension: normalize_extension(extension),
        })
    }

    pub fn from_next_index(next_index: usize, extension: &str) -> Self {
        Self {
            next_index,
            extension: normalize_extension(extension),
        }
    }

    pub fn allocate_next(&mut self) -> String {
        let file_name = part_filename(self.next_index, &self.extension);
        self.next_index += 1;
        file_name
    }
}

pub fn part_filename(index: usize, extension: &str) -> String {
    let extension = normalize_extension(extension);
    let stem = io::storage::paths::build_part_stem(index);
    io::storage::paths::build_output_filename(&stem, "", &extension)
}

pub fn list_local_part_files(base_path: &Path, extension: &str) -> FloeResult<Vec<PartFile>> {
    if base_path.as_os_str().is_empty() || !base_path.exists() || base_path.is_file() {
        return Ok(Vec::new());
    }

    let extension = normalize_extension(extension);
    let mut parts = Vec::new();
    for entry in std::fs::read_dir(base_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(index) = parse_part_index(file_name, &extension) else {
            continue;
        };
        parts.push(PartFile {
            path: entry.path(),
            file_name: file_name.to_string(),
            index,
        });
    }
    parts.sort_by(|left, right| {
        left.index
            .cmp(&right.index)
            .then_with(|| left.file_name.cmp(&right.file_name))
    });
    Ok(parts)
}

pub fn next_local_part_index(base_path: &Path, extension: &str) -> FloeResult<usize> {
    let part_files = list_local_part_files(base_path, extension)?;
    Ok(part_files.last().map(|part| part.index + 1).unwrap_or(0))
}

pub fn next_local_part_filename(base_path: &Path, extension: &str) -> FloeResult<String> {
    let next_index = next_local_part_index(base_path, extension)?;
    Ok(part_filename(next_index, extension))
}

pub fn clear_local_part_files(base_path: &Path, extension: &str) -> FloeResult<usize> {
    if base_path.as_os_str().is_empty() || !base_path.exists() {
        return Ok(0);
    }
    if base_path.is_file() {
        std::fs::remove_file(base_path)?;
        std::fs::create_dir_all(base_path)?;
        return Ok(0);
    }

    let part_files = list_local_part_files(base_path, extension)?;
    for part_file in &part_files {
        std::fs::remove_file(&part_file.path)?;
    }
    Ok(part_files.len())
}

fn parse_part_index(file_name: &str, extension: &str) -> Option<usize> {
    let path = Path::new(file_name);
    if path.extension()?.to_str()? != extension {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let digits = stem.strip_prefix("part-")?;
    if digits.len() < 5 || !digits.bytes().all(|value| value.is_ascii_digit()) {
        return None;
    }
    digits.parse::<usize>().ok()
}

fn normalize_extension(extension: &str) -> String {
    extension.trim_start_matches('.').to_string()
}
