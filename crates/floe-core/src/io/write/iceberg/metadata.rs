use std::fs;
use std::path::{Path, PathBuf};

use crate::io::storage::ObjectRef;
use crate::{io, FloeResult};

pub(crate) fn latest_local_metadata_location(table_root: &Path) -> FloeResult<Option<String>> {
    let metadata_dir = table_root.join("metadata");
    if !metadata_dir.exists() {
        return Ok(None);
    }

    let mut best: Option<(i64, PathBuf)> = None;
    for entry in fs::read_dir(&metadata_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_path)) => {
                version > *best_version || (version == *best_version && path > *best_path)
            }
        };
        if replace {
            best = Some((version, path));
        }
    }

    Ok(best.map(|(_, path)| path.display().to_string()))
}

pub(crate) fn latest_s3_metadata_location(
    client: &mut dyn io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

pub(crate) fn latest_gcs_metadata_location(
    client: &mut dyn io::storage::StorageClient,
    base_key: &str,
) -> FloeResult<Option<String>> {
    let metadata_prefix = if base_key.trim_matches('/').is_empty() {
        "metadata/".to_string()
    } else {
        format!("{}/metadata/", base_key.trim_matches('/'))
    };
    let listed = client.list(&metadata_prefix)?;
    latest_metadata_location_from_objects(listed)
}

fn latest_metadata_location_from_objects(objects: Vec<ObjectRef>) -> FloeResult<Option<String>> {
    let mut best: Option<(i64, String, String)> = None;
    for object in objects {
        let file_name = object
            .key
            .rsplit('/')
            .next()
            .unwrap_or(object.key.as_str())
            .to_string();
        if !file_name.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version_from_filename(&file_name) else {
            continue;
        };
        let replace = match &best {
            None => true,
            Some((best_version, best_key, _)) => {
                version > *best_version || (version == *best_version && object.key > *best_key)
            }
        };
        if replace {
            best = Some((version, object.key.clone(), object.uri.clone()));
        }
    }
    Ok(best.map(|(_, _, uri)| uri))
}

pub(super) fn parse_metadata_version_from_location(location: &str) -> Option<i64> {
    let file_name = Path::new(location).file_name()?.to_str()?;
    parse_metadata_version_from_filename(file_name)
}

pub(super) fn parse_metadata_version_from_filename(file_name: &str) -> Option<i64> {
    let prefix = file_name.split_once('-')?.0;
    prefix.parse::<i64>().ok()
}
