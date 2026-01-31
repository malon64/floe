use std::path::{Path, PathBuf};

use crate::io::storage::{paths, CloudClient, Target};
use crate::{config, report, FloeResult};

pub fn write_entity_report(
    target: &Target,
    run_id: &str,
    entity: &config::EntityConfig,
    report: &report::RunReport,
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
) -> FloeResult<String> {
    let relative = report::ReportWriter::report_relative_path(run_id, &entity.name);
    write_report_json(target, &relative, report, cloud, resolver, &entity.name)
}

pub fn write_summary_report(
    target: &Target,
    run_id: &str,
    report: &report::RunSummaryReport,
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
) -> FloeResult<String> {
    let relative = report::ReportWriter::summary_relative_path(run_id);
    write_report_json(target, &relative, report, cloud, resolver, "report")
}

fn write_report_json<T: serde::Serialize>(
    target: &Target,
    relative: &str,
    report: &T,
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
    context: &str,
) -> FloeResult<String> {
    let json = serde_json::to_string_pretty(report)?;
    match target {
        Target::Local { base_path, .. } => {
            let output_path = paths::resolve_output_dir_path(base_path, relative);
            write_json_file(&output_path, &json)?;
            Ok(output_path.display().to_string())
        }
        _ => {
            let uri = target.join_relative(relative);
            let temp_dir = tempfile::tempdir()?;
            let filename = Path::new(relative)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("report.json");
            let temp_path = temp_dir.path().join(filename);
            write_json_file(&temp_path, &json)?;
            let client = cloud.client_for_context(resolver, target.storage(), context)?;
            client.upload_from_path(&temp_path, &uri)?;
            Ok(uri)
        }
    }
}

fn write_json_file(path: &Path, json: &str) -> FloeResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = temp_path(path);
    let mut file = std::fs::File::create(&tmp_path)?;
    use std::io::Write;
    file.write_all(json.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("report.json");
    let tmp_name = format!("{file_name}.tmp-{}", unique_suffix());
    path.parent().unwrap_or(path).join(tmp_name)
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{}-{}", std::process::id(), nanos)
}
