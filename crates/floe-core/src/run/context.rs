use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::io::storage::Target;
use crate::{config, report, FloeResult, RunOptions};

pub struct RunContext {
    pub config: config::RootConfig,
    pub config_path: PathBuf,
    pub config_dir: PathBuf,
    pub storage_resolver: config::StorageResolver,
    pub catalog_resolver: config::CatalogResolver,
    pub report_base_path: Option<String>,
    pub report_target: Option<Target>,
    pub run_id: String,
    pub started_at: String,
    pub run_timer: Instant,
}

impl RunContext {
    pub fn new(
        config_path: &Path,
        config_base: config::ConfigBase,
        options: &RunOptions,
        profile_vars: HashMap<String, String>,
    ) -> FloeResult<Self> {
        let mut config = config::parse_config_with_vars(config_path, &profile_vars)?;
        crate::apply_profile_catalogs(
            &mut config,
            options
                .profile
                .as_ref()
                .and_then(|profile| profile.catalogs.as_ref()),
        );
        crate::apply_profile_storages(
            &mut config,
            options
                .profile
                .as_ref()
                .and_then(|profile| profile.storages.as_ref()),
        );
        crate::apply_profile_lineage(
            &mut config,
            options
                .profile
                .as_ref()
                .and_then(|profile| profile.lineage.as_ref()),
        );
        let storage_resolver = config::StorageResolver::new(&config, config_base)?;
        let catalog_resolver = config::CatalogResolver::new(&config)?;
        let config_dir =
            crate::io::storage::paths::normalize_local_path(storage_resolver.config_dir());
        let config_path = crate::io::storage::paths::normalize_local_path(config_path);
        let (report_target, report_base_path) = match config.report.as_ref() {
            Some(report) => {
                let resolved = storage_resolver
                    .resolve_report_path(report.storage.as_deref(), &report.path)?;
                let target = Target::from_resolved(&resolved)?;
                let base_path = match resolved.local_path.as_ref() {
                    Some(path) => path.display().to_string(),
                    None => resolved.uri.clone(),
                };
                (Some(target), Some(base_path))
            }
            None => (None, None),
        };
        let started_at = report::now_rfc3339();
        let run_id = options
            .run_id
            .clone()
            .unwrap_or_else(|| report::run_id_from_timestamp(&started_at));

        Ok(Self {
            config,
            config_path,
            config_dir,
            storage_resolver,
            catalog_resolver,
            report_base_path,
            report_target,
            run_id,
            started_at,
            run_timer: Instant::now(),
        })
    }

    /// Build a RunContext from a pre-parsed RootConfig (manifest mode).
    /// `manifest_path` is used for the config_path field (reporting only).
    pub fn from_config(
        config: config::RootConfig,
        config_base: config::ConfigBase,
        manifest_path: &Path,
        report_base_uri: &str,
        options: &RunOptions,
    ) -> FloeResult<Self> {
        let storage_resolver = config::StorageResolver::new(&config, config_base)?;
        let catalog_resolver = config::CatalogResolver::new(&config)?;
        let config_dir =
            crate::io::storage::paths::normalize_local_path(storage_resolver.config_dir());
        let config_path = crate::io::storage::paths::normalize_local_path(manifest_path);

        // The manifest embeds report_base_uri; resolve it to a target if it looks local.
        let (report_target, report_base_path) = if !report_base_uri.is_empty()
            && report_base_uri != "report"
        {
            let local_path = if let Some(stripped) = report_base_uri.strip_prefix("local://") {
                Some(std::path::PathBuf::from(stripped))
            } else if !report_base_uri.contains("://") {
                Some(std::path::PathBuf::from(report_base_uri))
            } else {
                None
            };
            let base_path = local_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| report_base_uri.to_string());
            // Only attempt to create a Target for local paths; skip for remote URIs.
            let report_target = local_path.as_ref().and_then(|path| {
                let resolved = config::ResolvedPath {
                    storage: "local".to_string(),
                    uri: report_base_uri.to_string(),
                    local_path: Some(path.clone()),
                };
                Target::from_resolved(&resolved).ok()
            });
            (report_target, Some(base_path))
        } else {
            (None, None)
        };

        let started_at = report::now_rfc3339();
        let run_id = options
            .run_id
            .clone()
            .unwrap_or_else(|| report::run_id_from_timestamp(&started_at));

        Ok(Self {
            config,
            config_path,
            config_dir,
            storage_resolver,
            catalog_resolver,
            report_base_path,
            report_target,
            run_id,
            started_at,
            run_timer: Instant::now(),
        })
    }
}
