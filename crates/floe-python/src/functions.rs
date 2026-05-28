use std::collections::HashMap;
use std::path::{Path, PathBuf};

use floe_core::config::ConfigBase;
use floe_core::{ProfileConfig, RunOptions, ValidateOptions};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::types::config::PyRootConfig;
use crate::types::errors::{to_py_err, FloeConfigError, FloeError};
use crate::types::outcome::PyRunOutcome;

fn load_optional_profile(
    profile_path: Option<String>,
    profile_vars: Option<HashMap<String, String>>,
) -> PyResult<Option<ProfileConfig>> {
    match profile_path {
        Some(path_str) => {
            let path = PathBuf::from(&path_str);
            let mut profile = floe_core::validate_profile_file(&path).map_err(|e| {
                FloeConfigError::new_err(format!("failed to load profile {path_str:?}: {e}"))
            })?;
            if let Some(vars) = profile_vars {
                profile.variables.extend(vars);
            }
            Ok(Some(profile))
        }
        None => Ok(profile_vars.map(|vars| ProfileConfig {
            api_version: String::new(),
            kind: String::new(),
            metadata: floe_core::profile::ProfileMetadata {
                name: String::new(),
                description: None,
                env: None,
                tags: None,
            },
            execution: None,
            variables: vars,
            validation: None,
            catalogs: None,
            storages: None,
            lineage: None,
        })),
    }
}

#[pyfunction]
#[pyo3(signature = (config_path, entities=None, profile_vars=None, profile_path=None))]
pub fn validate(
    py: Python<'_>,
    config_path: &str,
    entities: Option<Vec<String>>,
    profile_vars: Option<HashMap<String, String>>,
    profile_path: Option<String>,
) -> PyResult<()> {
    let path = PathBuf::from(config_path);
    let profile = load_optional_profile(profile_path, profile_vars)?;
    let options = ValidateOptions {
        entities: entities.unwrap_or_default(),
        profile_vars: profile
            .as_ref()
            .map(|p| p.variables.clone())
            .unwrap_or_default(),
        profile_catalogs: profile.as_ref().and_then(|p| p.catalogs.clone()),
        profile_storages: profile.as_ref().and_then(|p| p.storages.clone()),
        profile_lineage: profile.and_then(|p| p.lineage),
    };
    py.allow_threads(|| floe_core::validate(&path, options))
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (config_path, entities=None, dry_run=false, run_id=None, full_refresh=false, profile_vars=None, profile_path=None))]
pub fn run(
    py: Python<'_>,
    config_path: &str,
    entities: Option<Vec<String>>,
    dry_run: bool,
    run_id: Option<String>,
    full_refresh: bool,
    profile_vars: Option<HashMap<String, String>>,
    profile_path: Option<String>,
) -> PyResult<PyRunOutcome> {
    let path = PathBuf::from(config_path);
    let profile = load_optional_profile(profile_path, profile_vars)?;
    let options = RunOptions {
        run_id,
        entities: entities.unwrap_or_default(),
        dry_run,
        full_refresh,
        profile,
    };
    let outcome = py
        .allow_threads(|| floe_core::run(&path, options))
        .map_err(to_py_err)?;
    PyRunOutcome::try_from(outcome)
        .map_err(|e| FloeError::new_err(format!("failed to serialize run outcome: {e}")))
}

#[pyfunction]
pub fn load_config(config_path: &str) -> PyResult<PyRootConfig> {
    let path = Path::new(config_path);
    floe_core::load_config(path)
        .map(PyRootConfig::from)
        .map_err(to_py_err)
}

#[pyfunction]
pub fn extract_config_env_vars(config_path: &str) -> PyResult<HashMap<String, String>> {
    let path = Path::new(config_path);
    floe_core::extract_config_env_vars(path).map_err(to_py_err)
}

#[pyfunction]
pub fn inspect_entity_state(
    py: Python<'_>,
    config_path: &str,
    entity_name: &str,
) -> PyResult<PyObject> {
    let path = PathBuf::from(config_path);
    let entity_name = entity_name.to_string();
    let inspection = py
        .allow_threads(|| {
            let config_base = ConfigBase::local_from_path(&path);
            floe_core::inspect_entity_state_with_base(&path, config_base, &entity_name)
        })
        .map_err(to_py_err)?;

    let d = PyDict::new_bound(py);
    d.set_item("entity_name", &inspection.entity_name)?;
    d.set_item("incremental_mode", inspection.incremental_mode.as_str())?;
    d.set_item("path_uri", &inspection.path.uri)?;
    d.set_item(
        "path_local",
        inspection.path.local_path.as_ref().and_then(|p| p.to_str()),
    )?;
    if let Some(state) = &inspection.state {
        let state_json = serde_json::to_string(state)
            .map_err(|e| FloeError::new_err(format!("failed to serialize state: {e}")))?;
        let state_dict = py
            .import_bound("json")?
            .call_method1("loads", (&state_json,))?;
        d.set_item("state", state_dict)?;
    } else {
        d.set_item("state", py.None())?;
    }
    Ok(d.into_py(py))
}

#[pyfunction]
pub fn reset_entity_state(py: Python<'_>, config_path: &str, entity_name: &str) -> PyResult<bool> {
    let path = PathBuf::from(config_path);
    let entity_name = entity_name.to_string();
    py.allow_threads(|| {
        let config_base = ConfigBase::local_from_path(&path);
        floe_core::reset_entity_state_with_base(&path, config_base, &entity_name)
    })
    .map_err(to_py_err)
}
