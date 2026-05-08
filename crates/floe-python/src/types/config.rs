use floe_core::config::{EntityConfig, RootConfig};
use pyo3::prelude::*;

#[pyclass(name = "EntityConfig")]
#[derive(Clone)]
pub struct PyEntityConfig {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub domain: Option<String>,
    #[pyo3(get)]
    pub source_format: String,
    #[pyo3(get)]
    pub source_path: String,
    #[pyo3(get)]
    pub source_storage: Option<String>,
    #[pyo3(get)]
    pub accepted_format: String,
    #[pyo3(get)]
    pub accepted_path: String,
    #[pyo3(get)]
    pub accepted_storage: Option<String>,
    #[pyo3(get)]
    pub incremental_mode: String,
}

#[pymethods]
impl PyEntityConfig {
    fn __repr__(&self) -> String {
        format!(
            "EntityConfig(name={:?}, source_format={:?}, accepted_format={:?})",
            self.name, self.source_format, self.accepted_format
        )
    }
}

impl From<&EntityConfig> for PyEntityConfig {
    fn from(e: &EntityConfig) -> Self {
        PyEntityConfig {
            name: e.name.clone(),
            domain: e.domain.clone(),
            source_format: e.source.format.clone(),
            source_path: e.source.path.clone(),
            source_storage: e.source.storage.clone(),
            accepted_format: e.sink.accepted.format.clone(),
            accepted_path: e.sink.accepted.path.clone(),
            accepted_storage: e.sink.accepted.storage.clone(),
            incremental_mode: e.incremental_mode.as_str().to_string(),
        }
    }
}

#[pyclass(name = "RootConfig")]
pub struct PyRootConfig {
    pub version: String,
    pub entity_names: Vec<String>,
    pub entities: Vec<PyEntityConfig>,
}

#[pymethods]
impl PyRootConfig {
    #[getter]
    fn version(&self) -> &str {
        &self.version
    }

    #[getter]
    fn entity_names(&self) -> Vec<String> {
        self.entity_names.clone()
    }

    #[getter]
    fn entities(&self) -> Vec<PyEntityConfig> {
        self.entities.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "RootConfig(version={:?}, entities={})",
            self.version,
            self.entities.len()
        )
    }
}

impl From<RootConfig> for PyRootConfig {
    fn from(c: RootConfig) -> Self {
        let entity_names = c.entities.iter().map(|e| e.name.clone()).collect();
        let entities = c.entities.iter().map(PyEntityConfig::from).collect();
        PyRootConfig {
            version: c.version,
            entity_names,
            entities,
        }
    }
}
