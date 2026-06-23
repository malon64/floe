use floe_core::errors::{
    ConfigError, FloeError as CoreFloeError, FloeErrorKind, IoError, RunError, StorageError,
};
use pyo3::prelude::*;

pyo3::create_exception!(floe._floe, FloeError, pyo3::exceptions::PyException);
pyo3::create_exception!(floe._floe, FloeConfigError, FloeError);
pyo3::create_exception!(floe._floe, FloeRunError, FloeError);
pyo3::create_exception!(floe._floe, FloeStorageError, FloeError);
pyo3::create_exception!(floe._floe, FloeIoError, FloeError);

pub fn to_py_err(err: Box<dyn std::error::Error + Send + Sync>) -> PyErr {
    let msg = err.to_string();
    // Prefer the structured enum (#395): migrated subsystems box a CoreFloeError,
    // which we map by kind. The legacy wrapper checks below cover not-yet-migrated
    // modules and keep behaviour identical during the incremental migration.
    if let Some(core) = err.downcast_ref::<CoreFloeError>() {
        return match core.kind() {
            FloeErrorKind::Storage => FloeStorageError::new_err(msg),
            FloeErrorKind::Io => FloeIoError::new_err(msg),
            FloeErrorKind::Config | FloeErrorKind::Validation => FloeConfigError::new_err(msg),
            FloeErrorKind::Run | FloeErrorKind::Sink | FloeErrorKind::State => {
                FloeRunError::new_err(msg)
            }
        };
    }
    if err.is::<ConfigError>() {
        FloeConfigError::new_err(msg)
    } else if err.is::<RunError>() {
        FloeRunError::new_err(msg)
    } else if err.is::<StorageError>() {
        FloeStorageError::new_err(msg)
    } else if err.is::<IoError>() || err.is::<std::io::Error>() {
        FloeIoError::new_err(msg)
    } else {
        FloeError::new_err(msg)
    }
}
