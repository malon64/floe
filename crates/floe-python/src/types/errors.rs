use floe_core::errors::{FloeError as CoreFloeError, FloeErrorKind};
use pyo3::prelude::*;

pyo3::create_exception!(floe._floe, FloeError, pyo3::exceptions::PyException);
pyo3::create_exception!(floe._floe, FloeConfigError, FloeError);
pyo3::create_exception!(floe._floe, FloeRunError, FloeError);
pyo3::create_exception!(floe._floe, FloeStorageError, FloeError);
pyo3::create_exception!(floe._floe, FloeIoError, FloeError);

pub fn to_py_err(err: Box<dyn std::error::Error + Send + Sync>) -> PyErr {
    let msg = err.to_string();
    // Every floe-core failure is a structured FloeError (#395); map it by kind.
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
    // Foreign errors that reach the boundary unwrapped (e.g. a bare std::io::Error).
    if err.is::<std::io::Error>() {
        FloeIoError::new_err(msg)
    } else {
        FloeError::new_err(msg)
    }
}
