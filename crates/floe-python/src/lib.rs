// PyO3's #[pyfunction] macro emits identity From<PyErr> conversions and
// create_exception! emits cfg(gil-refs) checks — both are known false positives.
#![allow(clippy::useless_conversion, unexpected_cfgs)]

mod functions;
mod observer;
mod types;

use pyo3::prelude::*;
use types::config::{PyEntityConfig, PyRootConfig};
use types::errors::{FloeConfigError, FloeError, FloeIoError, FloeRunError, FloeStorageError};
use types::outcome::PyRunOutcome;

#[pymodule]
fn _floe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Exception hierarchy
    m.add("FloeError", m.py().get_type_bound::<FloeError>())?;
    m.add(
        "FloeConfigError",
        m.py().get_type_bound::<FloeConfigError>(),
    )?;
    m.add("FloeRunError", m.py().get_type_bound::<FloeRunError>())?;
    m.add(
        "FloeStorageError",
        m.py().get_type_bound::<FloeStorageError>(),
    )?;
    m.add("FloeIoError", m.py().get_type_bound::<FloeIoError>())?;

    // Types
    m.add_class::<PyRootConfig>()?;
    m.add_class::<PyEntityConfig>()?;
    m.add_class::<PyRunOutcome>()?;

    // Core functions
    m.add_function(wrap_pyfunction!(functions::validate, m)?)?;
    m.add_function(wrap_pyfunction!(functions::run, m)?)?;
    m.add_function(wrap_pyfunction!(functions::load_config, m)?)?;
    m.add_function(wrap_pyfunction!(functions::extract_config_env_vars, m)?)?;
    m.add_function(wrap_pyfunction!(functions::inspect_entity_state, m)?)?;
    m.add_function(wrap_pyfunction!(functions::reset_entity_state, m)?)?;

    // Observer
    m.add_function(wrap_pyfunction!(observer::set_observer, m)?)?;
    m.add_function(wrap_pyfunction!(observer::clear_observer, m)?)?;

    Ok(())
}
