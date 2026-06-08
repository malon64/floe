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

// Shared registration for both the lean (`_floe`) and full (`_floe_duckdb`)
// extension modules. The two entry points below differ only in name so the
// compiled symbol (`PyInit__floe` vs `PyInit__floe_duckdb`) matches the dotted
// module each wheel installs; everything they expose is identical save for the
// `HAS_DUCKDB` capability flag the lean Python wrapper uses to decide whether to
// delegate a DuckDB-sink run to the companion module.
fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
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

    // Capability flag: true only in the `+duckdb` build. The lean Python wrapper
    // reads this to know whether it must delegate DuckDB sinks to the companion.
    m.add("HAS_DUCKDB", cfg!(feature = "duckdb"))?;

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
    m.add_function(wrap_pyfunction!(functions::config_targets_duckdb, m)?)?;

    // Observer
    m.add_function(wrap_pyfunction!(observer::set_observer, m)?)?;
    m.add_function(wrap_pyfunction!(observer::clear_observer, m)?)?;

    Ok(())
}

/// Lean extension module, installed as `floe._floe` by the PyPI `floe` wheel.
#[cfg(not(feature = "duckdb"))]
#[pymodule]
fn _floe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register(m)
}

/// Full extension module, installed as `floe._floe_duckdb` by the off-PyPI
/// `floe-duckdb` companion wheel. Ships only the compiled `.so` into the shared
/// `floe` package so it can coexist with the lean wheel.
#[cfg(feature = "duckdb")]
#[pymodule]
fn _floe_duckdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register(m)
}
