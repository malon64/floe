use std::sync::{Arc, Mutex, OnceLock};

use floe_core::run::events::{RunEvent, RunObserver};
use pyo3::prelude::*;

use crate::types::errors::FloeError;

/// A permanent observer installed once into floe-core's OnceLock.
/// The Python-side callable is stored in a Mutex so it can be swapped
/// between notebook cells without reinstalling the observer.
pub struct MutablePythonObserver {
    callback: Mutex<Option<PyObject>>,
}

// SAFETY: PyObject is not Send by default, but we only access it
// inside Python::with_gil which correctly re-acquires the GIL.
unsafe impl Send for MutablePythonObserver {}
unsafe impl Sync for MutablePythonObserver {}

impl RunObserver for MutablePythonObserver {
    fn on_event(&self, event: RunEvent) {
        let guard = match self.callback.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let Some(ref cb) = *guard else { return };

        Python::with_gil(|py| {
            let Ok(s) = serde_json::to_string(&event) else { return };
            let Ok(json_mod) = py.import_bound("json") else { return };
            let Ok(event_dict) = json_mod.call_method1("loads", (&s,)) else { return };
            // Ignore callback errors so a buggy observer never aborts the run
            let _ = cb.call1(py, (event_dict,));
        });
    }
}

static MUTABLE_OBSERVER: OnceLock<Arc<MutablePythonObserver>> = OnceLock::new();

fn get_or_install_observer() -> &'static MutablePythonObserver {
    MUTABLE_OBSERVER.get_or_init(|| {
        let obs = Arc::new(MutablePythonObserver {
            callback: Mutex::new(None),
        });
        let obs_dyn: Arc<dyn RunObserver> = obs.clone();
        let _ = floe_core::set_observer(obs_dyn);
        obs
    })
}

#[pyo3::pyfunction]
pub fn set_observer(py: Python<'_>, callback: PyObject) -> PyResult<bool> {
    if !callback.bind(py).is_callable() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "observer callback must be callable",
        ));
    }
    let obs = get_or_install_observer();
    let mut guard = obs
        .callback
        .lock()
        .map_err(|_| FloeError::new_err("observer lock poisoned"))?;
    *guard = Some(callback);
    Ok(true)
}

#[pyo3::pyfunction]
pub fn clear_observer() -> PyResult<()> {
    if let Some(obs) = MUTABLE_OBSERVER.get() {
        let mut guard = obs
            .callback
            .lock()
            .map_err(|_| FloeError::new_err("observer lock poisoned"))?;
        *guard = None;
    }
    Ok(())
}
