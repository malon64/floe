use floe_core::run::{DryRunEntityPreview, RunOutcome};
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass(name = "RunOutcome")]
pub struct PyRunOutcome {
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub report_base_path: Option<String>,
    #[pyo3(get)]
    pub dry_run: bool,
    summary_json: String,
    entity_reports_json: Vec<String>,
    dry_run_previews_data: Option<Vec<DryRunPreviewData>>,
}

#[derive(Clone)]
struct DryRunPreviewData {
    name: String,
    input_path: String,
    input_format: String,
    accepted_path: String,
    accepted_format: String,
    rejected_path: Option<String>,
    rejected_format: Option<String>,
    archive_path: String,
    archive_storage: Option<String>,
    report_file: Option<String>,
    scanned_files: Vec<String>,
}

impl From<DryRunEntityPreview> for DryRunPreviewData {
    fn from(p: DryRunEntityPreview) -> Self {
        DryRunPreviewData {
            name: p.name,
            input_path: p.input_path,
            input_format: p.input_format,
            accepted_path: p.accepted_path,
            accepted_format: p.accepted_format,
            rejected_path: p.rejected_path,
            rejected_format: p.rejected_format,
            archive_path: p.archive_path,
            archive_storage: p.archive_storage,
            report_file: p.report_file,
            scanned_files: p.scanned_files,
        }
    }
}

impl TryFrom<RunOutcome> for PyRunOutcome {
    type Error = serde_json::Error;

    fn try_from(outcome: RunOutcome) -> Result<Self, Self::Error> {
        let summary_json = serde_json::to_string(&outcome.summary)?;
        let entity_reports_json = outcome
            .entity_outcomes
            .iter()
            .map(|e| serde_json::to_string(&e.report))
            .collect::<Result<Vec<_>, _>>()?;
        let dry_run_previews_data = outcome
            .dry_run_previews
            .map(|previews| previews.into_iter().map(DryRunPreviewData::from).collect());

        Ok(PyRunOutcome {
            run_id: outcome.run_id,
            report_base_path: outcome.report_base_path,
            dry_run: dry_run_previews_data.is_some(),
            summary_json,
            entity_reports_json,
            dry_run_previews_data,
        })
    }
}

fn json_loads<'py>(py: Python<'py>, s: &str) -> PyResult<Bound<'py, PyAny>> {
    py.import_bound("json")?.call_method1("loads", (s,))
}

#[pymethods]
impl PyRunOutcome {
    #[getter]
    fn summary(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(json_loads(py, &self.summary_json)?.into_py(py))
    }

    #[getter]
    fn entity_reports(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.entity_reports_json
            .iter()
            .map(|s| Ok(json_loads(py, s)?.into_py(py)))
            .collect()
    }

    #[getter]
    fn dry_run_previews(&self, py: Python<'_>) -> PyResult<Option<Vec<PyObject>>> {
        let Some(ref previews) = self.dry_run_previews_data else {
            return Ok(None);
        };
        let result: PyResult<Vec<PyObject>> = previews
            .iter()
            .map(|p| {
                let d = PyDict::new_bound(py);
                d.set_item("name", &p.name)?;
                d.set_item("input_path", &p.input_path)?;
                d.set_item("input_format", &p.input_format)?;
                d.set_item("accepted_path", &p.accepted_path)?;
                d.set_item("accepted_format", &p.accepted_format)?;
                d.set_item("rejected_path", p.rejected_path.as_deref())?;
                d.set_item("rejected_format", p.rejected_format.as_deref())?;
                d.set_item("archive_path", &p.archive_path)?;
                d.set_item("archive_storage", p.archive_storage.as_deref())?;
                d.set_item("report_file", p.report_file.as_deref())?;
                d.set_item("scanned_files", &p.scanned_files)?;
                Ok(d.into_py(py))
            })
            .collect();
        Ok(Some(result?))
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let d = PyDict::new_bound(py);
        d.set_item("run_id", &self.run_id)?;
        d.set_item("report_base_path", self.report_base_path.as_deref())?;
        d.set_item("dry_run", self.dry_run)?;
        d.set_item("summary", self.summary(py)?)?;
        d.set_item("entity_reports", self.entity_reports(py)?)?;
        d.set_item("dry_run_previews", self.dry_run_previews(py)?)?;
        Ok(d.into_py(py))
    }

    fn __repr__(&self) -> String {
        format!(
            "RunOutcome(run_id={:?}, entities={}, dry_run={})",
            self.run_id,
            self.entity_reports_json.len(),
            self.dry_run
        )
    }
}
