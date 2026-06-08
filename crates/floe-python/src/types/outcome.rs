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

impl DryRunPreviewData {
    fn from_dict(d: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(DryRunPreviewData {
            name: d.get_item("name")?.extract()?,
            input_path: d.get_item("input_path")?.extract()?,
            input_format: d.get_item("input_format")?.extract()?,
            accepted_path: d.get_item("accepted_path")?.extract()?,
            accepted_format: d.get_item("accepted_format")?.extract()?,
            rejected_path: d.get_item("rejected_path")?.extract()?,
            rejected_format: d.get_item("rejected_format")?.extract()?,
            archive_path: d.get_item("archive_path")?.extract()?,
            archive_storage: d.get_item("archive_storage")?.extract()?,
            report_file: d.get_item("report_file")?.extract()?,
            scanned_files: d.get_item("scanned_files")?.extract()?,
        })
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
    /// Rebuild a lean `RunOutcome` from the `to_dict()` payload of a companion's
    /// `RunOutcome`. The lean `_floe` and companion `_floe_duckdb` are separate
    /// native libraries, so a companion-returned outcome is a *different* Python
    /// class. Delegated DuckDB runs round-trip through this so callers always get
    /// the lean `RunOutcome` type they imported, not the companion's.
    #[staticmethod]
    fn from_dict(data: Bound<'_, PyAny>) -> PyResult<Self> {
        let py = data.py();
        let json = py.import_bound("json")?;
        let dumps = |obj: Bound<'_, PyAny>| -> PyResult<String> {
            json.call_method1("dumps", (obj,))?.extract()
        };
        let run_id: String = data.get_item("run_id")?.extract()?;
        let report_base_path: Option<String> = data.get_item("report_base_path")?.extract()?;
        let dry_run: bool = data.get_item("dry_run")?.extract()?;
        let summary_json = dumps(data.get_item("summary")?)?;
        let mut entity_reports_json = Vec::new();
        for item in data.get_item("entity_reports")?.iter()? {
            entity_reports_json.push(dumps(item?)?);
        }
        let previews = data.get_item("dry_run_previews")?;
        let dry_run_previews_data = if previews.is_none() {
            None
        } else {
            let mut v = Vec::new();
            for item in previews.iter()? {
                v.push(DryRunPreviewData::from_dict(&item?)?);
            }
            Some(v)
        };
        Ok(PyRunOutcome {
            run_id,
            report_base_path,
            dry_run,
            summary_json,
            entity_reports_json,
            dry_run_previews_data,
        })
    }

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

    fn _repr_html_(&self) -> String {
        let summary: serde_json::Value =
            serde_json::from_str(&self.summary_json).unwrap_or(serde_json::Value::Null);

        let run_status = summary
            .get("run")
            .and_then(|r| r.get("status"))
            .and_then(|s| s.as_str())
            .unwrap_or("unknown");

        let totals = &summary["results"];
        let accepted = totals
            .get("accepted_total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let rejected = totals
            .get("rejected_total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let rows = totals
            .get("rows_total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let status_color = match run_status {
            "Success" => "#2e7d32",
            "SuccessWithWarnings" => "#e65100",
            _ => "#c62828",
        };

        let mut entity_rows = String::new();
        if let Some(entities) = summary.get("entities").and_then(|e| e.as_array()) {
            for entity in entities {
                let name = entity.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                let status = entity.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                let e_accepted = entity
                    .get("results")
                    .and_then(|r| r.get("accepted_total"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let e_rejected = entity
                    .get("results")
                    .and_then(|r| r.get("rejected_total"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let e_color = match status {
                    "Success" => "#2e7d32",
                    "SuccessWithWarnings" => "#e65100",
                    _ => "#c62828",
                };
                entity_rows.push_str(&format!(
                    "<tr><td style=\"padding:4px 8px\">{name}</td>\
                     <td style=\"padding:4px 8px;color:{e_color};font-weight:bold\">{status}</td>\
                     <td style=\"padding:4px 8px;text-align:right\">{e_accepted}</td>\
                     <td style=\"padding:4px 8px;text-align:right\">{e_rejected}</td></tr>"
                ));
            }
        }

        format!(
            "<div style=\"font-family:monospace;border:1px solid #ddd;border-radius:4px;padding:12px;max-width:600px\">\
             <div style=\"margin-bottom:8px\">\
               <strong>RunOutcome</strong> \
               <code style=\"color:#555\">{run_id}</code> \
               <span style=\"color:{status_color};font-weight:bold;margin-left:8px\">{run_status}</span>\
             </div>\
             <div style=\"margin-bottom:8px;color:#555\">\
               {rows} rows &nbsp;·&nbsp; \
               <span style=\"color:#2e7d32\">{accepted} accepted</span> &nbsp;·&nbsp; \
               <span style=\"color:#c62828\">{rejected} rejected</span>\
             </div>\
             <table style=\"border-collapse:collapse;width:100%\">\
               <thead><tr style=\"border-bottom:1px solid #ddd\">\
                 <th style=\"padding:4px 8px;text-align:left\">Entity</th>\
                 <th style=\"padding:4px 8px;text-align:left\">Status</th>\
                 <th style=\"padding:4px 8px;text-align:right\">Accepted</th>\
                 <th style=\"padding:4px 8px;text-align:right\">Rejected</th>\
               </tr></thead>\
               <tbody>{entity_rows}</tbody>\
             </table>\
             </div>",
            run_id = self.run_id,
        )
    }
}
