use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::config::LineageConfig;
use crate::run::events::{RunEvent, RunObserver};

pub struct OpenLineageObserver {
    client: reqwest::blocking::Client,
    config: LineageConfig,
    entity_start_ms: Mutex<HashMap<String, u128>>,
    run_start_ms: Mutex<Option<u128>>,
}

impl OpenLineageObserver {
    pub fn new(config: &LineageConfig) -> crate::FloeResult<Self> {
        let timeout = Duration::from_secs(config.timeout_secs.unwrap_or(5));
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                Box::new(crate::errors::ConfigError(format!(
                    "lineage: failed to build HTTP client: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        Ok(Self {
            client,
            config: config.clone(),
            entity_start_ms: Mutex::new(HashMap::new()),
            run_start_ms: Mutex::new(None),
        })
    }

    fn post_event(&self, body: Value) {
        let url = format!("{}/api/v1/lineage", self.config.url.trim_end_matches('/'));
        let mut req = self.client.post(&url).json(&body);
        if let Some(api_key) = self.config.api_key.as_deref() {
            req = req.bearer_auth(api_key);
        }
        if let Err(e) = req.send() {
            crate::warnings::emit(
                "",
                None,
                None,
                Some("lineage_http_error"),
                &format!("lineage: POST {url} failed: {e}"),
            );
        }
    }

    fn producer(&self) -> &str {
        self.config
            .producer
            .as_deref()
            .unwrap_or("https://github.com/malon64/floe")
    }

    fn parent_run_facet(&self) -> Option<Value> {
        if let Ok(run_id) = std::env::var("AIRFLOW_CTX_DAG_RUN_ID") {
            let dag = std::env::var("AIRFLOW_CTX_DAG_ID").unwrap_or_default();
            let task = std::env::var("AIRFLOW_CTX_TASK_ID").unwrap_or_default();
            let job_name = if task.is_empty() {
                dag.clone()
            } else {
                format!("{dag}.{task}")
            };
            return Some(json!({
                "run": { "runId": run_id },
                "job": {
                    "namespace": self.config.namespace,
                    "name": job_name
                },
                "_producer": self.producer(),
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
            }));
        }
        if let Ok(run_id) = std::env::var("DAGSTER_RUN_ID") {
            let job = std::env::var("DAGSTER_JOB_NAME").unwrap_or_default();
            return Some(json!({
                "run": { "runId": run_id },
                "job": {
                    "namespace": self.config.namespace,
                    "name": job
                },
                "_producer": self.producer(),
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
            }));
        }
        None
    }

    fn emit_entity_run_event(
        &self,
        run_id: &str,
        name: &str,
        event_type: &str,
        ts_ms: u128,
        stats: Option<EntityStats>,
    ) {
        let event_time = ms_to_iso8601(ts_ms);
        let job_name = format!("{}.{}", run_id, name);

        let mut run_facets = json!({});
        if let Some(parent) = self.parent_run_facet() {
            run_facets["parent"] = parent;
        }

        let mut facets = json!({});
        if let Some(ref s) = stats {
            facets["dataQualityMetrics"] = json!({
                "rowCount": s.rows,
                "validCount": s.accepted,
                "invalidCount": s.rejected,
                "_producer": self.producer(),
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsInputDatasetFacet.json"
            });
            let rejection_rate = if s.rows > 0 {
                s.rejected as f64 / s.rows as f64
            } else {
                0.0
            };
            facets["floeQualityRun"] = json!({
                "entity": name,
                "rejectionRate": rejection_rate,
                "files": s.files,
                "rows": s.rows,
                "accepted": s.accepted,
                "rejected": s.rejected,
                "warnings": s.warnings,
                "errors": s.errors,
                "_producer": self.producer(),
                "_schemaURL": "https://github.com/malon64/floe/schemas/FloeQualityRunFacet.json"
            });
        }

        let body = json!({
            "eventType": event_type,
            "eventTime": event_time,
            "run": {
                "runId": run_id,
                "facets": run_facets
            },
            "job": {
                "namespace": self.config.namespace,
                "name": job_name,
                "facets": {}
            },
            "inputs": [],
            "outputs": [],
            "producer": self.producer(),
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
        });
        self.post_event(body);

        if event_type == "COMPLETE" {
            if let Some(ref s) = stats {
                let schema_facet = json!({
                    "fields": s.schema_fields.iter().map(|(col_name, col_type)| {
                        json!({ "name": col_name, "type": col_type })
                    }).collect::<Vec<_>>(),
                    "_producer": self.producer(),
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
                });
                let complete_body = json!({
                    "eventType": "COMPLETE",
                    "eventTime": event_time,
                    "run": {
                        "runId": run_id,
                        "facets": run_facets
                    },
                    "job": {
                        "namespace": self.config.namespace,
                        "name": job_name,
                        "facets": {}
                    },
                    "inputs": [{
                        "namespace": self.config.namespace,
                        "name": name,
                        "facets": {
                            "schema": schema_facet,
                            "dataQualityMetrics": facets.get("dataQualityMetrics").cloned().unwrap_or(json!(null)),
                            "floeQualityRun": facets.get("floeQualityRun").cloned().unwrap_or(json!(null))
                        }
                    }],
                    "outputs": [],
                    "producer": self.producer(),
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
                });
                self.post_event(complete_body);
            }
        }
    }
}

struct EntityStats {
    files: u64,
    rows: u64,
    accepted: u64,
    rejected: u64,
    warnings: u64,
    errors: u64,
    schema_fields: Vec<(String, String)>,
}

fn ms_to_iso8601(ms: u128) -> String {
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as i64;
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(dt) => {
            let ns = time::Duration::nanoseconds(nanos);
            let dt = dt.saturating_add(ns);
            dt.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| ms.to_string())
        }
        Err(_) => ms.to_string(),
    }
}

impl RunObserver for OpenLineageObserver {
    fn on_event(&self, event: RunEvent) {
        match event {
            RunEvent::RunStarted { ts_ms, .. } => {
                if let Ok(mut guard) = self.run_start_ms.lock() {
                    *guard = Some(ts_ms);
                }
            }
            RunEvent::EntityStarted { name, ts_ms, .. } => {
                if let Ok(mut guard) = self.entity_start_ms.lock() {
                    guard.insert(name.clone(), ts_ms);
                }
                // emit START event
                self.emit_entity_run_event(&format!("entity-{name}"), &name, "START", ts_ms, None);
            }
            RunEvent::EntityFinished {
                run_id,
                name,
                files,
                rows,
                accepted,
                rejected,
                warnings,
                errors,
                ts_ms,
                ..
            } => {
                let stats = EntityStats {
                    files,
                    rows,
                    accepted,
                    rejected,
                    warnings,
                    errors,
                    schema_fields: vec![],
                };
                self.emit_entity_run_event(&run_id, &name, "COMPLETE", ts_ms, Some(stats));
            }
            RunEvent::RunFinished {
                run_id,
                status,
                ts_ms,
                ..
            } => {
                let event_type = if status == "failed" || status == "aborted" {
                    "FAIL"
                } else {
                    "COMPLETE"
                };
                let event_time = ms_to_iso8601(ts_ms);
                let body = json!({
                    "eventType": event_type,
                    "eventTime": event_time,
                    "run": {
                        "runId": run_id,
                        "facets": {}
                    },
                    "job": {
                        "namespace": self.config.namespace,
                        "name": run_id,
                        "facets": {}
                    },
                    "inputs": [],
                    "outputs": [],
                    "producer": self.producer(),
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
                });
                self.post_event(body);
            }
            _ => {}
        }
    }
}

pub fn build_observer(config: &LineageConfig) -> crate::FloeResult<Arc<dyn RunObserver>> {
    let obs = OpenLineageObserver::new(config)?;
    Ok(Arc::new(obs))
}
