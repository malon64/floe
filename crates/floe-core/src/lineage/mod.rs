use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::config::{EntityConfig, LineageConfig};
use crate::run::events::{RunEvent, RunObserver};

const DEFAULT_PRODUCER: &str = concat!(
    "https://github.com/malon64/floe/releases/tag/v",
    env!("CARGO_PKG_VERSION")
);

#[derive(Clone)]
struct ColumnMapping {
    output_name: String,
    column_type: String,
    source_field: Option<String>,
}

struct EntityUris {
    source: String,
    accepted: String,
    rejected: Option<String>,
}

pub struct OpenLineageObserver {
    client: reqwest::blocking::Client,
    config: LineageConfig,
    entity_start_ms: Mutex<HashMap<String, u128>>,
    entity_run_ids: Mutex<HashMap<String, String>>,
    run_start_ms: Mutex<Option<u128>>,
    entity_schemas: HashMap<String, Vec<ColumnMapping>>,
    entity_uris: HashMap<String, EntityUris>,
    run_job_name: String,
    consecutive_failures: AtomicUsize,
    circuit_open: AtomicBool,
}

impl OpenLineageObserver {
    pub fn new(
        config: &LineageConfig,
        entities: &[EntityConfig],
        config_path: &str,
    ) -> crate::FloeResult<Self> {
        let timeout = Duration::from_secs(config.timeout_secs.unwrap_or(5));
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                Box::new(crate::errors::ConfigError(format!(
                    "lineage: failed to build HTTP client: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;

        let run_job_name = config
            .job_name
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                std::path::Path::new(config_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("floe-run")
                    .to_string()
            });

        let entity_schemas = entities
            .iter()
            .map(|e| {
                let fields: Vec<ColumnMapping> = e
                    .schema
                    .columns
                    .iter()
                    .map(|c| ColumnMapping {
                        output_name: c.name.clone(),
                        column_type: c.column_type.clone(),
                        source_field: c.source.clone(),
                    })
                    .collect();
                (e.name.clone(), fields)
            })
            .collect();

        let entity_uris = entities
            .iter()
            .map(|e| {
                (
                    e.name.clone(),
                    EntityUris {
                        source: e.source.path.clone(),
                        accepted: e.sink.accepted.path.clone(),
                        rejected: e.sink.rejected.as_ref().map(|r| r.path.clone()),
                    },
                )
            })
            .collect();

        Ok(Self {
            client,
            config: config.clone(),
            entity_start_ms: Mutex::new(HashMap::new()),
            entity_run_ids: Mutex::new(HashMap::new()),
            run_start_ms: Mutex::new(None),
            entity_schemas,
            entity_uris,
            run_job_name,
            consecutive_failures: AtomicUsize::new(0),
            circuit_open: AtomicBool::new(false),
        })
    }

    fn attempt_post(&self, url: &str, body: &Value) -> Result<(), bool> {
        let mut req = self.client.post(url).json(body);
        if let Some(api_key) = self.config.api_key.as_deref() {
            req = req.bearer_auth(api_key);
        }
        match req.send() {
            Err(_) => Err(true),
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    Ok(())
                } else if status.as_u16() == 429 || status.is_server_error() {
                    Err(true)
                } else {
                    Err(false)
                }
            }
        }
    }

    fn post_event(&self, body: Value) {
        if self.circuit_open.load(Ordering::Relaxed) {
            return;
        }

        let url = format!("{}/api/v1/lineage", self.config.url.trim_end_matches('/'));
        let max_failures = self.config.max_failures.unwrap_or(3) as usize;
        let retry_delays_ms: &[u64] = &[0, 100, 500];

        let mut succeeded = false;

        'retry: for (attempt, &delay_ms) in retry_delays_ms.iter().enumerate() {
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            match self.attempt_post(&url, &body) {
                Ok(()) => {
                    self.consecutive_failures.store(0, Ordering::Relaxed);
                    succeeded = true;
                    break 'retry;
                }
                Err(is_retryable) => {
                    if !is_retryable || attempt == retry_delays_ms.len() - 1 {
                        break 'retry;
                    }
                }
            }
        }

        if succeeded {
            return;
        }

        // Always warn when an event is dropped, whether the failure was retryable or not.
        crate::warnings::emit(
            "",
            None,
            None,
            Some("lineage_http_error"),
            &format!("lineage: POST {url} failed — event dropped"),
        );

        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= max_failures && !self.circuit_open.swap(true, Ordering::Relaxed) {
            crate::warnings::emit(
                "",
                None,
                None,
                Some("lineage_circuit_open"),
                &format!(
                    "lineage: disabled for this run after {failures} consecutive failures — check endpoint {url}"
                ),
            );
        }
    }

    fn producer(&self) -> &str {
        self.config.producer.as_deref().unwrap_or(DEFAULT_PRODUCER)
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
        entity_run_id: &str,
        name: &str,
        event_type: &str,
        ts_ms: u128,
        stats: Option<EntityStats>,
        uris: Option<&EntityUris>,
    ) {
        let event_time = ms_to_iso8601(ts_ms);
        let job_name = format!("{}.{name}", self.config.namespace);

        let mut run_facets = json!({});
        if let Some(parent) = self.parent_run_facet() {
            run_facets["parent"] = parent;
        }

        // Build inputs/outputs based on whether stats and uris are present (COMPLETE/FAIL)
        // or absent (START — keep both empty).
        let (inputs, outputs) = match (stats.as_ref(), uris) {
            (Some(s), Some(u)) => {
                let rejection_rate = if s.rows > 0 {
                    s.rejected as f64 / s.rows as f64
                } else {
                    0.0
                };

                // Input: source dataset — sub-namespace avoids collision with real entity names.
                let (src_ns, src_path) = split_storage_uri(&u.source);
                let inputs = json!([{
                    "namespace": format!("{}.source", self.config.namespace),
                    "name": name,
                    "facets": {
                        "symlinks": symlinks_facet(self.producer(), &src_ns, &src_path, "DIRECTORY")
                    }
                }]);

                // Accepted output: entity name as logical identifier, TABLE type.
                let (acc_ns, acc_path) = split_storage_uri(&u.accepted);
                let schema_facet = json!({
                    "fields": s.schema_fields.iter().map(|col| {
                        json!({ "name": col.output_name, "type": col.column_type })
                    }).collect::<Vec<_>>(),
                    "_producer": self.producer(),
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
                });
                let accepted_dq_facet = json!({
                    "rowCount": s.accepted,
                    "validCount": s.accepted,
                    "invalidCount": 0u64,
                    "_producer": self.producer(),
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsOutputDatasetFacet.json"
                });
                let floe_facet = json!({
                    "entity": name,
                    "rejectionRate": rejection_rate,
                    "files": s.files,
                    "rows": s.rows,
                    "warnings": s.warnings,
                    "errors": s.errors,
                    "_producer": self.producer(),
                    "_schemaURL": "https://github.com/malon64/floe/schemas/FloeQualityRunFacet.json"
                });

                let mut accepted_facets = json!({
                    "symlinks": symlinks_facet(self.producer(), &acc_ns, &acc_path, "TABLE"),
                    "schema": schema_facet,
                    "dataQualityMetrics": accepted_dq_facet,
                    "floeQualityRun": floe_facet
                });

                if !s.schema_fields.is_empty() {
                    let fields_map: serde_json::Map<String, Value> = s
                        .schema_fields
                        .iter()
                        .map(|col| {
                            let src = col.source_field.as_deref().unwrap_or(&col.output_name);
                            let entry = json!({
                                "inputFields": [{
                                    "namespace": format!("{}.source", self.config.namespace),
                                    "name": name,
                                    "field": src
                                }]
                            });
                            (col.output_name.clone(), entry)
                        })
                        .collect();
                    accepted_facets["columnLineage"] = json!({
                        "fields": fields_map,
                        "_producer": self.producer(),
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ColumnLineageDatasetFacet.json"
                    });
                }

                let mut out = vec![json!({
                    "namespace": self.config.namespace,
                    "name": name,
                    "facets": accepted_facets
                })];

                // Rejected output (when configured): DIRECTORY type, rejected-row quality metrics.
                if let Some(ref rej) = u.rejected {
                    let (rej_ns, rej_path) = split_storage_uri(rej);
                    let rejected_dq_facet = json!({
                        "rowCount": s.rejected,
                        "validCount": 0u64,
                        "invalidCount": s.rejected,
                        "_producer": self.producer(),
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsOutputDatasetFacet.json"
                    });
                    out.push(json!({
                        "namespace": format!("{}.rejected", self.config.namespace),
                        "name": name,
                        "facets": {
                            "symlinks": symlinks_facet(self.producer(), &rej_ns, &rej_path, "DIRECTORY"),
                            "dataQualityMetrics": rejected_dq_facet
                        }
                    }));
                }

                (inputs, json!(out))
            }
            _ => (json!([]), json!([])),
        };

        let body = json!({
            "eventType": event_type,
            "eventTime": event_time,
            "run": {
                "runId": entity_run_id,
                "facets": run_facets
            },
            "job": {
                "namespace": self.config.namespace,
                "name": job_name,
                "facets": {}
            },
            "inputs": inputs,
            "outputs": outputs,
            "producer": self.producer(),
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
        });
        self.post_event(body);
    }
}

struct EntityStats {
    files: u64,
    rows: u64,
    accepted: u64,
    rejected: u64,
    warnings: u64,
    errors: u64,
    schema_fields: Vec<ColumnMapping>,
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

fn split_storage_uri(uri: &str) -> (String, String) {
    // abfss:// must precede abfs:// so the longer prefix matches first.
    let cloud_prefixes = ["s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://"];
    for prefix in cloud_prefixes {
        if let Some(after_scheme) = uri.strip_prefix(prefix) {
            if let Some(slash) = after_scheme.find('/') {
                let authority = uri[..prefix.len() + slash].to_string();
                let path = after_scheme[slash..].to_string();
                return (authority, path);
            }
            return (uri.to_string(), "/".to_string());
        }
    }
    ("file".to_string(), uri.to_string())
}

fn symlinks_facet(producer: &str, namespace: &str, name: &str, ds_type: &str) -> Value {
    json!({
        "identifiers": [{ "namespace": namespace, "name": name, "type": ds_type }],
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json"
    })
}

impl RunObserver for OpenLineageObserver {
    fn on_event(&self, event: RunEvent) {
        match event {
            RunEvent::RunStarted { run_id, ts_ms, .. } => {
                // Reset circuit breaker at the start of each run so a recovered endpoint
                // is retried in subsequent runs within the same long-lived process.
                self.consecutive_failures.store(0, Ordering::Relaxed);
                self.circuit_open.store(false, Ordering::Relaxed);
                if let Ok(mut guard) = self.run_start_ms.lock() {
                    *guard = Some(ts_ms);
                }
                let event_time = ms_to_iso8601(ts_ms);
                let mut run_facets = json!({});
                if let Some(parent) = self.parent_run_facet() {
                    run_facets["parent"] = parent;
                }
                let body = json!({
                    "eventType": "START",
                    "eventTime": event_time,
                    "run": {
                        "runId": run_id,
                        "facets": run_facets
                    },
                    "job": {
                        "namespace": self.config.namespace,
                        "name": self.run_job_name,
                        "facets": {}
                    },
                    "inputs": [],
                    "outputs": [],
                    "producer": self.producer(),
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
                });
                self.post_event(body);
            }
            RunEvent::EntityStarted {
                run_id,
                name,
                ts_ms,
            } => {
                // Use a per-entity run_id (overall_run_id.entity.name) so that
                // the START and COMPLETE events for the same entity share the same run_id.
                let entity_run_id = format!("{run_id}.entity.{name}");
                if let Ok(mut guard) = self.entity_start_ms.lock() {
                    guard.insert(name.clone(), ts_ms);
                }
                if let Ok(mut guard) = self.entity_run_ids.lock() {
                    guard.insert(name.clone(), entity_run_id.clone());
                }
                self.emit_entity_run_event(&entity_run_id, &name, "START", ts_ms, None, None);
            }
            RunEvent::EntityFinished {
                run_id,
                name,
                status,
                files,
                files_skipped: _,
                rows,
                accepted,
                rejected,
                warnings,
                errors,
                ts_ms,
            } => {
                let entity_run_id = self
                    .entity_run_ids
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&name).cloned())
                    .unwrap_or_else(|| format!("{run_id}.entity.{name}"));
                let event_type = if status == "failed" || status == "aborted" {
                    "FAIL"
                } else {
                    "COMPLETE"
                };
                let schema_fields = self.entity_schemas.get(&name).cloned().unwrap_or_default();
                let stats = EntityStats {
                    files,
                    rows,
                    accepted,
                    rejected,
                    warnings,
                    errors,
                    schema_fields,
                };
                let uris = self.entity_uris.get(&name);
                self.emit_entity_run_event(
                    &entity_run_id,
                    &name,
                    event_type,
                    ts_ms,
                    Some(stats),
                    uris,
                );
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
                let mut run_facets = json!({});
                if let Some(parent) = self.parent_run_facet() {
                    run_facets["parent"] = parent;
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
                        "name": self.run_job_name,
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

pub fn build_observer(
    config: &LineageConfig,
    entities: &[EntityConfig],
    config_path: &str,
) -> crate::FloeResult<Arc<dyn RunObserver>> {
    let obs = OpenLineageObserver::new(config, entities, config_path)?;
    Ok(Arc::new(obs))
}

impl OpenLineageObserver {
    pub fn is_circuit_open(&self) -> bool {
        self.circuit_open.load(Ordering::Relaxed)
    }

    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}
