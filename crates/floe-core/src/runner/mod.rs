//! Run-layer helpers for connector-owned execution.
//!
//! floe-core does **not** own runner abstractions, traits, or factories.
//! Job submission, polling, scheduling, and resource provisioning belong to
//! connector crates (Airflow, Dagster, Kubernetes adapters, …) or a separate
//! orchestration crate.
//!
//! This module exposes only the outcome/log-parsing helpers that connectors
//! need to map their execution results onto Floe's normalized types.

pub mod outcome;

pub use outcome::{parse_run_status_from_logs, ConnectorRunStatus};
