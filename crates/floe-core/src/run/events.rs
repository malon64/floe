use std::sync::{Arc, OnceLock};

use serde::Serialize;

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    RunStarted {
        run_id: String,
        config: String,
        report_base: Option<String>,
        ts_ms: u128,
    },
    EntityStarted {
        run_id: String,
        name: String,
        ts_ms: u128,
    },
    FileStarted {
        run_id: String,
        entity: String,
        input: String,
        ts_ms: u128,
    },
    FileFinished {
        run_id: String,
        entity: String,
        input: String,
        status: String,
        rows: u64,
        accepted: u64,
        rejected: u64,
        elapsed_ms: u64,
        ts_ms: u128,
    },
    EntityFinished {
        run_id: String,
        name: String,
        status: String,
        files: u64,
        rows: u64,
        accepted: u64,
        rejected: u64,
        warnings: u64,
        errors: u64,
        ts_ms: u128,
    },
    RunFinished {
        run_id: String,
        status: String,
        exit_code: i32,
        files: u64,
        rows: u64,
        accepted: u64,
        rejected: u64,
        warnings: u64,
        errors: u64,
        summary_uri: Option<String>,
        ts_ms: u128,
    },
}

pub trait RunObserver: Send + Sync {
    fn on_event(&self, event: RunEvent);
}

pub struct NoopObserver;

impl RunObserver for NoopObserver {
    fn on_event(&self, _event: RunEvent) {}
}

static NOOP_OBSERVER: NoopObserver = NoopObserver;

static OBSERVER: OnceLock<Arc<dyn RunObserver>> = OnceLock::new();

pub fn set_observer(observer: Arc<dyn RunObserver>) -> bool {
    OBSERVER.set(observer).is_ok()
}

pub fn default_observer() -> &'static dyn RunObserver {
    OBSERVER
        .get()
        .map(|observer| observer.as_ref())
        .unwrap_or(&NOOP_OBSERVER)
}

pub fn event_time_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
