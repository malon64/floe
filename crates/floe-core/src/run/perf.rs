use std::sync::OnceLock;

use crate::run::events::{event_time_ms, RunEvent, RunObserver};

pub(crate) fn phase_timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FLOE_PERF_PHASE_TIMINGS")
            .ok()
            .map(|value| {
                let normalized = value.trim().to_ascii_lowercase();
                !(normalized.is_empty()
                    || normalized == "0"
                    || normalized == "false"
                    || normalized == "off")
            })
            .unwrap_or(false)
    })
}

pub(crate) fn emit_perf_log(
    observer: &dyn RunObserver,
    run_id: &str,
    entity: Option<&str>,
    code: &'static str,
    payload: serde_json::Value,
) {
    if !phase_timing_enabled() {
        return;
    }
    observer.on_event(RunEvent::Log {
        run_id: run_id.to_string(),
        log_level: "debug".to_string(),
        code: Some(code.to_string()),
        message: payload.to_string(),
        entity: entity.map(ToOwned::to_owned),
        input: None,
        ts_ms: event_time_ms(),
    });
}
