use std::sync::OnceLock;

use serde_json::Value;

pub(crate) fn write_timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FLOE_PERF_WRITE_TIMINGS")
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

pub(crate) fn emit_write_perf_log(code: &'static str, payload: Value) {
    if !write_timing_enabled() {
        return;
    }
    eprintln!(
        "{{\"log_level\":\"debug\",\"code\":\"{code}\",\"message\":{}}}",
        payload
    );
}
