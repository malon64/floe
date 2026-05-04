use crate::run::events::{event_time_ms, is_observer_set, RunEvent};

pub(crate) fn emit_log(
    level: &str,
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    if is_observer_set() {
        let observer = crate::run::events::default_observer();
        observer.on_event(RunEvent::Log {
            run_id: run_id.to_string(),
            log_level: level.to_string(),
            code: code.map(ToString::to_string),
            message: message.to_string(),
            entity: entity.map(ToString::to_string),
            input: input.map(ToString::to_string),
            ts_ms: event_time_ms(),
        });
    } else {
        eprintln!("{level}: {message}");
    }
}
