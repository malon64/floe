use crate::run::events::{event_time_ms, is_observer_set, RunEvent};

fn emit_stderr(message: &str) {
    eprintln!("warn: {message}");
}

pub fn emit(
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
            level: "warn".to_string(),
            code: code.map(ToString::to_string),
            message: message.to_string(),
            entity: entity.map(ToString::to_string),
            input: input.map(ToString::to_string),
            ts_ms: event_time_ms(),
        });
        return;
    }

    emit_stderr(message);
}

pub fn emit_once(
    flag: &mut bool,
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    if !*flag {
        emit(run_id, entity, input, code, message);
        *flag = true;
    }
}
