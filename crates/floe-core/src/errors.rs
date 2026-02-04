use std::fmt;

use crate::run::events::{event_time_ms, is_observer_set, RunEvent};

#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug)]
pub struct RunError(pub String);

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RunError {}

#[derive(Debug)]
pub struct StorageError(pub String);

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug)]
pub struct IoError(pub String);

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IoError {}

fn emit_stderr(message: &str) {
    eprintln!("error: {message}");
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
            log_level: "error".to_string(),
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
