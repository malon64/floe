use clap::ValueEnum;
use floe_core::{set_observer, RunEvent, RunObserver};
use std::io::Write;
use std::sync::Arc;

#[derive(Clone, Debug, ValueEnum)]
pub enum LogFormat {
    Off,
    Text,
    Json,
}

pub fn install_observer(format: LogFormat) {
    if matches!(format, LogFormat::Off) {
        return;
    }
    let _ = set_observer(Arc::new(CliObserver {
        format,
        lock: std::sync::Mutex::new(()),
    }));
}

struct CliObserver {
    format: LogFormat,
    lock: std::sync::Mutex<()>,
}

impl RunObserver for CliObserver {
    fn on_event(&self, event: RunEvent) {
        let _guard = self.lock.lock();
        match self.format {
            LogFormat::Json => {
                if let Some(line) = format_event_json(&event) {
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "{line}");
                    let _ = out.flush();
                }
            }
            LogFormat::Text => {
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{}", format_event_text(&event));
                let _ = out.flush();
            }
            LogFormat::Off => {}
        }
    }
}

fn error_code_for(err: &(dyn std::error::Error + 'static)) -> &'static str {
    if err.is::<floe_core::ConfigError>() {
        return "config_error";
    }
    if err.is::<floe_core::errors::RunError>() {
        return "run_error";
    }
    if err.is::<floe_core::errors::StorageError>() {
        return "storage_error";
    }
    if err.is::<floe_core::errors::IoError>() {
        return "io_error";
    }
    "error"
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

pub fn emit_failed_run_events(
    run_id: &str,
    err: &(dyn std::error::Error + 'static),
    log_format: &LogFormat,
) {
    if matches!(log_format, LogFormat::Off) {
        return;
    }

    let observer = floe_core::run::events::default_observer();
    observer.on_event(RunEvent::Log {
        run_id: run_id.to_string(),
        log_level: "error".to_string(),
        code: Some(error_code_for(err).to_string()),
        message: err.to_string(),
        entity: None,
        input: None,
        ts_ms: now_ms(),
    });
    observer.on_event(RunEvent::RunFinished {
        run_id: run_id.to_string(),
        status: "failed".to_string(),
        exit_code: 1,
        files: 0,
        rows: 0,
        accepted: 0,
        rejected: 0,
        warnings: 0,
        errors: 1,
        summary_uri: None,
        ts_ms: now_ms(),
    });
}

#[derive(Clone, Copy, Debug)]
enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

fn level_for_event(event: &RunEvent) -> Level {
    match event {
        RunEvent::Log { log_level, .. } => match log_level.as_str() {
            "warn" => Level::Warn,
            "error" => Level::Error,
            _ => Level::Info,
        },
        RunEvent::RunStarted { .. }
        | RunEvent::EntityStarted { .. }
        | RunEvent::FileStarted { .. } => Level::Info,
        RunEvent::FileFinished { status, .. } => match status.as_str() {
            "success" => Level::Info,
            "rejected" => Level::Warn,
            "aborted" | "failed" => Level::Error,
            _ => Level::Info,
        },
        RunEvent::EntityFinished { status, .. } | RunEvent::RunFinished { status, .. } => {
            match status.as_str() {
                "success" => Level::Info,
                "success_with_warnings" | "rejected" => Level::Warn,
                "aborted" | "failed" => Level::Error,
                _ => Level::Info,
            }
        }
    }
}

pub const LOG_SCHEMA: &str = "floe.log.v1";

#[derive(serde::Serialize)]
struct EventEnvelope<'a> {
    schema: &'static str,
    level: &'static str,
    #[serde(flatten)]
    event: &'a RunEvent,
}

pub fn format_event_json(event: &RunEvent) -> Option<String> {
    let level = level_for_event(event);
    let envelope = EventEnvelope {
        schema: LOG_SCHEMA,
        level: level.as_str(),
        event,
    };
    serde_json::to_string(&envelope).ok()
}

pub fn format_event_text(event: &RunEvent) -> String {
    match event {
        RunEvent::Log {
            log_level,
            code,
            message,
            entity,
            input,
            ..
        } => {
            let mut out = format!("log level={log_level}");
            if let Some(code) = code.as_deref() {
                out.push_str(&format!(" code={code}"));
            }
            if let Some(entity) = entity.as_deref() {
                out.push_str(&format!(" entity={entity}"));
            }
            if let Some(input) = input.as_deref() {
                out.push_str(&format!(" input={input}"));
            }
            out.push(' ');
            out.push_str(message);
            out
        }
        RunEvent::RunStarted {
            run_id,
            config,
            report_base,
            ..
        } => format!(
            "run_started run_id={} config={} report_base={}",
            run_id,
            config,
            report_base.as_deref().unwrap_or("disabled")
        ),
        RunEvent::EntityStarted { name, .. } => format!("entity_started name={name}"),
        RunEvent::FileStarted { entity, input, .. } => {
            format!("file_started entity={entity} input={input}")
        }
        RunEvent::FileFinished {
            entity,
            input,
            status,
            rows,
            accepted,
            rejected,
            elapsed_ms,
            ..
        } => format!(
            "file_finished entity={} input={} status={} rows={} accepted={} rejected={} elapsed_ms={}",
            entity, input, status, rows, accepted, rejected, elapsed_ms
        ),
        RunEvent::EntityFinished {
            name,
            status,
            files,
            rows,
            accepted,
            rejected,
            warnings,
            errors,
            ..
        } => format!(
            "entity_finished name={} status={} files={} rows={} accepted={} rejected={} warnings={} errors={}",
            name, status, files, rows, accepted, rejected, warnings, errors
        ),
        RunEvent::RunFinished {
            status,
            exit_code,
            summary_uri,
            ..
        } => format!(
            "run_finished status={} exit_code={} summary={}",
            status,
            exit_code,
            summary_uri.as_deref().unwrap_or("disabled")
        ),
    }
}
