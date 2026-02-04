use floe_core::RunEvent;

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
        RunEvent::Log { level, .. } => match level.as_str() {
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
            level,
            code,
            message,
            entity,
            input,
            ..
        } => {
            let mut out = format!("log level={level}");
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
