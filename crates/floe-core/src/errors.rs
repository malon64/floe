use crate::log::emit_log;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RunError(pub String);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StorageError(pub String);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct IoError(pub String);

pub fn emit(
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    emit_log("error", run_id, entity, input, code, message);
}
