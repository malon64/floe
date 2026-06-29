use crate::log::emit_log;

/// Structured, matchable error type for floe-core (#395).
///
/// floe-core formerly returned `Box<dyn Error>` over four stringly-typed
/// wrappers (`ConfigError`/`RunError`/`StorageError`/`IoError`), so consumers
/// could only classify failures by inspecting message strings. `FloeError`
/// gives each failure a typed variant plus structured context fields (entity,
/// path, rule). Every floe-core failure is now a `FloeError`; its `Display` is
/// just the message, so the exact-string assertions in the test suite still hold.
///
/// `FloeResult<T>` stays the boxed alias (`Result<T, Box<dyn Error + Send + Sync>>`)
/// so foreign errors (`io::Error`, serde, polars, deltalake, reqwest, …) keep
/// flowing through `?`. `FloeError` boxes into it for free (it is
/// `Error + Send + Sync`); always return a bare `FloeError` (never
/// `Box::new(FloeError)`, which would double-box and defeat the downcast).
/// Consumers recover the structured error with `err.downcast_ref::<FloeError>()`
/// and classify by `.kind()`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FloeError {
    /// Config parsing / shape errors.
    #[error("{message}")]
    Config {
        message: String,
        entity: Option<String>,
        rule: Option<String>,
    },
    /// Schema / data validation errors.
    #[error("{message}")]
    Validation {
        message: String,
        entity: Option<String>,
        rule: Option<String>,
    },
    /// Object-store / filesystem operation errors.
    #[error("{message}")]
    Storage {
        message: String,
        path: Option<String>,
    },
    /// Sink write / registration errors.
    #[error("{message}")]
    Sink {
        message: String,
        entity: Option<String>,
    },
    /// Incremental-state / CAS / locking errors.
    #[error("{message}")]
    State {
        message: String,
        entity: Option<String>,
    },
    /// Run orchestration errors that are not specific to a subsystem above.
    #[error("{message}")]
    Run {
        message: String,
        entity: Option<String>,
    },
    /// Local I/O (read/write/decode) errors.
    #[error("{message}")]
    Io {
        message: String,
        path: Option<String>,
    },
}

/// Lightweight discriminant for classifying a [`FloeError`] without matching all
/// fields — used by the CLI log-code mapping and the Python exception mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloeErrorKind {
    Config,
    Validation,
    Storage,
    Sink,
    State,
    Run,
    Io,
}

impl FloeError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
            entity: None,
            rule: None,
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
            entity: None,
            rule: None,
        }
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            path: None,
        }
    }

    /// Storage error carrying the object/path it concerns as a structured field.
    pub fn storage_at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            path: Some(path.into()),
        }
    }

    pub fn sink(message: impl Into<String>) -> Self {
        Self::Sink {
            message: message.into(),
            entity: None,
        }
    }

    pub fn state(message: impl Into<String>) -> Self {
        Self::State {
            message: message.into(),
            entity: None,
        }
    }

    pub fn run(message: impl Into<String>) -> Self {
        Self::Run {
            message: message.into(),
            entity: None,
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::Io {
            message: message.into(),
            path: None,
        }
    }

    /// I/O error carrying the path it concerns as a structured field.
    pub fn io_at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Io {
            message: message.into(),
            path: Some(path.into()),
        }
    }

    pub fn kind(&self) -> FloeErrorKind {
        match self {
            Self::Config { .. } => FloeErrorKind::Config,
            Self::Validation { .. } => FloeErrorKind::Validation,
            Self::Storage { .. } => FloeErrorKind::Storage,
            Self::Sink { .. } => FloeErrorKind::Sink,
            Self::State { .. } => FloeErrorKind::State,
            Self::Run { .. } => FloeErrorKind::Run,
            Self::Io { .. } => FloeErrorKind::Io,
        }
    }
}

pub fn emit(
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    emit_log("error", run_id, entity, input, code, message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_just_the_message_for_every_variant() {
        assert_eq!(FloeError::config("bad config").to_string(), "bad config");
        assert_eq!(FloeError::storage("disk gone").to_string(), "disk gone");
        assert_eq!(
            FloeError::storage_at("local://x", "disk gone").to_string(),
            "disk gone"
        );
        assert_eq!(FloeError::io("read failed").to_string(), "read failed");
    }

    #[test]
    fn storage_at_populates_the_path_field() {
        let err = FloeError::storage_at("local://bucket/obj", "upload failed");
        match err {
            FloeError::Storage { path, message } => {
                assert_eq!(path.as_deref(), Some("local://bucket/obj"));
                assert_eq!(message, "upload failed");
            }
            other => panic!("expected Storage, got {other:?}"),
        }
    }

    #[test]
    fn kind_reports_the_variant() {
        assert_eq!(FloeError::config("x").kind(), FloeErrorKind::Config);
        assert_eq!(FloeError::storage("x").kind(), FloeErrorKind::Storage);
        assert_eq!(FloeError::io("x").kind(), FloeErrorKind::Io);
        assert_eq!(FloeError::run("x").kind(), FloeErrorKind::Run);
    }

    #[test]
    fn floe_error_boxes_into_floe_result_via_question_mark() {
        fn inner() -> Result<(), FloeError> {
            Err(FloeError::storage("fail"))
        }
        fn outer() -> crate::FloeResult<()> {
            inner()?;
            Ok(())
        }
        let boxed = outer().unwrap_err();
        let recovered = boxed
            .downcast_ref::<FloeError>()
            .expect("should downcast back to FloeError");
        assert_eq!(recovered.kind(), FloeErrorKind::Storage);
    }
}
