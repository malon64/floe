//! A string newtype that redacts its contents in `Debug`/`Display` output.
//!
//! Resolved credentials (Unity / REST tokens, MotherDuck tokens) live in structs
//! that derive `Debug`. Nothing on production paths debug-prints them today, but
//! that invariant is enforced only by convention — one future `{:?}` in a log line
//! or error message would leak a PAT. [`Secret`] makes the safe behaviour the
//! default: `Debug` and `Display` render `[REDACTED]`, and the raw value is only
//! reachable through the explicit [`Secret::expose`] accessor at the few sites that
//! genuinely need it (HTTP bearer header, DuckDB connection config).
//!
//! Serde is intentionally **transparent**: the inner string is (de)serialized as a
//! plain string so YAML config parsing and manifest round-trips are unaffected.
//! Redaction guards accidental formatting, not deliberate serialization — manifest
//! token redaction is handled separately where secrets are persisted.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const REDACTED: &str = "[REDACTED]";

/// A sensitive string whose `Debug`/`Display` representation is `[REDACTED]`.
///
/// Construct from any string (`Secret::from`, `Secret::new`) and read the raw
/// value with [`Secret::expose`]. Equality compares the underlying value, so a
/// `Secret` can still key/deduplicate config the way a `String` did.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value as a secret.
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    /// Returns the raw secret value.
    ///
    /// Use only where the secret must cross a trust boundary it cannot avoid —
    /// an auth header or a driver connection config. Never log or format the
    /// returned `&str`.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Secret(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Secret(value.to_string())
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Secret(String::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_is_redacted() {
        let secret = Secret::from("super-secret-pat");
        assert_eq!(format!("{secret:?}"), "[REDACTED]");
    }

    #[test]
    fn display_is_redacted() {
        let secret = Secret::from("super-secret-pat");
        assert_eq!(format!("{secret}"), "[REDACTED]");
    }

    #[test]
    fn debug_of_containing_struct_does_not_leak() {
        #[derive(Debug)]
        struct Config {
            token: Secret,
        }
        let cfg = Config {
            token: Secret::from("dapi-leak-me"),
        };
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("dapi-leak-me"));
        // The raw value is retained (readable via `expose`); only its formatting is redacted.
        assert_eq!(cfg.token.expose(), "dapi-leak-me");
    }

    #[test]
    fn expose_returns_the_raw_value() {
        let secret = Secret::new("raw-value".to_string());
        assert_eq!(secret.expose(), "raw-value");
    }

    #[test]
    fn serde_round_trip_is_transparent() {
        let secret = Secret::from("pat-123");
        let json = serde_json::to_string(&secret).expect("serialize");
        // Serialization is transparent (the real value), not redacted.
        assert_eq!(json, "\"pat-123\"");
        let back: Secret = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.expose(), "pat-123");
    }
}
