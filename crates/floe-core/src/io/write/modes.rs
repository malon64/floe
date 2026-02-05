use crate::{config, ConfigError, FloeResult};

pub fn ensure_mode_supported(mode: config::WriteMode) -> FloeResult<()> {
    match mode {
        config::WriteMode::Overwrite => Ok(()),
        config::WriteMode::Append => Err(Box::new(ConfigError(
            "sink.accepted.write_mode=append is not supported yet".to_string(),
        ))),
    }
}
