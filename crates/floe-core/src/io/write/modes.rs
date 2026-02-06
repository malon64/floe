use crate::{config, FloeResult};

pub fn ensure_mode_supported(mode: config::WriteMode) -> FloeResult<()> {
    match mode {
        config::WriteMode::Overwrite | config::WriteMode::Append => Ok(()),
    }
}
