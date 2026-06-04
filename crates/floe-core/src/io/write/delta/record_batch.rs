use deltalake::protocol::SaveMode;

use crate::config;

// The generic `DataFrame` -> Arrow `RecordBatch` builders live in
// `crate::io::write::arrow_convert` so they remain available when the `delta`
// feature is disabled (the DuckDB sink reuses them). This module keeps only the
// Delta-specific `SaveMode` mapping.
pub(crate) fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    match mode {
        config::WriteMode::Overwrite => SaveMode::Overwrite,
        config::WriteMode::Append => SaveMode::Append,
        config::WriteMode::MergeScd1 => SaveMode::Overwrite,
        config::WriteMode::MergeScd2 => SaveMode::Overwrite,
    }
}
