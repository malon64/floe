use crate::errors::RunError;
use crate::FloeResult;

#[allow(dead_code)]
pub(crate) fn not_implemented() -> FloeResult<()> {
    Err(Box::new(RunError(
        "write_mode=merge_scd2 is not implemented yet".to_string(),
    )))
}
