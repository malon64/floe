use crate::log::emit_log;

pub fn emit(
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    emit_log("warn", run_id, entity, input, code, message);
}

pub fn emit_once(
    flag: &mut bool,
    run_id: &str,
    entity: Option<&str>,
    input: Option<&str>,
    code: Option<&str>,
    message: &str,
) {
    if !*flag {
        emit(run_id, entity, input, code, message);
        *flag = true;
    }
}
