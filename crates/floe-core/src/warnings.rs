pub fn emit(message: &str) {
    eprintln!("warn: {message}");
}

pub fn emit_once(flag: &mut bool, message: &str) {
    if !*flag {
        emit(message);
        *flag = true;
    }
}
