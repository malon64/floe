use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_CONFIG_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn write_temp_config(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TEMP_CONFIG_SEQ.fetch_add(1, Ordering::Relaxed);
    path.push(format!("floe-config-{nanos}-{seq}.yml"));
    fs::write(&path, contents).expect("write temp config");
    path
}
