use crate::io::format::AcceptedWriteMetrics;

pub const BYTES_PER_MIB: f64 = 1024.0 * 1024.0;
pub const DEFAULT_SMALL_FILE_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;

pub fn default_small_file_threshold_bytes(target_file_size_bytes: Option<u64>) -> u64 {
    match target_file_size_bytes {
        Some(target) if target > 1 => std::cmp::max(1, target / 2),
        Some(_) => 1,
        None => DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
    }
}

pub fn summarize_written_file_sizes(
    file_sizes: &[u64],
    small_file_threshold_bytes: u64,
) -> AcceptedWriteMetrics {
    if file_sizes.is_empty() {
        return AcceptedWriteMetrics {
            total_bytes_written: None,
            avg_file_size_mb: None,
            small_files_count: None,
        };
    }

    let total_bytes_written = file_sizes.iter().copied().sum::<u64>();
    let avg_file_size_mb = (total_bytes_written as f64 / file_sizes.len() as f64) / BYTES_PER_MIB;
    let small_files_count = file_sizes
        .iter()
        .filter(|size| **size < small_file_threshold_bytes)
        .count() as u64;

    AcceptedWriteMetrics {
        total_bytes_written: Some(total_bytes_written),
        avg_file_size_mb: Some(avg_file_size_mb),
        small_files_count: Some(small_files_count),
    }
}
