use std::time::{SystemTime, UNIX_EPOCH};

/// Buffer size for streaming file and pipe IO.
pub const IO_BUF_SIZE: usize = 64 * 1024;

pub const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// Bytes per second in one kilobit per second.
pub const BYTES_PER_KBPS: f64 = 1000.0 / 8.0;

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_secs() as i64
}
