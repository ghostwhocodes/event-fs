// Minimal local replacement for the upstream `nuid` crate used by `nats`.
// It keeps the same public API surface (next + NUID type) but uses
// simple time/sequence based generation instead of rand.

#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const TOTAL_LEN: usize = 22;

lazy_static! {
    static ref GLOBAL_NUID: Mutex<NUID> = Mutex::new(NUID::new());
}

/// Generate the next `NUID` string from the global locked `NUID` instance.
pub fn next() -> String {
    GLOBAL_NUID.lock().unwrap().next()
}

/// Simple time + counter based NUID.
pub struct NUID {
    counter: u64,
}

impl Default for NUID {
    fn default() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);
        let c = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self { counter: nanos ^ c }
    }
}

impl NUID {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate the next `NUID` string.
    pub fn next(&mut self) -> String {
        self.counter = self.counter.wrapping_add(1);
        // Encode as fixed-width base16 string to keep deterministic length.
        format!("{:0width$x}", self.counter, width = TOTAL_LEN)
    }
}

