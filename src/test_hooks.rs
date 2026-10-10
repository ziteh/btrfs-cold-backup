//! Hooks the VM tests use to steer a debug build of bcb through environment variables: pause
//! at a fixed point, so a test can SIGKILL bcb exactly there instead of racing on timing, or
//! shrink a size, so small fixtures exercise multi-part paths. Release builds compile them out.
//!
//! A paused bcb prints `BCB_TEST_PAUSE_REACHED:<marker>` to stderr, which the tests wait for.

#[cfg(debug_assertions)]
async fn park(marker: &str) {
    use std::io::Write;
    eprintln!("BCB_TEST_PAUSE_REACHED:{marker}");
    let _ = std::io::stderr().flush();
    std::future::pending::<()>().await;
}

/// Parks forever if env var `var` is set.
#[cfg(debug_assertions)]
pub async fn pause_if_set(var: &str, marker: &str) {
    if std::env::var_os(var).is_some() {
        park(marker).await;
    }
}

/// Parks forever if env var `var` holds `value`; the marker is `<kind>:<value>`.
#[cfg(debug_assertions)]
pub async fn pause_at(var: &str, kind: &str, value: u64) {
    if std::env::var(var).is_ok_and(|v| v.parse() == Ok(value)) {
        park(&format!("{kind}:{value}")).await;
    }
}

/// The byte count in env var `var`, or `default` if it is unset. A value that doesn't parse
/// panics instead of falling back, so a test can't silently run with the real size.
#[cfg(debug_assertions)]
pub fn size_override(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .map(|v| {
            v.parse()
                .unwrap_or_else(|_| panic!("{var} must be a byte count"))
        })
        .unwrap_or(default)
}

#[cfg(not(debug_assertions))]
pub async fn pause_if_set(_var: &str, _marker: &str) {}

#[cfg(not(debug_assertions))]
pub async fn pause_at(_var: &str, _kind: &str, _value: u64) {}

#[cfg(not(debug_assertions))]
pub fn size_override(_var: &str, default: u64) -> u64 {
    default
}
