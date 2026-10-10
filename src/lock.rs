use anyhow::{Context, Result, anyhow};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::Path;

/// Keeps other bcb processes from running while it is held.
///
/// The lock is an flock on the lock file, which the kernel releases when the process exits,
/// even on SIGKILL, so no stale-lock detection is needed. The file itself is left behind on
/// purpose: deleting it by path would race with another process that has already opened it.
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    pub fn acquire(lock_path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .with_context(|| format!("failed to open lock file {lock_path:?}"))?;

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(anyhow!(
                    "another instance is already running (lock held on {lock_path:?})"
                ));
            }
            Err(TryLockError::Error(e)) => {
                return Err(e).with_context(|| format!("failed to lock {lock_path:?}"));
            }
        }

        // Truncated only once the lock is ours, so a losing contender can't wipe the holder's
        // PID. The PID is for operators inspecting the file.
        file.set_len(0)
            .and_then(|()| writeln!(file, "{}", std::process::id()))
            .with_context(|| format!("failed to write PID to {lock_path:?}"))?;

        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_acquire_lock() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("test.lock");

        assert!(InstanceLock::acquire(&lock_path).is_ok());
        assert!(lock_path.exists());
    }

    #[test]
    fn test_second_acquire_fails_while_held() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("held.lock");

        let _lock = InstanceLock::acquire(&lock_path).unwrap();
        assert!(InstanceLock::acquire(&lock_path).is_err());
    }

    #[test]
    fn test_reacquire_after_release() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("release.lock");

        drop(InstanceLock::acquire(&lock_path).unwrap());
        assert!(InstanceLock::acquire(&lock_path).is_ok());
    }

    #[test]
    fn test_losing_contender_does_not_clobber_pid() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("pid.lock");

        let _lock = InstanceLock::acquire(&lock_path).unwrap();
        let _ = InstanceLock::acquire(&lock_path);

        let content = std::fs::read_to_string(&lock_path).unwrap();
        let pid: u32 = content.trim().parse().unwrap();
        assert_eq!(pid, std::process::id());
    }
}
