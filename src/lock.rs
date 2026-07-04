use anyhow::{Result, anyhow};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

// Held for the lifetime of the struct via flock; the kernel releases the lock when the
// process exits (even on SIGKILL), so no stale-lock detection or cleanup is needed. The
// lock file itself is left behind on purpose: deleting it by path would race with another
// process that has already reopened it.
pub struct PidLock {
    _file: File,
}

impl PidLock {
    pub fn acquire(lock_path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| anyhow!("failed to open lock file: {e}"))?;

        if Self::try_lock(&file).is_err() {
            return Err(anyhow!(
                "another instance is already running (lock held on {})",
                lock_path.display()
            ));
        }

        // Truncate only after the flock is ours, so a losing contender can't wipe the
        // holder's PID. The PID is informational, for operators inspecting the file.
        file.set_len(0)
            .map_err(|e| anyhow!("failed to truncate lock file: {e}"))?;
        writeln!(file, "{}", std::process::id())
            .map_err(|e| anyhow!("failed to write PID to lock file: {e}"))?;

        Ok(PidLock { _file: file })
    }

    fn try_lock(file: &File) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let fd = file.as_raw_fd();
            if libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
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

        let lock = PidLock::acquire(&lock_path);
        assert!(lock.is_ok());
        assert!(lock_path.exists());
    }

    #[test]
    fn test_second_acquire_fails_while_held() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("held.lock");

        let _lock = PidLock::acquire(&lock_path).unwrap();
        assert!(PidLock::acquire(&lock_path).is_err());
    }

    #[test]
    fn test_reacquire_after_release() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("release.lock");

        let lock = PidLock::acquire(&lock_path).unwrap();
        drop(lock);
        assert!(PidLock::acquire(&lock_path).is_ok());
    }

    #[test]
    fn test_losing_contender_does_not_clobber_pid() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("pid.lock");

        let _lock = PidLock::acquire(&lock_path).unwrap();
        let _ = PidLock::acquire(&lock_path);

        let content = std::fs::read_to_string(&lock_path).unwrap();
        let pid: u32 = content.trim().parse().unwrap();
        assert_eq!(pid, std::process::id());
    }
}
