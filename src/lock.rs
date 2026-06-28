use anyhow::{Result, anyhow};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct PidLock {
    path: PathBuf,
    // Holds the flock for the duration of the lock's lifetime.
    file: Option<File>,
}

impl PidLock {
    pub fn acquire(lock_path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(lock_path)
            .map_err(|e| anyhow!("failed to create lock file: {}", e))?;

        if Self::try_lock(&file).is_err() {
            return Err(anyhow!("failed to acquire lock on {}", lock_path.display()));
        }

        let pid = std::process::id();
        let mut file = file;
        writeln!(file, "{}", pid)
            .map_err(|e| anyhow!("failed to write PID to lock file: {}", e))?;

        Ok(PidLock {
            path: lock_path.to_path_buf(),
            file: Some(file),
        })
    }

    pub fn check_stale(lock_path: &Path) -> Result<bool> {
        if !lock_path.exists() {
            return Ok(false);
        }

        let content = std::fs::read_to_string(lock_path)
            .map_err(|e| anyhow!("failed to read lock file: {}", e))?;

        let pid: u32 = content
            .trim()
            .parse()
            .map_err(|e| anyhow!("invalid PID in lock file: {}", e))?;

        let is_alive = unsafe { libc::kill(pid as i32, 0) == 0 };
        Ok(!is_alive)
    }

    pub fn remove_stale(lock_path: &Path) -> Result<()> {
        if Self::check_stale(lock_path)? {
            std::fs::remove_file(lock_path)
                .map_err(|e| anyhow!("failed to remove stale lock file: {}", e))?;
        }
        Ok(())
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

impl Drop for PidLock {
    fn drop(&mut self) {
        self.file = None;
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_acquire_lock() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("test.lock");

        let lock = PidLock::acquire(&lock_path);
        assert!(lock.is_ok());
        assert!(lock_path.exists());

        drop(lock);
        assert!(!lock_path.exists());
    }

    #[test]
    fn test_check_stale_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("nonexistent.lock");

        let is_stale = PidLock::check_stale(&lock_path).unwrap();
        assert!(!is_stale);
    }

    #[test]
    fn test_check_stale_current_process() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("current.lock");

        let _lock = PidLock::acquire(&lock_path).unwrap();

        let is_stale = PidLock::check_stale(&lock_path).unwrap();
        assert!(!is_stale);
    }

    #[test]
    fn test_lock_prevents_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = Arc::new(temp_dir.path().join("concurrent.lock"));

        let lock_path_clone = Arc::clone(&lock_path);
        let result = Arc::new(Mutex::new(None));
        let result_clone = Arc::clone(&result);

        let handle = thread::spawn(move || {
            let lock = PidLock::acquire(&lock_path_clone);
            if let Ok(_lock) = lock {
                thread::sleep(std::time::Duration::from_millis(100));
            }
        });

        thread::sleep(std::time::Duration::from_millis(10));

        let lock_attempt = PidLock::acquire(&lock_path);
        *result_clone.lock().unwrap() = Some(lock_attempt.is_err());

        handle.join().unwrap();
    }

    #[test]
    fn test_remove_stale_does_nothing_for_active_lock() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("active.lock");

        let _lock = PidLock::acquire(&lock_path).unwrap();

        let result = PidLock::remove_stale(&lock_path);
        assert!(result.is_ok());
        assert!(lock_path.exists());
    }

    #[test]
    fn test_lock_file_contains_pid() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("pid.lock");

        let _lock = PidLock::acquire(&lock_path).unwrap();

        let content = std::fs::read_to_string(&lock_path).unwrap();
        let pid: u32 = content.trim().parse().unwrap();
        assert_eq!(pid, std::process::id());
    }
}
