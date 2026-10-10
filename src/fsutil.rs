//! Filesystem helpers: durability, free space, and base_dir setup.

use anyhow::{Context, Result, anyhow};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Makes renames, creations and deletions of entries in `dir` durable.
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Replaces `path` so that after a crash it holds either the old or the new content in full.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fsync_dir(parent)
    };
    write().with_context(|| format!("failed to write {path:?}"))
}

/// Creates base_dir and restricts it to its owner, since it holds decrypted restore data.
/// Applied on every run, so a pre-existing directory is tightened too.
pub fn prepare_base_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path)
        .and_then(|()| std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)))
        .with_context(|| format!("failed to prepare base_dir {path:?}"))
}

/// Free space available to unprivileged users on the filesystem containing `path`, in bytes.
pub fn available_space_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow!("path {path:?} is not a valid C string: {e}"))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_path is a valid, NUL-terminated C string for its lifetime, and `stat` is a
    // plain-old-data struct that statvfs(3) fully initializes on success.
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("statvfs failed for {path:?}"));
    }
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

/// The device of `path`, or of its nearest existing ancestor if it doesn't exist yet.
pub fn device_of(path: &Path) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;
    let path = std::path::absolute(path)?;
    let existing = path
        .ancestors()
        .find(|p| p.exists())
        .ok_or_else(|| anyhow!("no existing ancestor of {path:?}"))?;
    Ok(std::fs::metadata(existing)?.dev())
}

/// Disk usage of `path` as `du -sb` reports it.
pub async fn disk_usage_bytes(path: &Path) -> Result<u64> {
    let output = tokio::process::Command::new("du")
        .arg("-sb")
        .arg(path)
        .output()
        .await
        .with_context(|| format!("failed to run du on {path:?}"))?;
    if !output.status.success() {
        return Err(anyhow!(
            "du failed for {path:?}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let size = stdout
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("unexpected du output for {path:?}: {stdout:?}"))?;
    size.parse()
        .map_err(|e| anyhow!("failed to parse du output '{size}' for {path:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_atomic_should_replace_content_and_leave_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.yaml");
        write_atomic(&path, b"old").unwrap();
        write_atomic(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn prepare_base_dir_should_tighten_existing_dir_to_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("bcb");
        std::fs::create_dir(&base).unwrap();
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o755)).unwrap();

        prepare_base_dir(&base).unwrap();
        let mode = std::fs::metadata(&base).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn available_space_should_be_positive_for_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(available_space_bytes(dir.path()).unwrap() > 0);
    }

    #[test]
    fn available_space_should_fail_for_missing_path() {
        assert!(available_space_bytes(Path::new("/nonexistent/path/for/bcb/test")).is_err());
    }

    #[test]
    fn device_of_missing_path_should_use_existing_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            device_of(&dir.path().join("not/yet/created")).unwrap(),
            device_of(dir.path()).unwrap()
        );
    }
}
