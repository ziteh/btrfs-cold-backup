use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Filename prefix of split stream parts, both locally and in S3 keys.
pub const SNAPSHOT_PART_PREFIX: &str = "snapshot.part-";

/// Buffer size for streaming file/pipe IO.
pub const IO_BUF_SIZE: usize = 64 * 1024;

/// Bytes per second per 1 kbps (kilobit/s = 1000 bits/s / 8 bits per byte).
pub const BYTES_PER_KBPS: f64 = 1000.0 / 8.0;

// SIGKILL bypasses Drop, so kill_on_drop alone can't stop a child (btrfs send / split)
// from outliving a killed bcb process; PR_SET_PDEATHSIG asks the kernel to do it instead.
#[cfg(target_os = "linux")]
pub fn die_with_parent() -> std::io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Returns the free space available to unprivileged users on the filesystem containing
/// `path`, in bytes.
pub fn available_space_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow!("path {path:?} is not a valid C string: {e}"))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_path is a valid, NUL-terminated C string for its lifetime, and `stat` is a
    // plain-old-data struct that statvfs(3) fully initializes on success.
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return Err(anyhow!(
            "statvfs failed for {path:?}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

/// Makes renames, creations and deletions of entries in `dir` durable.
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Replaces `path` so that after a crash it holds either the old or the new content in full.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fsync_dir(parent)?;
    Ok(())
}

/// Creates base_dir and restricts it to its owner: it holds decrypted restore data and
/// plaintext split parts. Applied on every run, so a pre-existing directory is tightened too.
pub fn prepare_base_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_secs() as i64
}

/// Parses a compact date/time into ISO 8601 (`YYYY-MM-DDTHH:MM`, local time, no zone).
pub fn compact_to_iso(s: &str) -> Result<String> {
    // btrbk appends `_<n>` when it creates two snapshots of the same subvolume within one
    // minute. bcb does not back those up (see snapshot_timestamp), so there is never a
    // backup addressable by such a date; say so rather than blaming the date format.
    if let Some((_, counter)) = s.split_once('_')
        && !counter.is_empty()
        && counter.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(anyhow!(
            "'{s}' names a btrbk same-minute snapshot, which bcb does not back up, \
             so no backup exists under that date"
        ));
    }

    if s.len() == 12 && s.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!(
            "{}-{}-{}T{}:{}",
            &s[0..4],
            &s[4..6],
            &s[6..8],
            &s[8..10],
            &s[10..12]
        ))
    } else if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!("{}-{}-{}T00:00", &s[0..4], &s[4..6], &s[6..8]))
    } else {
        Err(anyhow!("date must be YYYYMMDD or YYYYMMDDHHMM, got: {s}"))
    }
}

pub fn sanitize_subvolume_path(subvolume: &Path) -> Result<String> {
    let path_str = subvolume
        .to_str()
        .ok_or_else(|| anyhow!("subvolume path is not valid UTF-8"))?;

    if path_str.is_empty() {
        return Err(anyhow!("subvolume path cannot be empty"));
    }

    let sanitized = path_str.trim_matches('/').replace(['/', '\\'], "_");

    if sanitized.is_empty() {
        return Err(anyhow!(
            "subvolume path sanitization resulted in empty string"
        ));
    }

    Ok(sanitized)
}

/// Builds the directory/key component identifying one backup, from an ISO 8601 timestamp.
pub fn task_dir_name(level: u32, timestamp: &str) -> String {
    // The first 12 digits of an ISO 8601 timestamp are exactly YYYYMMDDHHMM.
    let compact: String = timestamp
        .chars()
        .filter(char::is_ascii_digit)
        .take(12)
        .collect();
    format!("bcb_level{level}_{compact}")
}

/// Inverse of [`task_dir_name`]: extracts (level, ISO timestamp) from a task dir name.
pub fn parse_task_dir_name(dir: &str) -> Option<(u32, String)> {
    let parts: Vec<&str> = dir.split('_').collect();
    let [_, level_str, compact_ts] = parts.as_slice() else {
        return None;
    };
    let level: u32 = level_str.strip_prefix("level")?.parse().ok()?;
    let timestamp = compact_to_iso(compact_ts).ok()?;
    Some((level, timestamp))
}

pub fn s3_backup_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
    index: &str,
) -> String {
    let dir_name = task_dir_name(level, timestamp);
    format!("{prefix}backup-data/{sanitized_subvol}/{dir_name}/{SNAPSHOT_PART_PREFIX}{index}.age")
}

pub fn s3_manifest_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
) -> String {
    let dir_name = task_dir_name(level, timestamp);
    format!("{prefix}manifests/{sanitized_subvol}/{dir_name}/manifest.yaml")
}

pub fn state_path(base_dir: &Path, sanitized_subvol: &str) -> PathBuf {
    base_dir
        .join("run")
        .join(sanitized_subvol)
        .join("backup_state.yaml")
}

pub fn backup_output_dir(
    base_dir: &Path,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
) -> PathBuf {
    let dir_name = task_dir_name(level, timestamp);
    base_dir.join("task").join(sanitized_subvol).join(dir_name)
}

pub fn s3_latest_manifest_key(prefix: &str, sanitized_subvol: &str) -> String {
    format!("{prefix}manifests/{sanitized_subvol}/latest.yaml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_subvolume_path_basic() {
        let result = sanitize_subvolume_path(Path::new("/mnt/data")).unwrap();
        assert_eq!(result, "mnt_data");
    }

    #[test]
    fn test_sanitize_subvolume_path_trailing_slash() {
        let result = sanitize_subvolume_path(Path::new("/mnt/data/")).unwrap();
        assert_eq!(result, "mnt_data");
    }

    #[test]
    fn test_sanitize_subvolume_path_leading_slash() {
        let result = sanitize_subvolume_path(Path::new("/mnt")).unwrap();
        assert_eq!(result, "mnt");
    }

    #[test]
    fn test_sanitize_subvolume_path_nested() {
        let result = sanitize_subvolume_path(Path::new("/mnt/data/movies/2024")).unwrap();
        assert_eq!(result, "mnt_data_movies_2024");
    }

    #[test]
    fn test_sanitize_subvolume_path_empty() {
        let result = sanitize_subvolume_path(Path::new(""));
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_subvolume_path_only_slashes() {
        let result = sanitize_subvolume_path(Path::new("///"));
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_subvolume_path_dot_vs_underscore_no_collision() {
        let a = sanitize_subvolume_path(Path::new("/mnt/data.1")).unwrap();
        let b = sanitize_subvolume_path(Path::new("/mnt/data_1")).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_task_dir_name_level_zero() {
        let result = task_dir_name(0, "2026-06-02T14:30");
        assert_eq!(result, "bcb_level0_202606021430");
    }

    #[test]
    fn test_task_dir_name_level_one() {
        let result = task_dir_name(1, "2026-06-02T14:30");
        assert_eq!(result, "bcb_level1_202606021430");
    }

    #[test]
    fn test_task_dir_name_different_date() {
        let result = task_dir_name(2, "2025-12-25T10:15");
        assert_eq!(result, "bcb_level2_202512251015");
    }

    #[test]
    fn test_task_dir_name_same_day_different_time() {
        let a = task_dir_name(0, "2026-06-25T08:00");
        let b = task_dir_name(0, "2026-06-25T20:00");
        assert_ne!(a, b);
    }

    #[test]
    fn test_task_dir_name_parse_round_trip() {
        let dir = task_dir_name(3, "2026-06-02T14:30");
        let (level, timestamp) = parse_task_dir_name(&dir).unwrap();
        assert_eq!(level, 3);
        assert_eq!(timestamp, "2026-06-02T14:30");
    }

    #[test]
    fn test_parse_task_dir_name_invalid() {
        assert!(parse_task_dir_name("bcb").is_none());
        assert!(parse_task_dir_name("bcb_levelX_202606021430").is_none());
        assert!(parse_task_dir_name("bcb_level0_notadate").is_none());
    }

    #[test]
    fn test_parse_task_dir_name_rejects_trailing_segment() {
        assert!(parse_task_dir_name("bcb_level0_202606021430_1").is_none());
    }

    #[test]
    fn test_s3_backup_key() {
        let key = s3_backup_key("backups/", "mnt_data", 0, "2026-06-02T14:30", "000000");
        assert_eq!(
            key,
            "backups/backup-data/mnt_data/bcb_level0_202606021430/snapshot.part-000000.age"
        );
    }

    #[test]
    fn test_s3_backup_key_multiple_parts() {
        let key = s3_backup_key(
            "backups/",
            "mnt_data_movies",
            1,
            "2026-06-02T15:00",
            "000004",
        );
        assert_eq!(
            key,
            "backups/backup-data/mnt_data_movies/bcb_level1_202606021500/snapshot.part-000004.age"
        );
    }

    #[test]
    fn test_s3_manifest_key() {
        let key = s3_manifest_key("backups/", "mnt_data", 0, "2026-06-02T14:30");
        assert_eq!(
            key,
            "backups/manifests/mnt_data/bcb_level0_202606021430/manifest.yaml"
        );
    }

    #[test]
    fn test_s3_latest_manifest_key() {
        let key = s3_latest_manifest_key("backups/", "mnt_data");
        assert_eq!(key, "backups/manifests/mnt_data/latest.yaml");
    }

    #[test]
    fn test_compact_to_iso_12() {
        assert_eq!(compact_to_iso("202606021430").unwrap(), "2026-06-02T14:30");
    }

    #[test]
    fn test_compact_to_iso_8() {
        assert_eq!(compact_to_iso("20260602").unwrap(), "2026-06-02T00:00");
    }

    #[test]
    fn test_compact_to_iso_same_minute_snapshot_error_is_specific() {
        let err = compact_to_iso("202606021430_1").unwrap_err().to_string();
        assert!(err.contains("same-minute"), "unhelpful message: {err}");
    }

    #[test]
    fn test_compact_to_iso_invalid() {
        assert!(compact_to_iso("invalid").is_err());
        assert!(compact_to_iso("2026060214").is_err());
    }

    #[test]
    fn test_s3_keys_no_path_collision() {
        let backup_key = s3_backup_key("prefix/", "sub", 0, "2026-06-02T14:30", "000000");
        let manifest_key = s3_manifest_key("prefix/", "sub", 0, "2026-06-02T14:30");
        let latest_key = s3_latest_manifest_key("prefix/", "sub");

        assert_ne!(backup_key, manifest_key);
        assert_ne!(backup_key, latest_key);
        assert_ne!(manifest_key, latest_key);
        assert!(backup_key.contains("backup-data"));
        assert!(manifest_key.contains("manifests"));
        assert!(latest_key.contains("manifests"));
    }

    #[test]
    fn test_write_atomic_replaces_content_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.yaml");
        write_atomic(&path, b"old").unwrap();
        write_atomic(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn test_prepare_base_dir_tightens_existing_dir() {
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
    fn test_available_space_bytes_returns_plausible_value() {
        let dir = tempfile::tempdir().unwrap();
        let available = available_space_bytes(dir.path()).unwrap();
        assert!(available > 0);
    }

    #[test]
    fn test_available_space_bytes_missing_path_is_err() {
        assert!(available_space_bytes(Path::new("/nonexistent/path/for/bcb/test")).is_err());
    }
}
