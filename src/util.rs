use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Filename prefix of split stream parts, both locally and in S3 keys.
pub const SNAPSHOT_PART_PREFIX: &str = "snapshot.part-";

/// Buffer size for streaming file/pipe IO.
pub const IO_BUF_SIZE: usize = 64 * 1024;

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
        .map_err(|e| anyhow!("path {:?} is not a valid C string: {e}", path))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_path is a valid, NUL-terminated C string for its lifetime, and `stat` is a
    // plain-old-data struct that statvfs(3) fully initializes on success.
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return Err(anyhow!(
            "statvfs failed for {:?}: {}",
            path,
            std::io::Error::last_os_error()
        ));
    }
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_secs() as i64
}

/// Parses a compact date/time into ISO 8601, splitting off an optional trailing
/// `_<digits>` collision-counter suffix (btrbk's same-minute snapshot counter) so a
/// specific same-minute snapshot stays addressable as a distinct backup.
pub fn compact_to_iso(s: &str) -> Result<(String, Option<String>)> {
    let (base, suffix) = match s.split_once('_') {
        Some((base, suf)) if !suf.is_empty() && suf.bytes().all(|b| b.is_ascii_digit()) => {
            (base, Some(suf.to_string()))
        }
        _ => (s, None),
    };

    let iso = if base.len() == 12 && base.chars().all(|c| c.is_ascii_digit()) {
        format!(
            "{}-{}-{}T{}:{}:00Z",
            &base[0..4],
            &base[4..6],
            &base[6..8],
            &base[8..10],
            &base[10..12]
        )
    } else if base.len() == 8 && base.chars().all(|c| c.is_ascii_digit()) {
        format!("{}-{}-{}T00:00:00Z", &base[0..4], &base[4..6], &base[6..8])
    } else {
        return Err(anyhow!("date must be YYYYMMDD or YYYYMMDDHHMM, got: {s}"));
    };

    Ok((iso, suffix))
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

pub fn task_dir_name(level: u32, timestamp: &str, suffix: Option<&str>) -> Result<String> {
    if timestamp.len() < 16 {
        return Err(anyhow!(
            "timestamp must be in ISO 8601 format (YYYY-MM-DDTHH:MM...)"
        ));
    }
    let date_no_dash = timestamp[0..10].replace('-', "");
    let time_no_colon = timestamp[11..16].replace(':', "");
    let mut name = format!("bcb_level{}_{}{}", level, date_no_dash, time_no_colon);
    if let Some(suffix) = suffix {
        name.push('_');
        name.push_str(suffix);
    }
    Ok(name)
}

/// Inverse of [`task_dir_name`]: extracts (level, ISO timestamp) from a task dir name.
pub fn parse_task_dir_name(dir: &str) -> Option<(u32, String)> {
    let parts: Vec<&str> = dir.split('_').collect();
    let [_, level_str, compact_ts, ..] = parts.as_slice() else {
        return None;
    };
    let level: u32 = level_str.strip_prefix("level")?.parse().ok()?;
    let (timestamp, _) = compact_to_iso(compact_ts).ok()?;
    Some((level, timestamp))
}

pub fn s3_backup_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
    suffix: Option<&str>,
    index: &str,
) -> Result<String> {
    let dir_name = task_dir_name(level, timestamp, suffix)?;
    let key = format!(
        "{prefix}backup-data/{sanitized_subvol}/{dir_name}/{SNAPSHOT_PART_PREFIX}{index}.age"
    );
    Ok(key)
}

pub fn s3_manifest_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
    suffix: Option<&str>,
) -> Result<String> {
    let dir_name = task_dir_name(level, timestamp, suffix)?;
    let key = format!(
        "{}manifests/{}/{}/task_manifest.yaml",
        prefix, sanitized_subvol, dir_name
    );
    Ok(key)
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
    suffix: Option<&str>,
) -> Result<PathBuf> {
    let dir_name = task_dir_name(level, timestamp, suffix)?;
    Ok(base_dir.join("task").join(sanitized_subvol).join(dir_name))
}

pub fn s3_last_manifest_key(prefix: &str, sanitized_subvol: &str) -> String {
    format!(
        "{}manifests/{}/last_backup_manifest.yaml",
        prefix, sanitized_subvol
    )
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
        let result = task_dir_name(0, "2026-06-02T14:30:00Z", None).unwrap();
        assert_eq!(result, "bcb_level0_202606021430");
    }

    #[test]
    fn test_task_dir_name_level_one() {
        let result = task_dir_name(1, "2026-06-02T14:30:00Z", None).unwrap();
        assert_eq!(result, "bcb_level1_202606021430");
    }

    #[test]
    fn test_task_dir_name_different_date() {
        let result = task_dir_name(2, "2025-12-25T10:15:30Z", None).unwrap();
        assert_eq!(result, "bcb_level2_202512251015");
    }

    #[test]
    fn test_task_dir_name_same_day_different_time() {
        let a = task_dir_name(0, "2026-06-25T08:00:00Z", None).unwrap();
        let b = task_dir_name(0, "2026-06-25T20:00:00Z", None).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_task_dir_name_parse_round_trip() {
        let dir = task_dir_name(3, "2026-06-02T14:30:00Z", None).unwrap();
        let (level, timestamp) = parse_task_dir_name(&dir).unwrap();
        assert_eq!(level, 3);
        assert_eq!(timestamp, "2026-06-02T14:30:00Z");
    }

    #[test]
    fn test_parse_task_dir_name_invalid() {
        assert!(parse_task_dir_name("bcb").is_none());
        assert!(parse_task_dir_name("bcb_levelX_202606021430").is_none());
        assert!(parse_task_dir_name("bcb_level0_notadate").is_none());
    }

    #[test]
    fn test_task_dir_name_empty_timestamp() {
        let result = task_dir_name(0, "", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_dir_name_invalid_timestamp() {
        let result = task_dir_name(0, "invalid", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_dir_name_with_collision_suffix_differs_from_without() {
        let a = task_dir_name(0, "2026-06-02T14:30:00Z", None).unwrap();
        let b = task_dir_name(0, "2026-06-02T14:30:00Z", Some("1")).unwrap();
        assert_ne!(a, b);
        assert_eq!(b, format!("{a}_1"));
    }

    #[test]
    fn test_s3_backup_key() {
        let key = s3_backup_key(
            "backups/",
            "mnt_data",
            0,
            "2026-06-02T14:30:00Z",
            None,
            "000000",
        )
        .unwrap();
        assert_eq!(
            key,
            "backups/backup-data/mnt_data/bcb_level0_202606021430/snapshot.part-000000.age"
        );
    }

    #[test]
    fn test_s3_backup_key_with_collision_suffix() {
        let key = s3_backup_key(
            "backups/",
            "mnt_data",
            0,
            "2026-06-02T14:30:00Z",
            Some("1"),
            "000000",
        )
        .unwrap();
        assert_eq!(
            key,
            "backups/backup-data/mnt_data/bcb_level0_202606021430_1/snapshot.part-000000.age"
        );
    }

    #[test]
    fn test_s3_backup_key_multiple_parts() {
        let key = s3_backup_key(
            "backups/",
            "mnt_data_movies",
            1,
            "2026-06-02T15:00:00Z",
            None,
            "000004",
        )
        .unwrap();
        assert_eq!(
            key,
            "backups/backup-data/mnt_data_movies/bcb_level1_202606021500/snapshot.part-000004.age"
        );
    }

    #[test]
    fn test_s3_manifest_key() {
        let key = s3_manifest_key("backups/", "mnt_data", 0, "2026-06-02T14:30:00Z", None).unwrap();
        assert_eq!(
            key,
            "backups/manifests/mnt_data/bcb_level0_202606021430/task_manifest.yaml"
        );
    }

    #[test]
    fn test_s3_last_manifest_key() {
        let key = s3_last_manifest_key("backups/", "mnt_data");
        assert_eq!(key, "backups/manifests/mnt_data/last_backup_manifest.yaml");
    }

    #[test]
    fn test_compact_to_iso_12() {
        let (iso, suffix) = compact_to_iso("202606021430").unwrap();
        assert_eq!(iso, "2026-06-02T14:30:00Z");
        assert!(suffix.is_none());
    }

    #[test]
    fn test_compact_to_iso_8() {
        let (iso, suffix) = compact_to_iso("20260602").unwrap();
        assert_eq!(iso, "2026-06-02T00:00:00Z");
        assert!(suffix.is_none());
    }

    #[test]
    fn test_compact_to_iso_with_collision_suffix() {
        let (iso, suffix) = compact_to_iso("202606021430_1").unwrap();
        assert_eq!(iso, "2026-06-02T14:30:00Z");
        assert_eq!(suffix.as_deref(), Some("1"));
    }

    #[test]
    fn test_compact_to_iso_invalid() {
        assert!(compact_to_iso("invalid").is_err());
        assert!(compact_to_iso("2026060214").is_err());
    }

    #[test]
    fn test_s3_keys_no_path_collision() {
        let backup_key =
            s3_backup_key("prefix/", "sub", 0, "2026-06-02T14:30:00Z", None, "000000").unwrap();
        let manifest_key =
            s3_manifest_key("prefix/", "sub", 0, "2026-06-02T14:30:00Z", None).unwrap();
        let last_key = s3_last_manifest_key("prefix/", "sub");

        assert_ne!(backup_key, manifest_key);
        assert_ne!(backup_key, last_key);
        assert_ne!(manifest_key, last_key);
        assert!(backup_key.contains("backup-data"));
        assert!(manifest_key.contains("manifests"));
        assert!(last_key.contains("manifests"));
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
