use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// SIGKILL bypasses Drop, so kill_on_drop alone can't stop a child (btrfs send / split)
// from outliving a killed bcb process; PR_SET_PDEATHSIG asks the kernel to do it instead.
#[cfg(target_os = "linux")]
pub fn die_with_parent() -> std::io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn compact_to_iso(s: &str) -> Result<String> {
    if s.len() == 12 && s.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!(
            "{}-{}-{}T{}:{}:00Z",
            &s[0..4],
            &s[4..6],
            &s[6..8],
            &s[8..10],
            &s[10..12]
        ))
    } else if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!("{}-{}-{}T00:00:00Z", &s[0..4], &s[4..6], &s[6..8]))
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

pub fn task_dir_name(level: u32, timestamp: &str) -> Result<String> {
    if timestamp.len() < 16 {
        return Err(anyhow!(
            "timestamp must be in ISO 8601 format (YYYY-MM-DDTHH:MM...)"
        ));
    }
    let date_no_dash = timestamp[0..10].replace('-', "");
    let time_no_colon = timestamp[11..16].replace(':', "");
    Ok(format!(
        "bcb_level{}_{}{}",
        level, date_no_dash, time_no_colon
    ))
}

pub fn s3_backup_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
    index: &str,
) -> Result<String> {
    let dir_name = task_dir_name(level, timestamp)?;
    let key = format!(
        "{}backup-data/{}/{}/snapshot.part-{}.age",
        prefix, sanitized_subvol, dir_name, index
    );
    Ok(key)
}

pub fn s3_manifest_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
) -> Result<String> {
    let dir_name = task_dir_name(level, timestamp)?;
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
) -> Result<PathBuf> {
    let dir_name = task_dir_name(level, timestamp)?;
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
        let result = task_dir_name(0, "2026-06-02T14:30:00Z").unwrap();
        assert_eq!(result, "bcb_level0_202606021430");
    }

    #[test]
    fn test_task_dir_name_level_one() {
        let result = task_dir_name(1, "2026-06-02T14:30:00Z").unwrap();
        assert_eq!(result, "bcb_level1_202606021430");
    }

    #[test]
    fn test_task_dir_name_different_date() {
        let result = task_dir_name(2, "2025-12-25T10:15:30Z").unwrap();
        assert_eq!(result, "bcb_level2_202512251015");
    }

    #[test]
    fn test_task_dir_name_same_day_different_time() {
        let a = task_dir_name(0, "2026-06-25T08:00:00Z").unwrap();
        let b = task_dir_name(0, "2026-06-25T20:00:00Z").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_task_dir_name_empty_timestamp() {
        let result = task_dir_name(0, "");
        assert!(result.is_err());
    }

    #[test]
    fn test_task_dir_name_invalid_timestamp() {
        let result = task_dir_name(0, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_backup_key() {
        let key =
            s3_backup_key("backups/", "mnt_data", 0, "2026-06-02T14:30:00Z", "000000").unwrap();
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
            "2026-06-02T15:00:00Z",
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
        let key = s3_manifest_key("backups/", "mnt_data", 0, "2026-06-02T14:30:00Z").unwrap();
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
        assert_eq!(
            compact_to_iso("202606021430").unwrap(),
            "2026-06-02T14:30:00Z"
        );
    }

    #[test]
    fn test_compact_to_iso_8() {
        assert_eq!(compact_to_iso("20260602").unwrap(), "2026-06-02T00:00:00Z");
    }

    #[test]
    fn test_compact_to_iso_invalid() {
        assert!(compact_to_iso("invalid").is_err());
        assert!(compact_to_iso("2026060214").is_err());
    }

    #[test]
    fn test_s3_keys_no_path_collision() {
        let backup_key =
            s3_backup_key("prefix/", "sub", 0, "2026-06-02T14:30:00Z", "000000").unwrap();
        let manifest_key = s3_manifest_key("prefix/", "sub", 0, "2026-06-02T14:30:00Z").unwrap();
        let last_key = s3_last_manifest_key("prefix/", "sub");

        assert_ne!(backup_key, manifest_key);
        assert_ne!(backup_key, last_key);
        assert_ne!(manifest_key, last_key);
        assert!(backup_key.contains("backup-data"));
        assert!(manifest_key.contains("manifests"));
        assert!(last_key.contains("manifests"));
    }
}
