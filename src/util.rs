use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

pub fn sanitize_subvolume_path(subvolume: &Path) -> Result<String> {
    let path_str = subvolume
        .to_str()
        .ok_or_else(|| anyhow!("subvolume path is not valid UTF-8"))?;

    if path_str.is_empty() {
        return Err(anyhow!("subvolume path cannot be empty"));
    }

    // Remove leading/trailing slashes and convert remaining slashes to underscores
    let sanitized = path_str
        .trim_matches('/')
        .replace('/', "_")
        .replace("\\", "_")
        .replace(".", "_");

    if sanitized.is_empty() {
        return Err(anyhow!(
            "subvolume path sanitization resulted in empty string"
        ));
    }

    Ok(sanitized)
}

pub fn task_dir_name(level: u32, timestamp: &str) -> Result<String> {
    if timestamp.is_empty() {
        return Err(anyhow!("timestamp cannot be empty"));
    }
    const TARGET_LEN: usize = "YYYY-MM-DD".len();
    if timestamp.len() < TARGET_LEN {
        return Err(anyhow!("timestamp must be in ISO 8601 format"));
    }

    let date_part = &timestamp[0..10];
    let date_no_dash = date_part.replace("-", "");

    Ok(format!("bcr_level{}_{}", level, date_no_dash))
}

pub fn s3_backup_key(
    prefix: &str,
    sanitized_subvol: &str,
    level: u32,
    timestamp: &str,
    part_number: u32,
) -> Result<String> {
    let dir_name = task_dir_name(level, timestamp)?;
    let key = format!(
        "{}backup-data/{}/{}/snapshot.part-{}.age",
        prefix, sanitized_subvol, dir_name, part_number
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

pub fn snapshot_name(_level: u32, date_str: &str, time_str: &str) -> Result<String> {
    if date_str.is_empty() || time_str.is_empty() {
        return Err(anyhow!("date_str and time_str cannot be empty"));
    }

    // Validate date format
    const DATE_LEN: usize = "YYYY-MM-DD".len();
    if date_str.len() != DATE_LEN || !date_str.contains('-') {
        return Err(anyhow!("date_str must be in YYYY-MM-DD format"));
    }

    // Validate time format
    const TIME_LEN: usize = "HH-MM".len();
    if time_str.len() != TIME_LEN || !time_str.contains('-') {
        return Err(anyhow!("time_str must be in HH-MM format"));
    }

    Ok(format!("bcr_level0_{}_{}", date_str, time_str))
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
    fn test_task_dir_name_level_zero() {
        let result = task_dir_name(0, "2026-06-02T14:30:00Z").unwrap();
        assert_eq!(result, "bcr_level0_20260602");
    }

    #[test]
    fn test_task_dir_name_level_one() {
        let result = task_dir_name(1, "2026-06-02T14:30:00Z").unwrap();
        assert_eq!(result, "bcr_level1_20260602");
    }

    #[test]
    fn test_task_dir_name_different_date() {
        let result = task_dir_name(2, "2025-12-25T10:15:30Z").unwrap();
        assert_eq!(result, "bcr_level2_20251225");
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
        let key = s3_backup_key("backups/", "mnt_data", 0, "2026-06-02T14:30:00Z", 1).unwrap();
        assert_eq!(
            key,
            "backups/backup-data/mnt_data/bcr_level0_20260602/snapshot.part-1.age"
        );
    }

    #[test]
    fn test_s3_backup_key_multiple_parts() {
        let key =
            s3_backup_key("backups/", "mnt_data_movies", 1, "2026-06-02T15:00:00Z", 5).unwrap();
        assert_eq!(
            key,
            "backups/backup-data/mnt_data_movies/bcr_level1_20260602/snapshot.part-5.age"
        );
    }

    #[test]
    fn test_s3_manifest_key() {
        let key = s3_manifest_key("backups/", "mnt_data", 0, "2026-06-02T14:30:00Z").unwrap();
        assert_eq!(
            key,
            "backups/manifests/mnt_data/bcr_level0_20260602/task_manifest.yaml"
        );
    }

    #[test]
    fn test_s3_last_manifest_key() {
        let key = s3_last_manifest_key("backups/", "mnt_data");
        assert_eq!(key, "backups/manifests/mnt_data/last_backup_manifest.yaml");
    }

    #[test]
    fn test_snapshot_name() {
        let name = snapshot_name(0, "2026-06-02", "14-30").unwrap();
        assert_eq!(name, "bcr_level0_2026-06-02_14-30");
    }

    #[test]
    fn test_snapshot_name_empty_date() {
        let result = snapshot_name(0, "", "14-30");
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_name_empty_time() {
        let result = snapshot_name(0, "2026-06-02", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_name_invalid_date_format() {
        let result = snapshot_name(0, "2026/06/02", "14-30");
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_name_invalid_time_format() {
        let result = snapshot_name(0, "2026-06-02", "14:30");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_keys_no_path_collision() {
        let backup_key = s3_backup_key("prefix/", "sub", 0, "2026-06-02T14:30:00Z", 1).unwrap();
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
