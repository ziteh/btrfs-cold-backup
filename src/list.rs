use anyhow::{Result, anyhow};

use crate::{
    Config, RemoteBackend, Task,
    util::{compact_to_iso, sanitize_subvolume_path},
};

pub struct BackupEntry {
    pub level: u32,
    pub timestamp: String,
}

fn parse_entry(key: &str) -> Option<BackupEntry> {
    let dir = key.split('/').find(|s| s.starts_with("bcb_level"))?;
    let parts: Vec<&str> = dir.split('_').collect();
    let [_, level_str, compact_ts, ..] = parts.as_slice() else {
        return None;
    };
    let level: u32 = level_str.strip_prefix("level")?.parse().ok()?;
    let timestamp = compact_to_iso(compact_ts).ok()?;
    Some(BackupEntry { level, timestamp })
}

pub async fn list_backups<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    remote: &R,
) -> Result<Vec<BackupEntry>> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let prefix = format!("{}manifests/{}/", config.s3.prefix, sanitized);
    let keys = remote.list_with_prefix(&prefix).await?;

    let mut entries: Vec<BackupEntry> = keys
        .iter()
        .filter(|k| k.ends_with("task_manifest.yaml"))
        .filter_map(|k| parse_entry(k))
        .collect();

    entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.level.cmp(&b.level)));

    if entries.is_empty() {
        return Err(anyhow!("no backups found for task '{}'", task.name));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::MockRemoteBackend;
    use std::path::PathBuf;

    #[test]
    fn test_parse_entry_valid_key() {
        let entry =
            parse_entry("backups/manifests/mnt_data/bcb_level0_202606021430/task_manifest.yaml")
                .unwrap();
        assert_eq!(entry.level, 0);
        assert_eq!(entry.timestamp, "2026-06-02T14:30:00Z");
    }

    #[test]
    fn test_parse_entry_higher_level() {
        let entry =
            parse_entry("backups/manifests/mnt_data/bcb_level12_202612251015/task_manifest.yaml")
                .unwrap();
        assert_eq!(entry.level, 12);
    }

    #[test]
    fn test_parse_entry_rejects_unrelated_keys() {
        assert!(parse_entry("backups/manifests/mnt_data/last_backup_manifest.yaml").is_none());
        assert!(
            parse_entry("backups/manifests/mnt_data/bcb_levelX_bad/task_manifest.yaml").is_none()
        );
        assert!(parse_entry("unrelated/key").is_none());
    }

    fn test_task() -> Task {
        Task {
            name: "data".to_string(),
            description: String::new(),
            subvolume: PathBuf::from("/mnt/data"),
            snapshot_dir: PathBuf::from("/mnt/.snapshots"),
            enabled: true,
        }
    }

    #[tokio::test]
    async fn test_list_backups_sorted_and_filtered() {
        let config = Config {
            base_dir: PathBuf::from("/tmp"),
            age_public_key: String::new(),
            s3: Default::default(),
            tasks: vec![],
        };
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "manifests/mnt_data/bcb_level1_202606031000/task_manifest.yaml".to_string(),
                "manifests/mnt_data/bcb_level0_202606021430/task_manifest.yaml".to_string(),
                "manifests/mnt_data/last_backup_manifest.yaml".to_string(),
            ])
        });

        let entries = list_backups(&config, &test_task(), &remote).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].level, 0);
        assert_eq!(entries[1].level, 1);
        assert!(entries[0].timestamp < entries[1].timestamp);
    }

    #[tokio::test]
    async fn test_list_backups_empty_is_error() {
        let config = Config {
            base_dir: PathBuf::from("/tmp"),
            age_public_key: String::new(),
            s3: Default::default(),
            tasks: vec![],
        };
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| Ok(vec![]));

        assert!(list_backups(&config, &test_task(), &remote).await.is_err());
    }
}
