use anyhow::Result;

use crate::{layout::TaskLayout, remote::RemoteBackend, timestamp::BackupId};

/// The backup in whose manifest directory `key` lies, if it is a backup manifest's key.
fn manifest_backup_id(key: &str) -> Option<BackupId> {
    let dir = key.strip_suffix("/manifest.yaml")?.rsplit('/').next()?;
    BackupId::parse_dir_name(dir)
}

/// Every backup of the task that has a manifest on S3, oldest first.
pub async fn list_backups<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
) -> Result<Vec<BackupId>> {
    let keys = remote.list_with_prefix(&layout.manifests_prefix()).await?;
    let mut ids: Vec<BackupId> = keys.iter().filter_map(|k| manifest_backup_id(k)).collect();
    ids.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.level.cmp(&b.level)));
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Task};
    use crate::remote::MockRemoteBackend;
    use std::path::PathBuf;

    fn layout() -> TaskLayout {
        let config = Config {
            base_dir: PathBuf::from("/tmp"),
            age_public_key: String::new(),
            s3: Default::default(),
            oversize_guard: None,
            tasks: vec![],
        };
        let task = Task {
            name: "data".to_string(),
            description: String::new(),
            subvolume: PathBuf::from("/mnt/data"),
            snapshot_dir: PathBuf::from("/mnt/.snapshots"),
            enabled: true,
        };
        TaskLayout::new(&config, &task).unwrap()
    }

    #[test]
    fn manifest_key_should_parse_to_its_backup_id() {
        let id =
            manifest_backup_id("backups/manifests/mnt_data/bcb_level12_202606021430/manifest.yaml")
                .unwrap();
        assert_eq!(id.level, 12);
        assert_eq!(id.timestamp.as_str(), "2026-06-02T14:30");
    }

    #[test]
    fn latest_or_unrelated_keys_should_not_parse_as_backup() {
        assert!(manifest_backup_id("backups/manifests/mnt_data/latest.yaml").is_none());
        assert!(
            manifest_backup_id("backups/manifests/mnt_data/bcb_levelX_bad/manifest.yaml").is_none()
        );
        assert!(manifest_backup_id("unrelated/key").is_none());
    }

    #[tokio::test]
    async fn list_backups_should_skip_latest_and_sort_chronologically() {
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "manifests/mnt_data/bcb_level1_202606031000/manifest.yaml".to_string(),
                "manifests/mnt_data/bcb_level0_202606021430/manifest.yaml".to_string(),
                "manifests/mnt_data/latest.yaml".to_string(),
            ])
        });

        let ids = list_backups(&remote, &layout()).await.unwrap();
        assert_eq!(ids.iter().map(|id| id.level).collect::<Vec<_>>(), [0, 1]);
    }

    #[tokio::test]
    async fn list_backups_should_return_empty_when_no_manifests() {
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| Ok(vec![]));
        assert!(list_backups(&remote, &layout()).await.unwrap().is_empty());
    }
}
