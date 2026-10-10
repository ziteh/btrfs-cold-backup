use anyhow::Result;
use std::collections::HashSet;

use crate::{
    config::{Config, StorageClass, Task},
    layout::TaskLayout,
    list::list_backups,
    manifest::{BackupManifest, fetch_manifest},
    remote::RemoteBackend,
    util::now_unix,
};

/// A problem found auditing a task's backups on S3.
pub struct VerifyIssue {
    /// The level of the backup at fault; None for a problem of the task as a whole.
    pub level: Option<u32>,
    pub description: String,
}

/// Audits every backup on record for `task`: chain completeness, part existence and size,
/// storage class, and, if `max_age_days` is set, freshness. Uses only HeadObject,
/// ListObjectsV2 and manifest downloads, so it never requests or waits on an archive
/// retrieval.
pub async fn verify_task<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    remote: &R,
    max_age_days: Option<u64>,
) -> Result<Vec<VerifyIssue>> {
    let layout = TaskLayout::new(config, task)?;
    let ids = list_backups(remote, &layout).await?;
    if ids.is_empty() {
        return Ok(vec![VerifyIssue {
            level: None,
            description: "no backups found for this task".to_string(),
        }]);
    }

    let mut issues = Vec::new();
    // Snapshots whose manifest is on S3. Backups are visited oldest first, so a parent not
    // in here by the time its child is checked has no manifest: the chain is broken there.
    let mut backed_up_snapshots: HashSet<String> = HashSet::new();
    let mut newest_created_at = 0i64;

    for id in &ids {
        let mut issue = |description: String| {
            issues.push(VerifyIssue {
                level: Some(id.level),
                description,
            })
        };
        let backup = match fetch_manifest(remote, &layout.manifest_key(id)).await {
            Ok(backup) => backup,
            Err(e) => {
                issue(format!("{e:#}"));
                continue;
            }
        };
        newest_created_at = newest_created_at.max(backup.created_at);

        if let Some(parent) = &backup.parent_snapshot
            && !backed_up_snapshots.contains(parent)
        {
            issue(format!(
                "parent snapshot '{parent}' has no manifest on S3; incremental chain is broken"
            ));
        }

        let expected_class = config.s3.storage_class.for_level(id.level).ok();
        match check_parts(remote, &layout, &backup, expected_class).await {
            Ok(part_issues) => part_issues.into_iter().for_each(|p| issue(p.description)),
            Err(e) => issue(format!("could not check parts: {e}")),
        }

        backed_up_snapshots.insert(backup.snapshot_name);
    }

    if let Some(max_age_days) = max_age_days {
        let age_days = (now_unix() - newest_created_at) / 86400;
        if age_days > max_age_days as i64 {
            issues.push(VerifyIssue {
                level: None,
                description: format!(
                    "newest backup is {age_days} day(s) old, exceeding the {max_age_days} day \
                     freshness threshold"
                ),
            });
        }
    }

    Ok(issues)
}

/// A part of a backup that is not on S3 as its manifest records it.
pub struct PartIssue {
    pub index: String,
    pub description: String,
}

/// HEADs every part of `backup` and reports each that is missing, not the size its manifest
/// records, or, if `expected_class` is given, of another storage class. A HEAD that fails for
/// any other reason (e.g. the network) fails the whole check: it says nothing about the part,
/// so callers must not act on it as missing.
pub async fn check_parts<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    backup: &BackupManifest,
    expected_class: Option<StorageClass>,
) -> Result<Vec<PartIssue>> {
    let id = backup.id();
    let mut issues = Vec::new();
    for part in &backup.parts {
        let key = layout.part_key(&id, &part.index);
        let mut issue = |description: String| {
            issues.push(PartIssue {
                index: part.index.clone(),
                description,
            })
        };
        let Some(meta) = remote.head_metadata(&key).await? else {
            issue(format!("part {key} is missing"));
            continue;
        };
        if meta.size_bytes != part.size_bytes {
            issue(format!(
                "part {key}: manifest says {} bytes, S3 reports {} bytes",
                part.size_bytes, meta.size_bytes
            ));
        }
        if let Some(expected) = expected_class
            && meta.storage_class != expected.as_str()
        {
            issue(format!(
                "part {key}: storage class is {}, but level {} is configured for {}",
                meta.storage_class,
                id.level,
                expected.as_str()
            ));
        }
    }
    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{S3Config, StorageClasses};
    use crate::manifest::{ManifestPart, SystemInfo};
    use crate::remote::{MockRemoteBackend, ObjectMetadata};
    use std::path::PathBuf;

    const MANIFEST_KEY: &str =
        "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml";

    fn test_task() -> Task {
        Task {
            name: "movies".to_string(),
            description: String::new(),
            subvolume: PathBuf::from("/mnt/data/movies"),
            snapshot_dir: PathBuf::from("/mnt/.snapshots/movies"),
            enabled: true,
        }
    }

    fn test_config() -> Config {
        Config {
            base_dir: PathBuf::from("/tmp"),
            age_public_key: String::new(),
            s3: S3Config {
                prefix: "backups/".to_string(),
                storage_class: StorageClasses {
                    manifest: StorageClass::Standard,
                    backup_data: vec![StorageClass::DeepArchive],
                },
                ..Default::default()
            },
            oversize_guard: None,
            tasks: vec![],
        }
    }

    fn backup_with_one_part(size_bytes: u64) -> BackupManifest {
        BackupManifest {
            format_version: crate::manifest::FORMAT_VERSION.to_string(),
            timestamp: "2026-06-02T14:30".parse().unwrap(),
            created_at: now_unix(),
            level: 0,
            snapshot_name: "bcb_level0.20260602T1430".to_string(),
            parent_snapshot: None,
            source_uuid: "src-uuid".to_string(),
            snapshot_uuid: "snap-uuid".to_string(),
            system: SystemInfo::default(),
            age_public_key: String::new(),
            total_size_bytes: size_bytes,
            blake3: "hash".to_string(),
            parts: vec![ManifestPart {
                index: "000000".to_string(),
                size_bytes,
                blake3: "parthash".to_string(),
            }],
        }
    }

    /// Verifies a task whose only backup is `backup`, at `manifest_key`, with its one part
    /// answered by `head`. Returns each issue's description.
    async fn verify_one(
        manifest_key: &'static str,
        backup: BackupManifest,
        head: Option<ObjectMetadata>,
        max_age_days: Option<u64>,
    ) -> Vec<String> {
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_list_with_prefix()
            .returning(move |_| Ok(vec![manifest_key.to_string()]));
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote
            .expect_head_metadata()
            .returning(move |_| Ok(head.clone()));
        verify_task(&test_config(), &test_task(), &remote, max_age_days)
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.description)
            .collect()
    }

    fn on_s3(size_bytes: u64, storage_class: &str) -> Option<ObjectMetadata> {
        Some(ObjectMetadata {
            size_bytes,
            storage_class: storage_class.to_string(),
        })
    }

    fn assert_single_issue(issues: &[String], needle: &str) {
        assert_eq!(issues.len(), 1, "issues: {issues:?}");
        assert!(issues[0].contains(needle), "issues: {issues:?}");
    }

    #[tokio::test]
    async fn test_verify_task_no_backups_is_a_task_issue() {
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| Ok(vec![]));

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].level, None);
        assert!(issues[0].description.contains("no backups found"));
    }

    #[tokio::test]
    async fn test_verify_task_clean_backup_has_no_issues() {
        let backup = backup_with_one_part(1024);
        let issues = verify_one(MANIFEST_KEY, backup, on_s3(1024, "DEEP_ARCHIVE"), None).await;
        assert!(issues.is_empty(), "issues: {issues:?}");
    }

    #[tokio::test]
    async fn test_verify_task_detects_size_mismatch() {
        let backup = backup_with_one_part(1024);
        let issues = verify_one(MANIFEST_KEY, backup, on_s3(999, "DEEP_ARCHIVE"), None).await;
        assert_single_issue(&issues, "S3 reports 999 bytes");
    }

    #[tokio::test]
    async fn test_verify_task_detects_storage_class_drift() {
        let backup = backup_with_one_part(1024);
        let issues = verify_one(MANIFEST_KEY, backup, on_s3(1024, "STANDARD"), None).await;
        assert_single_issue(&issues, "storage class");
    }

    #[tokio::test]
    async fn test_verify_task_detects_missing_part() {
        let backup = backup_with_one_part(1024);
        let issues = verify_one(MANIFEST_KEY, backup, None, None).await;
        assert_single_issue(&issues, "is missing");
    }

    #[tokio::test]
    async fn test_verify_task_detects_broken_chain() {
        let mut backup = backup_with_one_part(1024);
        backup.level = 1;
        backup.snapshot_name = "bcb_level1.20260602T1430".to_string();
        backup.parent_snapshot = Some("bcb_level0.20260601T1430".to_string());
        let issues = verify_one(
            "backups/manifests/mnt_data_movies/bcb_level1_202606021430/manifest.yaml",
            backup,
            on_s3(1024, "DEEP_ARCHIVE"),
            None,
        )
        .await;
        assert_single_issue(&issues, "chain is broken");
    }

    #[tokio::test]
    async fn test_verify_task_detects_stale_backup() {
        let mut backup = backup_with_one_part(1024);
        backup.created_at = now_unix() - 100 * 86400;
        let issues = verify_one(MANIFEST_KEY, backup, on_s3(1024, "DEEP_ARCHIVE"), Some(30)).await;
        assert_single_issue(&issues, "freshness threshold");
    }

    #[tokio::test]
    async fn test_verify_task_fresh_backup_within_threshold_is_ok() {
        let backup = backup_with_one_part(1024);
        let issues = verify_one(MANIFEST_KEY, backup, on_s3(1024, "DEEP_ARCHIVE"), Some(30)).await;
        assert!(issues.is_empty(), "issues: {issues:?}");
    }
}
