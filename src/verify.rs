use anyhow::Result;
use std::collections::HashSet;

use crate::{
    config::{Config, Task},
    list::list_backups,
    manifest::{Backup, parse_backup},
    remote::ObjectMetadata,
    remote::RemoteBackend,
    util::{now_unix, s3_backup_key, s3_manifest_key, sanitize_subvolume_path},
};

/// A problem found auditing a task's backups on S3.
pub struct VerifyIssue {
    pub level: u32,
    pub timestamp: String,
    pub description: String,
}

/// Audits every backup on record for `task` — chain completeness, part existence and size,
/// storage class, and (if `max_age_days` is set) freshness — entirely via HeadObject and
/// ListObjectsV2, so this never requests or waits on an archive retrieval. Pass `None` for
/// `max_age_days` to skip the freshness check.
pub async fn verify_task<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    remote: &R,
    max_age_days: Option<u64>,
) -> Result<Vec<VerifyIssue>> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let entries = list_backups(config, task, remote).await?;

    let mut issues = Vec::new();

    if entries.is_empty() {
        issues.push(VerifyIssue {
            level: 0,
            timestamp: String::new(),
            description: "no backups found for this task".to_string(),
        });
        return Ok(issues);
    }

    // Collects every snapshot_name seen so far, so a level-N backup's parent_snapshot can be
    // checked against it: a parent that's not in this map has no manifest currently on S3
    // (deleted, expired, or never uploaded), so the chain is broken at that point.
    let mut by_snapshot: HashSet<String> = HashSet::new();
    let mut newest_created_at = 0i64;

    for entry in &entries {
        let manifest_key =
            s3_manifest_key(&config.s3.prefix, &sanitized, entry.level, &entry.timestamp);
        let backup = match remote.download(&manifest_key).await {
            Ok(bytes) => match parse_backup(&bytes) {
                Ok(b) => b,
                Err(e) => {
                    issues.push(VerifyIssue {
                        level: entry.level,
                        timestamp: entry.timestamp.clone(),
                        description: format!("manifest {manifest_key} is corrupt: {e}"),
                    });
                    continue;
                }
            },
            Err(e) => {
                issues.push(VerifyIssue {
                    level: entry.level,
                    timestamp: entry.timestamp.clone(),
                    description: format!("manifest {manifest_key} unreadable: {e}"),
                });
                continue;
            }
        };
        newest_created_at = newest_created_at.max(backup.created_at);

        if let Some(parent) = &backup.parent_snapshot
            && !by_snapshot.contains(parent)
        {
            issues.push(VerifyIssue {
                level: entry.level,
                timestamp: entry.timestamp.clone(),
                description: format!(
                    "parent snapshot '{parent}' has no manifest on S3; incremental chain is broken"
                ),
            });
        }

        let level_class = config
            .s3
            .storage_class
            .backup_data
            .get(entry.level as usize);
        let part_issues = check_parts(
            remote,
            &config.s3.prefix,
            &sanitized,
            entry.level,
            &entry.timestamp,
            &backup,
            |part_key, meta| match level_class {
                Some(expected) if &meta.storage_class != expected => Some(format!(
                    "part {part_key}: storage class is {}, but level {} is configured for \
                     {expected}",
                    meta.storage_class, entry.level
                )),
                _ => None,
            },
        )
        .await
        .unwrap_or_else(|e| vec![(String::new(), format!("could not check parts: {e}"))]);
        issues.extend(part_issues.into_iter().map(|(_, description)| VerifyIssue {
            level: entry.level,
            timestamp: entry.timestamp.clone(),
            description,
        }));

        by_snapshot.insert(backup.snapshot_name.clone());
    }

    if let Some(max_age_days) = max_age_days {
        let age_days = (now_unix() - newest_created_at) / 86400;
        if age_days > max_age_days as i64 {
            issues.push(VerifyIssue {
                level: 0,
                timestamp: String::new(),
                description: format!(
                    "newest backup is {age_days} day(s) old, exceeding the {max_age_days} day \
                     freshness threshold"
                ),
            });
        }
    }

    Ok(issues)
}

/// HEADs every part of `backup` and describes each that is missing or not the size its
/// manifest records, paired with the part's index. `check_found` adds caller-specific checks
/// on parts that exist. A HEAD that fails for any other reason (e.g. the network) fails the
/// whole check: it says nothing about the part, so callers must not act on it as missing.
pub async fn check_parts<R: RemoteBackend>(
    remote: &R,
    prefix: &str,
    sanitized: &str,
    level: u32,
    timestamp: &str,
    backup: &Backup,
    mut check_found: impl FnMut(&str, &ObjectMetadata) -> Option<String>,
) -> Result<Vec<(String, String)>> {
    let mut issues = Vec::new();
    for part in &backup.parts {
        let part_key = s3_backup_key(prefix, sanitized, level, timestamp, &part.index);
        match remote.head_metadata(&part_key).await? {
            Some(meta) => {
                if meta.size_bytes != part.size_bytes {
                    issues.push((
                        part.index.clone(),
                        format!(
                            "part {part_key}: manifest says {} bytes, S3 reports {} bytes",
                            part.size_bytes, meta.size_bytes
                        ),
                    ));
                }
                issues
                    .extend(check_found(&part_key, &meta).map(|issue| (part.index.clone(), issue)));
            }
            None => issues.push((part.index.clone(), format!("part {part_key} is missing"))),
        }
    }
    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3StorageClass;
    use crate::manifest::{PartInfo, SystemInfo};
    use crate::remote::MockRemoteBackend;
    use std::path::PathBuf;

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
            s3: crate::config::S3Config {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                prefix: "backups/".to_string(),
                endpoint: String::new(),
                storage_class: S3StorageClass {
                    manifest: "STANDARD".to_string(),
                    backup_data: vec!["DEEP_ARCHIVE".to_string()],
                },
                retry: Default::default(),
                upload_speed_limit_kbps: 0,
                restore: Default::default(),
            },
            tasks: vec![],
        }
    }

    fn backup_with_one_part(size_bytes: u64) -> Backup {
        Backup {
            format_version: crate::manifest::FORMAT_VERSION.to_string(),
            timestamp: "2026-06-02T14:30".to_string(),
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
            parts: vec![PartInfo {
                index: "000000".to_string(),
                size_bytes,
                blake3: "parthash".to_string(),
            }],
        }
    }

    #[tokio::test]
    async fn test_verify_task_no_backups_is_an_issue() {
        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| Ok(vec![]));

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].description.contains("no backups found"));
    }

    #[tokio::test]
    async fn test_verify_task_clean_backup_has_no_issues() {
        let backup = backup_with_one_part(1024);
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 1024,
                storage_class: "DEEP_ARCHIVE".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert!(
            issues.is_empty(),
            "expected no issues, got: {:?}",
            issues.iter().map(|i| &i.description).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_verify_task_detects_size_mismatch() {
        let backup = backup_with_one_part(1024);
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 999,
                storage_class: "DEEP_ARCHIVE".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(
            issues[0].description.contains("size mismatch")
                || issues[0].description.contains("bytes")
        );
    }

    #[tokio::test]
    async fn test_verify_task_detects_storage_class_drift() {
        let backup = backup_with_one_part(1024);
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 1024,
                storage_class: "STANDARD".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].description.contains("storage class"));
    }

    #[tokio::test]
    async fn test_verify_task_detects_missing_part() {
        let backup = backup_with_one_part(1024);
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| Ok(None));

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].description.contains("is missing"));
    }

    #[tokio::test]
    async fn test_verify_task_detects_broken_chain() {
        let mut backup = backup_with_one_part(1024);
        backup.level = 1;
        backup.snapshot_name = "bcb_level1.20260602T1430".to_string();
        backup.parent_snapshot = Some("bcb_level0.20260601T1430".to_string());
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level1_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 1024,
                storage_class: "DEEP_ARCHIVE".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, None)
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].description.contains("chain is broken"));
    }

    #[tokio::test]
    async fn test_verify_task_detects_stale_backup() {
        let mut backup = backup_with_one_part(1024);
        backup.created_at = now_unix() - 100 * 86400;
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 1024,
                storage_class: "DEEP_ARCHIVE".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, Some(30))
            .await
            .unwrap();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].description.contains("freshness threshold"));
    }

    #[tokio::test]
    async fn test_verify_task_fresh_backup_within_threshold_is_ok() {
        let backup = backup_with_one_part(1024);
        let manifest_yaml = serde_yaml::to_string(&backup).unwrap().into_bytes();

        let mut remote = MockRemoteBackend::new();
        remote.expect_list_with_prefix().returning(|_| {
            Ok(vec![
                "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml"
                    .to_string(),
            ])
        });
        remote
            .expect_download()
            .returning(move |_| Ok(manifest_yaml.clone()));
        remote.expect_head_metadata().returning(|_| {
            Ok(Some(ObjectMetadata {
                size_bytes: 1024,
                storage_class: "DEEP_ARCHIVE".to_string(),
            }))
        });

        let issues = verify_task(&test_config(), &test_task(), &remote, Some(30))
            .await
            .unwrap();
        assert!(issues.is_empty());
    }
}
