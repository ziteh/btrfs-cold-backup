use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

use crate::{BtrfsOps, Config, RemoteBackend, Task};

/// Verifies the external executables bcb shells out to are present on PATH.
pub fn check_binaries() -> Result<()> {
    for bin in ["btrfs", "split", "du"] {
        if !on_path(bin) {
            return Err(anyhow!("required executable '{bin}' not found on PATH"));
        }
    }
    Ok(())
}

fn on_path(bin: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    any_dir_has_executable(std::env::split_paths(&paths), bin)
}

/// Extracted from `on_path` so the search logic is testable without mutating the real
/// process-wide `PATH` (which, being global and shared across parallel test threads, would
/// make tests racy).
fn any_dir_has_executable(dirs: impl Iterator<Item = PathBuf>, bin: &str) -> bool {
    dirs.into_iter().any(|dir| is_executable(&dir.join(bin)))
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

pub async fn check<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
) -> Result<()> {
    config.validate()?;

    for task in &config.tasks {
        if !task.enabled {
            continue;
        }
        let source = btrfs
            .subvolume_uuids(&task.subvolume)
            .await
            .map_err(|e| anyhow!("task '{}': {e}", task.name))?;
        match tokio::fs::metadata(&task.snapshot_dir).await {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(anyhow!(
                    "task '{}': snapshot_dir {:?} is not a directory",
                    task.name,
                    task.snapshot_dir
                ));
            }
            Err(e) => {
                return Err(anyhow!(
                    "task '{}': cannot access snapshot_dir {:?}: {e}",
                    task.name,
                    task.snapshot_dir
                ));
            }
        }
        check_newest_snapshot_origin(btrfs, task, &source.uuid).await?;
    }

    remote.verify_credentials().await?;

    Ok(())
}

/// Catches a snapshot_dir that holds another subvolume's snapshots before a backup does.
async fn check_newest_snapshot_origin<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    source_uuid: &str,
) -> Result<()> {
    let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
    // Names are bcb_level<N>.<timestamp>; compare by timestamp so level doesn't dominate.
    let Some(newest) = snapshots
        .iter()
        .max_by_key(|s| s.split_once('.').map(|(_, ts)| ts))
    else {
        return Ok(());
    };
    let snapshot = btrfs
        .subvolume_uuids(&task.snapshot_dir.join(newest))
        .await
        .map_err(|e| anyhow!("task '{}': {e}", task.name))?;
    if snapshot.parent_uuid.as_deref() != Some(source_uuid) {
        return Err(anyhow!(
            "task '{}': snapshot '{newest}' was not taken from {:?} (its Parent UUID is {}, the \
             subvolume's UUID is {source_uuid}); check that snapshot_dir holds this subvolume's \
             snapshots",
            task.name,
            task.subvolume,
            snapshot.parent_uuid.as_deref().unwrap_or("-"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btrfs::{MockBtrfsOps, SubvolumeUuids};
    use crate::config::{S3Config, S3StorageClass, Task};
    use crate::remote::MockRemoteBackend;
    use std::os::unix::fs::PermissionsExt;

    fn make_executable(path: &Path) {
        std::fs::write(path, b"#!/bin/sh\n").unwrap();
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn test_is_executable_true_for_executable_file() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("mybin");
        make_executable(&bin);
        assert!(is_executable(&bin));
    }

    #[test]
    fn test_is_executable_false_for_non_executable_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("notexec");
        std::fs::write(&file, b"data").unwrap();
        assert!(!is_executable(&file));
    }

    #[test]
    fn test_is_executable_false_for_missing_path() {
        assert!(!is_executable(Path::new("/nonexistent/path/for/bcb/test")));
    }

    #[test]
    fn test_any_dir_has_executable_finds_it_in_second_dir() {
        let empty_dir = tempfile::tempdir().unwrap();
        let bin_dir = tempfile::tempdir().unwrap();
        make_executable(&bin_dir.path().join("mybin"));

        let dirs = vec![empty_dir.path().to_path_buf(), bin_dir.path().to_path_buf()];
        assert!(any_dir_has_executable(dirs.into_iter(), "mybin"));
    }

    #[test]
    fn test_any_dir_has_executable_false_when_nowhere_found() {
        let empty_dir = tempfile::tempdir().unwrap();
        let dirs = vec![empty_dir.path().to_path_buf()];
        assert!(!any_dir_has_executable(dirs.into_iter(), "mybin"));
    }

    fn valid_config(snapshot_dir: PathBuf) -> Config {
        Config {
            base_dir: PathBuf::from("/var/lib/bcb"),
            age_public_key: age::x25519::Identity::generate().to_public().to_string(),
            s3: S3Config {
                bucket: "my-bucket".to_string(),
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
            tasks: vec![Task {
                name: "test".to_string(),
                description: String::new(),
                subvolume: PathBuf::from("/mnt/data"),
                snapshot_dir,
                enabled: true,
            }],
        }
    }

    #[tokio::test]
    async fn test_check_fails_when_config_invalid() {
        let mut config = valid_config(PathBuf::from("/mnt/snapshots"));
        config.s3.bucket = String::new();

        // No expectations set on either mock: config validation must fail before any
        // btrfs or remote call is made.
        let btrfs = MockBtrfsOps::new();
        let remote = MockRemoteBackend::new();

        assert!(check(&config, &btrfs, &remote).await.is_err());
    }

    const SOURCE_UUID: &str = "src-uuid";

    /// The subvolume has SOURCE_UUID; every snapshot's parent is `snapshot_parent`.
    fn btrfs_with_snapshots(snapshots: &[&str], snapshot_parent: &'static str) -> MockBtrfsOps {
        let snapshots: Vec<String> = snapshots.iter().map(|s| s.to_string()).collect();
        let mut btrfs = MockBtrfsOps::new();
        btrfs
            .expect_list_snapshots()
            .returning(move |_| Ok(snapshots.clone()));
        btrfs.expect_subvolume_uuids().returning(move |path| {
            Ok(if path == Path::new("/mnt/data") {
                SubvolumeUuids {
                    uuid: SOURCE_UUID.to_string(),
                    parent_uuid: None,
                }
            } else {
                SubvolumeUuids {
                    uuid: format!("uuid-of-{}", path.display()),
                    parent_uuid: Some(snapshot_parent.to_string()),
                }
            })
        });
        btrfs
    }

    #[tokio::test]
    async fn test_check_fails_when_subvolume_missing() {
        let config = valid_config(PathBuf::from("/mnt/snapshots"));

        let mut btrfs = MockBtrfsOps::new();
        btrfs
            .expect_subvolume_uuids()
            .returning(|_| Err(anyhow::anyhow!("\"/mnt/data\" is not a btrfs subvolume")));
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("not a btrfs subvolume"));
    }

    #[tokio::test]
    async fn test_check_fails_when_newest_snapshot_is_from_another_subvolume() {
        let dir = tempfile::tempdir().unwrap();
        let config = valid_config(dir.path().to_path_buf());

        let btrfs = btrfs_with_snapshots(
            &["bcb_level1.20260602T1430", "bcb_level0.20260603T1430"],
            "other-subvol-uuid",
        );
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("was not taken from"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("bcb_level0.20260603T1430"),
            "not the newest: {msg}"
        );
    }

    #[tokio::test]
    async fn test_check_skips_disabled_tasks() {
        let mut config = valid_config(PathBuf::from("/mnt/snapshots"));
        config.tasks[0].enabled = false;

        // No expectations set: a disabled task must not trigger any btrfs or remote call.
        let btrfs = MockBtrfsOps::new();
        let mut remote = MockRemoteBackend::new();
        remote.expect_verify_credentials().returning(|| Ok(()));

        assert!(check(&config, &btrfs, &remote).await.is_ok());
    }

    #[tokio::test]
    async fn test_check_fails_when_snapshot_dir_not_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("plain_file");
        std::fs::write(&not_a_dir, b"oops").unwrap();
        let config = valid_config(not_a_dir);

        let btrfs = btrfs_with_snapshots(&[], SOURCE_UUID);
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("not a directory"));
    }

    #[tokio::test]
    async fn test_check_fails_when_snapshot_dir_missing() {
        let config = valid_config(PathBuf::from("/nonexistent/snapshot/dir/for/bcb/test"));

        let btrfs = btrfs_with_snapshots(&[], SOURCE_UUID);
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("cannot access"));
    }

    #[tokio::test]
    async fn test_check_succeeds_and_verifies_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = valid_config(dir.path().to_path_buf());

        let btrfs = btrfs_with_snapshots(&["bcb_level0.20260602T1430"], SOURCE_UUID);
        let mut remote = MockRemoteBackend::new();
        remote.expect_verify_credentials().returning(|| Ok(()));

        assert!(check(&config, &btrfs, &remote).await.is_ok());
    }

    #[tokio::test]
    async fn test_check_fails_when_verify_credentials_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = valid_config(dir.path().to_path_buf());

        let btrfs = btrfs_with_snapshots(&[], SOURCE_UUID);
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_verify_credentials()
            .returning(|| Err(anyhow::anyhow!("no access")));

        assert!(check(&config, &btrfs, &remote).await.is_err());
    }
}
