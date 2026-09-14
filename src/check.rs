use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

use crate::{BtrfsOps, Config, RemoteBackend};

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
        if !btrfs.subvolume_exists(&task.subvolume).await? {
            return Err(anyhow!(
                "task '{}': subvolume {:?} does not exist",
                task.name,
                task.subvolume
            ));
        }
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
    }

    remote.verify_credentials().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btrfs::MockBtrfsOps;
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

    #[tokio::test]
    async fn test_check_fails_when_subvolume_missing() {
        let config = valid_config(PathBuf::from("/mnt/snapshots"));

        let mut btrfs = MockBtrfsOps::new();
        btrfs.expect_subvolume_exists().returning(|_| Ok(false));
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("does not exist"));
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

        let mut btrfs = MockBtrfsOps::new();
        btrfs.expect_subvolume_exists().returning(|_| Ok(true));
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("not a directory"));
    }

    #[tokio::test]
    async fn test_check_fails_when_snapshot_dir_missing() {
        let config = valid_config(PathBuf::from("/nonexistent/snapshot/dir/for/bcb/test"));

        let mut btrfs = MockBtrfsOps::new();
        btrfs.expect_subvolume_exists().returning(|_| Ok(true));
        let remote = MockRemoteBackend::new();

        let err = check(&config, &btrfs, &remote).await.unwrap_err();
        assert!(err.to_string().contains("cannot access"));
    }

    #[tokio::test]
    async fn test_check_succeeds_and_verifies_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = valid_config(dir.path().to_path_buf());

        let mut btrfs = MockBtrfsOps::new();
        btrfs.expect_subvolume_exists().returning(|_| Ok(true));
        let mut remote = MockRemoteBackend::new();
        remote.expect_verify_credentials().returning(|| Ok(()));

        assert!(check(&config, &btrfs, &remote).await.is_ok());
    }

    #[tokio::test]
    async fn test_check_fails_when_verify_credentials_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = valid_config(dir.path().to_path_buf());

        let mut btrfs = MockBtrfsOps::new();
        btrfs.expect_subvolume_exists().returning(|_| Ok(true));
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_verify_credentials()
            .returning(|| Err(anyhow::anyhow!("no access")));

        assert!(check(&config, &btrfs, &remote).await.is_err());
    }
}
