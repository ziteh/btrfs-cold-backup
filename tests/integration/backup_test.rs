use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    BtrfsOps, Config, RemoteBackend, S3Config, S3RetryConfig, S3StorageClass, Task,
    backup::backup_task,
    manifest::{Last, LastEntry},
    restore::restore_task,
};
use mockall::mock;
#[cfg(target_os = "linux")]
use secrecy::ExposeSecret;
use tokio::io::AsyncRead;

mock! {
    Btrfs {}
    #[allow(async_fn_in_trait)]
    impl BtrfsOps for Btrfs {
        async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>>;
        async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
        async fn send_incremental(&self, parent: &Path, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
        async fn create_snapshot(&self, subvolume: &Path, snapshot_dir: &Path, name: &str) -> Result<PathBuf>;
        async fn delete_snapshot(&self, snapshot: &Path) -> Result<()>;
        async fn subvolume_exists(&self, path: &Path) -> Result<bool>;
    }
}

mock! {
    Remote {}
    #[allow(async_fn_in_trait)]
    impl RemoteBackend for Remote {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str) -> Result<()>;
        async fn upload_file(&self, key: &str, path: &Path, storage_class: &str) -> Result<()>;
        async fn head(&self, key: &str) -> Result<Option<u64>>;
        async fn download(&self, key: &str) -> Result<Vec<u8>>;
        async fn verify_credentials(&self) -> Result<()>;
        async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    }
}

fn test_config(base_dir: &Path) -> Config {
    let identity = age::x25519::Identity::generate();
    Config {
        base_dir: base_dir.to_path_buf(),
        age_public_key: identity.to_public().to_string(),
        s3: S3Config {
            enabled: true,
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "backups/".to_string(),
            endpoint: String::new(),
            storage_class: S3StorageClass::default(),
            retry: S3RetryConfig::default(),
            upload_speed_limit_kbps: 0,
        },
        tasks: vec![],
    }
}

fn movies_task() -> Task {
    Task {
        name: "movies".to_string(),
        description: String::new(),
        subvolume: PathBuf::from("/mnt/data/movies"),
        snapshot_dir: PathBuf::from("/mnt/.snapshots/movies"),
        enabled: true,
    }
}

fn fake_stream() -> Box<dyn AsyncRead + Send + Unpin> {
    Box::new(Cursor::new(b"btrfs send data".to_vec()))
}

fn last_manifest_yaml(snapshot_name: &str, level: u32) -> Vec<u8> {
    let mut last = Last::new();
    last.insert(
        level,
        LastEntry {
            timestamp: "2026-06-01T14:30:00Z".to_string(),
            snapshot_name: snapshot_name.to_string(),
        },
    );
    serde_yaml::to_string(&last).unwrap().into_bytes()
}

/// Full level-0 backup: find snapshot, send_full, encrypt, upload, write manifests.
#[tokio::test]
async fn level0_full_backup_should_succeed_and_return_manifest() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0_20260602_14-30";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut remote = MockRemote::new();
    // no previous backup
    remote
        .expect_head()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(None));
    remote.expect_upload_file().returning(|_, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _| Ok(()));

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;

    let backup = result.expect("level 0 backup should succeed");
    assert_eq!(backup.level, 0);
    assert_eq!(backup.snapshot_name, snap);
    assert!(backup.parent_snapshot.is_none());
}

/// When no snapshot exists for the requested level, backup must return an error without calling remote.
#[tokio::test]
async fn backup_should_fail_when_no_matching_snapshot() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs.expect_list_snapshots().returning(|_| Ok(vec![]));

    // no expectations set — any remote call would panic
    let remote = MockRemote::new();

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;
    assert!(result.is_err(), "should fail with no matching snapshot");
}

/// Level-1 incremental backup: read parent from last manifest, use send_incremental.
#[tokio::test]
async fn level1_incremental_should_use_level0_as_parent() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0_20260601_14-30";
    let snap = "bcb_level1_20260602_14-30";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));
    btrfs
        .expect_send_incremental()
        .withf(move |p, _| p.ends_with(parent))
        .returning(|_, _| Ok(fake_stream()));

    let manifest_bytes = last_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_head()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(Some(100)));
    remote
        .expect_download()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(move |_| Ok(manifest_bytes.clone()));
    remote.expect_upload_file().returning(|_, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _| Ok(()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;

    let backup = result.expect("level 1 backup should succeed");
    assert_eq!(backup.level, 1);
    assert_eq!(backup.parent_snapshot.as_deref(), Some(parent));
}

/// Level-1 backup must be rejected when no last manifest exists on remote (level 0 never ran).
#[tokio::test]
async fn level1_should_fail_when_no_remote_level0_exists() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level1_20260602_14-30".to_string()]));

    let mut remote = MockRemote::new();
    remote
        .expect_head()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(None)); // no level-0 backup

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    assert!(result.is_err(), "should fail without level 0 remote backup");
}

/// Level-1 backup must be rejected when the parent snapshot no longer exists locally.
#[tokio::test]
async fn level1_should_fail_when_parent_snapshot_missing_locally() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0_20260601_14-30";
    let snap = "bcb_level1_20260602_14-30";

    let mut btrfs = MockBtrfs::new();
    // parent snapshot has been deleted locally
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));

    let manifest_bytes = last_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_head()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(Some(100)));
    remote
        .expect_download()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(move |_| Ok(manifest_bytes.clone()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    assert!(
        result.is_err(),
        "should fail when parent snapshot missing locally"
    );
}

/// Restore flow: download, verify BLAKE3, decrypt, btrfs receive.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn restore_should_download_and_pipe_to_btrfs_receive() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir.path().to_path_buf(),
        age_public_key: identity.to_public().to_string(),
        s3: S3Config {
            enabled: true,
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "backups/".to_string(),
            endpoint: String::new(),
            storage_class: S3StorageClass::default(),
            retry: S3RetryConfig::default(),
            upload_speed_limit_kbps: 0,
        },
        tasks: vec![],
    };
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // build a real encrypted part so restore has something valid to download
    let plaintext = b"btrfs receive payload";
    let plain_path = work_dir.path().join("plain.bin");
    std::fs::write(&plain_path, plaintext).unwrap();
    let plaintext_hash = btrfs_cold_backup::blake3_file(&plain_path).await.unwrap();

    let encrypted_path = work_dir.path().join("part-1.age");
    btrfs_cold_backup::encrypt_file(
        &plain_path,
        &encrypted_path,
        &identity.to_public().to_string(),
    )
    .await
    .unwrap();
    let encrypted_bytes = std::fs::read(&encrypted_path).unwrap();
    let encrypted_hash = btrfs_cold_backup::blake3_file(&encrypted_path)
        .await
        .unwrap();

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0_20260602_14-30".to_string(),
        parent_snapshot: None,
        total_size_bytes: encrypted_bytes.len() as u64,
        blake3: plaintext_hash,
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: encrypted_bytes.len() as u64,
            blake3: encrypted_hash,
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let encrypted_bytes_clone = encrypted_bytes.clone();
    let mut remote = MockRemote::new();
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download()
        .withf(|key| key.contains("snapshot.part"))
        .returning(move |_| Ok(encrypted_bytes_clone.clone()));

    let mut btrfs = MockBtrfs::new();
    btrfs.expect_subvolume_exists().returning(|_| Ok(false));

    let private_key_secret = identity.to_string();
    let result = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        private_key_secret.expose_secret(),
        &remote,
    )
    .await;
    assert!(result.is_ok(), "restore should succeed: {result:?}");
}

/// Restore must abort early when BLAKE3 hash does not match.
#[tokio::test]
async fn restore_should_fail_on_blake3_mismatch() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // manifest hash intentionally does not match the actual bytes
    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0_20260602_14-30".to_string(),
        parent_snapshot: None,
        total_size_bytes: 10,
        blake3: "wrong_hash_that_will_never_match".to_string(),
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: 10,
            blake3: "wrong_hash_that_will_never_match".to_string(),
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download()
        .withf(|key| key.contains("snapshot.part"))
        .returning(|_| Ok(b"some random bytes".to_vec()));

    let result = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &remote,
    )
    .await;
    assert!(result.is_err(), "restore should fail on BLAKE3 mismatch");
}
