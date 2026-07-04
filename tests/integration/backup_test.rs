use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    BtrfsOps, Config, RemoteBackend, RestoreStatus, RetrievalOptions, S3Config, S3RetryConfig,
    S3StorageClass, Task,
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
        async fn subvolume_exists(&self, path: &Path) -> Result<bool>;
    }
}

mock! {
    Remote {}
    #[allow(async_fn_in_trait)]
    impl RemoteBackend for Remote {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str, tags: &str) -> Result<()>;
        async fn upload_file(&self, key: &str, path: &Path, storage_class: &str, tags: &str) -> Result<()>;
        async fn exists(&self, key: &str) -> Result<bool>;
        async fn download(&self, key: &str) -> Result<Vec<u8>>;
        async fn download_to_file(&self, key: &str, path: &Path) -> Result<()>;
        async fn restore_status(&self, key: &str) -> Result<RestoreStatus>;
        async fn request_restore(&self, key: &str, days: i32, tier: &str) -> Result<()>;
        async fn verify_credentials(&self) -> Result<()>;
        async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    }
}

fn retrieval() -> RetrievalOptions {
    RetrievalOptions {
        days: 1,
        tier: "Standard".to_string(),
    }
}

fn test_config(base_dir: &Path) -> Config {
    let identity = age::x25519::Identity::generate();
    Config {
        base_dir: base_dir.to_path_buf(),
        age_public_key: identity.to_public().to_string(),
        s3: S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "backups/".to_string(),
            endpoint: String::new(),
            storage_class: S3StorageClass {
                manifest: "STANDARD".to_string(),
                backup_data: vec!["STANDARD".to_string(), "STANDARD".to_string()],
            },
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
    let snap = "bcb_level0.20260602T1430";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut remote = MockRemote::new();
    // no previous backup, and no manifest for this snapshot yet
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(false));
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));
    remote
        .expect_upload_file()
        .withf(|_, _, _, tags| tags == "bcb-backup-level=0")
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|_, _, _, tags| tags == "bcb-backup-level=manifest")
        .returning(|_, _, _, _| Ok(()));

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;

    let backup = result
        .expect("level 0 backup should succeed")
        .expect("must not be skipped");
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
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

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
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(true));
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));
    remote
        .expect_download()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(move |_| Ok(manifest_bytes.clone()));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;

    let backup = result
        .expect("level 1 backup should succeed")
        .expect("must not be skipped");
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
        .returning(|_| Ok(vec!["bcb_level1.20260602T1430".to_string()]));

    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(false)); // no level-0 backup
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    assert!(result.is_err(), "should fail without level 0 remote backup");
}

/// Level-1 backup must be rejected when the parent snapshot no longer exists locally.
#[tokio::test]
async fn level1_should_fail_when_parent_snapshot_missing_locally() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

    let mut btrfs = MockBtrfs::new();
    // parent snapshot has been deleted locally
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));

    let manifest_bytes = last_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(true));
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));
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
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir.path().to_path_buf(),
        age_public_key: identity.to_public().to_string(),
        s3: S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "backups/".to_string(),
            endpoint: String::new(),
            storage_class: S3StorageClass {
                manifest: "STANDARD".to_string(),
                backup_data: vec!["STANDARD".to_string(), "STANDARD".to_string()],
            },
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
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
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
        .expect_restore_status()
        .returning(|_| Ok(RestoreStatus::Ready));
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_to_file()
        .withf(|key, _| key.contains("snapshot.part"))
        .returning(move |_, path| {
            std::fs::write(path, &encrypted_bytes_clone)?;
            Ok(())
        });

    let private_key_secret = identity.to_string();
    let result = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        private_key_secret.expose_secret(),
        &retrieval(),
        &remote,
    )
    .await;
    assert!(result.is_ok(), "restore should succeed: {result:?}");
}

/// Restore must abort early when BLAKE3 hash does not match.
#[tokio::test]
async fn restore_should_fail_on_blake3_mismatch() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // manifest hash intentionally does not match the actual bytes
    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
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
        .expect_restore_status()
        .returning(|_| Ok(RestoreStatus::Ready));
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_to_file()
        .withf(|key, _| key.contains("snapshot.part"))
        .returning(|_, path| {
            std::fs::write(path, b"some random bytes")?;
            Ok(())
        });

    let result = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &retrieval(),
        &remote,
    )
    .await;
    assert!(result.is_err(), "restore should fail on BLAKE3 mismatch");
}

/// A completed backup (manifest already on remote) must be skipped, not re-run and
/// overwritten: overwriting archived objects incurs minimum-storage-duration charges, and
/// skipping keeps re-runs idempotent.
#[tokio::test]
async fn backup_should_skip_already_completed_backup() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260602T1430".to_string()]));

    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(true));

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;
    assert!(
        result.expect("skip must not be an error").is_none(),
        "completed backup must be skipped"
    );
}

/// A level-N snapshot older than its recorded parent means the snapshot is stale; the
/// incremental chain would be semantically wrong, so backup must refuse.
#[tokio::test]
async fn level1_should_fail_when_snapshot_not_newer_than_parent() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    // level-1 snapshot predates the level-0 parent recorded in the last manifest
    let snap = "bcb_level1.20260501T1430";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));

    let manifest_bytes = last_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(true));
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));
    remote
        .expect_download()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(move |_| Ok(manifest_bytes.clone()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    let err = result
        .expect_err("stale snapshot must be rejected")
        .to_string();
    assert!(err.contains("not newer"), "unexpected error: {err}");
}

/// Resuming after an interruption must upload only the parts not yet completed.
#[tokio::test]
async fn resume_should_skip_already_uploaded_parts() {
    use btrfs_cold_backup::{BackupState, CompletedPart, state_path};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("task")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();
    // part 000000 was already uploaded (gone from disk); 000001 is still pending
    std::fs::write(output_dir.join("snapshot.part-000001"), b"pending part").unwrap();

    let mut parts_completed = std::collections::HashMap::new();
    parts_completed.insert(
        "000000".to_string(),
        CompletedPart {
            size_bytes: 12,
            hash: "already-uploaded".to_string(),
        },
    );
    let mut state = BackupState {
        task_name: task.name.clone(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        output_dir: output_dir.clone(),
        stream_hash: Some("streamhash".to_string()),
        total_parts: Some(2),
        parts_completed,
        manifest_uploaded: false,
        last_updated: 0,
    };
    let sp = state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    // resume path: no btrfs calls at all
    let btrfs = MockBtrfs::new();

    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("part-000001"))
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(false));

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("resumed backup should succeed")
        .expect("resume must not be skipped");
    assert_eq!(backup.parts.len(), 2);
    assert_eq!(backup.blake3, "streamhash");
}

/// If parts recorded in the state are gone from disk and not uploaded, the resumed backup
/// must fail loudly instead of silently uploading an incomplete set.
#[tokio::test]
async fn resume_should_fail_when_parts_missing_from_disk() {
    use btrfs_cold_backup::{BackupState, state_path};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("task")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();
    // state says 2 parts, but only one exists and none were uploaded
    std::fs::write(output_dir.join("snapshot.part-000000"), b"only part").unwrap();

    let mut state = BackupState {
        task_name: task.name.clone(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        output_dir: output_dir.clone(),
        stream_hash: Some("streamhash".to_string()),
        total_parts: Some(2),
        parts_completed: std::collections::HashMap::new(),
        manifest_uploaded: false,
        last_updated: 0,
    };
    let sp = state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect_err("incomplete part set must fail")
        .to_string();
    assert!(err.contains("expected 2 parts"), "unexpected error: {err}");
}

/// Archived parts must trigger a retrieval request and a retry-later error, not a download.
#[tokio::test]
async fn restore_should_request_retrieval_for_archived_objects() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let mut remote = MockRemote::new();
    remote
        .expect_restore_status()
        .returning(|_| Ok(RestoreStatus::NotRequested));
    remote
        .expect_request_restore()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let result = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &retrieval(),
        &remote,
    )
    .await;
    let err = result
        .expect_err("archived manifest must not be downloaded")
        .to_string();
    assert!(err.contains("re-run"), "unexpected error: {err}");
}

/// A whole-stream hash mismatch must abort BEFORE anything reaches btrfs receive, even when
/// every individual part hash checks out.
#[tokio::test]
async fn restore_should_fail_before_receive_on_stream_mismatch() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // A valid encrypted part with a correct part-level hash...
    let plain_path = work_dir.path().join("plain.bin");
    std::fs::write(&plain_path, b"stream payload").unwrap();
    let encrypted_path = work_dir.path().join("part.age");
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

    // ...but a manifest whose whole-stream hash does not match the decrypted content.
    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: encrypted_bytes.len() as u64,
        blake3: "not_the_stream_hash".to_string(),
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: encrypted_bytes.len() as u64,
            blake3: encrypted_hash,
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote
        .expect_restore_status()
        .returning(|_| Ok(RestoreStatus::Ready));
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_to_file()
        .withf(|key, _| key.contains("snapshot.part"))
        .returning(move |_, path| {
            std::fs::write(path, &encrypted_bytes)?;
            Ok(())
        });

    let private_key = identity.to_string();
    use secrecy::ExposeSecret;
    let err = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        private_key.expose_secret(),
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("stream mismatch must fail before receive")
    .to_string();
    assert!(
        err.contains("stream BLAKE3 mismatch"),
        "unexpected error: {err}"
    );
    // verification failed before receive: the target must be untouched
    assert!(
        !target_dir.exists(),
        "target_dir must not be created before verification"
    );
}
