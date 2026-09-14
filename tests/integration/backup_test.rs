use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    BtrfsOps, Config, ObjectMetadata, RemoteBackend, RestoreConfig, RestoreStatus,
    RestoreStatusDetail, RetrievalOptions, S3Config, S3RetryConfig, S3StorageClass, Task,
    backup::backup_task,
    manifest::{Last, LastEntry},
    restore::restore_task,
};
use mockall::mock;
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
        async fn download_range_to_file(&self, key: &str, start: u64, end: u64, path: &Path) -> Result<()>;
        async fn restore_status_detail(&self, key: &str) -> Result<RestoreStatusDetail>;
        async fn request_restore(&self, key: &str, days: i32, tier: &str) -> Result<()>;
        async fn head_metadata(&self, key: &str) -> Result<ObjectMetadata>;
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
            restore: RestoreConfig::default(),
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

/// A corrupted last-backup manifest must fail loudly rather than being silently reset to
/// empty — a silent reset would drop other levels' entries and break their incremental
/// chains without warning.
#[tokio::test]
async fn backup_should_fail_when_last_manifest_is_corrupt() {
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
        .returning(|_| Ok(b"[not, a, valid, last-manifest, mapping]".to_vec()));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("failed to parse last manifest"),
        "unexpected error: {err}"
    );
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

/// Level-2 incremental backup: the chain logic must not be special-cased to just level 0/1 —
/// a level-2 backup reads its parent from the last manifest's level-1 entry the same way a
/// level-1 backup reads its level-0 entry.
#[tokio::test]
async fn level2_incremental_should_use_level1_as_parent() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    config
        .s3
        .storage_class
        .backup_data
        .push("GLACIER".to_string());
    let task = movies_task();
    let parent = "bcb_level1.20260601T1430";
    let snap = "bcb_level2.20260602T1430";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));
    btrfs
        .expect_send_incremental()
        .withf(move |p, _| p.ends_with(parent))
        .returning(|_, _| Ok(fake_stream()));

    let manifest_bytes = last_manifest_yaml(parent, 1);
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
    remote
        .expect_upload_file()
        .withf(|_, _, _, tags| tags == "bcb-backup-level=2")
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    let result = backup_task(&config, &task, 2, &btrfs, &remote).await;

    let backup = result
        .expect("level 2 backup should succeed")
        .expect("must not be skipped");
    assert_eq!(backup.level, 2);
    assert_eq!(backup.parent_snapshot.as_deref(), Some(parent));
}

/// A level whose storage class isn't configured must fail loudly before ever uploading,
/// rather than silently falling back to some default storage class.
#[tokio::test]
async fn backup_should_fail_when_no_storage_class_configured_for_level() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    // Only level 0 has a configured storage class.
    config.s3.storage_class.backup_data = vec!["STANDARD".to_string()];
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));
    btrfs
        .expect_send_incremental()
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
    // No upload_file/upload_bytes expectations: the storage-class lookup must fail before
    // ever calling upload_file (any such call would itself be a test failure).

    let err = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("no storage class configured for level 1"),
        "unexpected error: {err}"
    );
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
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::Ready,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(|_, _, _, path| {
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

/// Regression test: btrbk's same-minute collision suffix (`_<n>`) must get its own S3 key.
/// Before the fix, two same-minute snapshots produced the exact same ISO timestamp and
/// therefore the exact same manifest key, so the second snapshot was silently treated as
/// "already backed up" and skipped — even though it was never actually uploaded.
#[tokio::test]
async fn collision_suffixed_snapshot_should_get_its_own_key_not_be_silently_skipped() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs.expect_list_snapshots().returning(|_| {
        Ok(vec![
            "bcb_level0.20260602T1430".to_string(),
            "bcb_level0.20260602T1430_1".to_string(),
        ])
    });
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(false));
    // The first (unsuffixed) snapshot's manifest already exists on remote; the collided
    // one's must not, or this test would prove nothing about the fix.
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest") && !key.contains("_1/"))
        .returning(|_| Ok(true));
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest") && key.contains("_1/"))
        .returning(|_| Ok(false));
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("_1/"))
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("task_manifest"))
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("last_backup_manifest"))
        .returning(|_, _, _, _| Ok(()));

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;

    let backup = result
        .expect("collision-suffixed snapshot must back up successfully")
        .expect("must not be skipped as already backed up");
    assert_eq!(backup.snapshot_name, "bcb_level0.20260602T1430_1");
}

/// Regression test: completing a new level-0 backup must drop any deeper level's entry from
/// the last-backup manifest. Those entries point at an incremental chain built on the
/// snapshot this run just superseded; left in place, a later level-N (N > 0) backup could
/// silently chain onto a stale parent instead of failing loudly and asking for a fresh one.
#[tokio::test]
async fn new_level0_backup_should_prune_stale_deeper_level_entries_from_last_manifest() {
    use std::sync::{Arc, Mutex};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260701T0100".to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    // Existing last manifest carries stale level 1 and level 2 entries from an older chain.
    let mut stale_last = Last::new();
    stale_last.insert(
        0,
        LastEntry {
            timestamp: "2026-01-01T01:00:00Z".to_string(),
            snapshot_name: "bcb_level0.20260101T0100".to_string(),
        },
    );
    stale_last.insert(
        1,
        LastEntry {
            timestamp: "2026-01-07T03:00:00Z".to_string(),
            snapshot_name: "bcb_level1.20260107T0300".to_string(),
        },
    );
    stale_last.insert(
        2,
        LastEntry {
            timestamp: "2026-01-14T03:00:00Z".to_string(),
            snapshot_name: "bcb_level2.20260114T0300".to_string(),
        },
    );
    let stale_last_yaml = serde_yaml::to_string(&stale_last).unwrap().into_bytes();

    let uploaded_last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let uploaded_last_clone = uploaded_last.clone();

    let mut remote = MockRemote::new();
    remote
        .expect_exists()
        .withf(|key| key.contains("task_manifest"))
        .returning(|_| Ok(false));
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(true));
    remote
        .expect_download()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(move |_| Ok(stale_last_yaml.clone()));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("task_manifest"))
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("last_backup_manifest"))
        .returning(move |_, data, _, _| {
            *uploaded_last_clone.lock().unwrap() = Some(data);
            Ok(())
        });

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;
    result
        .expect("level 0 backup should succeed")
        .expect("must not be skipped");

    let uploaded_bytes = uploaded_last
        .lock()
        .unwrap()
        .clone()
        .expect("last manifest must be re-uploaded");
    let uploaded: Last = serde_yaml::from_slice(&uploaded_bytes).unwrap();
    assert!(
        uploaded.contains_key(&0),
        "new level 0 entry must be present"
    );
    assert!(
        !uploaded.contains_key(&1),
        "stale level 1 entry must be pruned after a new level 0 backup"
    );
    assert!(
        !uploaded.contains_key(&2),
        "stale level 2 entry must be pruned after a new level 0 backup"
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
        collision_suffix: None,
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

/// A part left with both its raw file and a `.age` file (encryption was interrupted mid-run
/// on a previous attempt, so the `.age` may be truncated) must have the existing `.age`
/// discarded and freshly re-encrypted from the raw file — not uploaded as whatever partial
/// bytes were left behind.
#[tokio::test]
async fn encrypt_should_discard_and_redo_truncated_age_file_from_interrupted_run() {
    use btrfs_cold_backup::{BackupState, state_path};
    use secrecy::ExposeSecret;
    use std::sync::{Arc, Mutex};

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let mut config = test_config(work_dir.path());
    config.age_public_key = identity.to_public().to_string();
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("task")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();
    let raw_content = b"the real snapshot data for this part".to_vec();
    std::fs::write(output_dir.join("snapshot.part-000000"), &raw_content).unwrap();
    // Simulates a run killed mid-encryption in a previous attempt: a truncated/garbage .age
    // file sitting alongside the still-present raw part.
    std::fs::write(
        output_dir.join("snapshot.part-000000.age"),
        b"truncated garbage",
    )
    .unwrap();

    let mut state = BackupState {
        task_name: task.name.clone(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        collision_suffix: None,
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        output_dir: output_dir.clone(),
        stream_hash: Some("streamhash".to_string()),
        total_parts: Some(1),
        parts_completed: std::collections::HashMap::new(),
        manifest_uploaded: false,
        last_updated: 0,
    };
    let sp = state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    // resume path: no btrfs calls at all
    let btrfs = MockBtrfs::new();

    // upload_file() only gets the path; production code deletes that file right after a
    // successful upload, so the bytes must be captured here, before that happens.
    let uploaded_bytes: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let uploaded_bytes_clone = uploaded_bytes.clone();

    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("part-000000"))
        .times(1)
        .returning(move |_, path, _, _| {
            *uploaded_bytes_clone.lock().unwrap() = Some(std::fs::read(path)?);
            Ok(())
        });
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote
        .expect_exists()
        .withf(|key| key.contains("last_backup_manifest"))
        .returning(|_| Ok(false));

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("resumed backup should succeed")
        .expect("resume must not be skipped");
    assert_eq!(backup.parts.len(), 1);

    // Decrypt what was actually uploaded and confirm it's the real content — proving the
    // truncated .age was discarded and freshly re-encrypted, not uploaded as-is.
    let uploaded_bytes = uploaded_bytes
        .lock()
        .unwrap()
        .clone()
        .expect("part must have been uploaded");
    let uploaded_copy_path = work_dir.path().join("uploaded_copy.age");
    std::fs::write(&uploaded_copy_path, &uploaded_bytes).unwrap();
    let decrypted_path = work_dir.path().join("decrypted");
    btrfs_cold_backup::decrypt_file(
        &uploaded_copy_path,
        &decrypted_path,
        identity.to_string().expose_secret(),
    )
    .await
    .expect("uploaded part must decrypt with the real key, not be leftover garbage");
    assert_eq!(std::fs::read(&decrypted_path).unwrap(), raw_content);
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
        collision_suffix: None,
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
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // Create a simple manifest with one part
    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: 10,
        blake3: "wrong_hash".to_string(),
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: 10,
            blake3: "wrong_hash".to_string(),
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::NotRequested,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
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
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::Ready,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(move |_, _, _, path| {
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

/// Regression test: with `expected_download_kbps` left at its default (0, "no pacing
/// estimate"), the buffer target used to compute as 0 bytes, so `total_buffered <
/// buffer_target` was never true and archived parts never had retrieval requested at all.
/// Unset pacing must fall back to requesting every remaining part immediately.
#[tokio::test]
async fn default_config_should_still_request_retrieval_for_all_archived_parts() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: 20,
        blake3: "irrelevant".to_string(),
        parts: vec![
            PartInfo {
                index: "000000".to_string(),
                size_bytes: 10,
                blake3: "h0".to_string(),
            },
            PartInfo {
                index: "000001".to_string(),
                size_bytes: 10,
                blake3: "h1".to_string(),
            },
        ],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        if key.contains("task_manifest") {
            Ok(RestoreStatusDetail {
                status: RestoreStatus::Ready,
                expires_at: None,
            })
        } else {
            Ok(RestoreStatusDetail {
                status: RestoreStatus::NotRequested,
                expires_at: None,
            })
        }
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_restore()
        .times(2)
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
    .await
    .expect("archived-but-not-yet-ready parts must report progress, not error");

    assert!(!result.completed);
    assert_eq!(result.parts_downloaded, 0);
    assert_eq!(result.parts_total, 2);
    assert_eq!(result.bytes_remaining, 20);
}

/// A manifest whose retrieval was already requested (still `InProgress`) must bail out with
/// an actionable "re-run later" error, not fall through and attempt a `GetObject` against an
/// archived-and-not-yet-restored object (which S3 would reject).
#[tokio::test]
async fn restore_should_bail_cleanly_when_manifest_retrieval_in_progress() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::InProgress,
            expires_at: None,
        })
    });
    // No expect_download / expect_request_restore registered: either being called at all
    // is itself a test failure (mockall panics on an unmatched call).

    let err = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("manifest retrieval in progress must bail, not attempt download");

    assert!(
        err.to_string().contains("in progress"),
        "unexpected error: {err}"
    );
}

/// Regression test for the "false completion" bug: `stream_verified` and `receive_completed`
/// must be tracked as two separate flags. If a run persists `stream_verified = true` but then
/// fails (or is killed) before `btrfs receive` completes, a resumed run must actually retry
/// the receive step — not silently return success because the hash was already verified.
#[tokio::test]
async fn resume_after_stream_verified_should_retry_receive_not_silently_succeed() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};
    use secrecy::ExposeSecret;

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let plain_path = work_dir.path().join("plain.bin");
    std::fs::write(&plain_path, b"stream payload").unwrap();
    let stream_hash = btrfs_cold_backup::blake3_file(&plain_path).await.unwrap();

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

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: encrypted_bytes.len() as u64,
        blake3: stream_hash,
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
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::Ready,
            expires_at: None,
        })
    });
    // Exactly once across both runs: the second run must reuse the manifest cached in the
    // work dir instead of re-fetching it from S3 (whose retrieved copy may have expired).
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .times(1)
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(move |_, _, _, path| {
            std::fs::write(path, &encrypted_bytes)?;
            Ok(())
        });

    let private_key = identity.to_string();

    // First run: downloads and verifies successfully, then fails while actually attempting
    // btrfs receive (no real btrfs subvolume/binary in this test environment).
    let first_err = restore_task(
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
    .expect_err("btrfs receive must fail in this non-btrfs test environment");

    // Second run (simulating a re-run after the first crashed/failed): must retry the
    // receive step again, not silently report success just because stream_verified was
    // already persisted as true.
    let second_err = restore_task(
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
    .expect_err("resumed run must retry receive, not silently succeed");

    assert_eq!(
        first_err.to_string(),
        second_err.to_string(),
        "both runs should fail identically by actually attempting receive"
    );
}

/// Regression test: a part left half-downloaded by an interrupted run must be picked up
/// again by the next run (resuming from the saved offset), not left stuck forever because
/// no code path considered `PartiallyDownloaded` parts downloadable.
#[tokio::test]
async fn restore_should_resume_partially_downloaded_part() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};
    use btrfs_cold_backup::{PartDownloadState, RestorePartState, RestoreState};
    use secrecy::ExposeSecret;
    use std::collections::BTreeMap;

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let plain_path = work_dir.path().join("plain.bin");
    std::fs::write(&plain_path, b"stream payload").unwrap();
    let stream_hash = btrfs_cold_backup::blake3_file(&plain_path).await.unwrap();

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

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: encrypted_bytes.len() as u64,
        blake3: stream_hash.clone(),
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: encrypted_bytes.len() as u64,
            blake3: encrypted_hash.clone(),
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    // Pre-seed the deterministic restore work dir as an interrupted run would have left
    // it: state says half the part is downloaded, and the on-disk file holds that half.
    let half = encrypted_bytes.len() as u64 / 2;
    let restore_dir = work_dir
        .path()
        .join("restore-movies-level0-2026-06-02T00:00:00Z");
    std::fs::create_dir_all(&restore_dir).unwrap();
    std::fs::write(
        restore_dir.join("part-000000.age"),
        &encrypted_bytes[..half as usize],
    )
    .unwrap();
    let mut parts = BTreeMap::new();
    parts.insert(
        "000000".to_string(),
        RestorePartState {
            index: "000000".to_string(),
            size_bytes: encrypted_bytes.len() as u64,
            blake3: encrypted_hash,
            download_state: PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: half,
            },
        },
    );
    let mut seeded = RestoreState {
        task_name: task.name.clone(),
        level: 0,
        timestamp: "2026-06-02T00:00:00Z".to_string(),
        snapshot_name: backup_manifest.snapshot_name.clone(),
        manifest_blake3: stream_hash,
        parts,
        stream_verified: false,
        receive_completed: false,
        last_updated: 0,
    };
    seeded
        .save(&restore_dir.join("restore-state.yaml"))
        .unwrap();

    let remaining: Vec<u8> = encrypted_bytes[half as usize..].to_vec();
    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::Ready,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    // Only the missing byte range may be fetched — exactly once, starting at the saved
    // offset. Any request for start == 0 would mean the resume was lost.
    remote
        .expect_download_range_to_file()
        .withf(move |_, start, _, _| *start == half)
        .times(1)
        .returning(move |_, _, _, path| {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(path)?;
            f.write_all(&remaining)?;
            Ok(())
        });

    let private_key = identity.to_string();
    // Fails only at the btrfs receive step (no btrfs in this test environment); by then
    // the part must have been resumed, verified, and decrypted.
    restore_task(
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
    .expect_err("btrfs receive must fail in this non-btrfs test environment");

    let state: RestoreState = serde_yaml::from_str(
        &std::fs::read_to_string(restore_dir.join("restore-state.yaml")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        state.parts["000000"].download_state,
        PartDownloadState::Downloaded,
        "resumed part must reach Downloaded"
    );
    assert!(state.stream_verified, "stream must verify after resume");
}

/// Regression test for retrieval/download pipelining: the run that drains a ready wave
/// must also request the next wave in the same run. Counting the ready (about-to-be-
/// downloaded) bytes against the request budget used to leave the budget "full", so no
/// new retrievals went out until a later run — serializing every cycle into
/// download-then-wait instead of overlapping the two.
#[tokio::test]
async fn run_draining_ready_wave_should_request_next_wave_within_budget() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};
    use secrecy::ExposeSecret;

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let mut config = test_config(work_dir.path());
    // 1 KiB/s expected download; lead_days chosen so the buffer target is 15 bytes —
    // room for exactly one of the two 10-byte archived parts below.
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1,
        lead_days: Some(15.0 / (1024.0 * 86400.0)),
        ..RestoreConfig::default()
    };
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // Part 000000 is Ready with real content (this run downloads it); 000001 and 000002
    // are still archived and compete for the 15-byte request budget.
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

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: encrypted_bytes.len() as u64 + 20,
        blake3: "stream_hash_not_reached".to_string(),
        parts: vec![
            PartInfo {
                index: "000000".to_string(),
                size_bytes: encrypted_bytes.len() as u64,
                blake3: encrypted_hash,
            },
            PartInfo {
                index: "000001".to_string(),
                size_bytes: 10,
                blake3: "h1".to_string(),
            },
            PartInfo {
                index: "000002".to_string(),
                size_bytes: 10,
                blake3: "h2".to_string(),
            },
        ],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        let status = if key.contains("task_manifest") || key.contains("000000") {
            RestoreStatus::Ready
        } else {
            RestoreStatus::NotRequested
        };
        Ok(RestoreStatusDetail {
            status,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000000"))
        .returning(move |_, _, _, path| {
            std::fs::write(path, &encrypted_bytes)?;
            Ok(())
        });
    // Only 000001 fits the budget; requesting 000002 as well would blow past it, and
    // requesting nothing would mean ready bytes were wrongly counted against the budget.
    remote
        .expect_request_restore()
        .withf(|key, _, _| key.contains("000001"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let private_key = identity.to_string();
    let progress = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        private_key.expose_secret(),
        &RetrievalOptions {
            days: 2,
            tier: "Standard".to_string(),
        },
        &remote,
    )
    .await
    .expect("partial progress must not be an error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 1);
    assert_eq!(progress.parts_total, 3);
}

/// Regression test for the "budget smaller than any part" stall: when the configured pacing
/// budget can't cover even one remaining part, the plain budget gate would skip every part
/// forever — nothing ready, nothing in progress, nothing requested — and the restore would
/// report "in progress" on every run without ever advancing. The gate must force the
/// smallest skipped part through regardless of budget so the run always makes progress.
#[tokio::test]
async fn restore_budget_smaller_than_any_part_should_force_one_request_instead_of_stalling() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};

    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    // 1 KiB/s expected download with a tiny lead_days: buffer_target computes to about 1
    // byte, far under the 10-byte part below, so the plain budget check skips it forever.
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1,
        lead_days: Some(1.0 / (1024.0 * 86400.0)),
        ..RestoreConfig::default()
    };
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: 10,
        blake3: "irrelevant".to_string(),
        parts: vec![PartInfo {
            index: "000000".to_string(),
            size_bytes: 10,
            blake3: "h0".to_string(),
        }],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        let status = if key.contains("task_manifest") {
            RestoreStatus::Ready
        } else {
            RestoreStatus::NotRequested
        };
        Ok(RestoreStatusDetail {
            status,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    // Despite the budget being far smaller than the part, exactly one request must go out —
    // not zero (the stall) and not a loop requesting it repeatedly within the same run.
    remote
        .expect_request_restore()
        .withf(|key, _, _| key.contains("000000"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let progress = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &RetrievalOptions {
            days: 2,
            tier: "Standard".to_string(),
        },
        &remote,
    )
    .await
    .expect("must report progress, not stall or error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 0);
    assert_eq!(progress.parts_total, 1);
}

/// One part's download failing mid-run (expired copy, transient network error) must not
/// abandon the parts that are still downloadable: the run keeps going, saves what it can,
/// and reports in-progress so the scheduler retries — not exit-1 fatal.
#[tokio::test]
async fn part_download_failure_should_not_abort_remaining_parts() {
    use btrfs_cold_backup::manifest::{Backup, PartInfo, SystemInfo};
    use secrecy::ExposeSecret;

    let work_dir = tempfile::tempdir().unwrap();
    let identity = age::x25519::Identity::generate();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

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

    let backup_manifest = Backup {
        timestamp: "2026-06-02T14:30:00Z".to_string(),
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        parent_snapshot: None,
        created_at: 0,
        system: SystemInfo::default(),
        age_public_key: String::new(),
        total_size_bytes: encrypted_bytes.len() as u64 + 10,
        blake3: "stream_hash_not_reached".to_string(),
        parts: vec![
            PartInfo {
                index: "000000".to_string(),
                size_bytes: 10,
                blake3: "h0".to_string(),
            },
            PartInfo {
                index: "000001".to_string(),
                size_bytes: encrypted_bytes.len() as u64,
                blake3: encrypted_hash,
            },
        ],
    };
    let manifest_yaml = serde_yaml::to_string(&backup_manifest)
        .unwrap()
        .into_bytes();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|_| {
        Ok(RestoreStatusDetail {
            status: RestoreStatus::Ready,
            expires_at: None,
        })
    });
    remote
        .expect_download()
        .withf(|key| key.contains("task_manifest"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000000"))
        .returning(|_, _, _, _| Err(anyhow::anyhow!("connection lost")));
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000001"))
        .times(1)
        .returning(move |_, _, _, path| {
            std::fs::write(path, &encrypted_bytes)?;
            Ok(())
        });

    let private_key = identity.to_string();
    let progress = restore_task(
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
    .expect("one failed part must not fail the run");

    assert!(!progress.completed);
    assert_eq!(
        progress.parts_downloaded, 1,
        "the healthy part must still be downloaded"
    );
    assert_eq!(progress.parts_total, 2);
}

/// With pacing enabled, retrieved copies must live long enough for the paced download to
/// reach them; otherwise every wave tail expires and gets re-requested at full per-GB
/// retrieval cost. Such a configuration must be rejected before any S3 call is made.
#[tokio::test]
async fn restore_days_shorter_than_lead_window_should_be_rejected_before_any_s3_call() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1024,
        lead_days: None,
        ..RestoreConfig::default()
    };
    let task = movies_task();
    let target_dir = work_dir.path().join("restore_target");

    // No expectations registered: any S3 call at all is itself a test failure
    // (mockall panics on an unmatched call).
    let remote = MockRemote::new();

    // Bulk tier defaults to a multi-day lead window; 3 days of copy lifetime cannot
    // cover it.
    let err = restore_task(
        &config,
        &task,
        0,
        "20260602",
        &target_dir,
        "AGE-SECRET-KEY-1FAKE",
        &RetrievalOptions {
            days: 3,
            tier: "Bulk".to_string(),
        },
        &remote,
    )
    .await
    .expect_err("insufficient restore-days must be rejected");

    assert!(
        err.to_string().contains("restore-days"),
        "unexpected error: {err}"
    );
}
