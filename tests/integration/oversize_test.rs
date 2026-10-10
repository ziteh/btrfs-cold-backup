use std::collections::HashMap;
use std::path::Path;

use btrfs_cold_backup::{
    backup::{Oversize, backup_task},
    config::{Config, OversizeGuard, StorageClass},
    manifest::{BackupManifest, LatestEntry, LatestManifest},
    remote::ObjectMetadata,
    timestamp::BackupId,
};

use crate::common::*;

const LEVEL0: &str = "bcb_level0.20270101T0100";
const LEVEL0_PART_SIZE: u64 = 100;

/// A guard that only the size per day or the level 0 ratio can trip: the minimum size is
/// below the few hundred bytes the test stream encrypts to.
fn guard(history: usize, fallback_level0_ratio: f64) -> OversizeGuard {
    OversizeGuard {
        history,
        rate_factor: 2.0,
        min_size_gib: 1e-9,
        fallback_level0_ratio,
    }
}

fn config(base_dir: &Path, guard: OversizeGuard) -> Config {
    let mut config = test_config(base_dir);
    config.s3.storage_class.backup_data = vec![StorageClass::Standard; 4];
    config.oversize_guard = Some(guard);
    config
}

/// The manifest of `snapshot`, an incremental of `total_size_bytes` against `parent`.
fn incremental(snapshot: &str, parent: &str, total_size_bytes: u64) -> BackupManifest {
    let id = BackupId::from_snapshot_name(snapshot).unwrap();
    BackupManifest {
        timestamp: id.timestamp,
        level: id.level,
        snapshot_name: snapshot.to_string(),
        parent_snapshot: Some(parent.to_string()),
        snapshot_uuid: snap_uuid(snapshot),
        total_size_bytes,
        ..manifest(vec![])
    }
}

/// S3 holding a level 0 of `level0_size` bytes as the only latest entry, and `history`
/// incrementals, expecting exactly `uploads` part uploads.
fn remote(
    base_dir: &Path,
    level0_size: u64,
    history: Vec<BackupManifest>,
    uploads: usize,
) -> MockRemote {
    let mut manifests: HashMap<String, Vec<u8>> = HashMap::new();
    let key = |m: &BackupManifest| {
        format!(
            "backups/manifests/mnt_data_movies/{}/manifest.yaml",
            m.id().dir_name()
        )
    };
    let level0 = BackupManifest {
        timestamp: BackupId::from_snapshot_name(LEVEL0).unwrap().timestamp,
        snapshot_name: LEVEL0.to_string(),
        snapshot_uuid: snap_uuid(LEVEL0),
        total_size_bytes: level0_size,
        ..manifest(vec![part("000000", LEVEL0_PART_SIZE, "")])
    };
    manifests.insert(key(&level0), yaml(&level0));
    for m in &history {
        manifests.insert(key(m), yaml(m));
    }

    let mut latest = LatestManifest::new(SOURCE_UUID.to_string());
    latest.levels.insert(
        0,
        LatestEntry {
            timestamp: level0.timestamp.clone(),
            snapshot_name: LEVEL0.to_string(),
            snapshot_uuid: snap_uuid(LEVEL0),
        },
    );
    let latest = yaml(&latest);

    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.ends_with("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.ends_with("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.ends_with("latest.yaml"))
        .returning(move |_| Ok(latest.clone()));
    let listed: Vec<String> = manifests.keys().cloned().collect();
    remote
        .expect_list_with_prefix()
        .returning(move |_| Ok(listed.clone()));
    remote
        .expect_download()
        .returning(move |key| Ok(manifests[key].clone()));
    expect_parts_on_s3(&mut remote, base_dir);
    remote.expect_head_metadata().returning(|_| {
        Ok(Some(ObjectMetadata {
            size_bytes: LEVEL0_PART_SIZE,
            storage_class: "STANDARD".to_string(),
        }))
    });
    remote
        .expect_upload_file()
        .times(uploads)
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote
}

/// Three weekly level 3 backups of `bytes_per_day` bytes per day each.
fn weekly_history(bytes_per_day: u64) -> Vec<BackupManifest> {
    [
        ("bcb_level3.20270108T0100", LEVEL0),
        ("bcb_level3.20270115T0100", LEVEL0),
        ("bcb_level3.20270122T0100", LEVEL0),
    ]
    .iter()
    .enumerate()
    .map(|(i, (snap, parent))| incremental(snap, parent, bytes_per_day * 7 * (i as u64 + 1)))
    .collect()
}

/// Runs a level 3 backup of a snapshot taken `snapshot` against LEVEL0.
async fn level3(
    config: &Config,
    snapshot: &'static str,
    remote: &MockRemote,
    allow_oversize: bool,
) -> anyhow::Result<()> {
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![LEVEL0.to_string(), snapshot.to_string()]));
    btrfs
        .expect_send_incremental()
        .returning(|_, _| Ok(fake_stream()));
    backup_task(config, &movies_task(), 3, &btrfs, remote, allow_oversize)
        .await
        .map(|_| ())
}

#[tokio::test]
async fn incremental_far_above_the_usual_size_per_day_should_be_held_before_upload() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config(work_dir.path(), guard(3, 1.0));
    let remote = remote(work_dir.path(), 1_000_000, weekly_history(1), 0);

    let err = level3(&config, "bcb_level3.20270129T0100", &remote, false)
        .await
        .unwrap_err();

    let oversize = err.downcast_ref::<Oversize>().expect("must be Oversize");
    assert!(oversize.basis.contains("median"), "{}", oversize.basis);
    let task_dir = work_dir.path().join("backup/mnt_data_movies");
    assert!(task_dir.join("state-level3.yaml").exists());
    assert!(task_dir.join("bcb_level3_202701290100").exists());
}

/// A held-back incremental resumes from its kept parts once allowed, without a new send.
#[tokio::test]
async fn allow_oversize_should_upload_a_held_incremental() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config(work_dir.path(), guard(3, 1.0));
    let held = remote(work_dir.path(), 1_000_000, weekly_history(1), 0);
    level3(&config, "bcb_level3.20270129T0100", &held, false)
        .await
        .unwrap_err();

    let allowing = remote(work_dir.path(), 1_000_000, weekly_history(1), 1);
    // No btrfs expectations: the resume must not send again.
    backup_task(
        &config,
        &movies_task(),
        3,
        &MockBtrfs::new(),
        &allowing,
        true,
    )
    .await
    .expect("allowed backup should succeed");
}

/// The first incremental after a level 0 accumulates weeks of changes; per day it is usual.
#[tokio::test]
async fn cumulative_incremental_of_usual_size_per_day_should_pass() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config(work_dir.path(), guard(3, 1.0));
    let remote = remote(work_dir.path(), 1_000_000, weekly_history(10), 1);

    level3(&config, "bcb_level3.20270212T0100", &remote, false)
        .await
        .expect("a usual size per day must pass");
}

#[tokio::test]
async fn without_enough_history_level0_ratio_should_apply() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config(work_dir.path(), guard(4, 0.1));
    let remote = remote(work_dir.path(), 1000, weekly_history(1000), 0);

    let err = level3(&config, "bcb_level3.20270129T0100", &remote, false)
        .await
        .unwrap_err();

    let oversize = err.downcast_ref::<Oversize>().expect("must be Oversize");
    assert!(oversize.basis.contains("level 0"), "{}", oversize.basis);
}

#[tokio::test]
async fn level0_should_not_be_checked() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config(work_dir.path(), guard(3, 1e-9));
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20280101T0100".to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));
    // No list or download expectations: a level 0 must not look at history.
    let mut remote = MockRemote::new();
    expect_no_manifests(&mut remote);
    expect_parts_on_s3(&mut remote, work_dir.path());
    remote
        .expect_upload_file()
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    backup_task(&config, &movies_task(), 0, &btrfs, &remote, false)
        .await
        .expect("level 0 backup should succeed");
}
