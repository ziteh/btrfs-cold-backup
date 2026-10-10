use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use btrfs_cold_backup::{
    btrfs::SubvolumeUuids,
    manifest::{LatestEntry, LatestManifest},
    prune::prune_task,
    state::{BackupState, BackupTarget, StateFile},
    timestamp::BackupId,
};

use crate::common::*;

/// Btrfs holding `snapshots` of movies, recording the paths it is asked to delete.
fn btrfs_holding(snapshots: &[&str]) -> (MockBtrfs, Arc<Mutex<Vec<PathBuf>>>) {
    let names: Vec<String> = snapshots.iter().map(|s| s.to_string()).collect();
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(names.clone()));
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let recorded = deleted.clone();
    btrfs.expect_delete_subvolume().returning(move |path| {
        recorded.lock().unwrap().push(path.to_path_buf());
        Ok(())
    });
    (btrfs, deleted)
}

fn latest_of(source_uuid: &str, snapshot_names: &[&str]) -> Vec<u8> {
    let mut latest = LatestManifest::new(source_uuid.to_string());
    for name in snapshot_names {
        let id = BackupId::from_snapshot_name(name).unwrap();
        latest.levels.insert(
            id.level,
            LatestEntry {
                timestamp: id.timestamp,
                snapshot_name: name.to_string(),
                snapshot_uuid: snap_uuid(name),
            },
        );
    }
    yaml(&latest)
}

fn remote_with_latest(latest: Option<Vec<u8>>) -> MockRemote {
    let mut remote = MockRemote::new();
    let exists = latest.is_some();
    remote
        .expect_head_metadata()
        .withf(|key| key.ends_with("latest.yaml"))
        .returning(move |_| head(exists));
    if let Some(latest) = latest {
        remote
            .expect_download()
            .withf(|key| key.ends_with("latest.yaml"))
            .returning(move |_| Ok(latest.clone()));
    }
    remote
}

fn save_state(base_dir: &Path, snapshot: &str, parent: Option<&str>) {
    let id = BackupId::from_snapshot_name(snapshot).unwrap();
    let path = base_dir.join(format!(
        "backup/mnt_data_movies/state-level{}.yaml",
        id.level
    ));
    let target = BackupTarget {
        id,
        snapshot_name: snapshot.to_string(),
        parent_snapshot: parent.map(str::to_string),
        source_uuid: SOURCE_UUID.to_string(),
        snapshot_uuid: snap_uuid(snapshot),
    };
    StateFile::create(path, BackupState::new(target)).unwrap();
}

/// Runs a prune of movies and returns what it reported and the names it actually deleted.
async fn prune(
    base_dir: &Path,
    btrfs: &MockBtrfs,
    deleted: &Mutex<Vec<PathBuf>>,
    remote: &MockRemote,
    dry_run: bool,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let reported = prune_task(
        &test_config(base_dir),
        &movies_task(),
        btrfs,
        remote,
        dry_run,
    )
    .await?;
    let deleted = deleted
        .lock()
        .unwrap()
        .iter()
        .map(|p| {
            assert_eq!(p.parent().unwrap(), movies_task().snapshot_dir);
            p.file_name().unwrap().to_str().unwrap().to_string()
        })
        .collect();
    Ok((reported, deleted))
}

const L0_OLD: &str = "bcb_level0.20260101T0100";
const L0: &str = "bcb_level0.20270101T0100";
const L2_OLD: &str = "bcb_level2.20270115T0100";
const L2: &str = "bcb_level2.20270215T0100";
const L3_OLD: &str = "bcb_level3.20270207T0100";
const L3_BACKED_UP: &str = "bcb_level3.20270221T0100";
const L3_NEWEST: &str = "bcb_level3.20270228T0100";

#[tokio::test]
async fn prune_should_keep_latest_entries_and_newest_of_each_level() {
    let work_dir = tempfile::tempdir().unwrap();
    let (btrfs, deleted) =
        btrfs_holding(&[L0_OLD, L0, L2_OLD, L2, L3_OLD, L3_BACKED_UP, L3_NEWEST]);
    let remote = remote_with_latest(Some(latest_of(SOURCE_UUID, &[L0, L2, L3_BACKED_UP])));

    let (reported, deleted) = prune(work_dir.path(), &btrfs, &deleted, &remote, false)
        .await
        .unwrap();

    assert_eq!(reported, [L0_OLD, L2_OLD, L3_OLD]);
    assert_eq!(deleted, reported);
}

/// An unfinished backup still sends its target relative to its parent, which a lower level's
/// completion may already have dropped from the latest manifest.
#[tokio::test]
async fn prune_should_keep_target_and_parent_of_unfinished_backups() {
    let work_dir = tempfile::tempdir().unwrap();
    save_state(work_dir.path(), L3_OLD, Some(L2_OLD));
    let (btrfs, deleted) = btrfs_holding(&[L0_OLD, L0, L2_OLD, L2, L3_OLD, L3_NEWEST]);
    let remote = remote_with_latest(Some(latest_of(SOURCE_UUID, &[L0])));

    let (reported, _) = prune(work_dir.path(), &btrfs, &deleted, &remote, false)
        .await
        .unwrap();

    assert_eq!(reported, [L0_OLD]);
}

#[tokio::test]
async fn dry_run_should_report_without_deleting() {
    let work_dir = tempfile::tempdir().unwrap();
    let (btrfs, deleted) = btrfs_holding(&[L0_OLD, L0]);
    let remote = remote_with_latest(Some(latest_of(SOURCE_UUID, &[L0])));

    let (reported, deleted) = prune(work_dir.path(), &btrfs, &deleted, &remote, true)
        .await
        .unwrap();

    assert_eq!(reported, [L0_OLD]);
    assert!(deleted.is_empty());
}

/// A missing latest manifest may mean a wrong bucket or prefix rather than no backup yet, and
/// then deleting would cost the snapshots the real chain builds on.
#[tokio::test]
async fn prune_should_delete_nothing_without_latest_manifest() {
    let work_dir = tempfile::tempdir().unwrap();
    let (btrfs, deleted) = btrfs_holding(&[L0_OLD, L0, L3_OLD, L3_NEWEST]);

    let (reported, deleted) = prune(
        work_dir.path(),
        &btrfs,
        &deleted,
        &remote_with_latest(None),
        false,
    )
    .await
    .unwrap();

    assert!(reported.is_empty());
    assert!(deleted.is_empty());
}

/// Runs a prune that must fail without deleting anything, and returns its error.
async fn prune_error(remote: MockRemote, snapshots: &[&str]) -> String {
    let work_dir = tempfile::tempdir().unwrap();
    let (btrfs, deleted) = btrfs_holding(snapshots);
    let err = prune(work_dir.path(), &btrfs, &deleted, &remote, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(deleted.lock().unwrap().is_empty());
    err
}

#[tokio::test]
async fn prune_should_delete_nothing_when_latest_manifest_is_corrupt() {
    let err = prune_error(
        remote_with_latest(Some(b"not: [valid".to_vec())),
        &[L0_OLD, L0],
    )
    .await;
    assert!(err.contains("latest.yaml"), "unexpected error: {err}");
}

#[tokio::test]
async fn prune_should_delete_nothing_when_latest_manifest_is_of_another_subvolume() {
    let err = prune_error(
        remote_with_latest(Some(latest_of("someone-else-uuid", &[L0]))),
        &[L0_OLD, L0],
    )
    .await;
    assert!(err.contains("s3.prefix"), "unexpected error: {err}");
}

#[tokio::test]
async fn prune_should_delete_nothing_when_a_latest_entry_is_not_local() {
    let err = prune_error(
        remote_with_latest(Some(latest_of(SOURCE_UUID, &[L0, L2]))),
        &[L0_OLD, L0, L3_NEWEST],
    )
    .await;
    assert!(err.contains(L2), "unexpected error: {err}");
}

#[tokio::test]
async fn prune_should_leave_snapshots_of_other_subvolumes_and_foreign_names_alone() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut btrfs = MockBtrfs::new();
    btrfs.expect_list_snapshots().returning(|_| {
        Ok(vec![
            "bcb_level0.20250101T0100".to_string(),
            L0_OLD.to_string(),
            L0.to_string(),
            "bcb_level0.20260101T0100_1".to_string(),
        ])
    });
    btrfs.expect_subvolume_uuids().returning(|path| {
        if path == movies_task().subvolume {
            return Ok(SubvolumeUuids {
                uuid: SOURCE_UUID.to_string(),
                parent_uuid: None,
            });
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let parent = match name {
            "bcb_level0.20250101T0100" => "another-subvol-uuid",
            _ => SOURCE_UUID,
        };
        Ok(SubvolumeUuids {
            uuid: snap_uuid(name),
            parent_uuid: Some(parent.to_string()),
        })
    });
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let recorded = deleted.clone();
    btrfs.expect_delete_subvolume().returning(move |path| {
        recorded.lock().unwrap().push(path.to_path_buf());
        Ok(())
    });
    let remote = remote_with_latest(Some(latest_of(SOURCE_UUID, &[L0])));

    let (reported, _) = prune(work_dir.path(), &btrfs, &deleted, &remote, false)
        .await
        .unwrap();

    assert_eq!(reported, [L0_OLD]);
}
