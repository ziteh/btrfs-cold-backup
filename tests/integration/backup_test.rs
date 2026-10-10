use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    backup::backup_task,
    btrfs::SubvolumeUuids,
    config::{Config, StorageClass},
    manifest::{LatestEntry, LatestManifest},
    remote::ObjectMetadata,
    state::{BackupState, BackupTarget, PartRecord, SentStream, StateFile},
    timestamp::{BackupId, Timestamp},
};
use tokio::io::AsyncRead;

use crate::common::*;

#[tokio::test]
async fn level0_full_backup_should_succeed_and_return_manifest() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_upload_file()
        .withf(|_, _, _, tags| tags == "bcb-backup-level=0")
        .returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());
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

struct FailingReader;

impl AsyncRead for FailingReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::other("send died")))
    }
}

/// A send that fails partway must not leave its parts behind to take space from the tasks
/// that run after it.
#[tokio::test]
async fn failed_send_should_remove_output_dir() {
    use tokio::io::AsyncReadExt;

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| {
        Ok(Box::new(
            Cursor::new(b"partial stream".to_vec()).chain(FailingReader),
        ))
    });

    let mut remote = MockRemote::new();
    expect_no_manifests(&mut remote);

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("send died"), "unexpected error: {err}");
    assert!(
        !work_dir
            .path()
            .join("backup/mnt_data_movies/bcb_level0_202606021430")
            .exists(),
        "output_dir must be removed after a failed send"
    );
}

#[tokio::test]
async fn backup_should_fail_when_no_matching_snapshot() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs.expect_list_snapshots().returning(|_| Ok(vec![]));

    let remote = MockRemote::new();

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;
    assert!(result.is_err(), "should fail with no matching snapshot");
}

/// A corrupted latest manifest must fail loudly rather than being silently reset to
/// empty — a silent reset would drop other levels' entries and break their incremental
/// chains without warning.
#[tokio::test]
async fn backup_should_fail_when_latest_manifest_is_corrupt() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| Ok(b"[not, a, valid, latest-manifest, mapping]".to_vec()));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("failed to parse latest manifest"),
        "unexpected error: {err}"
    );
}

/// Makes every ancestor manifest downloadable, each listing one part of `ANCESTOR_PART_SIZE`
/// bytes. `head_metadata` answers each part with the size `part_size` returns for its key,
/// None meaning the part doesn't exist.
fn expect_ancestors(remote: &mut MockRemote, part_size: fn(&str) -> Result<Option<u64>>) {
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| {
            Ok(yaml(&manifest(vec![part(
                "000000",
                ANCESTOR_PART_SIZE,
                "",
            )])))
        });
    remote.expect_head_metadata().returning(move |key| {
        Ok(part_size(key)?.map(|size_bytes| ObjectMetadata {
            size_bytes,
            storage_class: "STANDARD".to_string(),
        }))
    });
}

const ANCESTOR_PART_SIZE: u64 = 100;

#[tokio::test]
async fn level1_incremental_should_use_level0_as_parent() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));
    btrfs
        .expect_send_incremental()
        .withf(move |p, _| p.ends_with(parent))
        .returning(|_, _| Ok(fake_stream()));

    let manifest_bytes = latest_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(manifest_bytes.clone()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    expect_ancestors(&mut remote, |_| Ok(Some(ANCESTOR_PART_SIZE)));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;

    let backup = result
        .expect("level 1 backup should succeed")
        .expect("must not be skipped");
    assert_eq!(backup.level, 1);
    assert_eq!(backup.parent_snapshot.as_deref(), Some(parent));
}

/// The chain logic must not be special-cased to levels 0 and 1.
#[tokio::test]
async fn level2_incremental_should_use_level1_as_parent() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    config
        .s3
        .storage_class
        .backup_data
        .push(StorageClass::Glacier);
    let task = movies_task();
    let parent = "bcb_level1.20260601T1430";
    let snap = "bcb_level2.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));
    btrfs
        .expect_send_incremental()
        .withf(move |p, _| p.ends_with(parent))
        .returning(|_, _| Ok(fake_stream()));

    let manifest_bytes = latest_manifest_yaml(parent, 1);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(manifest_bytes.clone()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    expect_ancestors(&mut remote, |_| Ok(Some(ANCESTOR_PART_SIZE)));
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

/// Latest manifest of movies with an entry per snapshot name, each at the level and time its
/// name holds.
fn latest_with(snapshot_names: &[&str]) -> Vec<u8> {
    let mut latest = LatestManifest::new(SOURCE_UUID.to_string());
    for name in snapshot_names {
        let level = name["bcb_level".len()..name.find('.').unwrap()]
            .parse()
            .unwrap();
        latest.levels.insert(
            level,
            LatestEntry {
                timestamp: Timestamp::from_snapshot_name(name).unwrap(),
                snapshot_name: name.to_string(),
                snapshot_uuid: snap_uuid(name),
            },
        );
    }
    yaml(&latest)
}

/// Remote for an incremental backup of movies in `base_dir` against a latest manifest
/// holding `latest`, with every ancestor's parts on S3.
fn remote_for_incremental(base_dir: &Path, latest: &[&str]) -> MockRemote {
    let latest_bytes = latest_with(latest);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(latest_bytes.clone()));
    expect_parts_on_s3(&mut remote, base_dir);
    expect_ancestors(&mut remote, |_| Ok(Some(ANCESTOR_PART_SIZE)));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote
}

/// Config for movies in `base_dir` with a storage class for levels 0 to 3.
fn four_level_config(base_dir: &Path) -> Config {
    let mut config = test_config(base_dir);
    config.s3.storage_class.backup_data = vec![StorageClass::Standard; 4];
    config
}

/// Btrfs holding `snapshots` of movies, any of which can be sent incrementally.
fn btrfs_for_incremental(snapshots: Vec<String>) -> MockBtrfs {
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(snapshots.clone()));
    btrfs
        .expect_send_incremental()
        .returning(|_, _| Ok(fake_stream()));
    btrfs
}

/// Runs a level `level` backup of `snap` against a latest manifest holding `latest` and
/// returns the parent it was sent relative to, or the error.
async fn incremental_parent(latest: &[&str], snap: &'static str, level: u32) -> Result<String> {
    let work_dir = tempfile::tempdir().unwrap();
    let mut local: Vec<String> = latest.iter().map(|s| s.to_string()).collect();
    local.push(snap.to_string());
    let backup = backup_task(
        &four_level_config(work_dir.path()),
        &movies_task(),
        level,
        &btrfs_for_incremental(local),
        &remote_for_incremental(work_dir.path(), latest),
    )
    .await?
    .expect("must not be skipped");
    Ok(backup.parent_snapshot.expect("an incremental has a parent"))
}

/// Without a level 1 backup, a level 2 builds on level 0 directly, as in Unix `dump`.
#[tokio::test]
async fn level2_should_use_level0_as_parent_when_no_level1_exists() {
    let parent = incremental_parent(&["bcb_level0.20260101T0100"], "bcb_level2.20260215T0100", 2)
        .await
        .unwrap();
    assert_eq!(parent, "bcb_level0.20260101T0100");
}

#[tokio::test]
async fn level3_should_use_deepest_lower_level_as_parent() {
    let parent = incremental_parent(
        &["bcb_level0.20260101T0100", "bcb_level1.20260401T0100"],
        "bcb_level3.20260405T0100",
        3,
    )
    .await
    .unwrap();
    assert_eq!(parent, "bcb_level1.20260401T0100");
}

/// Only lower levels can be parents: a backup never builds on one of its own level or deeper.
#[tokio::test]
async fn level1_should_fail_when_latest_has_no_lower_level() {
    let err = incremental_parent(&["bcb_level2.20260101T0100"], "bcb_level1.20260215T0100", 1)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("run a level 0 backup first"),
        "unexpected error: {err}"
    );
}

/// Saves an unfinished backup of `snapshot` against `parent`, sent as one part that is not
/// yet uploaded, as an interrupted run of its level leaves it. Returns its state's path.
fn save_unfinished(base_dir: &Path, snapshot: &str, parent: Option<&str>) -> PathBuf {
    let id = BackupId {
        level: snapshot["bcb_level".len()..snapshot.find('.').unwrap()]
            .parse()
            .unwrap(),
        timestamp: Timestamp::from_snapshot_name(snapshot).unwrap(),
    };
    let task_dir = base_dir.join("backup/mnt_data_movies");
    let parts_dir = task_dir.join(id.dir_name());
    std::fs::create_dir_all(&parts_dir).unwrap();
    let bytes = b"unfinished part";
    std::fs::write(parts_dir.join("snapshot.part-000000.age"), bytes).unwrap();
    let mut state = BackupState::new(BackupTarget {
        id: id.clone(),
        snapshot_name: snapshot.to_string(),
        parent_snapshot: parent.map(str::to_string),
        source_uuid: SOURCE_UUID.to_string(),
        snapshot_uuid: snap_uuid(snapshot),
    });
    state.sent = Some(SentStream {
        blake3: "streamhash".to_string(),
        parts: [(
            "000000".to_string(),
            PartRecord {
                size_bytes: bytes.len() as u64,
                blake3: blake3::hash(bytes).to_hex().to_string(),
            },
        )]
        .into(),
    });
    let path = task_dir.join(format!("state-level{}.yaml", id.level));
    StateFile::create(path.clone(), state).unwrap();
    path
}

/// A long backup of one level, e.g. a level 0 taking weeks to upload, must not hold up the
/// other levels, nor be disturbed by them.
#[tokio::test]
async fn unfinished_backup_of_another_level_should_neither_block_nor_be_touched() {
    let work_dir = tempfile::tempdir().unwrap();
    let level0_state = save_unfinished(work_dir.path(), "bcb_level0.20270101T0100", None);
    let before = std::fs::read(&level0_state).unwrap();

    let backup = backup_task(
        &four_level_config(work_dir.path()),
        &movies_task(),
        3,
        &btrfs_for_incremental(vec![
            "bcb_level0.20260101T0100".to_string(),
            "bcb_level3.20270103T0100".to_string(),
        ]),
        &remote_for_incremental(work_dir.path(), &["bcb_level0.20260101T0100"]),
    )
    .await
    .expect("level 3 backup should succeed")
    .expect("must not be skipped");

    assert_eq!(
        backup.parent_snapshot.as_deref(),
        Some("bcb_level0.20260101T0100")
    );
    assert_eq!(std::fs::read(&level0_state).unwrap(), before);
    assert!(
        work_dir
            .path()
            .join("backup/mnt_data_movies/bcb_level0_202701010100")
            .exists()
    );
}

#[tokio::test]
async fn unfinished_incremental_should_resume_when_its_parent_is_current() {
    let work_dir = tempfile::tempdir().unwrap();
    let parent = "bcb_level0.20260101T0100";
    save_unfinished(work_dir.path(), "bcb_level3.20260104T0100", Some(parent));

    // No btrfs expectations: a resume must neither list snapshots nor send again.
    let backup = backup_task(
        &four_level_config(work_dir.path()),
        &movies_task(),
        3,
        &MockBtrfs::new(),
        &remote_for_incremental(work_dir.path(), &[parent]),
    )
    .await
    .expect("resumed backup should succeed")
    .expect("must not be skipped");

    assert_eq!(backup.snapshot_name, "bcb_level3.20260104T0100");
    assert_eq!(backup.parent_snapshot.as_deref(), Some(parent));
}

/// Once a lower level completes, an unfinished incremental against the old chain is worthless:
/// committing it would put a stale chain in the latest manifest. It is dropped and the newest
/// snapshot is backed up against the new parent instead, without anyone stepping in.
#[tokio::test]
async fn unfinished_incremental_should_be_redone_when_its_parent_was_superseded() {
    let work_dir = tempfile::tempdir().unwrap();
    let old_state = save_unfinished(
        work_dir.path(),
        "bcb_level3.20261230T0100",
        Some("bcb_level2.20261215T0100"),
    );
    let old_parts = work_dir
        .path()
        .join("backup/mnt_data_movies/bcb_level3_202612300100");
    let new_parent = "bcb_level0.20270101T0100";

    let backup = backup_task(
        &four_level_config(work_dir.path()),
        &movies_task(),
        3,
        &btrfs_for_incremental(vec![
            new_parent.to_string(),
            "bcb_level3.20270103T0100".to_string(),
        ]),
        &remote_for_incremental(work_dir.path(), &[new_parent]),
    )
    .await
    .expect("level 3 backup should succeed")
    .expect("must not be skipped");

    assert_eq!(backup.snapshot_name, "bcb_level3.20270103T0100");
    assert_eq!(backup.parent_snapshot.as_deref(), Some(new_parent));
    assert!(!old_parts.exists(), "the abandoned parts must be removed");
    assert!(!old_state.exists(), "the abandoned state must be removed");
}

/// Level 1 backup whose level 0 ancestor's parts are answered by `part_size`.
async fn level1_backup_with_ancestor_parts(part_size: fn(&str) -> Result<Option<u64>>) -> String {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));

    let mut remote = remote_with_latest(latest_manifest_yaml(parent, 0));
    expect_ancestors(&mut remote, part_size);

    backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string()
}

/// An ancestor part that is gone (e.g. expired by a lifecycle rule, which leaves manifests
/// alone) makes the new incremental unrestorable, so the backup must fail before sending.
#[tokio::test]
async fn level1_should_fail_before_send_when_level0_part_is_missing() {
    let err = level1_backup_with_ancestor_parts(|_| Ok(None)).await;
    assert!(
        err.contains("level 0 backup") && err.contains("snapshot.part-000000"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn level1_should_fail_before_send_when_level0_part_size_differs() {
    let err = level1_backup_with_ancestor_parts(|_| Ok(Some(ANCESTOR_PART_SIZE - 1))).await;
    assert!(err.contains("S3 reports"), "unexpected error: {err}");
}

/// A level whose storage class isn't configured must fail loudly before ever uploading,
/// rather than silently falling back to some default storage class.
#[tokio::test]
async fn backup_should_fail_when_no_storage_class_configured_for_level() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    config.s3.storage_class.backup_data = vec![StorageClass::Standard];
    let task = movies_task();

    // Fails before the send, which can take hours, not after it.
    let btrfs = MockBtrfs::new();
    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("no storage class configured for level 1"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn level1_should_fail_when_no_remote_level0_exists() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level1.20260602T1430".to_string()]));

    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(false)); // no level-0 backup
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    assert!(result.is_err(), "should fail without level 0 remote backup");
}

#[tokio::test]
async fn level1_should_fail_when_parent_snapshot_missing_locally() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";
    let snap = "bcb_level1.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));

    let manifest_bytes = latest_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(manifest_bytes.clone()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    assert!(
        result.is_err(),
        "should fail when parent snapshot missing locally"
    );
}

/// A completed backup (manifest already on remote) must be skipped, not re-run and
/// overwritten: overwriting archived objects incurs minimum-storage-duration charges, and
/// skipping keeps re-runs idempotent.
#[tokio::test]
async fn backup_should_skip_already_completed_backup() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260602T1430".to_string()]));

    let manifest = backup_manifest_yaml("bcb_level0.20260602T1430", SOURCE_UUID);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest.clone()));

    let result = backup_task(&config, &task, 0, &btrfs, &remote).await;
    assert!(
        result.expect("skip must not be an error").is_none(),
        "completed backup must be skipped"
    );
}

fn backup_manifest_yaml(snapshot_name: &str, source_uuid: &str) -> Vec<u8> {
    let mut backup = manifest(vec![]);
    backup.snapshot_name = snapshot_name.to_string();
    backup.snapshot_uuid = snap_uuid(snapshot_name);
    backup.source_uuid = source_uuid.to_string();
    yaml(&backup)
}

/// A manifest at this snapshot's key that records another source means the bucket/prefix is
/// shared with something else; treating it as "already backed up" would silently leave this
/// snapshot without a backup.
#[tokio::test]
async fn skip_should_fail_when_existing_manifest_is_from_another_source() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260602T1430".to_string()]));

    let manifest = backup_manifest_yaml("bcb_level0.20260602T1430", "someone-else-uuid");
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest.clone()));

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("another host"), "unexpected error: {err}");
}

#[tokio::test]
async fn backup_should_fail_when_snapshot_parent_uuid_mismatches() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260602T1430".to_string()]));
    btrfs.expect_subvolume_uuids().returning(|path| {
        Ok(SubvolumeUuids {
            uuid: path.display().to_string(),
            parent_uuid: Some("other-subvol-uuid".to_string()),
        })
    });

    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("was not taken from"),
        "unexpected error: {err}"
    );
}

/// Remote mock for a level-1 backup whose latest manifest is `latest`.
fn remote_with_latest(latest: Vec<u8>) -> MockRemote {
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(latest.clone()));
    remote
}

#[tokio::test]
async fn level1_should_fail_when_latest_manifest_is_from_another_source() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs.expect_list_snapshots().returning(move |_| {
        Ok(vec![
            parent.to_string(),
            "bcb_level1.20260602T1430".to_string(),
        ])
    });

    let mut latest = LatestManifest::new("replaced-disk-uuid".to_string());
    latest.levels.insert(
        0,
        LatestEntry {
            timestamp: "2026-06-01T14:30".parse().unwrap(),
            snapshot_name: parent.to_string(),
            snapshot_uuid: snap_uuid(parent),
        },
    );
    let remote = remote_with_latest(serde_yaml::to_string(&latest).unwrap().into_bytes());

    let err = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("level 0 backup"), "unexpected error: {err}");
}

/// A local snapshot that merely shares the parent's name (e.g. recreated after deletion) is
/// not the stream the remote chain was built on; sending against it would produce an
/// incremental that can't be applied on restore.
#[tokio::test]
async fn level1_should_fail_when_local_parent_uuid_mismatches() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let parent = "bcb_level0.20260601T1430";

    let mut btrfs = MockBtrfs::new();
    btrfs.expect_list_snapshots().returning(move |_| {
        Ok(vec![
            parent.to_string(),
            "bcb_level1.20260602T1430".to_string(),
        ])
    });
    btrfs.expect_subvolume_uuids().returning(move |path| {
        let name = path.file_name().unwrap().to_str().unwrap();
        Ok(if path == movies_task().subvolume {
            SubvolumeUuids {
                uuid: SOURCE_UUID.to_string(),
                parent_uuid: None,
            }
        } else if name == parent {
            SubvolumeUuids {
                uuid: "recreated-parent-uuid".to_string(),
                parent_uuid: Some(SOURCE_UUID.to_string()),
            }
        } else {
            SubvolumeUuids {
                uuid: snap_uuid(name),
                parent_uuid: Some(SOURCE_UUID.to_string()),
            }
        })
    });
    let remote = remote_with_latest(latest_manifest_yaml(parent, 0));

    let err = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("different snapshot"),
        "unexpected error: {err}"
    );
}

/// A level 0 of a replaced source starts a new chain: the old latest manifest's entries (all
/// levels) belong to the previous source and must not survive.
#[tokio::test]
async fn level0_of_new_source_should_replace_latest_manifest() {
    use std::sync::{Arc, Mutex};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0.20260701T0100";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut old = LatestManifest::new("old-disk-uuid".to_string());
    old.levels.insert(
        0,
        LatestEntry {
            timestamp: "2026-01-01T01:00".parse().unwrap(),
            snapshot_name: "bcb_level0.20260101T0100".to_string(),
            snapshot_uuid: "old-snap-uuid".to_string(),
        },
    );
    let old_yaml = serde_yaml::to_string(&old).unwrap().into_bytes();

    let uploaded: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let uploaded_clone = uploaded.clone();
    let mut remote = remote_with_latest(old_yaml);
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("/manifest.yaml"))
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("latest.yaml"))
        .returning(move |_, data, _, _| {
            *uploaded_clone.lock().unwrap() = Some(data);
            Ok(())
        });

    backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("level 0 backup should succeed")
        .expect("must not be skipped");

    let bytes = uploaded.lock().unwrap().clone().unwrap();
    let latest = LatestManifest::parse(&bytes).unwrap();
    assert_eq!(latest.source_uuid, SOURCE_UUID);
    assert_eq!(latest.levels.len(), 1);
    assert_eq!(latest.levels[&0].snapshot_uuid, snap_uuid(snap));
}

/// bcb identifies a backup down to the minute, so btrbk's same-minute snapshot (`_<n>`) has no
/// key of its own. It must fail loudly: reusing the first snapshot's key would make it look
/// already backed up and skip it, although it was never uploaded.
#[tokio::test]
async fn same_minute_snapshot_should_fail_loudly_not_be_silently_skipped() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs.expect_list_snapshots().returning(|_| {
        Ok(vec![
            "bcb_level0.20260602T1430".to_string(),
            "bcb_level0.20260602T1430_1".to_string(),
        ])
    });

    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect_err("a same-minute snapshot must not be silently skipped")
        .to_string();
    assert!(err.contains("same-minute"), "unhelpful message: {err}");
}

/// A new level 0 backup must drop every deeper level's entry from the latest manifest. Those
/// entries belong to a chain built on the superseded snapshot; left in place, a later
/// incremental could chain onto a stale parent instead of failing and asking for a fresh one.
#[tokio::test]
async fn new_level0_backup_should_prune_stale_deeper_level_entries_from_latest_manifest() {
    use std::sync::{Arc, Mutex};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260701T0100".to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    let mut stale_last = LatestManifest::new(SOURCE_UUID.to_string());
    stale_last.levels.insert(
        0,
        LatestEntry {
            timestamp: "2026-01-01T01:00".parse().unwrap(),
            snapshot_name: "bcb_level0.20260101T0100".to_string(),
            snapshot_uuid: snap_uuid("bcb_level0.20260101T0100"),
        },
    );
    stale_last.levels.insert(
        1,
        LatestEntry {
            timestamp: "2026-01-07T03:00".parse().unwrap(),
            snapshot_name: "bcb_level1.20260107T0300".to_string(),
            snapshot_uuid: snap_uuid("bcb_level1.20260107T0300"),
        },
    );
    stale_last.levels.insert(
        2,
        LatestEntry {
            timestamp: "2026-01-14T03:00".parse().unwrap(),
            snapshot_name: "bcb_level2.20260114T0300".to_string(),
            snapshot_uuid: snap_uuid("bcb_level2.20260114T0300"),
        },
    );
    let stale_last_yaml = serde_yaml::to_string(&stale_last).unwrap().into_bytes();

    let uploaded_last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let uploaded_last_clone = uploaded_last.clone();

    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(stale_last_yaml.clone()));
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("/manifest.yaml"))
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("latest.yaml"))
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
        .expect("latest manifest must be re-uploaded");
    let uploaded = LatestManifest::parse(&uploaded_bytes).unwrap();
    assert!(
        uploaded.levels.contains_key(&0),
        "new level 0 entry must be present"
    );
    assert!(
        !uploaded.levels.contains_key(&1),
        "stale level 1 entry must be pruned after a new level 0 backup"
    );
    assert!(
        !uploaded.levels.contains_key(&2),
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
    let snap = "bcb_level1.20260501T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![parent.to_string(), snap.to_string()]));

    let manifest_bytes = latest_manifest_yaml(parent, 0);
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| head(true));
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(|_| head(false));
    remote
        .expect_download()
        .withf(|key| key.contains("latest.yaml"))
        .returning(move |_| Ok(manifest_bytes.clone()));

    let result = backup_task(&config, &task, 1, &btrfs, &remote).await;
    let err = result
        .expect_err("stale snapshot must be rejected")
        .to_string();
    assert!(err.contains("not newer"), "unexpected error: {err}");
}
