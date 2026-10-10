use std::path::PathBuf;

use btrfs_cold_backup::{BtrfsDriver, BtrfsOps};
use tokio::io::AsyncReadExt;

use crate::common::*;

/// The btrfs test mount, or None (after logging why) to skip the test. These tests need no
/// S3, so they don't require the whole VM environment.
fn btrfs_mount() -> Option<PathBuf> {
    let mount = std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from);
    if mount.is_none() {
        skip("BCB_BTRFS_MOUNT not set");
    }
    mount
}

#[tokio::test]
async fn subvolume_uuids_should_link_snapshot_to_its_source() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "uuids");
    let subvol = dir.join("subvol");
    let snap = dir.join("snap");
    btrfs_subvolume_create(&subvol);
    btrfs_snapshot(&subvol, &snap);

    let driver = BtrfsDriver;
    let source = driver.subvolume_uuids(&subvol).await.unwrap();
    let snapshot = driver.subvolume_uuids(&snap).await.unwrap();
    assert!(source.parent_uuid.is_none());
    assert_ne!(snapshot.uuid, source.uuid);
    assert_eq!(snapshot.parent_uuid.as_deref(), Some(source.uuid.as_str()));

    btrfs_subvolume_delete(&snap);
    btrfs_subvolume_delete(&subvol);
}

#[tokio::test]
async fn subvolume_uuids_should_fail_for_missing_path() {
    let Some(mount) = btrfs_mount() else { return };
    let driver = BtrfsDriver;
    let missing = mount.join("does-not-exist-subvol");
    assert!(driver.subvolume_uuids(&missing).await.is_err());
}

#[tokio::test]
async fn list_snapshots_should_return_only_bcb_prefixed_dirs() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "list-snaps");
    let snap_dir = dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();

    std::fs::create_dir(snap_dir.join("bcb_level0.20260101T0000")).unwrap();
    std::fs::create_dir(snap_dir.join("bcb_level1.20260102T0000")).unwrap();
    std::fs::create_dir(snap_dir.join("unrelated")).unwrap();

    let driver = BtrfsDriver;
    let names = driver.list_snapshots(&snap_dir).await.unwrap();

    assert_eq!(
        names,
        vec!["bcb_level0.20260101T0000", "bcb_level1.20260102T0000"]
    );
}

#[tokio::test]
async fn send_full_should_produce_non_empty_stream() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "send-full");
    let subvol = dir.join("subvol");
    let snap_dir = dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();
    btrfs_subvolume_create(&subvol);
    std::fs::write(subvol.join("data.txt"), b"btrfs send test").unwrap();

    let snap = snap_dir.join("bcb_level0.20260101T0000");
    btrfs_snapshot(&subvol, &snap);

    let driver = BtrfsDriver;
    let mut stream = driver.send_full(&snap).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    assert!(!buf.is_empty());

    btrfs_subvolume_delete(&snap);
    btrfs_subvolume_delete(&subvol);
}

#[tokio::test]
async fn send_incremental_should_produce_smaller_stream_than_full() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "send-incr");
    let subvol = dir.join("subvol");
    let snap_dir = dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();
    btrfs_subvolume_create(&subvol);
    std::fs::write(subvol.join("data.txt"), b"initial content").unwrap();

    let snap0 = snap_dir.join("bcb_level0.20260101T0000");
    btrfs_snapshot(&subvol, &snap0);

    std::fs::write(subvol.join("extra.txt"), b"added later").unwrap();
    let snap1 = snap_dir.join("bcb_level1.20260102T0000");
    btrfs_snapshot(&subvol, &snap1);

    let driver = BtrfsDriver;
    let mut full_buf = Vec::new();
    driver
        .send_full(&snap0)
        .await
        .unwrap()
        .read_to_end(&mut full_buf)
        .await
        .unwrap();

    let mut incr_buf = Vec::new();
    driver
        .send_incremental(&snap0, &snap1)
        .await
        .unwrap()
        .read_to_end(&mut incr_buf)
        .await
        .unwrap();

    assert!(!incr_buf.is_empty());
    assert!(incr_buf.len() < full_buf.len());

    btrfs_subvolume_delete(&snap1);
    btrfs_subvolume_delete(&snap0);
    btrfs_subvolume_delete(&subvol);
}
