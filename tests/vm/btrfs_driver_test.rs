use std::path::{Path, PathBuf};
use std::process::Command;

use btrfs_cold_backup::{BtrfsDriver, BtrfsOps};
use tokio::io::AsyncReadExt;

/// Returns the btrfs test mount path from BCB_BTRFS_MOUNT, or None to skip.
fn btrfs_mount() -> Option<PathBuf> {
    std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from)
}

/// Creates a unique working directory under the btrfs mount for one test.
fn test_dir(mount: &Path, name: &str) -> PathBuf {
    let dir = mount.join(format!("test-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn btrfs_subvolume_create(path: &Path) {
    let out = Command::new("btrfs")
        .args(["subvolume", "create", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn btrfs_snapshot(src: &Path, dst: &Path) {
    let out = Command::new("btrfs")
        .args([
            "subvolume",
            "snapshot",
            "-r",
            src.to_str().unwrap(),
            dst.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn btrfs_subvolume_delete(path: &Path) {
    Command::new("btrfs")
        .args(["subvolume", "delete", path.to_str().unwrap()])
        .output()
        .unwrap();
}

#[tokio::test]
async fn subvolume_exists_should_return_true_for_btrfs_subvolume() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "exists-true");
    let subvol = dir.join("subvol");
    btrfs_subvolume_create(&subvol);

    let driver = BtrfsDriver;
    assert!(driver.subvolume_exists(&subvol).await.unwrap());

    btrfs_subvolume_delete(&subvol);
}

#[tokio::test]
async fn subvolume_exists_should_return_false_for_missing_path() {
    let Some(mount) = btrfs_mount() else { return };
    let driver = BtrfsDriver;
    let missing = mount.join("does-not-exist-subvol");
    assert!(!driver.subvolume_exists(&missing).await.unwrap());
}

#[tokio::test]
async fn list_snapshots_should_return_only_bcb_prefixed_dirs() {
    let Some(mount) = btrfs_mount() else { return };
    let dir = test_dir(&mount, "list-snaps");
    let snap_dir = dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();

    // Create bcb-prefixed snapshot dirs and one unrelated dir
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

    // Make the subvolume writable again by deleting and recreating
    // (btrfs snapshot -r is read-only; add data to parent subvol)
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
