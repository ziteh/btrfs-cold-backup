use std::path::{Path, PathBuf};
use std::process::Command;

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    BtrfsDriver, BtrfsOps, Config, S3Backend, S3Config, S3RetryConfig, S3StorageClass, Task,
    backup::backup_task, restore::restore_task,
};

struct E2eEnv {
    btrfs_mount: PathBuf,
    s3_endpoint: String,
    s3_bucket: String,
}

/// Returns the e2e env from env vars, or None to skip the test.
fn e2e_env() -> Option<E2eEnv> {
    Some(E2eEnv {
        btrfs_mount: std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from)?,
        s3_endpoint: std::env::var("BCB_S3_ENDPOINT").ok()?,
        s3_bucket: std::env::var("BCB_S3_BUCKET").ok()?,
    })
}

fn test_dir(mount: &Path, name: &str) -> PathBuf {
    let dir = mount.join(format!("e2e-{name}-{}", std::process::id()));
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

fn btrfs_subvolume_delete(path: &Path) {
    Command::new("btrfs")
        .args(["subvolume", "delete", path.to_str().unwrap()])
        .output()
        .unwrap();
}

async fn ensure_bucket(_endpoint: &str, bucket: &str) {
    Command::new("mc")
        .args(["mb", "--ignore-existing", &format!("local/{bucket}")])
        .output()
        .unwrap();
}

fn s3_config(env: &E2eEnv) -> S3Config {
    S3Config {
        enabled: true,
        bucket: env.s3_bucket.clone(),
        region: "us-east-1".to_string(),
        prefix: "e2e-test/".to_string(),
        endpoint: env.s3_endpoint.clone(),
        storage_class: S3StorageClass::default(),
        retry: S3RetryConfig::default(),
        upload_speed_limit_kbps: 0,
    }
}

/// Level-0 backup followed by restore: the restored data must match the original.
#[tokio::test]
async fn level0_backup_and_restore_should_round_trip() {
    let Some(env) = e2e_env() else { return };
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket).await;

    let dir = test_dir(&env.btrfs_mount, "roundtrip");
    let subvol = dir.join("subvol");
    let snap_dir = dir.join("snapshots");
    let work_dir = dir.join("work");
    let restore_dir = dir.join("restore");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    // Create subvolume with test data
    btrfs_subvolume_create(&subvol);
    std::fs::write(subvol.join("hello.txt"), b"round-trip test").unwrap();

    // Take a read-only snapshot manually (BtrfsDriver.create_snapshot creates one)
    let btrfs = BtrfsDriver;
    let snap = btrfs
        .create_snapshot(&subvol, &snap_dir, "bcb_level0_20260601_00-00")
        .await
        .unwrap();

    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir.clone(),
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(&env),
        tasks: vec![],
    };
    let task = Task {
        name: "e2e-test".to_string(),
        description: String::new(),
        subvolume: subvol.clone(),
        snapshot_dir: snap_dir.clone(),
        enabled: true,
    };

    let remote = S3Backend::new(&config.s3).await.unwrap();

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap();
    assert_eq!(backup.level, 0);

    let private_key = identity.to_string();
    let result = restore_task(
        &config,
        &task,
        0,
        "20260601",
        &restore_dir,
        private_key.expose_secret(),
        &btrfs,
        &remote,
    )
    .await;
    // btrfs receive succeeds only on a real btrfs target
    assert!(result.is_ok(), "restore failed: {result:?}");

    // Verify the restored data
    let restored_file = restore_dir
        .join("bcb_level0_20260601_00-00")
        .join("hello.txt");
    let content = std::fs::read(&restored_file).unwrap();
    assert_eq!(content, b"round-trip test");

    btrfs_subvolume_delete(&snap);
    btrfs_subvolume_delete(&subvol);
}

/// Level-1 incremental backup must reference the level-0 snapshot as parent.
#[tokio::test]
async fn level1_incremental_backup_should_succeed_after_level0() {
    let Some(env) = e2e_env() else { return };
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket).await;

    let dir = test_dir(&env.btrfs_mount, "incr");
    let subvol = dir.join("subvol");
    let snap_dir = dir.join("snapshots");
    let work_dir = dir.join("work");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    btrfs_subvolume_create(&subvol);
    std::fs::write(subvol.join("base.txt"), b"base").unwrap();

    let btrfs = BtrfsDriver;
    let snap0 = btrfs
        .create_snapshot(&subvol, &snap_dir, "bcb_level0_20260601_00-00")
        .await
        .unwrap();

    std::fs::write(subvol.join("delta.txt"), b"delta").unwrap();
    let snap1 = btrfs
        .create_snapshot(&subvol, &snap_dir, "bcb_level1_20260602_00-00")
        .await
        .unwrap();

    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir,
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(&env),
        tasks: vec![],
    };
    let task = Task {
        name: "e2e-incr".to_string(),
        description: String::new(),
        subvolume: subvol.clone(),
        snapshot_dir: snap_dir.clone(),
        enabled: true,
    };

    let remote = S3Backend::new(&config.s3).await.unwrap();

    let b0 = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap();
    assert_eq!(b0.level, 0);

    let b1 = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap();
    assert_eq!(b1.level, 1);
    assert_eq!(
        b1.parent_snapshot.as_deref(),
        Some("bcb_level0_20260601_00-00")
    );

    btrfs_subvolume_delete(&snap1);
    btrfs_subvolume_delete(&snap0);
    btrfs_subvolume_delete(&subvol);
}
