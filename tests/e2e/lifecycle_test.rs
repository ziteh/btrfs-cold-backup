use std::path::{Path, PathBuf};
use std::process::Command;

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    BtrfsDriver, Config, RestoreConfig, RestoreTier, RetrievalOptions, S3Backend, S3Config,
    S3RetryConfig, S3StorageClass, Task, backup::backup_task, restore::restore_task,
};

fn retrieval() -> RetrievalOptions {
    RetrievalOptions {
        days: 1,
        tier: RestoreTier::Standard,
    }
}

struct E2eEnv {
    btrfs_mount: PathBuf,
    s3_endpoint: String,
    s3_bucket: String,
}

/// Returns the e2e env from env vars, or None to skip the test. `cargo test` reports a
/// skipped test identically to a real pass ("ok"); run with `-- --nocapture` to see this
/// skip reason instead of silently trusting a green "N passed".
fn e2e_env() -> Option<E2eEnv> {
    let btrfs_mount = std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from);
    let s3_endpoint = std::env::var("BCB_S3_ENDPOINT").ok();
    let s3_bucket = std::env::var("BCB_S3_BUCKET").ok();
    let (Some(btrfs_mount), Some(s3_endpoint), Some(s3_bucket)) =
        (btrfs_mount, s3_endpoint, s3_bucket)
    else {
        let msg = "skipping e2e test: BCB_BTRFS_MOUNT / BCB_S3_ENDPOINT / BCB_S3_BUCKET not set \
             (run via scripts/run-vm-tests.sh)";
        // A skipped test and a real pass both print "ok", so a misconfigured CI env would
        // otherwise go unnoticed as a green run. In CI this must fail loudly instead.
        assert!(std::env::var("CI").is_err(), "{msg}, but CI=true");
        eprintln!("{msg}");
        return None;
    };
    Some(E2eEnv {
        btrfs_mount,
        s3_endpoint,
        s3_bucket,
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

async fn ensure_bucket(endpoint: &str, bucket: &str) {
    // Ignore the exit status: this races other test binaries creating the same bucket, and
    // "already exists" is the expected outcome most of the time.
    Command::new("aws")
        .args([
            "--endpoint-url",
            endpoint,
            "s3",
            "mb",
            &format!("s3://{bucket}"),
        ])
        .output()
        .unwrap();
}

fn s3_config(env: &E2eEnv) -> S3Config {
    S3Config {
        bucket: env.s3_bucket.clone(),
        region: "us-east-1".to_string(),
        prefix: "e2e-test/".to_string(),
        endpoint: env.s3_endpoint.clone(),
        // Explicit because S3StorageClass::default() leaves `manifest` empty (serde
        // defaults only apply when deserializing).
        storage_class: S3StorageClass {
            manifest: "STANDARD".to_string(),
            backup_data: vec!["STANDARD".to_string(), "STANDARD".to_string()],
        },
        retry: S3RetryConfig::default(),
        upload_speed_limit_kbps: 0,
        restore: RestoreConfig::default(),
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

    let btrfs = BtrfsDriver;
    let snap = snap_dir.join("bcb_level0.20260601T0000");
    btrfs_snapshot(&subvol, &snap);

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
        .unwrap()
        .expect("must not be skipped");
    assert_eq!(backup.level, 0);

    let private_key = identity.to_string();
    let result = restore_task(
        &config,
        &task,
        0,
        "20260601",
        &restore_dir,
        private_key.expose_secret(),
        &retrieval(),
        &remote,
    )
    .await;
    // btrfs receive succeeds only on a real btrfs target
    assert!(result.is_ok(), "restore failed: {result:?}");
    assert!(
        result.unwrap().completed,
        "restore must complete in one run when nothing is archived"
    );

    // Verify the restored data
    let restored_file = restore_dir
        .join("bcb_level0.20260601T0000")
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
    let snap0 = snap_dir.join("bcb_level0.20260601T0000");
    btrfs_snapshot(&subvol, &snap0);

    std::fs::write(subvol.join("delta.txt"), b"delta").unwrap();
    let snap1 = snap_dir.join("bcb_level1.20260602T0000");
    btrfs_snapshot(&subvol, &snap1);

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
        .unwrap()
        .expect("must not be skipped");
    assert_eq!(b0.level, 0);

    let b1 = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap()
        .expect("must not be skipped");
    assert_eq!(b1.level, 1);
    assert_eq!(
        b1.parent_snapshot.as_deref(),
        Some("bcb_level0.20260601T0000")
    );

    btrfs_subvolume_delete(&snap1);
    btrfs_subvolume_delete(&snap0);
    btrfs_subvolume_delete(&subvol);
}

/// The real disaster-recovery path for incrementals: restore level 0, then apply the
/// level-1 incremental onto the same target, and end up with both files intact.
#[tokio::test]
async fn incremental_chain_restore_should_recover_both_levels() {
    let Some(env) = e2e_env() else { return };
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket).await;

    let dir = test_dir(&env.btrfs_mount, "chain");
    let subvol = dir.join("subvol");
    let snap_dir = dir.join("snapshots");
    let work_dir = dir.join("work");
    let restore_dir = dir.join("restore");
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    btrfs_subvolume_create(&subvol);
    std::fs::write(subvol.join("base.txt"), b"base").unwrap();

    let btrfs = BtrfsDriver;
    let snap0 = snap_dir.join("bcb_level0.20260601T0000");
    btrfs_snapshot(&subvol, &snap0);

    std::fs::write(subvol.join("delta.txt"), b"delta").unwrap();
    let snap1 = snap_dir.join("bcb_level1.20260602T0000");
    btrfs_snapshot(&subvol, &snap1);

    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir,
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(&env),
        tasks: vec![],
    };
    let task = Task {
        name: "e2e-chain".to_string(),
        description: String::new(),
        subvolume: subvol.clone(),
        snapshot_dir: snap_dir.clone(),
        enabled: true,
    };

    let remote = S3Backend::new(&config.s3).await.unwrap();

    backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap();
    backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap();

    let private_key = identity.to_string();
    let progress0 = restore_task(
        &config,
        &task,
        0,
        "20260601",
        &restore_dir,
        private_key.expose_secret(),
        &retrieval(),
        &remote,
    )
    .await
    .expect("level 0 restore failed");
    assert!(progress0.completed, "level 0 restore must complete");

    let progress1 = restore_task(
        &config,
        &task,
        1,
        "20260602",
        &restore_dir,
        private_key.expose_secret(),
        &retrieval(),
        &remote,
    )
    .await
    .expect("level 1 incremental restore failed");
    assert!(progress1.completed, "level 1 restore must complete");

    let restored1 = restore_dir.join("bcb_level1.20260602T0000");
    assert_eq!(std::fs::read(restored1.join("base.txt")).unwrap(), b"base");
    assert_eq!(
        std::fs::read(restored1.join("delta.txt")).unwrap(),
        b"delta"
    );

    btrfs_subvolume_delete(&restore_dir.join("bcb_level1.20260602T0000"));
    btrfs_subvolume_delete(&restore_dir.join("bcb_level0.20260601T0000"));
    btrfs_subvolume_delete(&snap1);
    btrfs_subvolume_delete(&snap0);
    btrfs_subvolume_delete(&subvol);
}
