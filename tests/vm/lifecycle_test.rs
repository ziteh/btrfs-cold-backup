//! Backup and restore driven through the library against a real btrfs and S3, rather than
//! through the binary.

use std::path::PathBuf;

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    BtrfsDriver, Config, RestoreTier, RetrievalOptions, S3Backend, Task, backup::backup_task,
    restore::restore_task,
};

use crate::common::*;

fn retrieval() -> RetrievalOptions {
    RetrievalOptions {
        days: 1,
        tier: RestoreTier::Standard,
    }
}

/// A subvolume with one snapshot per entry of `files`, taken after writing that file, at
/// successive levels starting from 0; plus a config and task backing the subvolume up.
struct Chain {
    dir: PathBuf,
    subvol: PathBuf,
    snapshots: Vec<PathBuf>,
    restore_dir: PathBuf,
    config: Config,
    task: Task,
    identity: age::x25519::Identity,
}

const SNAPSHOT_NAMES: [&str; 2] = ["bcb_level0.20260601T0000", "bcb_level1.20260602T0000"];

impl Chain {
    fn new(env: &VmEnv, name: &str, files: &[(&str, &[u8])]) -> Self {
        ensure_bucket(&env.s3_endpoint, &env.s3_bucket);
        let dir = test_dir(&env.btrfs_mount, name);
        let subvol = dir.join("subvol");
        let snap_dir = dir.join("snapshots");
        let work_dir = dir.join("work");
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();
        btrfs_subvolume_create(&subvol);

        let mut snapshots = Vec::new();
        for ((file, content), snapshot_name) in files.iter().zip(SNAPSHOT_NAMES) {
            std::fs::write(subvol.join(file), content).unwrap();
            let snapshot = snap_dir.join(snapshot_name);
            btrfs_snapshot(&subvol, &snapshot);
            snapshots.push(snapshot);
        }

        let identity = age::x25519::Identity::generate();
        Self {
            restore_dir: dir.join("restore"),
            config: Config {
                base_dir: work_dir,
                age_public_key: identity.to_public().to_string(),
                s3: s3_config(env),
                tasks: vec![],
            },
            task: Task {
                name: format!("vm-test-{name}"),
                description: String::new(),
                subvolume: subvol.clone(),
                snapshot_dir: snap_dir,
                enabled: true,
            },
            identity,
            dir,
            subvol,
            snapshots,
        }
    }
}

impl Drop for Chain {
    fn drop(&mut self) {
        for name in SNAPSHOT_NAMES.iter().rev() {
            btrfs_subvolume_delete(&self.restore_dir.join(name));
        }
        for snapshot in self.snapshots.iter().rev() {
            btrfs_subvolume_delete(snapshot);
        }
        btrfs_subvolume_delete(&self.subvol);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn level0_backup_and_restore_should_round_trip() {
    let Some(env) = vm_env() else { return };
    let chain = Chain::new(&env, "roundtrip", &[("hello.txt", b"round-trip test")]);
    let remote = S3Backend::new(&chain.config.s3).await.unwrap();

    let backup = backup_task(&chain.config, &chain.task, 0, &BtrfsDriver, &remote)
        .await
        .unwrap()
        .expect("must not be skipped");
    assert_eq!(backup.level, 0);

    let progress = restore_task(
        &chain.config,
        &chain.task,
        0,
        "20260601",
        &chain.restore_dir,
        chain.identity.to_string().expose_secret(),
        &retrieval(),
        &remote,
    )
    .await
    .expect("restore failed");
    assert!(
        progress.completed,
        "restore must complete in one run when nothing is archived"
    );

    let restored = chain.restore_dir.join(SNAPSHOT_NAMES[0]).join("hello.txt");
    assert_eq!(std::fs::read(restored).unwrap(), b"round-trip test");
}

#[tokio::test]
async fn level1_incremental_backup_should_use_level0_as_parent() {
    let Some(env) = vm_env() else { return };
    let chain = Chain::new(
        &env,
        "incr",
        &[("base.txt", b"base"), ("delta.txt", b"delta")],
    );
    let remote = S3Backend::new(&chain.config.s3).await.unwrap();

    backup_task(&chain.config, &chain.task, 0, &BtrfsDriver, &remote)
        .await
        .unwrap()
        .expect("must not be skipped");
    let b1 = backup_task(&chain.config, &chain.task, 1, &BtrfsDriver, &remote)
        .await
        .unwrap()
        .expect("must not be skipped");
    assert_eq!(b1.level, 1);
    assert_eq!(b1.parent_snapshot.as_deref(), Some(SNAPSHOT_NAMES[0]));
}

/// Restoring level 0 and then applying the level 1 incremental onto the same target must
/// recover the files of both.
#[tokio::test]
async fn incremental_chain_restore_should_recover_both_levels() {
    let Some(env) = vm_env() else { return };
    let chain = Chain::new(
        &env,
        "chain",
        &[("base.txt", b"base"), ("delta.txt", b"delta")],
    );
    let remote = S3Backend::new(&chain.config.s3).await.unwrap();

    for level in [0, 1] {
        backup_task(&chain.config, &chain.task, level, &BtrfsDriver, &remote)
            .await
            .unwrap();
    }
    let private_key = chain.identity.to_string();
    for (level, date) in [(0, "20260601"), (1, "20260602")] {
        let progress = restore_task(
            &chain.config,
            &chain.task,
            level,
            date,
            &chain.restore_dir,
            private_key.expose_secret(),
            &retrieval(),
            &remote,
        )
        .await
        .unwrap_or_else(|e| panic!("level {level} restore failed: {e:#}"));
        assert!(progress.completed, "level {level} restore must complete");
    }

    let restored = chain.restore_dir.join(SNAPSHOT_NAMES[1]);
    assert_eq!(std::fs::read(restored.join("base.txt")).unwrap(), b"base");
    assert_eq!(std::fs::read(restored.join("delta.txt")).unwrap(), b"delta");
}
