use age::secrecy::ExposeSecret;
use btrfs_cold_backup::config::{Config, Task};

use crate::common::*;

/// A backup run covering multiple tasks must not let one task's failure stop the others from
/// being attempted: `bcb backup` (no --task filter) should still complete the healthy task
/// and report it succeeded, while the overall process exits non-zero for the broken one.
#[tokio::test]
async fn one_failing_task_should_not_block_the_others() {
    let Some(env) = vm_env() else { return };
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket);

    let dir = test_dir(&env.btrfs_mount, "multi-task");
    let good_subvol = dir.join("good-subvol");
    let good_snap_dir = dir.join("good-snapshots");
    let work_dir = dir.join("work");
    std::fs::create_dir_all(&good_snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    btrfs_subvolume_create(&good_subvol);
    std::fs::write(good_subvol.join("hello.txt"), b"multi-task test").unwrap();
    let good_snap = good_snap_dir.join("bcb_level0.20260601T0000");
    let out = std::process::Command::new("btrfs")
        .args([
            "subvolume",
            "snapshot",
            "-r",
            good_subvol.to_str().unwrap(),
            good_snap.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    // The bad task's snapshot_dir has no level 0 snapshot, so its backup fails before touching
    // its subvolume, which only has to differ from the good task's to pass config validation.
    let bad_snap_dir = dir.join("bad-snapshots-empty");
    std::fs::create_dir_all(&bad_snap_dir).unwrap();

    let identity = age::x25519::Identity::generate();
    let config = Config {
        base_dir: work_dir.clone(),
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(&env),
        oversize_guard: None,
        tasks: vec![
            Task {
                name: "bad-task".to_string(),
                description: String::new(),
                subvolume: dir.join("bad-subvol-placeholder"),
                snapshot_dir: bad_snap_dir,
                enabled: true,
            },
            Task {
                name: "good-task".to_string(),
                description: String::new(),
                subvolume: good_subvol.clone(),
                snapshot_dir: good_snap_dir.clone(),
                enabled: true,
            },
        ],
    };

    let config_path = dir.join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let private_key_path = dir.join("key.txt");
    std::fs::write(&private_key_path, identity.to_string().expose_secret()).unwrap();

    let result = run_bcb(
        &env,
        &[
            "--config",
            config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
        ],
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        !result.status.success(),
        "run must fail overall because bad-task failed: stdout={stdout} stderr={stderr}"
    );
    assert!(
        stdout.contains("good-task: level 0 backup completed"),
        "good-task must still complete despite bad-task failing: stdout={stdout}"
    );
    assert!(
        stderr.contains("bad-task"),
        "bad-task's failure must be reported: stderr={stderr}"
    );

    btrfs_subvolume_delete(&good_snap);
    btrfs_subvolume_delete(&good_subvol);
    let _ = std::fs::remove_dir_all(&dir);
}
