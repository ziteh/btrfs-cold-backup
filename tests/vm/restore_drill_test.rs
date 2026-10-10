//! Failure drills against a real btrfs and S3: each one breaks something the way the world
//! does (a store losing an acknowledged object, a part damaged on disk, a wrong key, a missing
//! parent, an unreachable endpoint) and checks bcb's reaction end to end.

use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use btrfs_cold_backup::Config;

use crate::common::*;

const PART_SIZE: u64 = 4 * 1024 * 1024;

fn write_random(subvol: &Path, mib: u32) {
    let status = Command::new("dd")
        .args([
            "if=/dev/urandom",
            &format!("of={}", subvol.join("big.bin").display()),
            "bs=1M",
            &format!("count={mib}"),
        ])
        .status()
        .unwrap();
    assert!(status.success());
}

fn bcb(env: &VmEnv, config: &Path, args: &[&str], extra_env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(&env.bin_path);
    cmd.arg("--config").arg(config).args(args);
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.output().expect("failed to run bcb")
}

fn backup(env: &VmEnv, f: &Fixture, level: u32) -> Output {
    bcb(
        env,
        &f.config_path,
        &[
            "backup",
            "--level",
            &level.to_string(),
            "--task",
            &f.task_name,
        ],
        &[("BCB_TEST_SPLIT_SIZE", &PART_SIZE.to_string())],
    )
}

fn restore(env: &VmEnv, f: &Fixture, config: &Path, level: u32, date: &str, key: &Path) -> Output {
    bcb(
        env,
        config,
        &[
            "restore",
            "--task",
            &f.task_name,
            "--level",
            &level.to_string(),
            "--date",
            date,
            "--target",
            f.restore_dir.to_str().unwrap(),
            "--private-key",
            key.to_str().unwrap(),
        ],
        &[],
    )
}

fn assert_same_file(f: &Fixture, snapshot: &str) {
    let original = std::fs::read(f.subvol.join("big.bin")).unwrap();
    let recovered = std::fs::read(f.restore_dir.join(snapshot).join("big.bin")).unwrap();
    assert!(original == recovered, "restored content must match source");
}

async fn part_keys(env: &VmEnv, f: &Fixture) -> Vec<(String, u64)> {
    let s3 = raw_s3_client(env).await;
    let resp = s3
        .list_objects_v2()
        .bucket(&env.s3_bucket)
        .prefix(data_prefix(f))
        .send()
        .await
        .unwrap();
    let mut keys: Vec<(String, u64)> = resp
        .contents()
        .iter()
        .filter_map(|o| Some((o.key()?.to_string(), o.size()? as u64)))
        .collect();
    keys.sort();
    keys
}

/// S3 losing a part after acknowledging it must hold the manifest back, and the next run must
/// upload just that part again, leaving a backup that restores.
#[tokio::test]
async fn part_lost_after_upload_should_be_reuploaded_before_manifest() {
    let Some(env) = vm_env() else { return };
    let f = setup_fixture(&env, "lost-part", |s| write_random(s, 12));
    // Slow the upload so part 0 can be deleted while later parts are still on their way.
    let mut config: Config =
        serde_yaml::from_slice(&std::fs::read(&f.config_path).unwrap()).unwrap();
    config.s3.upload_speed_limit_kbps = 8_000;
    std::fs::write(&f.config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let child = Command::new(&env.bin_path)
        .arg("--config")
        .arg(&f.config_path)
        .args(["backup", "--level", "0", "--task", &f.task_name])
        .env("BCB_TEST_SPLIT_SIZE", PART_SIZE.to_string())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let s3 = raw_s3_client(&env).await;
    let first_key = format!(
        "{}bcb_level0_202606010000/snapshot.part-000000.age",
        data_prefix(&f)
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let found = s3
            .head_object()
            .bucket(&env.s3_bucket)
            .key(&first_key)
            .send()
            .await
            .is_ok();
        if found {
            s3.delete_object()
                .bucket(&env.s3_bucket)
                .key(&first_key)
                .send()
                .await
                .unwrap();
            break;
        }
        assert!(Instant::now() < deadline, "part 0 never reached S3");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let first = child.wait_with_output().unwrap();
    assert_exit(&first, 1, "not on S3 as uploaded");

    assert_ok(&backup(&env, &f, 0), "re-run backup");
    assert_ok(
        &restore(&env, &f, &f.config_path, 0, "20260601", &f.private_key_path),
        "restore",
    );
    assert_same_file(&f, "bcb_level0.20260601T0000");
}

/// A part damaged on disk between its download and the receive must cost one part's
/// download, not the restore.
#[tokio::test]
async fn part_damaged_before_finalize_should_be_downloaded_again() {
    let Some(env) = vm_env() else { return };
    let f = setup_fixture(&env, "damaged-part", |s| write_random(s, 10));
    assert_ok(&backup(&env, &f, 0), "backup");

    // Parts download in index order here (STANDARD copies have no expiry), so pausing after
    // the last part's bytes leaves every part on disk and the receive not yet started.
    let parts = part_keys(&env, &f).await;
    assert!(parts.len() >= 2, "expected a multi-part backup");
    let last_size = parts.last().unwrap().1;
    let mut child = Command::new(&env.bin_path)
        .arg("--config")
        .arg(&f.config_path)
        .args([
            "restore",
            "--task",
            &f.task_name,
            "--level",
            "0",
            "--date",
            "20260601",
        ])
        .arg("--target")
        .arg(&f.restore_dir)
        .arg("--private-key")
        .arg(&f.private_key_path)
        .env(
            "BCB_TEST_PAUSE_AFTER_DOWNLOADED_BYTES",
            last_size.to_string(),
        )
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let reached = std::thread::spawn(move || {
        use std::io::BufRead;
        std::io::BufReader::new(stderr)
            .lines()
            .map_while(Result::ok)
            .any(|l| l.contains("BCB_TEST_PAUSE_REACHED"))
    });
    // The pause point only fires on the last part, so earlier parts are complete by then.
    let deadline = Instant::now() + Duration::from_secs(120);
    while !reached.is_finished() {
        assert!(
            Instant::now() < deadline,
            "restore never reached the pause point"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        reached.join().unwrap(),
        "restore exited before the pause point"
    );
    child.kill().unwrap();
    child.wait().unwrap();

    let damaged = restore_work_dir(&f).join("part-000000.age");
    let mut bytes = std::fs::read(&damaged).unwrap();
    bytes[1000] ^= 0xff;
    std::fs::write(&damaged, bytes).unwrap();

    let second = restore(&env, &f, &f.config_path, 0, "20260601", &f.private_key_path);
    assert_exit(&second, 2, "restore in progress");
    assert_ok(
        &restore(&env, &f, &f.config_path, 0, "20260601", &f.private_key_path),
        "third restore run",
    );
    assert_same_file(&f, "bcb_level0.20260601T0000");
}

/// Wrong key, missing parent and an unreachable endpoint must each fail (exit 1) before
/// anything is received; then the right order restores the whole chain.
#[tokio::test]
async fn restore_preflight_failures_then_chain_should_round_trip() {
    let Some(env) = vm_env() else { return };
    let f = setup_fixture(&env, "preflight", |s| write_random(s, 6));
    assert_ok(&backup(&env, &f, 0), "level 0 backup");

    std::fs::write(f.subvol.join("big.bin"), b"changed for level 1").unwrap();
    let level1 = "bcb_level1.20260602T0000";
    let snap1 = f.snap_dir.join(level1);
    let out = Command::new("btrfs")
        .args(["subvolume", "snapshot", "-r"])
        .arg(&f.subvol)
        .arg(&snap1)
        .output()
        .unwrap();
    assert!(out.status.success());
    struct Cleanup(Vec<std::path::PathBuf>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for p in &self.0 {
                btrfs_subvolume_delete(p);
            }
        }
    }
    let _cleanup = Cleanup(vec![f.restore_dir.join(level1), snap1]);
    assert_ok(&backup(&env, &f, 1), "level 1 backup");

    let other_key = f.btrfs_dir.join("other-key.txt");
    let identity = age::x25519::Identity::generate();
    use age::secrecy::ExposeSecret;
    std::fs::write(&other_key, identity.to_string().expose_secret()).unwrap();
    let wrong = restore(&env, &f, &f.config_path, 0, "20260601", &other_key);
    assert_exit(&wrong, 1, "no retrieval was requested");

    let orphan = restore(&env, &f, &f.config_path, 1, "20260602", &f.private_key_path);
    assert_exit(&orphan, 1, "restore the level 0 backup");

    let mut offline: Config =
        serde_yaml::from_slice(&std::fs::read(&f.config_path).unwrap()).unwrap();
    offline.s3.endpoint = "http://127.0.0.1:1".to_string();
    offline.s3.retry.max_attempts = 1;
    let offline_path = f.btrfs_dir.join("offline.yaml");
    std::fs::write(&offline_path, serde_yaml::to_string(&offline).unwrap()).unwrap();
    let down = restore(&env, &f, &offline_path, 0, "20260601", &f.private_key_path);
    assert_exit(&down, 1, "dispatch failure");

    assert!(
        !f.restore_dir.join("bcb_level0.20260601T0000").exists(),
        "nothing may be received by the failed runs"
    );

    assert_ok(
        &restore(&env, &f, &f.config_path, 0, "20260601", &f.private_key_path),
        "level 0 restore",
    );
    assert_ok(
        &restore(&env, &f, &f.config_path, 1, "20260602", &f.private_key_path),
        "level 1 restore",
    );
    assert_same_file(&f, level1);
}
