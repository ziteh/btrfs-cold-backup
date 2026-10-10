use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::common::*;

// ListMultipartUploads' `prefix` filter isn't reliable against this S3-compatible test server
// (observed: a genuine string-prefix of an in-progress upload's key still came back empty), so
// list unfiltered and filter client-side instead of trusting server-side prefix matching.
async fn incomplete_upload_keys(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Vec<String> {
    let resp = client
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    resp.uploads
        .unwrap_or_default()
        .into_iter()
        .filter_map(|u| u.key().map(str::to_string))
        .filter(|k| k.starts_with(prefix))
        .collect()
}

/// Polls list_multipart_uploads until it matches `expected` or `timeout` elapses, printing
/// whatever it last saw on failure — diagnostic aid in case of listing-consistency timing.
async fn wait_for_incomplete_upload_count(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let keys = incomplete_upload_keys(client, bucket, prefix).await;
        if keys.len() == expected || Instant::now() >= deadline {
            return keys;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

fn spawn_bcb(env: &VmEnv, f: &Fixture, args: &[&str], extra_env: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(&env.bin_path);
    cmd.arg("--config")
        .arg(&f.config_path)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("failed to spawn bcb")
}

/// Starts a level 0 backup of the fixture's task.
fn spawn_backup(env: &VmEnv, f: &Fixture, extra_env: &[(&str, &str)]) -> Child {
    spawn_bcb(
        env,
        f,
        &["backup", "--level", "0", "--task", &f.task_name],
        extra_env,
    )
}

/// Starts a restore of the fixture's level 0 backup into its restore dir.
fn spawn_restore(env: &VmEnv, f: &Fixture, extra_env: &[(&str, &str)]) -> Child {
    spawn_bcb(
        env,
        f,
        &[
            "restore",
            "--task",
            &f.task_name,
            "--level",
            "0",
            "--date",
            "20260601",
            "--target",
            f.restore_dir.to_str().unwrap(),
            "--private-key",
            f.private_key_path.to_str().unwrap(),
        ],
        extra_env,
    )
}

/// Blocks until a line containing `marker` appears on the child's stderr, or `timeout` elapses.
fn wait_for_marker(child: &mut Child, marker: &str, timeout: Duration) -> bool {
    let stderr = child.stderr.take().expect("stderr not piped");
    let marker = marker.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains(&marker) {
                let _ = tx.send(());
                return;
            }
        }
    });
    rx.recv_timeout(timeout).is_ok()
}

fn no_process_matches(pgrep_args: &[&str]) -> bool {
    !Command::new("pgrep")
        .args(pgrep_args)
        .output()
        .unwrap()
        .status
        .success()
}

/// Polls until no btrfs-send process of this fixture remains, or returns false on timeout.
/// Matches on the fixture's snapshot dir, since other tests run btrfs send concurrently.
fn wait_no_orphan_children(snap_dir: &Path, timeout: Duration) -> bool {
    let pattern = format!("btrfs.*send.*{}", snap_dir.display());
    let deadline = Instant::now() + timeout;
    loop {
        if no_process_matches(&["-f", &pattern]) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Killing bcb mid-multipart-upload must leave a recoverable, resumable state: the orphaned S3
/// multipart session is detected and aborted on the next run, not left to leak storage forever.
#[tokio::test]
async fn kill_during_multipart_upload_then_resume_should_abort_orphan_and_succeed() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "multipart", |subvol| {
        // Larger than one S3 multipart chunk, so the part's upload spans several.
        let status = Command::new("dd")
            .args([
                "if=/dev/urandom",
                &format!("of={}", subvol.join("bigfile.bin").to_str().unwrap()),
                "bs=1M",
                "count=100",
            ])
            .status()
            .unwrap();
        assert!(status.success());
    });

    let prefix = data_prefix(&fixture);
    let s3 = raw_s3_client(&env).await;

    let mut child = spawn_backup(
        &env,
        &fixture,
        &[("BCB_TEST_PAUSE_AFTER_S3_PART_NUMBER", "1")],
    );
    assert!(
        wait_for_marker(
            &mut child,
            "BCB_TEST_PAUSE_REACHED:s3_part:1",
            Duration::from_secs(60)
        ),
        "bcb never reached the mid-multipart pause point"
    );
    let kill_result = child.kill();
    let wait_result = child.wait();

    // Diagnostics: disambiguate "session never existed / wrong prefix" from "completed normally".
    let bucket_wide = incomplete_upload_keys(&s3, &env.s3_bucket, "").await;
    let objects = s3
        .list_objects_v2()
        .bucket(&env.s3_bucket)
        .prefix(&prefix)
        .send()
        .await
        .unwrap();
    let object_keys: Vec<String> = objects
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();

    let keys =
        wait_for_incomplete_upload_count(&s3, &env.s3_bucket, &prefix, 1, Duration::from_secs(5))
            .await;
    assert_eq!(
        keys.len(),
        1,
        "expected exactly one orphaned multipart upload after the kill, found: {keys:?}\n\
         kill_result={kill_result:?} wait_result={wait_result:?}\n\
         bucket-wide incomplete uploads (no prefix filter): {bucket_wide:?}\n\
         completed objects under prefix: {object_keys:?}\n\
         (bucket={}, prefix={prefix})",
        env.s3_bucket
    );

    let resumed = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
            "--task",
            &fixture.task_name,
        ],
    );
    assert!(
        resumed.status.success(),
        "resumed backup failed: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );

    let keys = incomplete_upload_keys(&s3, &env.s3_bucket, &prefix).await;
    assert!(
        keys.is_empty(),
        "orphaned multipart upload should have been aborted by the resumed run, found: {keys:?}"
    );

    let restored = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "restore",
            "--task",
            &fixture.task_name,
            "--level",
            "0",
            "--date",
            "20260601",
            "--target",
            fixture.restore_dir.to_str().unwrap(),
            "--private-key",
            fixture.private_key_path.to_str().unwrap(),
        ],
    );
    assert!(
        restored.status.success(),
        "restore failed: {}",
        String::from_utf8_lossy(&restored.stderr)
    );

    let original = std::fs::read(fixture.subvol.join("bigfile.bin")).unwrap();
    let recovered = std::fs::read(
        fixture
            .restore_dir
            .join("bcb_level0.20260601T0000")
            .join("bigfile.bin"),
    )
    .unwrap();
    assert_eq!(original, recovered, "restored content must match source");
}

/// Killing bcb while btrfs-send is running must not leave them as orphan processes that
/// could collide with the next run (verifies the PR_SET_PDEATHSIG fix end-to-end).
#[tokio::test]
async fn kill_during_send_then_resume_should_leave_no_orphan_children() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "send-kill", |subvol| {
        std::fs::write(subvol.join("hello.txt"), b"kill during send").unwrap();
    });

    let mut child = spawn_backup(&env, &fixture, &[("BCB_TEST_PAUSE_DURING_SEND", "1")]);
    assert!(
        wait_for_marker(
            &mut child,
            "BCB_TEST_PAUSE_REACHED:during_send",
            Duration::from_secs(30)
        ),
        "bcb never reached the during-send pause point"
    );
    child.kill().unwrap();
    child.wait().unwrap();

    assert!(
        wait_no_orphan_children(&fixture.snap_dir, Duration::from_secs(5)),
        "btrfs send processes survived the parent's SIGKILL"
    );

    let resumed = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
            "--task",
            &fixture.task_name,
        ],
    );
    assert!(
        resumed.status.success(),
        "resumed backup failed: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
}

/// Killing bcb leaves the PID lock file holding a now-dead PID, but the kernel releases the
/// flock on process death (even SIGKILL) regardless — the next run must acquire it normally
/// rather than erroring out on lock contention, with no explicit stale-lock recovery needed.
#[tokio::test]
async fn kill_leaves_stale_lock_then_resume_should_not_error_on_lock_contention() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "stale-lock", |subvol| {
        std::fs::write(subvol.join("hello.txt"), b"stale lock test").unwrap();
    });

    let mut child = spawn_backup(&env, &fixture, &[("BCB_TEST_PAUSE_DURING_SEND", "1")]);
    assert!(
        wait_for_marker(
            &mut child,
            "BCB_TEST_PAUSE_REACHED:during_send",
            Duration::from_secs(30)
        ),
        "bcb never reached the during-send pause point"
    );
    child.kill().unwrap();
    child.wait().unwrap();

    let lock_path = fixture.work_dir.join("bcb.lock");
    assert!(lock_path.exists(), "lock file should remain after SIGKILL");
    let pid: u32 = std::fs::read_to_string(&lock_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "PID in lock file should no longer be alive"
    );

    let resumed = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
            "--task",
            &fixture.task_name,
        ],
    );
    assert!(
        resumed.status.success(),
        "resumed backup should not fail on stale lock contention: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
}

/// Killing bcb mid-chunk-download during a restore must leave a resumable state: the next
/// run resumes from the saved byte offset (verified directly from bcb's on-disk restore
/// state) rather than re-downloading the part from scratch, and the restored content still
/// matches the original exactly.
#[tokio::test]
async fn kill_during_restore_download_then_resume_should_recover_partial_download() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "restore-kill", |subvol| {
        // Large enough that a 1 MiB test chunk size leaves more than one chunk to fetch.
        let status = Command::new("dd")
            .args([
                "if=/dev/urandom",
                &format!("of={}", subvol.join("bigfile.bin").to_str().unwrap()),
                "bs=1M",
                "count=10",
            ])
            .status()
            .unwrap();
        assert!(status.success());
    });

    let backup = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
            "--task",
            &fixture.task_name,
        ],
    );
    assert!(
        backup.status.success(),
        "backup failed: {}",
        String::from_utf8_lossy(&backup.stderr)
    );

    let chunk_size: u64 = 1024 * 1024;
    let mut child = spawn_restore(
        &env,
        &fixture,
        &[
            ("BCB_TEST_PART_CHUNK_SIZE", &chunk_size.to_string()),
            (
                "BCB_TEST_PAUSE_AFTER_DOWNLOADED_BYTES",
                &chunk_size.to_string(),
            ),
        ],
    );
    assert!(
        wait_for_marker(
            &mut child,
            &format!("BCB_TEST_PAUSE_REACHED:downloaded_bytes:{chunk_size}"),
            Duration::from_secs(60)
        ),
        "bcb never reached the mid-download pause point"
    );
    child.kill().unwrap();
    child.wait().unwrap();

    // The state proves the kill landed mid-part rather than after the part had finished.
    let state =
        btrfs_cold_backup::state::StateFile::<btrfs_cold_backup::state::RestoreState>::load(
            restore_work_dir(&fixture).join("state.yaml"),
        )
        .unwrap()
        .expect("restore state must exist")
        .data;
    let part_state = state
        .parts
        .values()
        .next()
        .expect("restore state must have at least one part");
    assert_eq!(
        *part_state,
        btrfs_cold_backup::state::PartDownloadState::PartiallyDownloaded {
            downloaded_bytes: chunk_size
        },
        "state must reflect exactly one chunk downloaded when killed"
    );

    let resumed = run_bcb(
        &env,
        &[
            "--config",
            fixture.config_path.to_str().unwrap(),
            "restore",
            "--task",
            &fixture.task_name,
            "--level",
            "0",
            "--date",
            "20260601",
            "--target",
            fixture.restore_dir.to_str().unwrap(),
            "--private-key",
            fixture.private_key_path.to_str().unwrap(),
        ],
    );
    assert!(
        resumed.status.success(),
        "resumed restore failed: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert!(
        String::from_utf8_lossy(&resumed.stdout).contains("restore complete"),
        "resumed restore must report completion: {}",
        String::from_utf8_lossy(&resumed.stdout)
    );
    assert!(
        !restore_work_dir(&fixture).exists(),
        "restore work dir must be cleaned up after a completed restore"
    );

    let original = std::fs::read(fixture.subvol.join("bigfile.bin")).unwrap();
    let recovered = std::fs::read(
        fixture
            .restore_dir
            .join("bcb_level0.20260601T0000")
            .join("bigfile.bin"),
    )
    .unwrap();
    assert_eq!(original, recovered, "restored content must match source");
}
