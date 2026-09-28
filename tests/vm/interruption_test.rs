use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    Config, RestoreConfig, S3Config, S3RetryConfig, S3StorageClass, Task, sanitize_subvolume_path,
};

pub(crate) struct VmEnv {
    pub(crate) btrfs_mount: PathBuf,
    pub(crate) s3_endpoint: String,
    pub(crate) s3_bucket: String,
    pub(crate) bin_path: PathBuf,
}

/// Returns the VM env from env vars, or None to skip the test. `cargo test` reports a
/// skipped test identically to a real pass ("ok"); run with `-- --nocapture` to see this
/// skip reason instead of silently trusting a green "N passed".
pub(crate) fn vm_env() -> Option<VmEnv> {
    let btrfs_mount = std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from);
    let s3_endpoint = std::env::var("BCB_S3_ENDPOINT").ok();
    let s3_bucket = std::env::var("BCB_S3_BUCKET").ok();
    let bin_path = std::env::var("BCB_BIN_PATH").ok().map(PathBuf::from);
    let (Some(btrfs_mount), Some(s3_endpoint), Some(s3_bucket), Some(bin_path)) =
        (btrfs_mount, s3_endpoint, s3_bucket, bin_path)
    else {
        let msg = "skipping VM test: BCB_BTRFS_MOUNT / BCB_S3_ENDPOINT / BCB_S3_BUCKET / \
             BCB_BIN_PATH not set (run via scripts/run-vm-tests.sh)";
        // A skipped test and a real pass both print "ok", so a misconfigured CI env would
        // otherwise go unnoticed as a green run. In CI this must fail loudly instead.
        assert!(std::env::var("CI").is_err(), "{msg}, but CI=true");
        eprintln!("{msg}");
        return None;
    };
    Some(VmEnv {
        btrfs_mount,
        s3_endpoint,
        s3_bucket,
        bin_path,
    })
}

pub(crate) fn test_dir(mount: &Path, name: &str) -> PathBuf {
    let dir = mount.join(format!("interrupt-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn btrfs_subvolume_create(path: &Path) {
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

pub(crate) fn btrfs_subvolume_delete(path: &Path) {
    Command::new("btrfs")
        .args(["subvolume", "delete", path.to_str().unwrap()])
        .output()
        .unwrap();
}

pub(crate) fn ensure_bucket(endpoint: &str, bucket: &str) {
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

pub(crate) fn s3_config(env: &VmEnv) -> S3Config {
    S3Config {
        bucket: env.s3_bucket.clone(),
        region: "us-east-1".to_string(),
        prefix: "interrupt-test/".to_string(),
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

pub(crate) async fn raw_s3_client(env: &VmEnv) -> aws_sdk_s3::Client {
    let region = aws_sdk_s3::config::Region::new("us-east-1");
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let builder = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url(env.s3_endpoint.clone())
        .force_path_style(true);
    aws_sdk_s3::Client::from_conf(builder.build())
}

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

fn spawn_bcb_backup(
    env: &VmEnv,
    config_path: &Path,
    level: u32,
    task: &str,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut cmd = Command::new(&env.bin_path);
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "backup",
        "--level",
        &level.to_string(),
        "--task",
        task,
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("failed to spawn bcb")
}

#[allow(clippy::too_many_arguments)]
fn spawn_bcb_restore(
    env: &VmEnv,
    config_path: &Path,
    task: &str,
    level: u32,
    date: &str,
    target: &Path,
    private_key_path: &Path,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut cmd = Command::new(&env.bin_path);
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "restore",
        "--task",
        task,
        "--level",
        &level.to_string(),
        "--date",
        date,
        "--target",
        target.to_str().unwrap(),
        "--private-key",
        private_key_path.to_str().unwrap(),
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("failed to spawn bcb")
}

pub(crate) fn run_bcb(env: &VmEnv, args: &[&str]) -> std::process::Output {
    Command::new(&env.bin_path)
        .args(args)
        .output()
        .expect("failed to run bcb")
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

/// Polls until no btrfs-send process remains, or returns false on timeout.
fn wait_no_orphan_children(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if no_process_matches(&["-f", "btrfs.*send"]) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub(crate) struct Fixture {
    pub(crate) config_path: PathBuf,
    pub(crate) private_key_path: PathBuf,
    pub(crate) subvol: PathBuf,
    pub(crate) snap_dir: PathBuf,
    pub(crate) work_dir: PathBuf,
    pub(crate) restore_dir: PathBuf,
    pub(crate) task_name: String,
    /// Root of this fixture's directories on the btrfs device (snap_dir, restore_dir live here).
    pub(crate) btrfs_dir: PathBuf,
}

/// Creates a subvolume + snapshot + on-disk config/key files for one interruption test.
pub(crate) fn setup_fixture(env: &VmEnv, name: &str, write_file: impl FnOnce(&Path)) -> Fixture {
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket);

    let btrfs_dir = test_dir(&env.btrfs_mount, name);
    let subvol = btrfs_dir.join("subvol");
    let snap_dir = btrfs_dir.join("snapshots");
    let restore_dir = btrfs_dir.join("restore");
    // work_dir holds bcb's lock file, state file, and multi-GB encrypted parts — keep it
    // off the btrfs loop device (which is small) to avoid filling the btrfs device with bcb's
    // own working data.  /var/tmp is on the VM's root filesystem which has ample free space.
    let work_dir = PathBuf::from(format!("/var/tmp/bcb-work-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    btrfs_subvolume_create(&subvol);
    write_file(&subvol);

    Command::new("btrfs")
        .args([
            "subvolume",
            "snapshot",
            "-r",
            subvol.to_str().unwrap(),
            btrfs_dir
                .join("snapshots/bcb_level0.20260601T0000")
                .to_str()
                .unwrap(),
        ])
        .output()
        .unwrap();

    let identity = age::x25519::Identity::generate();
    let task_name = format!("interrupt-{name}");
    let config = Config {
        base_dir: work_dir.clone(),
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(env),
        tasks: vec![Task {
            name: task_name.clone(),
            description: String::new(),
            subvolume: subvol.clone(),
            snapshot_dir: snap_dir.clone(),
            enabled: true,
        }],
    };

    let config_path = btrfs_dir.join("config.yaml");
    std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let private_key_path = btrfs_dir.join("key.txt");
    std::fs::write(&private_key_path, identity.to_string().expose_secret()).unwrap();

    Fixture {
        config_path,
        private_key_path,
        subvol,
        snap_dir,
        work_dir,
        restore_dir,
        task_name,
        btrfs_dir,
    }
}

impl Drop for Fixture {
    // Runs even when a test assertion panics, so a failing run doesn't leak subvolumes or disk
    // space into the next run.
    fn drop(&mut self) {
        // Delete any restored subvolume first (btrfs receive creates a subvolume under restore_dir).
        btrfs_subvolume_delete(&self.restore_dir.join("bcb_level0.20260601T0000"));
        btrfs_subvolume_delete(&self.snap_dir.join("bcb_level0.20260601T0000"));
        btrfs_subvolume_delete(&self.subvol);
        // Remove plain directories left on the btrfs device and the work dir in /var/tmp.
        let _ = std::fs::remove_dir_all(&self.btrfs_dir);
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}

/// Killing bcb mid-multipart-upload must leave a recoverable, resumable state: the orphaned S3
/// multipart session is detected and aborted on the next run, not left to leak storage forever.
#[tokio::test]
async fn kill_during_multipart_upload_then_resume_should_abort_orphan_and_succeed() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "multipart", |subvol| {
        // >64MB (PART_SIZE) so the single part's .age upload spans >=2 S3 multipart chunks.
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

    let sanitized = sanitize_subvolume_path(&fixture.subvol).unwrap();
    let prefix = format!("interrupt-test/backup-data/{sanitized}/");
    let s3 = raw_s3_client(&env).await;

    let mut child = spawn_bcb_backup(
        &env,
        &fixture.config_path,
        0,
        &fixture.task_name,
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

    let mut child = spawn_bcb_backup(
        &env,
        &fixture.config_path,
        0,
        &fixture.task_name,
        &[("BCB_TEST_PAUSE_DURING_SEND", "1")],
    );
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
        wait_no_orphan_children(Duration::from_secs(5)),
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

    let mut child = spawn_bcb_backup(
        &env,
        &fixture.config_path,
        0,
        &fixture.task_name,
        &[("BCB_TEST_PAUSE_DURING_SEND", "1")],
    );
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
    let mut child = spawn_bcb_restore(
        &env,
        &fixture.config_path,
        &fixture.task_name,
        0,
        "20260601",
        &fixture.restore_dir,
        &fixture.private_key_path,
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

    // bcb's restore work dir is named deterministically from task/level/timestamp; read its
    // state directly (real file, no mocking involved) to confirm the kill actually landed
    // mid-part rather than after it had already finished.
    let restore_work_dir = fixture.work_dir.join(format!(
        "restore-{}-level0-2026-06-01T00:00",
        fixture.task_name
    ));
    let state: btrfs_cold_backup::RestoreState = serde_yaml::from_str(
        &std::fs::read_to_string(restore_work_dir.join("restore-state.yaml")).unwrap(),
    )
    .unwrap();
    let part_state = state
        .parts
        .values()
        .next()
        .expect("restore state must have at least one part");
    assert_eq!(
        part_state.download_state,
        btrfs_cold_backup::PartDownloadState::PartiallyDownloaded {
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
        !restore_work_dir.exists(),
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
