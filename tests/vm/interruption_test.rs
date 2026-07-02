use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    Config, S3Config, S3RetryConfig, S3StorageClass, Task, sanitize_subvolume_path,
};

struct VmEnv {
    btrfs_mount: PathBuf,
    s3_endpoint: String,
    s3_bucket: String,
    bin_path: PathBuf,
}

/// Returns the VM env from env vars, or None to skip the test.
fn vm_env() -> Option<VmEnv> {
    Some(VmEnv {
        btrfs_mount: std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from)?,
        s3_endpoint: std::env::var("BCB_S3_ENDPOINT").ok()?,
        s3_bucket: std::env::var("BCB_S3_BUCKET").ok()?,
        bin_path: std::env::var("BCB_BIN_PATH").ok().map(PathBuf::from)?,
    })
}

fn test_dir(mount: &Path, name: &str) -> PathBuf {
    let dir = mount.join(format!("interrupt-{name}-{}", std::process::id()));
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

fn ensure_bucket(bucket: &str) {
    Command::new("mc")
        .args(["mb", "--ignore-existing", &format!("local/{bucket}")])
        .output()
        .unwrap();
}

fn s3_config(env: &VmEnv) -> S3Config {
    S3Config {
        enabled: true,
        bucket: env.s3_bucket.clone(),
        region: "us-east-1".to_string(),
        prefix: "interrupt-test/".to_string(),
        endpoint: env.s3_endpoint.clone(),
        // S3StorageClass::default() is Rust's #[derive(Default)], which leaves `manifest`
        // empty — the serde `#[serde(default = ...)]` only applies when deserializing a YAML
        // file with the field absent, not to Default::default() called directly in Rust.
        storage_class: S3StorageClass {
            manifest: "STANDARD".to_string(),
            backup_data: std::collections::HashMap::new(),
        },
        retry: S3RetryConfig::default(),
        upload_speed_limit_kbps: 0,
    }
}

async fn raw_s3_client(env: &VmEnv) -> aws_sdk_s3::Client {
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

// ListMultipartUploads' `prefix` filter isn't reliable against this MinIO server (observed: a
// genuine string-prefix of an in-progress upload's key still came back empty), so list
// unfiltered and filter client-side instead of trusting server-side prefix matching.
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

fn run_bcb(env: &VmEnv, args: &[&str]) -> std::process::Output {
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

/// Polls until no btrfs-send / split process remains, or returns false on timeout.
fn wait_no_orphan_children(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if no_process_matches(&["-f", "btrfs.*send"]) && no_process_matches(&["-x", "split"]) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct Fixture {
    config_path: PathBuf,
    private_key_path: PathBuf,
    subvol: PathBuf,
    snap_dir: PathBuf,
    work_dir: PathBuf,
    restore_dir: PathBuf,
    task_name: String,
    /// Root of this fixture's directories on the btrfs device (snap_dir, restore_dir live here).
    btrfs_dir: PathBuf,
}

/// Creates a subvolume + snapshot + on-disk config/key files for one interruption test.
fn setup_fixture(env: &VmEnv, name: &str, write_file: impl FnOnce(&Path)) -> Fixture {
    ensure_bucket(&env.s3_bucket);

    let btrfs_dir = test_dir(&env.btrfs_mount, name);
    let subvol = btrfs_dir.join("subvol");
    let snap_dir = btrfs_dir.join("snapshots");
    let restore_dir = btrfs_dir.join("restore");
    // work_dir holds bcb's lock file, state file, and multi-GB split/encrypt parts — keep it
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
                .join("snapshots/bcb_level0_20260601_00-00")
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
        btrfs_subvolume_delete(&self.restore_dir.join("bcb_level0_20260601_00-00"));
        btrfs_subvolume_delete(&self.snap_dir.join("bcb_level0_20260601_00-00"));
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
        // >64MB (PART_SIZE) so the single split part's .age upload spans >=2 S3 multipart chunks.
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
            .join("bcb_level0_20260601_00-00")
            .join("bigfile.bin"),
    )
    .unwrap();
    assert_eq!(original, recovered, "restored content must match source");
}

/// Killing bcb while btrfs-send/split are running must not leave them as orphan processes that
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
        "btrfs send / split processes survived the parent's SIGKILL"
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

/// Killing bcb leaves the PID lock file holding a now-dead PID; the next run must auto-recover
/// via PidLock::remove_stale instead of erroring out on lock contention.
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
