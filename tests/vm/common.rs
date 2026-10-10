//! Fixtures shared by the VM tests, which run against a real btrfs mount and S3 endpoint
//! provided by scripts/run-vm-tests.sh.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use age::secrecy::ExposeSecret;
use btrfs_cold_backup::{
    config::{Config, S3Config, StorageClass, StorageClasses, Task},
    layout::sanitize_subvolume_path,
};

/// S3 prefix every VM test writes under.
pub(crate) const S3_PREFIX: &str = "vm-test/";

/// Logs why a test is skipped. `cargo test` reports a skipped test as "ok", so in CI, where
/// a skip means a misconfigured environment, this fails instead.
pub(crate) fn skip(reason: &str) {
    let msg = format!("skipping: {reason} (run via scripts/run-vm-tests.sh)");
    assert!(std::env::var("CI").is_err(), "{msg}, but CI=true");
    eprintln!("{msg}");
}

pub(crate) struct VmEnv {
    pub(crate) btrfs_mount: PathBuf,
    pub(crate) s3_endpoint: String,
    pub(crate) s3_bucket: String,
    pub(crate) bin_path: PathBuf,
}

/// The VM environment from env vars, or None (after logging why) to skip the test.
pub(crate) fn vm_env() -> Option<VmEnv> {
    let btrfs_mount = std::env::var("BCB_BTRFS_MOUNT").ok().map(PathBuf::from);
    let s3_endpoint = std::env::var("BCB_S3_ENDPOINT").ok();
    let s3_bucket = std::env::var("BCB_S3_BUCKET").ok();
    let bin_path = std::env::var("BCB_BIN_PATH").ok().map(PathBuf::from);
    let (Some(btrfs_mount), Some(s3_endpoint), Some(s3_bucket), Some(bin_path)) =
        (btrfs_mount, s3_endpoint, s3_bucket, bin_path)
    else {
        skip("BCB_BTRFS_MOUNT / BCB_S3_ENDPOINT / BCB_S3_BUCKET / BCB_BIN_PATH not set");
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
    let dir = mount.join(format!("vm-test-{name}-{}", std::process::id()));
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

pub(crate) fn btrfs_snapshot(src: &Path, dst: &Path) {
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
        prefix: S3_PREFIX.to_string(),
        endpoint: env.s3_endpoint.clone(),
        storage_class: StorageClasses {
            manifest: StorageClass::Standard,
            backup_data: vec![StorageClass::Standard, StorageClass::Standard],
        },
        ..Default::default()
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

pub(crate) fn run_bcb(env: &VmEnv, args: &[&str]) -> Output {
    Command::new(&env.bin_path)
        .args(args)
        .output()
        .expect("failed to run bcb")
}

pub(crate) fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `needle` may be on either stream: errors go to stderr, progress reports to stdout.
pub(crate) fn assert_exit(out: &Output, code: i32, needle: &str) {
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(code), "output: {output}");
    assert!(output.contains(needle), "expected {needle:?} in: {output}");
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

/// Creates a subvolume holding what `write_file` writes, its level 0 snapshot, and the config
/// and key files for one test.
pub(crate) fn setup_fixture(env: &VmEnv, name: &str, write_file: impl FnOnce(&Path)) -> Fixture {
    ensure_bucket(&env.s3_endpoint, &env.s3_bucket);

    let btrfs_dir = test_dir(&env.btrfs_mount, name);
    let subvol = btrfs_dir.join("subvol");
    let snap_dir = btrfs_dir.join("snapshots");
    let restore_dir = btrfs_dir.join("restore");
    // base_dir holds the encrypted parts, so it lives on the VM's root filesystem rather than
    // the small btrfs loop device.
    let work_dir = PathBuf::from(format!("/var/tmp/bcb-work-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&snap_dir).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    btrfs_subvolume_create(&subvol);
    write_file(&subvol);

    btrfs_snapshot(&subvol, &snap_dir.join("bcb_level0.20260601T0000"));

    let identity = age::x25519::Identity::generate();
    let task_name = format!("vm-test-{name}");
    let config = Config {
        base_dir: work_dir.clone(),
        age_public_key: identity.to_public().to_string(),
        s3: s3_config(env),
        oversize_guard: None,
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

/// Runs even when a test assertion panics, so a failing run doesn't leak subvolumes or disk
/// space into the next run.
impl Drop for Fixture {
    fn drop(&mut self) {
        btrfs_subvolume_delete(&self.restore_dir.join("bcb_level0.20260601T0000"));
        btrfs_subvolume_delete(&self.snap_dir.join("bcb_level0.20260601T0000"));
        btrfs_subvolume_delete(&self.subvol);
        let _ = std::fs::remove_dir_all(&self.btrfs_dir);
        let _ = std::fs::remove_dir_all(&self.work_dir);
    }
}

/// S3 prefix of the parts of every backup of the fixture's subvolume.
pub(crate) fn data_prefix(f: &Fixture) -> String {
    let sanitized = sanitize_subvolume_path(&f.subvol).unwrap();
    format!("{S3_PREFIX}backup-data/{sanitized}/")
}

/// Local work dir of a restore of the fixture's level 0 snapshot.
pub(crate) fn restore_work_dir(f: &Fixture) -> PathBuf {
    let sanitized = sanitize_subvolume_path(&f.subvol).unwrap();
    f.work_dir
        .join("restore")
        .join(sanitized)
        .join("bcb_level0_202606010000")
}
