//! Black-box tests of the `bcb` binary itself: argument parsing and the `main()` wiring for
//! commands that don't touch S3 or btrfs (genkey, test-keys). Commands that construct an
//! `S3Backend` (check/list/verify/backup/restore) are exercised against a real S3-compatible
//! server in the vm/e2e suites instead, since `aws-config` credential/region resolution isn't
//! something worth faking here.

use std::path::Path;
use std::process::{Command, Output};

use btrfs_cold_backup::{Config, RestoreConfig, S3Config, S3StorageClass, Task, generate_key_pair};

fn run_bcb(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bcb"))
        .args(args)
        .output()
        .expect("failed to run bcb")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn write_valid_config(dir: &Path, age_public_key: &str) -> std::path::PathBuf {
    let config = Config {
        base_dir: dir.join("state"),
        age_public_key: age_public_key.to_string(),
        s3: S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            storage_class: S3StorageClass {
                manifest: "STANDARD".to_string(),
                backup_data: vec!["STANDARD".to_string()],
            },
            restore: RestoreConfig::default(),
            ..Default::default()
        },
        tasks: vec![Task {
            name: "task".to_string(),
            description: String::new(),
            subvolume: dir.join("subvol"),
            snapshot_dir: dir.join("snapshots"),
            enabled: true,
        }],
    };
    let path = dir.join("config.yaml");
    std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
    path
}

#[test]
fn no_subcommand_should_exit_with_clap_usage_error() {
    let out = run_bcb(&[]);
    assert!(!out.status.success());
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("Usage"));
}

#[test]
fn unknown_subcommand_should_exit_with_clap_usage_error() {
    let out = run_bcb(&["not-a-real-command"]);
    assert!(!out.status.success());
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn genkey_should_write_private_key_file_and_print_matching_public_key() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("key.txt");

    let out = run_bcb(&["genkey", "--output", key_path.to_str().unwrap()]);
    assert!(out.status.success(), "stderr={}", stderr(&out));

    let public_key = stdout(&out).trim().to_string();
    assert!(public_key.starts_with("age1"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // The generated key file must actually pair with the printed public key.
    let out = run_bcb(&[
        "--config",
        write_valid_config(dir.path(), &public_key)
            .to_str()
            .unwrap(),
        "test-keys",
        "--private-key",
        key_path.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "stderr={}", stderr(&out));
}

#[test]
fn missing_config_file_should_fail_with_nonzero_exit() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_bcb(&[
        "--config",
        dir.path().join("does-not-exist.yaml").to_str().unwrap(),
        "test-keys",
        "--private-key",
        dir.path().join("key.txt").to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("failed to read config"));
}

#[test]
fn test_keys_with_missing_private_key_file_should_fail_with_nonzero_exit() {
    let dir = tempfile::tempdir().unwrap();
    let pair = generate_key_pair();
    let config_path = write_valid_config(dir.path(), &pair.public_key);

    let out = run_bcb(&[
        "--config",
        config_path.to_str().unwrap(),
        "test-keys",
        "--private-key",
        dir.path().join("does-not-exist.txt").to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("failed to read private key"));
}

#[test]
fn test_keys_with_mismatched_key_pair_should_fail_with_nonzero_exit() {
    use age::secrecy::ExposeSecret;

    let dir = tempfile::tempdir().unwrap();
    let config_pair = generate_key_pair();
    let config_path = write_valid_config(dir.path(), &config_pair.public_key);

    // A different, unrelated key pair: its private key won't decrypt for config's public key.
    let other_pair = generate_key_pair();
    let key_path = dir.path().join("key.txt");
    std::fs::write(&key_path, other_pair.private_key.expose_secret()).unwrap();

    let out = run_bcb(&[
        "--config",
        config_path.to_str().unwrap(),
        "test-keys",
        "--private-key",
        key_path.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
}

#[test]
fn restore_with_unknown_tier_should_exit_with_clap_usage_error() {
    let out = run_bcb(&[
        "restore",
        "--task",
        "task",
        "--level",
        "0",
        "--date",
        "20260602",
        "--target",
        "/nonexistent",
        "--private-key",
        "/nonexistent",
        "--restore-tier",
        "Fastest",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("Fastest"), "stderr={}", stderr(&out));
}
