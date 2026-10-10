//! The subcommand bodies in `commands`, driven with mocks in place of btrfs and S3.

use std::path::Path;
use std::process::ExitCode;

use btrfs_cold_backup::{
    commands::{
        EXIT_BACKUP_OVERSIZE, EXIT_RESTORE_IN_PROGRESS, cmd_backup, cmd_check, cmd_list,
        cmd_prune_snapshots, cmd_restore, cmd_verify,
    },
    config::{Config, Task},
    manifest::{LatestEntry, LatestManifest},
    remote::RetrievalStatus,
    timestamp::BackupId,
};

use crate::common::*;
use crate::oversize_test;

fn config_with(base_dir: &Path, tasks: Vec<Task>) -> Config {
    Config {
        tasks,
        ..test_config(base_dir)
    }
}

fn task(name: &str, enabled: bool) -> Task {
    Task {
        name: name.to_string(),
        enabled,
        ..movies_task()
    }
}

fn error_of<T>(result: anyhow::Result<T>) -> String {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(e) => format!("{e:#}"),
    }
}

/// A btrfs with one level 0 snapshot to send.
fn btrfs_with_level0() -> MockBtrfs {
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec!["bcb_level0.20260602T1430".to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));
    btrfs
}

fn remote_accepting_level0(base_dir: &Path) -> MockRemote {
    let mut remote = MockRemote::new();
    expect_no_manifests(&mut remote);
    expect_parts_on_s3(&mut remote, base_dir);
    remote.expect_upload_file().returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote
}

#[tokio::test]
async fn backup_should_succeed_for_a_task_and_skip_disabled_ones() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task(), task("off", false)]);
    let remote = remote_accepting_level0(work_dir.path());

    let code = cmd_backup(&config, &btrfs_with_level0(), &remote, 0, None, false)
        .await
        .unwrap();

    assert_eq!(code, ExitCode::SUCCESS);
}

#[tokio::test]
async fn backup_should_fail_without_matching_enabled_task() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![task("off", false)]);

    let err = error_of(
        cmd_backup(
            &config,
            &MockBtrfs::new(),
            &MockRemote::new(),
            0,
            None,
            false,
        )
        .await,
    );

    assert!(err.contains("no matching enabled tasks"), "{err}");
}

/// One task failing must not keep the next from running, and the run must still fail.
#[tokio::test]
async fn backup_should_run_every_task_and_name_the_failed_one() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(
        work_dir.path(),
        vec![
            Task {
                subvolume: "/mnt/data/missing".into(),
                ..task("broken", true)
            },
            movies_task(),
        ],
    );
    let mut btrfs = btrfs_with_uuids();
    btrfs.expect_list_snapshots().returning(|dir| {
        if dir.ends_with("movies") {
            Ok(vec!["bcb_level0.20260602T1430".to_string()])
        } else {
            Err(anyhow::anyhow!("no such directory"))
        }
    });
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));
    let remote = remote_accepting_level0(work_dir.path());

    let err = error_of(cmd_backup(&config, &btrfs, &remote, 0, None, false).await);

    assert_eq!(err, "backup failed for task(s): broken");
}

#[tokio::test]
async fn backup_should_exit_with_oversize_code_when_only_the_guard_held_back() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = oversize_test::config(work_dir.path(), oversize_test::guard(3, 1.0));
    config.tasks = vec![movies_task()];
    let remote = oversize_test::remote(
        work_dir.path(),
        1_000_000,
        oversize_test::weekly_history(1),
        0,
    );
    let mut btrfs = btrfs_with_uuids();
    btrfs.expect_list_snapshots().returning(|_| {
        Ok(vec![
            oversize_test::LEVEL0.to_string(),
            "bcb_level3.20270129T0100".to_string(),
        ])
    });
    btrfs
        .expect_send_incremental()
        .returning(|_, _| Ok(fake_stream()));

    let code = cmd_backup(&config, &btrfs, &remote, 3, None, false)
        .await
        .unwrap();

    assert_eq!(code, ExitCode::from(EXIT_BACKUP_OVERSIZE));
}

fn latest_of(snapshots: &[&str]) -> Vec<u8> {
    let mut latest = LatestManifest::new(SOURCE_UUID.to_string());
    for name in snapshots {
        let id = BackupId::from_snapshot_name(name).unwrap();
        latest.levels.insert(
            id.level,
            LatestEntry {
                timestamp: id.timestamp,
                snapshot_name: name.to_string(),
                snapshot_uuid: snap_uuid(name),
            },
        );
    }
    yaml(&latest)
}

const OLD: &str = "bcb_level0.20260101T0100";
const CURRENT: &str = "bcb_level0.20270101T0100";

fn btrfs_for_prune() -> MockBtrfs {
    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Ok(vec![OLD.to_string(), CURRENT.to_string()]));
    btrfs
}

fn remote_with_latest_of_current() -> MockRemote {
    let latest = latest_of(&[CURRENT]);
    let mut remote = MockRemote::new();
    remote.expect_head_metadata().returning(|_| head(true));
    remote
        .expect_download()
        .returning(move |_| Ok(latest.clone()));
    remote
}

#[tokio::test]
async fn prune_snapshots_should_delete_the_unneeded_snapshot() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);
    let mut btrfs = btrfs_for_prune();
    btrfs
        .expect_delete_subvolume()
        .withf(|path| path.ends_with(OLD))
        .times(1)
        .returning(|_| Ok(()));

    cmd_prune_snapshots(
        &config,
        &btrfs,
        &remote_with_latest_of_current(),
        None,
        false,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn prune_snapshots_dry_run_should_not_delete() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);

    // No delete_subvolume expectation: a dry run must not call it.
    cmd_prune_snapshots(
        &config,
        &btrfs_for_prune(),
        &remote_with_latest_of_current(),
        None,
        true,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn prune_snapshots_should_fail_without_matching_enabled_task() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);

    let err = error_of(
        cmd_prune_snapshots(
            &config,
            &MockBtrfs::new(),
            &MockRemote::new(),
            Some("nope"),
            false,
        )
        .await,
    );

    assert!(err.contains("no matching enabled tasks"), "{err}");
}

#[tokio::test]
async fn prune_snapshots_should_name_the_failed_task() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);
    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_list_snapshots()
        .returning(|_| Err(anyhow::anyhow!("no such directory")));

    let err = error_of(cmd_prune_snapshots(&config, &btrfs, &MockRemote::new(), None, false).await);

    assert_eq!(err, "prune-snapshots failed for task(s): movies");
}

fn remote_listing(keys: Vec<String>) -> MockRemote {
    let mut remote = MockRemote::new();
    remote
        .expect_list_with_prefix()
        .returning(move |_| Ok(keys.clone()));
    remote
}

const MANIFEST_KEY: &str =
    "backups/manifests/mnt_data_movies/bcb_level0_202606021430/manifest.yaml";

#[tokio::test]
async fn list_should_succeed_with_and_without_backups_and_level_filter() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);

    let with_backup = remote_listing(vec![MANIFEST_KEY.to_string()]);
    cmd_list(&config, &with_backup, None, None).await.unwrap();
    cmd_list(&config, &with_backup, None, Some(1))
        .await
        .unwrap();
    cmd_list(&config, &remote_listing(vec![]), Some("movies"), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn list_should_name_the_task_whose_listing_failed() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);
    let mut remote = MockRemote::new();
    remote
        .expect_list_with_prefix()
        .returning(|_| Err(anyhow::anyhow!("access denied")));

    let err = error_of(cmd_list(&config, &remote, None, None).await);

    assert_eq!(err, "list failed for task(s): movies");
}

#[tokio::test]
async fn verify_should_flag_a_task_without_backups() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);

    let err = error_of(cmd_verify(&config, &remote_listing(vec![]), None, None).await);

    assert_eq!(err, "verify failed for task(s): movies");
}

#[tokio::test]
async fn verify_should_name_the_task_whose_audit_errored() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);
    let mut remote = MockRemote::new();
    remote
        .expect_list_with_prefix()
        .returning(|_| Err(anyhow::anyhow!("access denied")));

    let err = error_of(cmd_verify(&config, &remote, None, None).await);

    assert_eq!(err, "verify failed for task(s): movies");
}

#[tokio::test]
async fn check_should_fail_when_the_remote_rejects_credentials() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![]);
    let mut remote = MockRemote::new();
    remote
        .expect_verify_credentials()
        .returning(|| Err(anyhow::anyhow!("invalid credentials")));

    let err = error_of(cmd_check(&config, &MockBtrfs::new(), &remote).await);

    assert!(err.contains("invalid credentials"), "{err}");
}

#[tokio::test]
async fn restore_should_fail_for_an_unknown_task() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);

    let err = error_of(
        cmd_restore(
            &config,
            &MockBtrfs::new(),
            &MockRemote::new(),
            "nope",
            &restore_request(work_dir.path(), 0),
        )
        .await,
    );

    assert!(err.contains("task 'nope' not found"), "{err}");
}

#[tokio::test]
async fn restore_should_exit_with_in_progress_code_while_parts_are_archived() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = config_with(work_dir.path(), vec![movies_task()]);
    let manifest_yaml = yaml(&manifest(vec![part("000000", 10, "h0")]));
    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(|key| {
        Ok(if key.contains("/manifest.yaml") {
            status(RetrievalStatus::Ready)
        } else {
            status(RetrievalStatus::NotRequested)
        })
    });
    remote
        .expect_download()
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_retrieval()
        .returning(|_, _, _| Ok(()));

    let code = cmd_restore(
        &config,
        &btrfs_without_receive(),
        &remote,
        "movies",
        &restore_request(work_dir.path(), 0),
    )
    .await
    .unwrap();

    assert_eq!(code, ExitCode::from(EXIT_RESTORE_IN_PROGRESS));
}
