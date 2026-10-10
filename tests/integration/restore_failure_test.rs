use std::path::Path;

use anyhow::Result;
use btrfs_cold_backup::{
    config::RestoreConfig,
    manifest::BackupManifest,
    remote::{RemoteError, Retrieval, RetrievalStatus, RetrievalTier},
    restore::{RestoreProgress, RestoreRequest, RetrievalOptions},
};

use crate::common::*;

/// One part's download failing mid-run (expired copy, transient network error) must not
/// abandon the parts that are still downloadable: the run keeps going and reports progress
/// so the scheduler retries.
#[tokio::test]
async fn part_download_failure_should_not_abort_remaining_parts() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let encrypted = encrypted_part("000001", b"stream payload");
    let mut backup = manifest(vec![part("000000", 10, "h0"), encrypted.part]);
    backup.blake3 = "stream_hash_not_reached".to_string();
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000000"))
        .returning(|_, _, _, _| Err(RemoteError::new("connection lost").into()));
    let bytes = encrypted.bytes;
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000001"))
        .times(1)
        .returning(move |_, _, _, path| {
            std::fs::write(path, &bytes)?;
            Ok(())
        });

    let progress = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
        &remote,
    )
    .await
    .expect("one failed part must not fail the run");

    assert!(!progress.completed);
    assert_eq!(
        progress.parts_downloaded, 1,
        "the healthy part must still be downloaded"
    );
    assert_eq!(progress.parts_total, 2);
}

/// Restore of a two-part backup. `status` answers each part's status check and `download`
/// each range request, by part key.
async fn restore_two_parts(
    work_dir: &Path,
    status: fn(&str) -> Result<Retrieval>,
    download: fn(&str, &Path) -> Result<()>,
) -> Result<RestoreProgress> {
    let manifest_yaml = yaml(&manifest(vec![
        part("000000", 10, "parthash"),
        part("000001", 10, "parthash"),
    ]));

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(ready);
    remote.expect_retrieval_status().returning(status);
    remote
        .expect_download()
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(move |key, _, _, path| download(key, path));

    restore(
        &test_config(work_dir),
        &restore_request(work_dir, 0),
        &btrfs_without_receive(),
        &remote,
    )
    .await
}

/// A local error such as a full disk would fail every other part the same way, so it must
/// fail the run at once instead of being skipped like a remote failure.
#[tokio::test]
async fn local_error_during_download_should_fail_the_run() {
    let work_dir = tempfile::tempdir().unwrap();
    let err = restore_two_parts(work_dir.path(), ready, |key, path| {
        if key.contains("000000") {
            return Err(std::io::Error::from_raw_os_error(libc::ENOSPC).into());
        }
        std::fs::write(path, b"0123456789")?;
        Ok(())
    })
    .await
    .expect_err("a local error must fail the run");
    assert!(
        err.to_string().contains("000000"),
        "unexpected error: {err}"
    );
    assert!(
        !restore_work_dir(work_dir.path())
            .join("snapshot.part-000001.age")
            .exists(),
        "no other part may be tried after a local error"
    );
}

/// With every attempted download failing (e.g. the network is down), reporting in progress
/// would let a scheduler retry forever without alerting.
#[tokio::test]
async fn all_part_downloads_failing_should_fail_the_run() {
    let work_dir = tempfile::tempdir().unwrap();
    let err = restore_two_parts(work_dir.path(), ready, |_, _| {
        Err(RemoteError::new("connection lost").into())
    })
    .await
    .expect_err("a run with no successful download must fail");
    assert!(
        format!("{err:#}").contains("connection lost"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn all_part_status_checks_failing_should_fail_the_run() {
    let work_dir = tempfile::tempdir().unwrap();
    let err = restore_two_parts(
        work_dir.path(),
        |_| Err(RemoteError::new("connection lost").into()),
        |_, _| panic!("nothing may be downloaded"),
    )
    .await
    .expect_err("a run with no successful status check must fail");
    assert!(
        format!("{err:#}").contains("connection lost"),
        "unexpected error: {err:#}"
    );
}

/// Restore of a level 1 backup whose manifest is readable and whose part is archived,
/// decrypting with `identity`; `tweak` adjusts the manifest. Requesting a retrieval fails the
/// test.
async fn restore_level1_without_requests(
    work_dir: &Path,
    identity: age::x25519::Identity,
    tweak: fn(&mut BackupManifest),
) -> Result<RestoreProgress> {
    let mut backup = manifest(vec![part("000000", 10, "parthash")]);
    backup.level = 1;
    backup.snapshot_name = "bcb_level1.20260602T1430".to_string();
    backup.snapshot_uuid = snap_uuid(&backup.snapshot_name);
    backup.parent_snapshot = Some("bcb_level0.20260601T1430".to_string());
    tweak(&mut backup);
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(|key| {
        Ok(status(if key.contains("/manifest.yaml") {
            RetrievalStatus::Ready
        } else {
            RetrievalStatus::NotRequested
        }))
    });
    remote
        .expect_download()
        .returning(move |_| Ok(manifest_yaml.clone()));

    let request = RestoreRequest {
        identity,
        ..restore_request(work_dir, 1)
    };
    restore(
        &test_config(work_dir),
        &request,
        &btrfs_without_receive(),
        &remote,
    )
    .await
}

fn create_level0_on_target(work_dir: &Path) {
    std::fs::create_dir_all(work_dir.join("restore_target/bcb_level0.20260601T1430")).unwrap();
}

/// A key that can't decrypt the backup must be caught before any retrieval is paid for.
#[tokio::test]
async fn restore_with_wrong_private_key_should_fail_before_requesting_retrieval() {
    let work_dir = tempfile::tempdir().unwrap();
    create_level0_on_target(work_dir.path());
    let other = age::x25519::Identity::generate();
    let err = restore_level1_without_requests(work_dir.path(), other, |_| {})
        .await
        .expect_err("a wrong key must fail the restore")
        .to_string();
    assert!(err.contains("encrypted to"), "unexpected error: {err}");
}

/// An incremental can't be received without its parent, so the restore must say so before
/// any retrieval is paid for.
#[tokio::test]
async fn restore_without_parent_on_target_should_fail_before_requesting_retrieval() {
    let work_dir = tempfile::tempdir().unwrap();
    let err = restore_level1_without_requests(work_dir.path(), TEST_IDENTITY.clone(), |_| {})
        .await
        .expect_err("a missing parent must fail the restore")
        .to_string();
    assert!(
        err.contains("bcb_level0.20260601T1430") && err.contains("restore the backup it names"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn restore_without_enough_space_should_fail_before_requesting_retrieval() {
    let work_dir = tempfile::tempdir().unwrap();
    create_level0_on_target(work_dir.path());
    let err = restore_level1_without_requests(work_dir.path(), TEST_IDENTITY.clone(), |b| {
        b.parts[0].size_bytes = u64::MAX / 2;
    })
    .await
    .expect_err("a part larger than the free space must fail the restore")
    .to_string();
    assert!(
        err.contains("insufficient free space"),
        "unexpected error: {err}"
    );
}

/// With pacing enabled, retrieved copies must live long enough for the paced download to
/// reach them; otherwise every wave tail expires and is re-requested at full retrieval cost.
#[tokio::test]
async fn restore_days_shorter_than_lead_window_should_be_rejected_before_any_s3_call() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1024,
        lead_days: None,
        ..RestoreConfig::default()
    };
    let remote = MockRemote::new();

    // The Bulk tier's default lead window is longer than the copy lifetime given here.
    let request = RestoreRequest {
        retrieval: RetrievalOptions {
            days: 3,
            tier: RetrievalTier::Bulk,
        },
        ..restore_request(work_dir.path(), 0)
    };
    let err = restore(&config, &request, &MockBtrfs::new(), &remote)
        .await
        .expect_err("insufficient restore-days must be rejected");

    assert!(
        err.to_string().contains("restore-days"),
        "unexpected error: {err}"
    );
}
