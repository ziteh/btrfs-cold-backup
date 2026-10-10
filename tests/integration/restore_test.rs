use btrfs_cold_backup::{
    config::RestoreConfig,
    remote::{RestoreStatus, RestoreTier},
    restore::{RetrievalOptions, restore_task},
    state::{PartDownloadState, RestorePartState, RestoreState},
};

use crate::common::*;

/// A part whose download doesn't match its manifest hash must fail the run, and the bad
/// download is discarded so the next run starts the part over.
#[tokio::test]
async fn restore_should_fail_on_blake3_mismatch() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");
    let manifest_yaml = yaml(&manifest(vec![part("000000", 10, "wrong_hash")]));

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(|_, _, _, path| {
            std::fs::write(path, b"some random bytes")?;
            Ok(())
        });

    let result = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await;
    assert!(result.is_err(), "restore should fail on BLAKE3 mismatch");
    let state: RestoreState = serde_yaml::from_slice(
        &std::fs::read(restore_work_dir(work_dir.path()).join("state.yaml")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        state.parts["000000"].download_state,
        PartDownloadState::NotDownloaded
    );
}

/// An archived manifest must trigger a retrieval request and a retry-later error, not a
/// download.
#[tokio::test]
async fn restore_should_request_retrieval_for_archived_objects() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let mut remote = MockRemote::new();
    remote
        .expect_restore_status_detail()
        .returning(|_| Ok(status(RestoreStatus::NotRequested)));
    remote
        .expect_request_restore()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let err = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("archived manifest must not be downloaded")
    .to_string();
    assert!(err.contains("re-run"), "unexpected error: {err}");
}

/// A whole-stream hash mismatch must abort before anything reaches btrfs receive, even when
/// every individual part hash checks out.
#[tokio::test]
async fn restore_should_fail_before_receive_on_stream_mismatch() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part]);
    backup.blake3 = "not_the_stream_hash".to_string();
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    let bytes = encrypted.bytes;
    remote
        .expect_download_range_to_file()
        .returning(move |_, _, _, path| {
            std::fs::write(path, &bytes)?;
            Ok(())
        });

    let err = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("stream mismatch must fail before receive")
    .to_string();
    assert!(
        err.contains("stream BLAKE3 mismatch"),
        "unexpected error: {err}"
    );
    assert!(
        !target_dir.exists(),
        "target_dir must not be created before verification"
    );
}

/// With `expected_download_kbps` left at its default (no pacing estimate), every remaining
/// archived part must be requested at once.
#[tokio::test]
async fn default_config_should_still_request_retrieval_for_all_archived_parts() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");
    let manifest_yaml = yaml(&manifest(vec![
        part("000000", 10, "h0"),
        part("000001", 10, "h1"),
    ]));

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        Ok(status(if key.contains("/manifest.yaml") {
            RestoreStatus::Ready
        } else {
            RestoreStatus::NotRequested
        }))
    });
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_restore()
        .times(2)
        .returning(|_, _, _| Ok(()));

    let result = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect("archived-but-not-yet-ready parts must report progress, not error");

    assert!(!result.completed);
    assert_eq!(result.parts_downloaded, 0);
    assert_eq!(result.parts_total, 2);
    assert_eq!(result.bytes_remaining, 20);
}

/// A manifest whose retrieval is still `InProgress` must bail out with a "re-run later" error
/// rather than attempt a GetObject, which S3 rejects for an archived object.
#[tokio::test]
async fn restore_should_bail_cleanly_when_manifest_retrieval_in_progress() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let mut remote = MockRemote::new();
    remote
        .expect_restore_status_detail()
        .returning(|_| Ok(status(RestoreStatus::InProgress)));

    let err = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("manifest retrieval in progress must bail, not attempt download");

    assert!(
        err.to_string().contains("in progress"),
        "unexpected error: {err}"
    );
}

/// A run whose `btrfs receive` failed after the stream verified must make the next run retry
/// the receive, not report success because the parts were already checked. The second run
/// must also reuse the manifest cached in the work dir, since its retrieved copy on S3 may
/// have expired.
#[tokio::test]
async fn resume_after_failed_receive_should_retry_receive_not_silently_succeed() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part]);
    backup.blake3 = encrypted.stream_blake3;
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .times(1)
        .returning(move |_| Ok(manifest_yaml.clone()));
    let bytes = encrypted.bytes;
    remote
        .expect_download_range_to_file()
        .returning(move |_, _, _, path| {
            std::fs::write(path, &bytes)?;
            Ok(())
        });

    let (task, private_key, retrieval) = (movies_task(), test_private_key(), retrieval());
    let run = || {
        restore_task(
            &config,
            &task,
            0,
            "20260602",
            &target_dir,
            &private_key,
            &retrieval,
            &remote,
        )
    };
    // There is no btrfs here, so the receive itself fails both times.
    let first_err = run()
        .await
        .expect_err("btrfs receive must fail in this non-btrfs test environment");
    let second_err = run()
        .await
        .expect_err("resumed run must retry receive, not silently succeed");

    assert_eq!(
        first_err.to_string(),
        second_err.to_string(),
        "both runs should fail identically by actually attempting receive"
    );
}

/// A part left half-downloaded by an interrupted run must be resumed from its saved offset.
#[tokio::test]
async fn restore_should_resume_partially_downloaded_part() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part.clone()]);
    backup.blake3 = encrypted.stream_blake3.clone();
    let manifest_yaml = yaml(&backup);

    // The work dir as an interrupted run left it: the state says half the part is
    // downloaded, and the file holds that half.
    let half = encrypted.bytes.len() as u64 / 2;
    let restore_dir = restore_work_dir(work_dir.path());
    std::fs::create_dir_all(&restore_dir).unwrap();
    std::fs::write(
        restore_dir.join("part-000000.age"),
        &encrypted.bytes[..half as usize],
    )
    .unwrap();
    let mut seeded = RestoreState {
        manifest_blake3: encrypted.stream_blake3,
        parts: [(
            "000000".to_string(),
            RestorePartState {
                size_bytes: encrypted.part.size_bytes,
                blake3: encrypted.part.blake3,
                download_state: PartDownloadState::PartiallyDownloaded {
                    downloaded_bytes: half,
                },
            },
        )]
        .into(),
        ..Default::default()
    };
    seeded.save(&restore_dir.join("state.yaml")).unwrap();

    let remaining: Vec<u8> = encrypted.bytes[half as usize..].to_vec();
    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .withf(move |_, start, _, _| *start == half)
        .times(1)
        .returning(move |_, _, _, path| {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(path)?;
            f.write_all(&remaining)?;
            Ok(())
        });

    // There is no btrfs here, so the run fails at the receive, after the part was resumed
    // and verified.
    restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect_err("btrfs receive must fail in this non-btrfs test environment");

    let state: RestoreState =
        serde_yaml::from_str(&std::fs::read_to_string(restore_dir.join("state.yaml")).unwrap())
            .unwrap();
    assert_eq!(
        state.parts["000000"].download_state,
        PartDownloadState::Downloaded,
        "resumed part must reach Downloaded"
    );
    assert!(
        restore_dir.join("part-000000.age").exists(),
        "the verified part must be kept for the receive to be retried"
    );
}

/// A part damaged on disk after its download check (power loss, bit rot) must be caught
/// before receive and cost only that part's download, reported as in progress.
#[tokio::test]
async fn part_damaged_before_finalize_should_be_reset_for_download() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let target_dir = work_dir.path().join("restore_target");

    let restore_dir = restore_work_dir(work_dir.path());
    std::fs::create_dir_all(&restore_dir).unwrap();
    let mut parts = Vec::new();
    for (index, bytes) in [("000000", &b"intact part"[..]), ("000001", b"damaged part")] {
        std::fs::write(restore_dir.join(format!("part-{index}.age")), bytes).unwrap();
        parts.push(part(
            index,
            bytes.len() as u64,
            &blake3::hash(bytes).to_hex(),
        ));
    }
    std::fs::write(restore_dir.join("part-000001.age"), b"damaged pa!t").unwrap();

    let backup = manifest(parts.clone());
    let mut seeded = RestoreState {
        manifest_blake3: backup.blake3.clone(),
        parts: parts
            .iter()
            .map(|p| {
                (
                    p.index.clone(),
                    RestorePartState {
                        size_bytes: p.size_bytes,
                        blake3: p.blake3.clone(),
                        download_state: PartDownloadState::Downloaded,
                    },
                )
            })
            .collect(),
        ..Default::default()
    };
    seeded.save(&restore_dir.join("state.yaml")).unwrap();
    let manifest_yaml = yaml(&backup);

    // Nothing is pending, so only the manifest's status may be checked.
    let mut remote = MockRemote::new();
    remote
        .expect_restore_status_detail()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(ready);
    remote
        .expect_download()
        .returning(move |_| Ok(manifest_yaml.clone()));

    let progress = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &retrieval(),
        &remote,
    )
    .await
    .expect("a damaged part must be reported as in progress, not as a failure");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 1);
    let state: RestoreState =
        serde_yaml::from_str(&std::fs::read_to_string(restore_dir.join("state.yaml")).unwrap())
            .unwrap();
    assert_eq!(
        state.parts["000001"].download_state,
        PartDownloadState::NotDownloaded
    );
    assert_eq!(
        state.parts["000000"].download_state,
        PartDownloadState::Downloaded
    );
    assert!(!target_dir.exists(), "nothing may reach btrfs receive");
}

/// A partially-downloaded part whose retrieved copy expired must be re-requested regardless
/// of the pacing budget: its bytes are already invested, and the resume offset stays valid
/// because the object is immutable.
#[tokio::test]
async fn restore_should_reissue_retrieval_for_expired_partial_download_regardless_of_budget() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    // A budget far smaller than the part, so the request can't merely fit within it.
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1,
        lead_days: Some(1.0 / (1024.0 * 86400.0)),
        ..RestoreConfig::default()
    };
    let target_dir = work_dir.path().join("restore_target");
    let mut backup = manifest(vec![part("000000", 100, "parthash")]);
    backup.blake3 = "irrelevant".to_string();
    let manifest_yaml = yaml(&backup);

    // Half the part was downloaded by an earlier run; its retrieved copy has since expired.
    let restore_dir = restore_work_dir(work_dir.path());
    std::fs::create_dir_all(&restore_dir).unwrap();
    let mut seeded = RestoreState {
        manifest_blake3: "irrelevant".to_string(),
        parts: [(
            "000000".to_string(),
            RestorePartState {
                size_bytes: 100,
                blake3: "parthash".to_string(),
                download_state: PartDownloadState::PartiallyDownloaded {
                    downloaded_bytes: 50,
                },
            },
        )]
        .into(),
        ..Default::default()
    };
    seeded.save(&restore_dir.join("state.yaml")).unwrap();

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        Ok(status(if key.contains("/manifest.yaml") {
            RestoreStatus::Ready
        } else {
            RestoreStatus::NotRequested
        }))
    });
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_restore()
        .withf(|key, _, _| key.contains("000000"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let progress = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &RetrievalOptions {
            days: 2,
            tier: RestoreTier::Standard,
        },
        &remote,
    )
    .await
    .expect("must report progress, not error");
    assert!(!progress.completed);

    let state: RestoreState =
        serde_yaml::from_str(&std::fs::read_to_string(restore_dir.join("state.yaml")).unwrap())
            .unwrap();
    assert_eq!(
        state.parts["000000"].download_state,
        PartDownloadState::PartiallyDownloaded {
            downloaded_bytes: 50
        },
        "resume offset must survive the re-request"
    );
}

/// The run that drains a ready wave must request the next wave in the same run, so retrieval
/// and download overlap. Bytes about to be downloaded don't count against the request budget.
#[tokio::test]
async fn run_draining_ready_wave_should_request_next_wave_within_budget() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    // lead_days chosen so the buffer target is 15 bytes: room for exactly one of the two
    // 10-byte archived parts below.
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1,
        lead_days: Some(15.0 / (125.0 * 86400.0)),
        ..RestoreConfig::default()
    };
    let target_dir = work_dir.path().join("restore_target");

    // Part 000000 is ready and downloaded this run; 000001 and 000002 are still archived.
    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![
        encrypted.part,
        part("000001", 10, "h1"),
        part("000002", 10, "h2"),
    ]);
    backup.blake3 = "stream_hash_not_reached".to_string();
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        Ok(status(
            if key.contains("/manifest.yaml") || key.contains("000000") {
                RestoreStatus::Ready
            } else {
                RestoreStatus::NotRequested
            },
        ))
    });
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    let bytes = encrypted.bytes;
    remote
        .expect_download_range_to_file()
        .withf(|key, _, _, _| key.contains("000000"))
        .returning(move |_, _, _, path| {
            std::fs::write(path, &bytes)?;
            Ok(())
        });
    remote
        .expect_request_restore()
        .withf(|key, _, _| key.contains("000001"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let progress = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &RetrievalOptions {
            days: 2,
            tier: RestoreTier::Standard,
        },
        &remote,
    )
    .await
    .expect("partial progress must not be an error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 1);
    assert_eq!(progress.parts_total, 3);
}

/// When the pacing budget can't cover even one remaining part, the smallest part must still
/// be requested, exactly once, or the restore would report "in progress" forever without
/// advancing.
#[tokio::test]
async fn restore_budget_smaller_than_any_part_should_force_one_request_instead_of_stalling() {
    let work_dir = tempfile::tempdir().unwrap();
    let mut config = test_config(work_dir.path());
    // A buffer target of about one byte, far under the part below.
    config.s3.restore = RestoreConfig {
        expected_download_kbps: 1,
        lead_days: Some(1.0 / (125.0 * 86400.0)),
        ..RestoreConfig::default()
    };
    let target_dir = work_dir.path().join("restore_target");
    let manifest_yaml = yaml(&manifest(vec![part("000000", 10, "h0")]));

    let mut remote = MockRemote::new();
    remote.expect_restore_status_detail().returning(|key| {
        Ok(status(if key.contains("/manifest.yaml") {
            RestoreStatus::Ready
        } else {
            RestoreStatus::NotRequested
        }))
    });
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_restore()
        .withf(|key, _, _| key.contains("000000"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let progress = restore_task(
        &config,
        &movies_task(),
        0,
        "20260602",
        &target_dir,
        &test_private_key(),
        &RetrievalOptions {
            days: 2,
            tier: RestoreTier::Standard,
        },
        &remote,
    )
    .await
    .expect("must report progress, not stall or error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 0);
    assert_eq!(progress.parts_total, 1);
}
