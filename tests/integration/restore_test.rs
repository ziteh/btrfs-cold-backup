use std::io::Read;
use std::sync::{Arc, Mutex};

use btrfs_cold_backup::{
    btrfs::SubvolumeUuids,
    config::RestoreConfig,
    remote::{Retrieval, RetrievalStatus, RetrievalTier},
    restore::{RestoreRequest, RetrievalOptions},
    state::{PartDownloadState, RestoreState, StateFile},
};

use crate::common::*;

/// Retrieval options with copies that last long enough for the paced tests below.
fn two_day_retrieval() -> RetrievalOptions {
    RetrievalOptions {
        days: 2,
        tier: RetrievalTier::Standard,
    }
}

/// Answers the manifest as ready and every part as archived with no retrieval requested.
fn manifest_ready_parts_archived(key: &str) -> anyhow::Result<Retrieval> {
    Ok(status(if key.contains("/manifest.yaml") {
        RetrievalStatus::Ready
    } else {
        RetrievalStatus::NotRequested
    }))
}

fn seed_restore_state(work_dir: &std::path::Path, state: RestoreState) {
    std::fs::create_dir_all(work_dir).unwrap();
    StateFile::create(work_dir.join("state.yaml"), state).unwrap();
}

/// A whole restore in one run: every part downloaded, verified, decrypted into btrfs receive,
/// and the work dir removed.
#[tokio::test]
async fn restore_should_receive_the_decrypted_stream_and_clean_up() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let first = encrypted_part("000000", b"first half, ");
    let second = encrypted_part("000001", b"second half");
    let mut backup = manifest(vec![first.part.clone(), second.part.clone()]);
    backup.blake3 = blake3::hash(b"first half, second half")
        .to_hex()
        .to_string();
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(move |key, _, _, path| {
            let bytes = if key.contains("000000") {
                &first.bytes
            } else {
                &second.bytes
            };
            std::fs::write(path, bytes)?;
            Ok(())
        });

    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let mut btrfs = MockBtrfs::new();
    btrfs.expect_receive().returning(move |_, mut stream| {
        stream.read_to_end(&mut received_clone.lock().unwrap())?;
        Ok(())
    });
    btrfs
        .expect_subvolume_uuids()
        .withf(|path| path.ends_with("restore_target/bcb_level0.20260602T1430"))
        .returning(|_| {
            Ok(SubvolumeUuids {
                uuid: "received".to_string(),
                parent_uuid: None,
            })
        });

    let progress = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs,
        &remote,
    )
    .await
    .expect("restore should complete");

    assert!(progress.completed);
    assert_eq!(progress.parts_downloaded, 2);
    assert_eq!(*received.lock().unwrap(), b"first half, second half");
    assert!(
        !restore_work_dir(work_dir.path()).exists(),
        "the work dir must be removed once the restore completes"
    );
}

/// A part whose download doesn't match its manifest hash must fail the run, and the bad
/// download is discarded so the next run starts the part over.
#[tokio::test]
async fn restore_should_fail_on_blake3_mismatch() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let manifest_yaml = yaml(&manifest(vec![part("000000", 10, "wrong_hash")]));

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_download_range_to_file()
        .returning(|_, _, _, path| {
            std::fs::write(path, b"10 bytes!!")?;
            Ok(())
        });

    let result = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
        &remote,
    )
    .await;
    let err = format!("{:#}", result.unwrap_err());
    assert!(
        err.contains("BLAKE3 mismatch for part 000000"),
        "unexpected error: {err}"
    );
    let state = saved_restore_state(&restore_work_dir(work_dir.path()));
    assert_eq!(state.parts["000000"], PartDownloadState::NotDownloaded);
}

/// An archived manifest must trigger a retrieval request and a retry-later error, not a
/// download.
#[tokio::test]
async fn restore_should_request_retrieval_for_archived_objects() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .returning(|_| Ok(status(RetrievalStatus::NotRequested)));
    remote
        .expect_request_retrieval()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let err = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
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

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part]);
    backup.blake3 = "not_the_stream_hash".to_string();
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
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

    let err = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &MockBtrfs::new(),
        &remote,
    )
    .await
    .expect_err("stream mismatch must fail before receive")
    .to_string();
    assert!(
        err.contains("stream BLAKE3 mismatch"),
        "unexpected error: {err}"
    );
}

/// With `expected_download_kbps` left at its default (no pacing), every remaining archived
/// part must be requested at once.
#[tokio::test]
async fn default_config_should_still_request_retrieval_for_all_archived_parts() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let manifest_yaml = yaml(&manifest(vec![
        part("000000", 10, "h0"),
        part("000001", 10, "h1"),
    ]));

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .returning(manifest_ready_parts_archived);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_retrieval()
        .times(2)
        .returning(|_, _, _| Ok(()));

    let progress = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
        &remote,
    )
    .await
    .expect("archived-but-not-yet-ready parts must report progress, not error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 0);
    assert_eq!(progress.parts_total, 2);
    assert_eq!(progress.bytes_remaining, 20);
}

/// A manifest whose retrieval is still in progress must bail out with a "re-run later" error
/// rather than attempt a GetObject, which S3 rejects for an archived object.
#[tokio::test]
async fn restore_should_bail_cleanly_when_manifest_retrieval_in_progress() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .returning(|_| Ok(status(RetrievalStatus::InProgress)));

    let err = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
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

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part]);
    backup.blake3 = encrypted.stream_blake3;
    let manifest_yaml = yaml(&backup);

    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
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

    let mut btrfs = MockBtrfs::new();
    btrfs
        .expect_receive()
        .times(2)
        .returning(|_, _| Err(anyhow::anyhow!("btrfs receive failed")));
    let request = restore_request(work_dir.path(), 0);
    for run in ["first", "resumed"] {
        let err = restore(&config, &request, &btrfs, &remote)
            .await
            .expect_err("the receive fails on every run");
        assert!(
            err.to_string().contains("btrfs receive failed"),
            "{run} run: unexpected error: {err}"
        );
    }
}

/// A part left half-downloaded by an interrupted run must be resumed from its saved offset.
#[tokio::test]
async fn restore_should_resume_partially_downloaded_part() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let encrypted = encrypted_part("000000", b"stream payload");
    let mut backup = manifest(vec![encrypted.part.clone()]);
    backup.blake3 = encrypted.stream_blake3.clone();
    let manifest_yaml = yaml(&backup);

    // The work dir as an interrupted run left it: the state says half the part is
    // downloaded, and the file holds that half.
    let half = encrypted.bytes.len() as u64 / 2;
    let restore_dir = restore_work_dir(work_dir.path());
    seed_restore_state(
        &restore_dir,
        RestoreState {
            manifest_blake3: encrypted.stream_blake3,
            parts: [(
                "000000".to_string(),
                PartDownloadState::PartiallyDownloaded {
                    downloaded_bytes: half,
                },
            )]
            .into(),
            receive_completed: false,
        },
    );
    std::fs::write(
        restore_dir.join("snapshot.part-000000.age"),
        &encrypted.bytes[..half as usize],
    )
    .unwrap();

    let remaining: Vec<u8> = encrypted.bytes[half as usize..].to_vec();
    let mut remote = MockRemote::new();
    remote.expect_retrieval_status().returning(ready);
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

    // The receive fails, so the work dir is kept for inspection.
    restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &btrfs_without_receive(),
        &remote,
    )
    .await
    .expect_err("the receive fails");

    assert_eq!(
        saved_restore_state(&restore_dir).parts["000000"],
        PartDownloadState::Downloaded,
        "resumed part must reach Downloaded"
    );
    assert!(
        restore_dir.join("snapshot.part-000000.age").exists(),
        "the verified part must be kept for the receive to be retried"
    );
}

/// A part damaged on disk after its download check (power loss, bit rot) must be caught
/// before receive and cost only that part's download, reported as in progress.
#[tokio::test]
async fn part_damaged_before_finalize_should_be_reset_for_download() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());

    let restore_dir = restore_work_dir(work_dir.path());
    std::fs::create_dir_all(&restore_dir).unwrap();
    let mut parts = Vec::new();
    for (index, bytes) in [("000000", &b"intact part"[..]), ("000001", b"damaged part")] {
        std::fs::write(
            restore_dir.join(format!("snapshot.part-{index}.age")),
            bytes,
        )
        .unwrap();
        parts.push(part(
            index,
            bytes.len() as u64,
            &blake3::hash(bytes).to_hex(),
        ));
    }
    std::fs::write(
        restore_dir.join("snapshot.part-000001.age"),
        b"damaged pa!t",
    )
    .unwrap();

    let backup = manifest(parts.clone());
    seed_restore_state(
        &restore_dir,
        RestoreState {
            manifest_blake3: backup.blake3.clone(),
            parts: parts
                .iter()
                .map(|p| (p.index.clone(), PartDownloadState::Downloaded))
                .collect(),
            receive_completed: false,
        },
    );
    let manifest_yaml = yaml(&backup);

    // Nothing is pending, so only the manifest's status may be checked.
    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(ready);
    remote
        .expect_download()
        .returning(move |_| Ok(manifest_yaml.clone()));

    let progress = restore(
        &config,
        &restore_request(work_dir.path(), 0),
        &MockBtrfs::new(),
        &remote,
    )
    .await
    .expect("a damaged part must be reported as in progress, not as a failure");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 1);
    let state = saved_restore_state(&restore_dir);
    assert_eq!(state.parts["000001"], PartDownloadState::NotDownloaded);
    assert_eq!(state.parts["000000"], PartDownloadState::Downloaded);
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
    let mut backup = manifest(vec![part("000000", 100, "parthash")]);
    backup.blake3 = "irrelevant".to_string();
    let manifest_yaml = yaml(&backup);

    // Half the part was downloaded by an earlier run; its retrieved copy has since expired.
    let restore_dir = restore_work_dir(work_dir.path());
    seed_restore_state(
        &restore_dir,
        RestoreState {
            manifest_blake3: "irrelevant".to_string(),
            parts: [(
                "000000".to_string(),
                PartDownloadState::PartiallyDownloaded {
                    downloaded_bytes: 50,
                },
            )]
            .into(),
            receive_completed: false,
        },
    );

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .returning(manifest_ready_parts_archived);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_retrieval()
        .withf(|key, _, _| key.contains("000000"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let request = RestoreRequest {
        retrieval: two_day_retrieval(),
        ..restore_request(work_dir.path(), 0)
    };
    let progress = restore(&config, &request, &btrfs_without_receive(), &remote)
        .await
        .expect("must report progress, not error");
    assert!(!progress.completed);
    assert_eq!(
        saved_restore_state(&restore_dir).parts["000000"],
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
    remote.expect_retrieval_status().returning(|key| {
        Ok(status(
            if key.contains("/manifest.yaml") || key.contains("000000") {
                RetrievalStatus::Ready
            } else {
                RetrievalStatus::NotRequested
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
        .expect_request_retrieval()
        .withf(|key, _, _| key.contains("000001"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let request = RestoreRequest {
        retrieval: two_day_retrieval(),
        ..restore_request(work_dir.path(), 0)
    };
    let progress = restore(&config, &request, &btrfs_without_receive(), &remote)
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
    let manifest_yaml = yaml(&manifest(vec![part("000000", 10, "h0")]));

    let mut remote = MockRemote::new();
    remote
        .expect_retrieval_status()
        .returning(manifest_ready_parts_archived);
    remote
        .expect_download()
        .withf(|key| key.contains("/manifest.yaml"))
        .returning(move |_| Ok(manifest_yaml.clone()));
    remote
        .expect_request_retrieval()
        .withf(|key, _, _| key.contains("000000"))
        .times(1)
        .returning(|_, _, _| Ok(()));

    let request = RestoreRequest {
        retrieval: two_day_retrieval(),
        ..restore_request(work_dir.path(), 0)
    };
    let progress = restore(&config, &request, &btrfs_without_receive(), &remote)
        .await
        .expect("must report progress, not stall or error");

    assert!(!progress.completed);
    assert_eq!(progress.parts_downloaded, 0);
    assert_eq!(progress.parts_total, 1);
}
