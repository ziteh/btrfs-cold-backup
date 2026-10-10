use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    backup::backup_task,
    remote::ObjectMetadata,
    state::{BackupState, PartRecord},
    util::backup_state_path,
};

use crate::common::*;

/// Writes a stand-in encrypted part into `output_dir` and returns its record. The upload
/// stage never decrypts, so any bytes serve.
fn write_part(output_dir: &Path, index: &str, bytes: &[u8]) -> PartRecord {
    std::fs::write(output_dir.join(format!("snapshot.part-{index}.age")), bytes).unwrap();
    PartRecord {
        size_bytes: bytes.len() as u64,
        blake3: blake3::hash(bytes).to_hex().to_string(),
    }
}

/// State of a level 0 backup of movies whose send already finished.
fn sent_state(output_dir: &Path) -> BackupState {
    BackupState {
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        source_uuid: SOURCE_UUID.to_string(),
        snapshot_uuid: snap_uuid("bcb_level0.20260602T1430"),
        timestamp: "2026-06-02T14:30".to_string(),
        output_dir: output_dir.to_path_buf(),
        stream_hash: Some("streamhash".to_string()),
        ..Default::default()
    }
}

/// Resuming after an interruption must upload only the parts not yet completed.
#[tokio::test]
async fn resume_should_skip_already_uploaded_parts() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    state.parts.insert(
        "000000".to_string(),
        write_part(&output_dir, "000000", b"uploaded part"),
    );
    state.uploaded_parts.insert("000000".to_string());
    state.parts.insert(
        "000001".to_string(),
        write_part(&output_dir, "000001", b"pending part"),
    );
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    let btrfs = MockBtrfs::new();

    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("part-000001"))
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());
    remote
        .expect_exists()
        .withf(|key| key.contains("latest.yaml"))
        .returning(|_| Ok(false));

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("resumed backup should succeed")
        .expect("resume must not be skipped");
    assert_eq!(backup.parts.len(), 2);
    assert_eq!(backup.blake3, "streamhash");
}

/// A crash after a part's upload but before the state records it must leave the part pending,
/// so the resumed run re-uploads just that part.
#[tokio::test]
async fn resume_should_succeed_when_crash_hit_between_upload_and_state_save() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    state.parts.insert(
        "000000".to_string(),
        write_part(&output_dir, "000000", b"only part"),
    );
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    // A directory squatting on the state's tmp path makes the save after the upload fail,
    // standing in for a crash at that point (and works even when tests run as root).
    let tmp_path = PathBuf::from(format!("{}.tmp", sp.display()));
    let squat = tmp_path.clone();
    let btrfs = MockBtrfs::new();
    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .times(1)
        .returning(move |_, _, _, _| {
            std::fs::create_dir(&squat).unwrap();
            Ok(())
        });
    assert!(
        backup_task(&config, &task, 0, &btrfs, &remote)
            .await
            .is_err()
    );

    std::fs::remove_dir(&tmp_path).unwrap();
    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("part-000000"))
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    remote.expect_exists().returning(|_| Ok(false));
    expect_parts_on_s3(&mut remote, work_dir.path());

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("resumed backup should succeed")
        .expect("resume must not be skipped");
    assert_eq!(backup.parts.len(), 1);
}

/// A store can lose a part it acknowledged; the state's upload record is no proof. The
/// manifest must be held back, and that part marked pending so the next run re-uploads only
/// it. `head` answers the HEAD of part 000001 on the first run.
async fn lost_part_should_be_reuploaded_before_manifest(
    head: fn(u64) -> Result<Option<ObjectMetadata>>,
) -> String {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    for (index, bytes) in [("000000", &b"kept part"[..]), ("000001", b"lost part")] {
        state
            .parts
            .insert(index.to_string(), write_part(&output_dir, index, bytes));
        state.uploaded_parts.insert(index.to_string());
    }
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let mut remote = MockRemote::new();
    let lost_size = state.parts["000001"].size_bytes;
    remote
        .expect_head_metadata()
        .withf(|key| key.contains("part-000001"))
        .returning(move |_| head(lost_size));
    expect_parts_on_s3(&mut remote, work_dir.path());
    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect_err("a part missing from S3 must hold the manifest back")
        .to_string();
    let saved: BackupState = serde_yaml::from_slice(&std::fs::read(&sp).unwrap()).unwrap();
    assert!(!saved.uploaded_parts.contains("000001"));
    assert!(saved.uploaded_parts.contains("000000"));

    let mut remote = MockRemote::new();
    remote
        .expect_upload_file()
        .withf(|key, _, _, _| key.contains("part-000001"))
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("/manifest.yaml"))
        .times(1)
        .returning(|_, _, _, _| Ok(()));
    remote
        .expect_upload_bytes()
        .withf(|key, _, _, _| key.contains("latest.yaml"))
        .returning(|_, _, _, _| Ok(()));
    remote.expect_exists().returning(|_| Ok(false));
    expect_parts_on_s3(&mut remote, work_dir.path());
    backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect("the re-run should re-upload the part and finish")
        .expect("must not be skipped");
    err
}

/// A HEAD that fails for a reason other than the part being gone (e.g. the network) says
/// nothing about the part: the manifest must still be held back, but no upload record may be
/// dropped, or an outage during the check would re-upload every part.
#[tokio::test]
async fn transient_head_failure_should_hold_manifest_and_keep_upload_records() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    for (index, bytes) in [("000000", &b"part a"[..]), ("000001", b"part b")] {
        state
            .parts
            .insert(index.to_string(), write_part(&output_dir, index, bytes));
        state.uploaded_parts.insert(index.to_string());
    }
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let mut remote = MockRemote::new();
    remote
        .expect_head_metadata()
        .returning(|_| Err(anyhow::anyhow!("dispatch failure")));

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect_err("an unchecked part must hold the manifest back")
        .to_string();
    assert!(err.contains("dispatch failure"), "unexpected error: {err}");
    let saved: BackupState = serde_yaml::from_slice(&std::fs::read(&sp).unwrap()).unwrap();
    assert_eq!(saved.uploaded_parts, state.uploaded_parts);
}

#[tokio::test]
async fn part_missing_from_s3_should_be_reuploaded_before_manifest() {
    let err = lost_part_should_be_reuploaded_before_manifest(|_| Ok(None)).await;
    assert!(err.contains("part-000001"), "unexpected error: {err}");
}

#[tokio::test]
async fn part_with_wrong_size_on_s3_should_be_reuploaded_before_manifest() {
    let err = lost_part_should_be_reuploaded_before_manifest(|size| {
        Ok(Some(ObjectMetadata {
            size_bytes: size - 1,
            storage_class: "STANDARD".to_string(),
        }))
    })
    .await;
    assert!(err.contains("part-000001"), "unexpected error: {err}");
}

/// An unfinished backup of another level must not be discarded to start this one: its state
/// is the only record of which parts already reached S3.
#[tokio::test]
async fn backup_should_fail_and_keep_state_when_other_level_is_unfinished() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let mut state = BackupState {
        level: 0,
        snapshot_name: "bcb_level0.20260602T1430".to_string(),
        timestamp: "2026-06-02T14:30".to_string(),
        output_dir: work_dir
            .path()
            .join("backup/mnt_data_movies/bcb_level0_202606021430"),
        stream_hash: Some("streamhash".to_string()),
        parts: [(
            "000000".to_string(),
            PartRecord {
                size_bytes: 1,
                blake3: String::new(),
            },
        )]
        .into(),
        ..Default::default()
    };
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();
    let before = std::fs::read(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 1, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("--level 0 --task movies"),
        "unexpected error: {err}"
    );
    assert_eq!(std::fs::read(&sp).unwrap(), before);
}

/// What reaches S3 must be the send stream encrypted to the configured key, with no
/// plaintext part left in base_dir along the way.
#[tokio::test]
async fn uploaded_part_should_decrypt_to_the_send_stream() {
    use std::sync::{Arc, Mutex};

    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let snap = "bcb_level0.20260602T1430";

    let mut btrfs = btrfs_with_uuids();
    btrfs
        .expect_list_snapshots()
        .returning(move |_| Ok(vec![snap.to_string()]));
    btrfs.expect_send_full().returning(|_| Ok(fake_stream()));

    // output_dir is removed once the backup completes, so the bytes and the directory listing
    // must be captured here, during the upload.
    let uploaded = Arc::new(Mutex::new(None::<(Vec<u8>, usize)>));
    let uploaded_clone = uploaded.clone();
    let mut remote = MockRemote::new();
    remote.expect_exists().returning(|_| Ok(false));
    remote
        .expect_upload_file()
        .times(1)
        .returning(move |_, path, _, _| {
            let files = std::fs::read_dir(path.parent().unwrap())?.count();
            *uploaded_clone.lock().unwrap() = Some((std::fs::read(path)?, files));
            Ok(())
        });
    remote.expect_upload_bytes().returning(|_, _, _, _| Ok(()));
    expect_parts_on_s3(&mut remote, work_dir.path());

    let backup = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap()
        .unwrap();

    let (bytes, files) = uploaded.lock().unwrap().clone().unwrap();
    assert_eq!(files, 1, "only the .age part may be in output_dir");
    assert_eq!(
        backup.parts[0].blake3,
        blake3::hash(&bytes).to_hex().to_string()
    );
    assert_eq!(decrypt(&bytes), b"btrfs send data");
}

/// A pending part that no longer matches its recorded hash must not be uploaded. With
/// nothing uploaded yet the state is reset so the next run redoes the send.
#[tokio::test]
async fn resume_should_reset_state_when_part_is_damaged_before_any_upload() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    state.parts.insert(
        "000000".to_string(),
        write_part(&output_dir, "000000", b"original"),
    );
    std::fs::write(output_dir.join("snapshot.part-000000.age"), b"damaged!").unwrap();
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("state cleared"), "unexpected error: {err}");
    let saved: BackupState = serde_yaml::from_slice(&std::fs::read(&sp).unwrap()).unwrap();
    assert!(saved.stream_hash.is_none());
    assert!(saved.parts.is_empty());
}

/// Once some parts are on S3, a missing pending part can't be recovered by redoing the send
/// behind the state's back; the backup must fail and keep the state.
#[tokio::test]
async fn resume_should_fail_when_parts_missing_from_disk() {
    let work_dir = tempfile::tempdir().unwrap();
    let config = test_config(work_dir.path());
    let task = movies_task();
    let sanitized = "mnt_data_movies";

    let output_dir = work_dir
        .path()
        .join("backup")
        .join(sanitized)
        .join("bcb_level0_202606021430");
    std::fs::create_dir_all(&output_dir).unwrap();

    let mut state = sent_state(&output_dir);
    state.parts.insert(
        "000000".to_string(),
        write_part(&output_dir, "000000", b"uploaded"),
    );
    state.uploaded_parts.insert("000000".to_string());
    state.parts.insert(
        "000001".to_string(),
        write_part(&output_dir, "000001", b"lost"),
    );
    std::fs::remove_file(output_dir.join("snapshot.part-000001.age")).unwrap();
    let sp = backup_state_path(work_dir.path(), sanitized);
    std::fs::create_dir_all(sp.parent().unwrap()).unwrap();
    state.save(&sp).unwrap();
    let before = std::fs::read(&sp).unwrap();

    let btrfs = MockBtrfs::new();
    let remote = MockRemote::new();

    let err = backup_task(&config, &task, 0, &btrfs, &remote)
        .await
        .expect_err("incomplete part set must fail")
        .to_string();
    assert!(err.contains("000001"), "unexpected error: {err}");
    assert_eq!(std::fs::read(&sp).unwrap(), before);
}
