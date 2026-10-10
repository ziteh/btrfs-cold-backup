//! Fixtures shared by the backup and restore tests.
//!
//! The mocks panic on any call without a matching expectation, so a test that sets no
//! expectation for a method asserts that the method is never called.

use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use btrfs_cold_backup::{
    BtrfsOps, Config, ObjectMetadata, RemoteBackend, RestoreConfig, RestoreStatus,
    RestoreStatusDetail, RestoreTier, RetrievalOptions, S3Config, S3RetryConfig, S3StorageClass,
    SubvolumeUuids, Task,
    manifest::{Backup, FORMAT_VERSION, LatestBackup, LatestManifest, PartInfo, SystemInfo},
};
use mockall::mock;
use tokio::io::AsyncRead;

mock! {
    pub Btrfs {}
    #[allow(async_fn_in_trait)]
    impl BtrfsOps for Btrfs {
        async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>>;
        async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
        async fn send_incremental(&self, parent: &Path, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
        async fn subvolume_uuids(&self, path: &Path) -> Result<SubvolumeUuids>;
    }
}

mock! {
    pub Remote {}
    #[allow(async_fn_in_trait)]
    impl RemoteBackend for Remote {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>, storage_class: &str, tags: &str) -> Result<()>;
        async fn upload_file(&self, key: &str, path: &Path, storage_class: &str, tags: &str) -> Result<()>;
        async fn exists(&self, key: &str) -> Result<bool>;
        async fn download(&self, key: &str) -> Result<Vec<u8>>;
        async fn download_range_to_file(&self, key: &str, start: u64, end: u64, path: &Path) -> Result<()>;
        async fn restore_status_detail(&self, key: &str) -> Result<RestoreStatusDetail>;
        async fn request_restore(&self, key: &str, days: i32, tier: RestoreTier) -> Result<()>;
        async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>>;
        async fn verify_credentials(&self) -> Result<()>;
        async fn list_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    }
}

/// The key restores in these tests decrypt with; manifests record its public half.
pub static TEST_IDENTITY: std::sync::LazyLock<age::x25519::Identity> =
    std::sync::LazyLock::new(age::x25519::Identity::generate);

pub fn test_private_key() -> String {
    use secrecy::ExposeSecret;
    TEST_IDENTITY.to_string().expose_secret().to_string()
}

pub fn test_public_key() -> String {
    TEST_IDENTITY.to_public().to_string()
}

pub fn retrieval() -> RetrievalOptions {
    RetrievalOptions {
        days: 1,
        tier: RestoreTier::Standard,
    }
}

pub fn test_config(base_dir: &Path) -> Config {
    Config {
        base_dir: base_dir.to_path_buf(),
        age_public_key: test_public_key(),
        s3: S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            prefix: "backups/".to_string(),
            endpoint: String::new(),
            storage_class: S3StorageClass {
                manifest: "STANDARD".to_string(),
                backup_data: vec!["STANDARD".to_string(), "STANDARD".to_string()],
            },
            retry: S3RetryConfig::default(),
            upload_speed_limit_kbps: 0,
            restore: RestoreConfig::default(),
        },
        tasks: vec![],
    }
}

pub fn movies_task() -> Task {
    Task {
        name: "movies".to_string(),
        description: String::new(),
        subvolume: PathBuf::from("/mnt/data/movies"),
        snapshot_dir: PathBuf::from("/mnt/.snapshots/movies"),
        enabled: true,
    }
}

pub fn fake_stream() -> Box<dyn AsyncRead + Send + Unpin> {
    Box::new(Cursor::new(b"btrfs send data".to_vec()))
}

pub const SOURCE_UUID: &str = "movies-subvol-uuid";

pub fn snap_uuid(snapshot_name: &str) -> String {
    format!("uuid-{snapshot_name}")
}

/// movies_task's subvolume has SOURCE_UUID; any other path is a snapshot of it whose UUID is
/// snap_uuid(file name).
pub fn btrfs_with_uuids() -> MockBtrfs {
    let mut btrfs = MockBtrfs::new();
    btrfs.expect_subvolume_uuids().returning(|path| {
        Ok(if path == movies_task().subvolume {
            SubvolumeUuids {
                uuid: SOURCE_UUID.to_string(),
                parent_uuid: None,
            }
        } else {
            SubvolumeUuids {
                uuid: snap_uuid(path.file_name().unwrap().to_str().unwrap()),
                parent_uuid: Some(SOURCE_UUID.to_string()),
            }
        })
    });
    btrfs
}

pub fn yaml<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_yaml::to_string(value).unwrap().into_bytes()
}

pub fn part(index: &str, size_bytes: u64, blake3: &str) -> PartInfo {
    PartInfo {
        index: index.to_string(),
        size_bytes,
        blake3: blake3.to_string(),
    }
}

/// Manifest of a level 0 backup of `bcb_level0.20260602T1430` of movies, encrypted to
/// TEST_IDENTITY. Tests override the fields they exercise.
pub fn manifest(parts: Vec<PartInfo>) -> Backup {
    let snapshot_name = "bcb_level0.20260602T1430";
    Backup {
        format_version: FORMAT_VERSION.to_string(),
        timestamp: "2026-06-02T14:30".to_string(),
        created_at: 0,
        level: 0,
        snapshot_name: snapshot_name.to_string(),
        parent_snapshot: None,
        source_uuid: SOURCE_UUID.to_string(),
        snapshot_uuid: snap_uuid(snapshot_name),
        system: SystemInfo::default(),
        age_public_key: test_public_key(),
        total_size_bytes: parts.iter().map(|p| p.size_bytes).sum(),
        blake3: "streamhash".to_string(),
        parts,
    }
}

/// Latest manifest of movies holding one entry, for `snapshot_name` at `level`.
pub fn latest_manifest_yaml(snapshot_name: &str, level: u32) -> Vec<u8> {
    let mut latest = LatestManifest::new(SOURCE_UUID.to_string());
    latest.levels.insert(
        level,
        LatestBackup {
            timestamp: "2026-06-01T14:30".to_string(),
            snapshot_name: snapshot_name.to_string(),
            snapshot_uuid: snap_uuid(snapshot_name),
        },
    );
    yaml(&latest)
}

/// `payload` encrypted to TEST_IDENTITY, as one part of a send stream that holds only it.
pub struct EncryptedPart {
    pub bytes: Vec<u8>,
    pub part: PartInfo,
    pub stream_blake3: String,
}

pub fn encrypted_part(index: &str, payload: &[u8]) -> EncryptedPart {
    let recipient = TEST_IDENTITY.to_public();
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .unwrap();
    let mut bytes = Vec::new();
    let mut writer = encryptor.wrap_output(&mut bytes).unwrap();
    writer.write_all(payload).unwrap();
    writer.finish().unwrap();
    let blake3 = blake3::hash(&bytes).to_hex().to_string();
    EncryptedPart {
        part: part(index, bytes.len() as u64, &blake3),
        bytes,
        stream_blake3: blake3::hash(payload).to_hex().to_string(),
    }
}

pub fn decrypt(bytes: &[u8]) -> Vec<u8> {
    let decryptor = age::Decryptor::new(bytes).unwrap();
    let mut reader = decryptor
        .decrypt(std::iter::once(&*TEST_IDENTITY as &dyn age::Identity))
        .unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).unwrap();
    out
}

pub fn status(status: RestoreStatus) -> RestoreStatusDetail {
    RestoreStatusDetail {
        status,
        expires_at: None,
    }
}

pub fn ready(_: &str) -> Result<RestoreStatusDetail> {
    Ok(status(RestoreStatus::Ready))
}

/// Answers HEADs of this backup's parts as S3 would after good uploads: with the size of the
/// `.age` that is still in `base_dir`. mockall tries expectations in the order they were set,
/// so set this before any catch-all `head_metadata` expectation.
pub fn expect_parts_on_s3(remote: &mut MockRemote, base_dir: &Path) {
    let local = {
        let base_dir = base_dir.to_path_buf();
        move |key: &str| {
            key.strip_prefix("backups/backup-data/")
                .map(|rest| base_dir.join("backup").join(rest))
        }
    };
    let local_clone = local.clone();
    remote
        .expect_head_metadata()
        .withf(move |key| local_clone(key).is_some_and(|p| p.exists()))
        .returning(move |key| {
            Ok(Some(ObjectMetadata {
                size_bytes: std::fs::metadata(local(key).unwrap())?.len(),
                storage_class: "STANDARD".to_string(),
            }))
        });
}

/// The restore work dir of movies' level 0 backup restored with `--date 20260602`.
pub fn restore_work_dir(base_dir: &Path) -> PathBuf {
    base_dir.join("restore/mnt_data_movies/bcb_level0_202606020000")
}
