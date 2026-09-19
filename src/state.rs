use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::util::now_unix;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedPart {
    pub size_bytes: u64,
    pub blake3: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BackupState {
    pub task_name: String,
    pub level: u32,
    pub snapshot_name: String,
    pub parent_snapshot: Option<String>,
    pub timestamp: String,
    pub output_dir: PathBuf,
    pub stream_hash: Option<String>,
    #[serde(default)]
    pub total_parts: Option<u32>,
    pub parts_completed: HashMap<String, UploadedPart>,
    pub manifest_uploaded: bool,
    pub last_updated: i64,
}

impl BackupState {
    pub fn save(&mut self, path: &Path) -> Result<()> {
        self.last_updated = now_unix();
        let tmp = path.with_extension("yaml.tmp");
        std::fs::write(&tmp, serde_yaml::to_string(self)?.as_bytes())?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Persisted download progress of one restore part. Retrieval status is not tracked here:
/// every run re-checks it live against S3.
///
/// `Downloaded` means fully downloaded *and* hash-verified and decrypted, so it cannot be
/// derived from a byte count: a run killed between the last byte and the hash check must
/// resume, not be taken for complete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartDownloadState {
    NotDownloaded,
    PartiallyDownloaded { downloaded_bytes: u64 },
    Downloaded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestorePartState {
    pub size_bytes: u64,
    pub blake3: String,
    pub download_state: PartDownloadState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RestoreState {
    pub task_name: String,
    pub level: u32,
    pub timestamp: String,
    pub snapshot_name: String,
    pub manifest_blake3: String,
    pub parts: BTreeMap<String, RestorePartState>,
    pub stream_verified: bool,
    /// Whether `btrfs receive` has completed and the target subvolume was verified.
    /// Tracked separately from `stream_verified` so a crash between the hash check and
    /// the receive step doesn't cause a resumed run to silently skip re-attempting receive.
    pub receive_completed: bool,
    pub last_updated: i64,
}

impl RestoreState {
    pub fn save(&mut self, path: &Path) -> Result<()> {
        self.last_updated = now_unix();
        let tmp = path.with_extension("yaml.tmp");
        std::fs::write(&tmp, serde_yaml::to_string(self)?.as_bytes())?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

pub fn read_restore_state(path: &Path) -> Result<Option<RestoreState>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_yaml::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn read_backup_state(path: &Path) -> Result<Option<BackupState>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_yaml::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_state_default() {
        let state = BackupState::default();
        assert!(state.task_name.is_empty());
        assert_eq!(state.level, 0);
        assert!(state.stream_hash.is_none());
        assert!(state.parts_completed.is_empty());
        assert!(!state.manifest_uploaded);
    }

    #[test]
    fn test_backup_state_serialize_deserialize() {
        let mut state = BackupState {
            task_name: "home".to_string(),
            level: 0,
            snapshot_name: "bcb_level0.20260602T1430".to_string(),
            parent_snapshot: None,
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            output_dir: PathBuf::from("/var/lib/bcb/task/mnt_data/bcb_level0_20260602"),
            stream_hash: Some("abc123".to_string()),
            total_parts: Some(2),
            parts_completed: HashMap::new(),
            manifest_uploaded: false,
            last_updated: 1748872200,
        };
        state.parts_completed.insert(
            "000000".to_string(),
            UploadedPart {
                size_bytes: 3221225472,
                blake3: "hash1".to_string(),
            },
        );
        state.parts_completed.insert(
            "000001".to_string(),
            UploadedPart {
                size_bytes: 1073741824,
                blake3: "hash2".to_string(),
            },
        );

        let yaml = serde_yaml::to_string(&state).unwrap();
        let deserialized: BackupState = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn test_read_state_not_found() {
        let result = read_backup_state(Path::new("/nonexistent/path/backup_state.yaml"));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_write_and_read_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backup_state.yaml");

        let mut state = BackupState {
            task_name: "test-task".to_string(),
            level: 1,
            snapshot_name: "bcb_level1.20260602T1430".to_string(),
            parent_snapshot: Some("bcb_level0.20260601T1000".to_string()),
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            output_dir: PathBuf::from("/tmp/bcb/task/mnt/bcb_level1_20260602"),
            stream_hash: Some("streamhash".to_string()),
            total_parts: None,
            parts_completed: HashMap::new(),
            manifest_uploaded: true,
            last_updated: 12345,
        };

        state.save(&path).unwrap();
        let loaded = read_backup_state(&path).unwrap().unwrap();
        assert_eq!(state.task_name, loaded.task_name);
        assert_eq!(state.output_dir, loaded.output_dir);
        assert_eq!(state.stream_hash, loaded.stream_hash);
    }
}
