use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::util::{now_unix, write_atomic};

/// An encrypted part as written by the send stage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartRecord {
    pub size_bytes: u64,
    pub blake3: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BackupState {
    pub level: u32,
    pub snapshot_name: String,
    pub parent_snapshot: Option<String>,
    pub source_uuid: String,
    pub snapshot_uuid: String,
    pub timestamp: String,
    pub output_dir: PathBuf,
    pub stream_hash: Option<String>,
    /// Every part of the stream, recorded together with `stream_hash`. Defaulted so a
    /// state from before these fields loads and can be recognized as such.
    #[serde(default)]
    pub parts: BTreeMap<String, PartRecord>,
    #[serde(default)]
    pub uploaded_parts: BTreeSet<String>,
    pub manifest_uploaded: bool,
    pub last_updated: i64,
}

impl BackupState {
    pub fn save(&mut self, path: &Path) -> Result<()> {
        self.last_updated = now_unix();
        write_atomic(path, serde_yaml::to_string(self)?.as_bytes())
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
        write_atomic(path, serde_yaml::to_string(self)?.as_bytes())
    }
}

/// Loads a state file, or returns None when there is none yet.
pub fn read_state<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_yaml::from_slice(&bytes).map(Some).with_context(|| {
            format!(
                "failed to parse state file {path:?}; if it is damaged (e.g. empty after a \
                 power loss), delete it to start over"
            )
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("failed to read state file {path:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_state_default() {
        let state = BackupState::default();
        assert_eq!(state.level, 0);
        assert!(state.stream_hash.is_none());
        assert!(state.parts.is_empty());
        assert!(!state.manifest_uploaded);
    }

    #[test]
    fn test_backup_state_serialize_deserialize() {
        let mut state = BackupState {
            level: 0,
            snapshot_name: "bcb_level0.20260602T1430".to_string(),
            parent_snapshot: None,
            source_uuid: "src-uuid".to_string(),
            snapshot_uuid: "snap-uuid".to_string(),
            timestamp: "2026-06-02T14:30".to_string(),
            output_dir: PathBuf::from("/var/lib/bcb/backup/mnt_data/bcb_level0_20260602"),
            stream_hash: Some("abc123".to_string()),
            parts: BTreeMap::new(),
            uploaded_parts: BTreeSet::new(),
            manifest_uploaded: false,
            last_updated: 1748872200,
        };
        state.parts.insert(
            "000000".to_string(),
            PartRecord {
                size_bytes: 3221225472,
                blake3: "hash1".to_string(),
            },
        );
        state.parts.insert(
            "000001".to_string(),
            PartRecord {
                size_bytes: 1073741824,
                blake3: "hash2".to_string(),
            },
        );

        state.uploaded_parts.insert("000000".to_string());

        let yaml = serde_yaml::to_string(&state).unwrap();
        let deserialized: BackupState = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn test_read_state_not_found() {
        let result = read_state::<BackupState>(Path::new("/nonexistent/path/state.yaml"));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_write_and_read_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.yaml");

        let mut state = BackupState {
            level: 1,
            snapshot_name: "bcb_level1.20260602T1430".to_string(),
            parent_snapshot: Some("bcb_level0.20260601T1000".to_string()),
            source_uuid: "src-uuid".to_string(),
            snapshot_uuid: "snap-uuid".to_string(),
            timestamp: "2026-06-02T14:30".to_string(),
            output_dir: PathBuf::from("/tmp/bcb/backup/mnt/bcb_level1_20260602"),
            stream_hash: Some("streamhash".to_string()),
            parts: BTreeMap::new(),
            uploaded_parts: BTreeSet::new(),
            manifest_uploaded: true,
            last_updated: 12345,
        };

        state.save(&path).unwrap();
        let loaded: BackupState = read_state(&path).unwrap().unwrap();
        assert_eq!(state.snapshot_name, loaded.snapshot_name);
        assert_eq!(state.output_dir, loaded.output_dir);
        assert_eq!(state.stream_hash, loaded.stream_hash);
    }

    #[test]
    fn test_read_state_error_names_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_state.yaml");
        std::fs::write(&path, b"").unwrap();

        let err = format!("{:#}", read_state::<RestoreState>(&path).unwrap_err());
        assert!(
            err.contains("restore_state.yaml"),
            "unexpected error: {err}"
        );
    }

    /// State files written before a field was removed must still load.
    #[test]
    fn test_read_state_ignores_removed_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_state.yaml");
        std::fs::write(
            &path,
            "task_name: movies\n\
             level: 0\n\
             timestamp: 2026-06-02T14:30\n\
             snapshot_name: bcb_level0.20260602T1430\n\
             manifest_blake3: abc\n\
             parts: {}\n\
             stream_verified: false\n\
             receive_completed: false\n\
             last_updated: 0\n",
        )
        .unwrap();

        let state: RestoreState = read_state(&path).unwrap().unwrap();
        assert_eq!(state.manifest_blake3, "abc");
    }
}
