//! Local progress of an unfinished backup or restore, saved after every step so a run that
//! is interrupted resumes where it stopped instead of starting over.

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::fsutil::write_atomic;
use crate::timestamp::BackupId;

/// A state of type `T` kept in the file at `path`.
pub struct StateFile<T> {
    path: PathBuf,
    pub data: T,
}

/// The on-disk form of a state. `saved_at` lets an operator see when a run last made
/// progress even after the file was copied elsewhere, or when base_dir is on a network share
/// whose mtime comes from another clock.
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    saved_at: String,
    state: T,
}

impl<T: Serialize + DeserializeOwned> StateFile<T> {
    /// Loads the state at `path`, or None when there is none yet.
    pub fn load(path: PathBuf) -> Result<Option<Self>> {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("failed to read state file {path:?}")),
        };
        let envelope: Envelope<T> = serde_yaml::from_slice(&bytes).with_context(|| {
            format!(
                "failed to parse state file {path:?}; if it is damaged (e.g. empty after a \
                 power loss), delete it to start over"
            )
        })?;
        Ok(Some(Self {
            path,
            data: envelope.state,
        }))
    }

    /// Creates the state at `path`, with its directory, and saves it.
    pub fn create(path: PathBuf, data: T) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("failed to create {dir:?}"))?;
        }
        let state = Self { path, data };
        state.save()?;
        Ok(state)
    }

    pub fn save(&self) -> Result<()> {
        let envelope = Envelope {
            saved_at: humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string(),
            state: &self.data,
        };
        write_atomic(&self.path, serde_yaml::to_string(&envelope)?.as_bytes())
    }

    /// Deletes the file once the run it tracked is complete.
    pub fn remove(self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::warn!("failed to remove state file {:?}: {e}", self.path);
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// An encrypted part as written by the send stage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartRecord {
    pub size_bytes: u64,
    pub blake3: String,
}

/// The snapshot a backup sends and what it was checked against when the backup started.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupTarget {
    pub id: BackupId,
    pub snapshot_name: String,
    /// The snapshot the incremental stream is relative to; None for level 0.
    pub parent_snapshot: Option<String>,
    /// btrfs UUID of the backed-up subvolume.
    pub source_uuid: String,
    /// btrfs UUID of the snapshot.
    pub snapshot_uuid: String,
}

/// What the send stage produced: the stream's BLAKE3 and every encrypted part of it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SentStream {
    pub blake3: String,
    pub parts: BTreeMap<String, PartRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupState {
    pub target: BackupTarget,
    /// None until the send has finished.
    pub sent: Option<SentStream>,
    pub uploaded_parts: BTreeSet<String>,
    pub manifest_uploaded: bool,
}

impl BackupState {
    pub fn new(target: BackupTarget) -> Self {
        Self {
            target,
            sent: None,
            uploaded_parts: BTreeSet::new(),
            manifest_uploaded: false,
        }
    }
}

/// Download progress of one restore part. Retrieval status is not kept here: every run
/// checks it live, since a retrieved copy may have expired since the last run.
///
/// `Downloaded` means fully downloaded *and* hash-verified, so it cannot be derived from a
/// byte count: a run killed between the last byte and the hash check must resume, not be
/// taken for complete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartDownloadState {
    NotDownloaded,
    PartiallyDownloaded { downloaded_bytes: u64 },
    Downloaded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreState {
    /// The stream BLAKE3 of the manifest this state belongs to.
    pub manifest_blake3: String,
    /// Keyed by part index.
    pub parts: BTreeMap<String, PartDownloadState>,
    /// Whether `btrfs receive` has completed and the received snapshot was verified.
    pub receive_completed: bool,
}

impl RestoreState {
    /// Indexes of the parts not yet downloaded and verified.
    pub fn pending_parts(&self) -> impl Iterator<Item = &String> {
        self.parts
            .iter()
            .filter(|(_, state)| **state != PartDownloadState::Downloaded)
            .map(|(index, _)| index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backup_state() -> BackupState {
        let mut state = BackupState::new(BackupTarget {
            id: BackupId {
                level: 1,
                timestamp: "2026-06-02T14:30".parse().unwrap(),
            },
            snapshot_name: "bcb_level1.20260602T1430".to_string(),
            parent_snapshot: Some("bcb_level0.20260601T1000".to_string()),
            source_uuid: "src-uuid".to_string(),
            snapshot_uuid: "snap-uuid".to_string(),
        });
        state.sent = Some(SentStream {
            blake3: "streamhash".to_string(),
            parts: [(
                "000000".to_string(),
                PartRecord {
                    size_bytes: 3,
                    blake3: "hash".to_string(),
                },
            )]
            .into(),
        });
        state.uploaded_parts.insert("000000".to_string());
        state
    }

    #[test]
    fn backup_and_restore_state_should_round_trip_through_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/state.yaml");

        StateFile::create(path.clone(), backup_state()).unwrap();
        let loaded = StateFile::<BackupState>::load(path.clone())
            .unwrap()
            .unwrap();
        assert_eq!(loaded.data, backup_state());

        let restore = RestoreState {
            manifest_blake3: "abc".to_string(),
            parts: [(
                "000000".to_string(),
                PartDownloadState::PartiallyDownloaded {
                    downloaded_bytes: 7,
                },
            )]
            .into(),
            receive_completed: false,
        };
        let restore_path = dir.path().join("restore.yaml");
        StateFile::create(restore_path.clone(), restore.clone()).unwrap();
        let loaded = StateFile::<RestoreState>::load(restore_path)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.data, restore);
    }

    #[test]
    fn saved_file_should_record_when_it_was_saved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.yaml");
        StateFile::create(path.clone(), backup_state()).unwrap();
        let yaml = std::fs::read_to_string(&path).unwrap();
        assert!(yaml.starts_with("saved_at: "), "{yaml}");
    }

    #[test]
    fn load_of_missing_file_should_return_none() {
        let loaded =
            StateFile::<BackupState>::load(PathBuf::from("/nonexistent/path/state.yaml")).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn load_error_should_name_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore_state.yaml");
        std::fs::write(&path, b"").unwrap();

        let err = format!("{:#}", StateFile::<RestoreState>::load(path).err().unwrap());
        assert!(
            err.contains("restore_state.yaml"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pending_parts_should_exclude_downloaded_ones() {
        let state = RestoreState {
            manifest_blake3: String::new(),
            parts: [
                ("000000".to_string(), PartDownloadState::Downloaded),
                ("000001".to_string(), PartDownloadState::NotDownloaded),
            ]
            .into(),
            receive_completed: false,
        };
        assert_eq!(state.pending_parts().collect::<Vec<_>>(), ["000001"]);
    }
}
