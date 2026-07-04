use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::util::now_unix;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartInfo {
    pub index: String,
    pub size_bytes: u64,
    pub blake3: String,
}

/// Provenance recorded in each manifest, so a backup found years later in a bucket is
/// self-describing enough to plan its own restore.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SystemInfo {
    pub os: String,
    pub btrfs_version: String,
    pub bcb_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Backup {
    pub timestamp: String,
    /// Unix time when the backup ran (`timestamp` is the snapshot's, not the run's).
    pub created_at: i64,
    pub level: u32,
    pub snapshot_name: String,
    pub parent_snapshot: Option<String>,
    pub system: SystemInfo,
    pub age_public_key: String,
    pub total_size_bytes: u64,
    pub blake3: String,
    pub parts: Vec<PartInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LastEntry {
    pub timestamp: String,
    pub snapshot_name: String,
}

pub type Last = HashMap<u32, LastEntry>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletedPart {
    pub size_bytes: u64,
    pub hash: String,
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
    pub parts_completed: HashMap<String, CompletedPart>,
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

pub fn read_state(path: &Path) -> Result<Option<BackupState>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_yaml::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_part_info_serialize_deserialize() {
        let part = PartInfo {
            index: "000000".to_string(),
            size_bytes: 1024,
            blake3: "abc123".to_string(),
        };
        let yaml = serde_yaml::to_string(&part).unwrap();
        let deserialized: PartInfo = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(part, deserialized);
    }

    #[test]
    fn test_backup_serialize_deserialize() {
        let backup = Backup {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcb_level0.20260602T1430".to_string(),
            parent_snapshot: None,
            created_at: 0,
            system: SystemInfo::default(),
            age_public_key: String::new(),
            total_size_bytes: 5120,
            blake3: "def456".to_string(),
            parts: vec![
                PartInfo {
                    index: "000000".to_string(),
                    size_bytes: 1024,
                    blake3: "part1".to_string(),
                },
                PartInfo {
                    index: "000001".to_string(),
                    size_bytes: 1024,
                    blake3: "part2".to_string(),
                },
            ],
        };
        let yaml = serde_yaml::to_string(&backup).unwrap();
        let deserialized: Backup = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(backup, deserialized);
    }

    #[test]
    fn test_backup_with_parent() {
        let backup = Backup {
            timestamp: "2026-06-02T15:30:00Z".to_string(),
            level: 1,
            snapshot_name: "bcb_level1.20260602T1530".to_string(),
            parent_snapshot: Some("bcb_level0.20260602T1430".to_string()),
            created_at: 0,
            system: SystemInfo::default(),
            age_public_key: String::new(),
            total_size_bytes: 2048,
            blake3: "ghi789".to_string(),
            parts: vec![PartInfo {
                index: "000000".to_string(),
                size_bytes: 2048,
                blake3: "incr_part1".to_string(),
            }],
        };
        let yaml = serde_yaml::to_string(&backup).unwrap();
        let deserialized: Backup = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(backup, deserialized);
        assert_eq!(
            deserialized.parent_snapshot,
            Some("bcb_level0.20260602T1430".to_string())
        );
    }

    #[test]
    fn test_last_serialize_deserialize() {
        let mut last: Last = HashMap::new();
        last.insert(
            0,
            LastEntry {
                timestamp: "2026-06-02T14:30:00Z".to_string(),
                snapshot_name: "bcb_level0.20260602T1430".to_string(),
            },
        );
        last.insert(
            1,
            LastEntry {
                timestamp: "2026-06-02T15:30:00Z".to_string(),
                snapshot_name: "bcb_level1.20260602T1530".to_string(),
            },
        );
        let yaml = serde_yaml::to_string(&last).unwrap();
        let deserialized: Last = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(last, deserialized);
    }

    #[test]
    fn test_backup_null_parent_roundtrip() {
        let backup = Backup {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcb_level0.20260602T1430".to_string(),
            parent_snapshot: None,
            created_at: 0,
            system: SystemInfo::default(),
            age_public_key: String::new(),
            total_size_bytes: 5120,
            blake3: "def456".to_string(),
            parts: vec![],
        };
        let yaml = serde_yaml::to_string(&backup).unwrap();
        let deserialized: Backup = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(backup.parent_snapshot, deserialized.parent_snapshot);
        assert!(deserialized.parent_snapshot.is_none());
    }

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
            CompletedPart {
                size_bytes: 3221225472,
                hash: "hash1".to_string(),
            },
        );
        state.parts_completed.insert(
            "000001".to_string(),
            CompletedPart {
                size_bytes: 1073741824,
                hash: "hash2".to_string(),
            },
        );

        let yaml = serde_yaml::to_string(&state).unwrap();
        let deserialized: BackupState = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn test_read_state_not_found() {
        let result = read_state(Path::new("/nonexistent/path/backup_state.yaml"));
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
        let loaded = read_state(&path).unwrap().unwrap();
        assert_eq!(state.task_name, loaded.task_name);
        assert_eq!(state.output_dir, loaded.output_dir);
        assert_eq!(state.stream_hash, loaded.stream_hash);
    }
}
