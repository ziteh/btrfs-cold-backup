use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartInfo {
    pub part_number: u32,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Backup {
    pub timestamp: String, // ISO 8601
    pub level: u32,
    pub snapshot_name: String,
    pub parent_snapshot: Option<String>,
    pub total_size_bytes: u64,
    pub sha256: String,
    pub parts: Vec<PartInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Last {
    pub timestamp: String, // ISO 8601
    pub level: u32,
    pub snapshot_name: String,
}

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
    pub output_dir: String,
    pub stream_hash: Option<String>,
    #[serde(default)]
    pub total_parts: Option<u32>,
    pub parts_completed: HashMap<u32, CompletedPart>,
    pub manifest_uploaded: bool,
    pub last_updated: i64,
}

pub fn read_state(path: &Path) -> Result<Option<BackupState>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_yaml::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn write_state(path: &Path, state: &BackupState) -> Result<()> {
    std::fs::write(path, serde_yaml::to_string(state)?.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_part_info_serialize_deserialize() {
        let part = PartInfo {
            part_number: 1,
            size_bytes: 1024,
            sha256: "abc123".to_string(),
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
            snapshot_name: "bcb_level0_2026-06-02_14-30".to_string(),
            parent_snapshot: None,
            total_size_bytes: 5120,
            sha256: "def456".to_string(),
            parts: vec![
                PartInfo {
                    part_number: 1,
                    size_bytes: 1024,
                    sha256: "part1".to_string(),
                },
                PartInfo {
                    part_number: 2,
                    size_bytes: 1024,
                    sha256: "part2".to_string(),
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
            snapshot_name: "bcb_level1_2026-06-02_15-30".to_string(),
            parent_snapshot: Some("bcb_level0_2026-06-02_14-30".to_string()),
            total_size_bytes: 2048,
            sha256: "ghi789".to_string(),
            parts: vec![PartInfo {
                part_number: 1,
                size_bytes: 2048,
                sha256: "incr_part1".to_string(),
            }],
        };
        let yaml = serde_yaml::to_string(&backup).unwrap();
        let deserialized: Backup = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(backup, deserialized);
        assert_eq!(
            deserialized.parent_snapshot,
            Some("bcb_level0_2026-06-02_14-30".to_string())
        );
    }

    #[test]
    fn test_last_serialize_deserialize() {
        let last = Last {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcb_level0_2026-06-02_14-30".to_string(),
        };
        let yaml = serde_yaml::to_string(&last).unwrap();
        let deserialized: Last = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(last, deserialized);
    }

    #[test]
    fn test_backup_null_parent_roundtrip() {
        let backup = Backup {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcb_level0_2026-06-02_14-30".to_string(),
            parent_snapshot: None,
            total_size_bytes: 5120,
            sha256: "def456".to_string(),
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
            snapshot_name: "bcb_level0_2026-06-02_14-30".to_string(),
            parent_snapshot: None,
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            output_dir: "/var/lib/bcb/task/mnt_data/bcb_level0_20260602".to_string(),
            stream_hash: Some("abc123".to_string()),
            total_parts: Some(2),
            parts_completed: HashMap::new(),
            manifest_uploaded: false,
            last_updated: 1748872200,
        };
        state.parts_completed.insert(
            1,
            CompletedPart {
                size_bytes: 3221225472,
                hash: "hash1".to_string(),
            },
        );
        state.parts_completed.insert(
            2,
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

        let state = BackupState {
            task_name: "test-task".to_string(),
            level: 1,
            snapshot_name: "bcb_level1_2026-06-02_14-30".to_string(),
            parent_snapshot: Some("bcb_level0_2026-06-01_10-00".to_string()),
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            output_dir: "/tmp/bcb/task/mnt/bcb_level1_20260602".to_string(),
            stream_hash: Some("streamhash".to_string()),
            total_parts: None,
            parts_completed: HashMap::new(),
            manifest_uploaded: true,
            last_updated: 12345,
        };

        write_state(&path, &state).unwrap();
        let loaded = read_state(&path).unwrap().unwrap();
        assert_eq!(state, loaded);
    }
}
