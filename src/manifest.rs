//! Backup manifest structures for serialization and state tracking

use serde::{Deserialize, Serialize};

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
pub struct State {
    pub upload_status: UploadStatus,
    pub last_successful_part: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UploadStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Last {
    pub timestamp: String, // ISO 8601
    pub level: u32,
    pub snapshot_name: String,
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
            snapshot_name: "bcr_level0_2026-06-02_14-30".to_string(),
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
            snapshot_name: "bcr_level1_2026-06-02_15-30".to_string(),
            parent_snapshot: Some("bcr_level0_2026-06-02_14-30".to_string()),
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
            Some("bcr_level0_2026-06-02_14-30".to_string())
        );
    }

    #[test]
    fn test_state_serialize_deserialize() {
        let state = State {
            upload_status: UploadStatus::InProgress,
            last_successful_part: Some(5),
        };
        let yaml = serde_yaml::to_string(&state).unwrap();
        let deserialized: State = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(state, deserialized);
    }

    #[test]
    fn test_state_no_successful_part() {
        let state = State {
            upload_status: UploadStatus::Pending,
            last_successful_part: None,
        };
        let yaml = serde_yaml::to_string(&state).unwrap();
        let deserialized: State = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(state, deserialized);
        assert_eq!(deserialized.last_successful_part, None);
    }

    #[test]
    fn test_last_serialize_deserialize() {
        let last = Last {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcr_level0_2026-06-02_14-30".to_string(),
        };
        let yaml = serde_yaml::to_string(&last).unwrap();
        let deserialized: Last = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(last, deserialized);
    }

    #[test]
    fn test_upload_status_variants() {
        let statuses = vec![
            UploadStatus::Pending,
            UploadStatus::InProgress,
            UploadStatus::Completed,
            UploadStatus::Failed,
        ];

        for status in statuses {
            let yaml = serde_yaml::to_string(&status).unwrap();
            let deserialized: UploadStatus = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(status, deserialized);
        }
    }

    #[test]
    fn test_backup_null_parent_roundtrip() {
        let backup = Backup {
            timestamp: "2026-06-02T14:30:00Z".to_string(),
            level: 0,
            snapshot_name: "bcr_level0_2026-06-02_14-30".to_string(),
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
}
