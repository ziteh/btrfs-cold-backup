use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
pub struct LatestBackup {
    pub timestamp: String,
    pub snapshot_name: String,
}

/// The newest backup of each level, keyed by level. Callers rely on the key being the level:
/// completing a level-N backup drops every entry above N, whose incremental chain it voided.
pub type LatestByLevel = HashMap<u32, LatestBackup>;

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut last: LatestByLevel = HashMap::new();
        last.insert(
            0,
            LatestBackup {
                timestamp: "2026-06-02T14:30:00Z".to_string(),
                snapshot_name: "bcb_level0.20260602T1430".to_string(),
            },
        );
        last.insert(
            1,
            LatestBackup {
                timestamp: "2026-06-02T15:30:00Z".to_string(),
                snapshot_name: "bcb_level1.20260602T1530".to_string(),
            },
        );
        let yaml = serde_yaml::to_string(&last).unwrap();
        let deserialized: LatestByLevel = serde_yaml::from_str(&yaml).unwrap();
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
}
