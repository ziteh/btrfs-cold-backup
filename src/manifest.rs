//! The formats bcb stores on S3: a manifest per backup, and per task a latest manifest
//! pointing at the newest backup of each level. They outlive any one bcb version, so field
//! changes must keep [`FORMAT_VERSION`]'s compatibility rule.

use anyhow::{Context, Result, anyhow};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::remote::RemoteBackend;
use crate::timestamp::{BackupId, Timestamp};

/// Shared by [`BackupManifest`] and [`LatestManifest`]. Readers reject any other major version; a
/// minor bump may only add fields that have a serde default, so older readers still parse it.
pub const FORMAT_VERSION: &str = "1.0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestPart {
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

/// Describes one backup: what was sent, from where, and how to verify and decrypt it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub format_version: String,
    pub timestamp: Timestamp,
    /// Unix time when the backup ran (`timestamp` is the snapshot's, not the run's).
    pub created_at: i64,
    pub level: u32,
    pub snapshot_name: String,
    pub parent_snapshot: Option<String>,
    /// btrfs UUID of the backed-up subvolume.
    pub source_uuid: String,
    /// btrfs UUID of the snapshot that was sent.
    pub snapshot_uuid: String,
    pub system: SystemInfo,
    pub age_public_key: String,
    pub total_size_bytes: u64,
    pub blake3: String,
    pub parts: Vec<ManifestPart>,
}

impl BackupManifest {
    pub fn id(&self) -> BackupId {
        BackupId {
            level: self.level,
            timestamp: self.timestamp.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestEntry {
    pub timestamp: Timestamp,
    pub snapshot_name: String,
    pub snapshot_uuid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestManifest {
    pub format_version: String,
    pub source_uuid: String,
    /// The newest backup of each level, keyed by level. Callers rely on the key being the
    /// level: completing a level-N backup drops every entry above N, whose incremental chain
    /// it voided.
    pub levels: BTreeMap<u32, LatestEntry>,
}

impl LatestManifest {
    pub fn new(source_uuid: String) -> Self {
        Self {
            format_version: FORMAT_VERSION.to_string(),
            source_uuid,
            levels: BTreeMap::new(),
        }
    }
}

// Probed before the full parse so a manifest from an unknown major version is reported as
// such, not as whatever field mismatch that version happens to trip first.
#[derive(Deserialize)]
struct VersionProbe {
    format_version: Option<String>,
}

fn major(version: &str) -> &str {
    version.split('.').next().unwrap_or(version)
}

fn check_format_version(bytes: &[u8]) -> Result<()> {
    let probe: VersionProbe = serde_yaml::from_slice(bytes)?;
    let version = probe
        .format_version
        .ok_or_else(|| anyhow!("manifest has no format_version"))?;
    if major(&version) != major(FORMAT_VERSION) {
        return Err(anyhow!(
            "manifest format_version {version} is not supported (this bcb reads {}.x)",
            major(FORMAT_VERSION)
        ));
    }
    Ok(())
}

fn parse<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    check_format_version(bytes)?;
    Ok(serde_yaml::from_slice(bytes)?)
}

impl BackupManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        parse(bytes)
    }
}

impl LatestManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        parse(bytes)
    }
}

/// Downloads and parses the backup manifest at `key`.
pub async fn fetch_manifest<R: RemoteBackend>(remote: &R, key: &str) -> Result<BackupManifest> {
    BackupManifest::parse(&remote.download(key).await?)
        .with_context(|| format!("failed to parse manifest {key}"))
}

/// Downloads and parses the latest manifest at `key`, or None if there is none yet.
pub async fn fetch_latest<R: RemoteBackend>(
    remote: &R,
    key: &str,
) -> Result<Option<LatestManifest>> {
    if remote.head_metadata(key).await?.is_none() {
        return Ok(None);
    }
    let latest = LatestManifest::parse(&remote.download(key).await?)
        .with_context(|| format!("failed to parse latest manifest {key}"))?;
    Ok(Some(latest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_backup(level: u32, parent: Option<&str>) -> BackupManifest {
        BackupManifest {
            format_version: FORMAT_VERSION.to_string(),
            timestamp: "2026-06-02T14:30".parse().unwrap(),
            created_at: 0,
            level,
            snapshot_name: format!("bcb_level{level}.20260602T1430"),
            parent_snapshot: parent.map(str::to_string),
            source_uuid: "src-uuid".to_string(),
            snapshot_uuid: "snap-uuid".to_string(),
            system: SystemInfo::default(),
            age_public_key: String::new(),
            total_size_bytes: 1024,
            blake3: "def456".to_string(),
            parts: vec![ManifestPart {
                index: "000000".to_string(),
                size_bytes: 1024,
                blake3: "part1".to_string(),
            }],
        }
    }

    #[test]
    fn test_backup_round_trip() {
        let backup = sample_backup(1, Some("bcb_level0.20260601T1430"));
        let yaml = serde_yaml::to_string(&backup).unwrap();
        assert_eq!(BackupManifest::parse(yaml.as_bytes()).unwrap(), backup);
    }

    #[test]
    fn test_backup_null_parent_round_trip() {
        let backup = sample_backup(0, None);
        let yaml = serde_yaml::to_string(&backup).unwrap();
        assert!(
            BackupManifest::parse(yaml.as_bytes())
                .unwrap()
                .parent_snapshot
                .is_none()
        );
    }

    #[test]
    fn test_latest_round_trip() {
        let mut latest = LatestManifest::new("src-uuid".to_string());
        latest.levels.insert(
            0,
            LatestEntry {
                timestamp: "2026-06-02T14:30".parse().unwrap(),
                snapshot_name: "bcb_level0.20260602T1430".to_string(),
                snapshot_uuid: "snap-uuid".to_string(),
            },
        );
        let yaml = serde_yaml::to_string(&latest).unwrap();
        assert_eq!(LatestManifest::parse(yaml.as_bytes()).unwrap(), latest);
    }

    #[test]
    fn test_parse_accepts_newer_minor_with_unknown_field() {
        let mut backup = sample_backup(0, None);
        backup.format_version = format!("{}.99", major(FORMAT_VERSION));
        let mut yaml = serde_yaml::to_string(&backup).unwrap();
        yaml.push_str("added_in_minor: true\n");
        assert!(BackupManifest::parse(yaml.as_bytes()).is_ok());
    }

    #[test]
    fn test_parse_rejects_unknown_major_version() {
        let yaml = "format_version: '999.0'\nsomething_else: 1\n";
        let err = BackupManifest::parse(yaml.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported"), "unexpected error: {err}");
        assert!(LatestManifest::parse(yaml.as_bytes()).is_err());
    }

    #[test]
    fn test_parse_rejects_missing_format_version() {
        let mut value = serde_yaml::to_value(sample_backup(0, None)).unwrap();
        value.as_mapping_mut().unwrap().remove("format_version");
        let yaml = serde_yaml::to_string(&value).unwrap();
        let err = BackupManifest::parse(yaml.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains("no format_version"), "unexpected error: {err}");
    }
}
