//! Where a task's data lives: its S3 keys and its local paths under base_dir.
//!
//! ```text
//! <prefix>backup-data/<subvolume>/<backup dir>/snapshot.part-NNNNNN.age
//! <prefix>manifests/<subvolume>/<backup dir>/manifest.yaml
//! <prefix>manifests/<subvolume>/latest.yaml
//!
//! <base_dir>/backup/<subvolume>/state-level<N>.yaml
//! <base_dir>/backup/<subvolume>/<backup dir>/snapshot.part-NNNNNN.age
//! <base_dir>/restore/<subvolume>/<backup dir>/{state.yaml,manifest.yaml,snapshot.part-NNNNNN.age}
//! ```
//!
//! `<subvolume>` is the task's subvolume path with `/` replaced by `_`, and `<backup dir>` is
//! [`BackupId::dir_name`].

use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

use crate::config::{Config, Task};
use crate::timestamp::BackupId;

/// Tag key every object carries, so one S3 lifecycle rule per level covers all subvolumes.
const LEVEL_TAG_KEY: &str = "bcb-backup-level";

/// The URL-query-encoded tag set of a part of a `level` backup.
pub fn level_tag(level: u32) -> String {
    format!("{LEVEL_TAG_KEY}={level}")
}

/// The tag set of manifests, which lifecycle rules must leave alone.
pub fn manifest_tag() -> String {
    format!("{LEVEL_TAG_KEY}=manifest")
}

/// The tag set of the throwaway object `bcb check` writes to probe permissions.
pub fn check_tag() -> String {
    format!("{LEVEL_TAG_KEY}=check")
}

/// File name of part `index`, the same locally and on S3.
pub fn part_file_name(index: &str) -> String {
    format!("snapshot.part-{index}.age")
}

/// Turns a subvolume path into the single path component that names it in keys and paths.
pub fn sanitize_subvolume_path(subvolume: &Path) -> Result<String> {
    let path = subvolume
        .to_str()
        .ok_or_else(|| anyhow!("subvolume path is not valid UTF-8"))?;
    let sanitized = path.trim_matches('/').replace(['/', '\\'], "_");
    if sanitized.is_empty() {
        return Err(anyhow!("subvolume path {path:?} names no directory"));
    }
    Ok(sanitized)
}

pub struct TaskLayout {
    s3_prefix: String,
    base_dir: PathBuf,
    subvolume: String,
}

impl TaskLayout {
    pub fn new(config: &Config, task: &Task) -> Result<Self> {
        Ok(Self {
            s3_prefix: config.s3.prefix.clone(),
            base_dir: config.base_dir.clone(),
            subvolume: sanitize_subvolume_path(&task.subvolume)?,
        })
    }

    pub fn part_key(&self, id: &BackupId, index: &str) -> String {
        format!(
            "{}backup-data/{}/{}/{}",
            self.s3_prefix,
            self.subvolume,
            id.dir_name(),
            part_file_name(index)
        )
    }

    /// Prefix of the keys of every manifest of this task, the latest manifest included.
    pub fn manifests_prefix(&self) -> String {
        format!("{}manifests/{}/", self.s3_prefix, self.subvolume)
    }

    pub fn manifest_key(&self, id: &BackupId) -> String {
        format!("{}{}/manifest.yaml", self.manifests_prefix(), id.dir_name())
    }

    pub fn latest_key(&self) -> String {
        format!("{}latest.yaml", self.manifests_prefix())
    }

    fn backup_root(&self) -> PathBuf {
        self.base_dir.join("backup").join(&self.subvolume)
    }

    /// The state of this task's unfinished backup of `level`. One per level, so a long
    /// backup of one level can be interrupted to run another and resumed after it.
    pub fn backup_state_path(&self, level: u32) -> PathBuf {
        self.backup_root().join(format!("state-level{level}.yaml"))
    }

    /// Where versions before per-level states kept the task's one unfinished backup.
    pub fn legacy_backup_state_path(&self) -> PathBuf {
        self.backup_root().join("state.yaml")
    }

    /// Where a backup's encrypted parts wait to be uploaded.
    pub fn backup_parts_dir(&self, id: &BackupId) -> PathBuf {
        self.backup_root().join(id.dir_name())
    }

    /// Holds one restore's state, cached manifest and downloaded parts.
    pub fn restore_work_dir(&self, id: &BackupId) -> PathBuf {
        self.base_dir
            .join("restore")
            .join(&self.subvolume)
            .join(id.dir_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3Config;

    fn layout(subvolume: &str) -> TaskLayout {
        let config = Config {
            base_dir: PathBuf::from("/base"),
            age_public_key: String::new(),
            s3: S3Config {
                prefix: "backups/".to_string(),
                ..Default::default()
            },
            tasks: vec![],
        };
        let task = Task {
            name: "t".to_string(),
            description: String::new(),
            subvolume: PathBuf::from(subvolume),
            snapshot_dir: PathBuf::from("/snap"),
            enabled: true,
        };
        TaskLayout::new(&config, &task).unwrap()
    }

    fn id(level: u32) -> BackupId {
        BackupId {
            level,
            timestamp: "2026-06-02T14:30".parse().unwrap(),
        }
    }

    #[test]
    fn test_sanitize_subvolume_path() {
        for (path, expected) in [
            ("/mnt/data", "mnt_data"),
            ("/mnt/data/", "mnt_data"),
            ("/mnt", "mnt"),
            ("/mnt/data/movies/2024", "mnt_data_movies_2024"),
        ] {
            assert_eq!(sanitize_subvolume_path(Path::new(path)).unwrap(), expected);
        }
        assert!(sanitize_subvolume_path(Path::new("")).is_err());
        assert!(sanitize_subvolume_path(Path::new("///")).is_err());
    }

    #[test]
    fn test_sanitize_dot_vs_underscore_no_collision() {
        let a = sanitize_subvolume_path(Path::new("/mnt/data.1")).unwrap();
        let b = sanitize_subvolume_path(Path::new("/mnt/data_1")).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_s3_keys() {
        let l = layout("/mnt/data");
        assert_eq!(
            l.part_key(&id(0), "000000"),
            "backups/backup-data/mnt_data/bcb_level0_202606021430/snapshot.part-000000.age"
        );
        assert_eq!(
            l.manifest_key(&id(1)),
            "backups/manifests/mnt_data/bcb_level1_202606021430/manifest.yaml"
        );
        assert_eq!(l.latest_key(), "backups/manifests/mnt_data/latest.yaml");
        assert!(l.manifest_key(&id(0)).starts_with(&l.manifests_prefix()));
    }

    #[test]
    fn test_local_paths() {
        let l = layout("/mnt/data");
        assert_eq!(
            l.backup_state_path(2),
            Path::new("/base/backup/mnt_data/state-level2.yaml")
        );
        assert_eq!(
            l.backup_parts_dir(&id(1)),
            Path::new("/base/backup/mnt_data/bcb_level1_202606021430")
        );
        assert_eq!(
            l.restore_work_dir(&id(1)),
            Path::new("/base/restore/mnt_data/bcb_level1_202606021430")
        );
    }

    #[test]
    fn test_tags() {
        assert_eq!(level_tag(2), "bcb-backup-level=2");
        assert_eq!(manifest_tag(), "bcb-backup-level=manifest");
    }
}
