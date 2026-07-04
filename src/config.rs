use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3RetryConfig {
    pub max_attempts: u32,
}

impl Default for S3RetryConfig {
    fn default() -> Self {
        Self { max_attempts: 8 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3Config {
    pub enabled: bool,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    /// For S3-compatible endpoints.
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub storage_class: S3StorageClass,
    #[serde(default)]
    pub retry: S3RetryConfig,
    /// KiB/s; 0 means unlimited.
    #[serde(default)]
    pub upload_speed_limit_kbps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3StorageClass {
    #[serde(default = "default_manifest_class")]
    pub manifest: String,
    /// Indexed by backup level; a level without an entry is a hard error rather than a
    /// silent default, since an unintended storage class directly changes storage cost.
    #[serde(default)]
    pub backup_data: Vec<String>,
}

fn default_manifest_class() -> String {
    "STANDARD".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub description: String,
    pub subvolume: PathBuf,
    pub snapshot_dir: PathBuf,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub base_dir: PathBuf,
    pub age_public_key: String,
    #[serde(default)]
    pub s3: S3Config,
    pub tasks: Vec<Task>,
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).map_err(|e| anyhow!("failed to read config {:?}: {e}", path))?;
        let config: Self = serde_yaml::from_slice(&bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.base_dir.as_os_str().is_empty() {
            return Err(anyhow!("base_dir cannot be empty"));
        }
        if self.age_public_key.is_empty() {
            return Err(anyhow!("age_public_key cannot be empty"));
        }
        if let Err(e) = self.age_public_key.parse::<age::x25519::Recipient>() {
            return Err(anyhow!("age_public_key is not a valid age public key: {e}"));
        }
        if self.tasks.is_empty() {
            return Err(anyhow!("at least one task must be defined"));
        }

        let mut task_names = HashSet::new();
        // Keyed by sanitized path because state files and S3 keys are too: distinct paths
        // like /mnt/data/movies and /mnt/data_movies collide after sanitization and would
        // silently overwrite each other's backups.
        let mut sanitized_subvols: HashMap<String, &str> = HashMap::new();
        for task in &self.tasks {
            if !task_names.insert(&task.name) {
                return Err(anyhow!("duplicate task name: {}", task.name));
            }
            if task.name.is_empty() {
                return Err(anyhow!("task name cannot be empty"));
            }
            if task.subvolume.as_os_str().is_empty() {
                return Err(anyhow!("task '{}': subvolume cannot be empty", task.name));
            }
            if task.snapshot_dir.as_os_str().is_empty() {
                return Err(anyhow!(
                    "task '{}': snapshot_dir cannot be empty",
                    task.name
                ));
            }
            if task.enabled {
                let sanitized = crate::util::sanitize_subvolume_path(&task.subvolume)
                    .map_err(|e| anyhow!("task '{}': {e}", task.name))?;
                if let Some(other) = sanitized_subvols.insert(sanitized, &task.name) {
                    return Err(anyhow!(
                        "tasks '{}' and '{}' back up the same subvolume (identical after \
                         path sanitization)",
                        other,
                        task.name
                    ));
                }
            }
        }

        if self.s3.enabled {
            if self.s3.bucket.is_empty() {
                return Err(anyhow!("s3.bucket cannot be empty when s3 is enabled"));
            }
            if self.s3.region.is_empty() {
                return Err(anyhow!("s3.region cannot be empty when s3 is enabled"));
            }
            if self.s3.retry.max_attempts == 0 {
                return Err(anyhow!("s3.retry.max_attempts must be > 0"));
            }
            if self.s3.storage_class.backup_data.is_empty() {
                return Err(anyhow!(
                    "s3.storage_class.backup_data must define a storage class per backup level"
                ));
            }
            crate::remote::parse_storage_class(&self.s3.storage_class.manifest)
                .map_err(|e| anyhow!("s3.storage_class.manifest: {e}"))?;
            for (level, class) in self.s3.storage_class.backup_data.iter().enumerate() {
                crate::remote::parse_storage_class(class)
                    .map_err(|e| anyhow!("s3.storage_class.backup_data[{level}]: {e}"))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn valid_config() -> Config {
        Config {
            base_dir: PathBuf::from("/var/lib/bcb"),
            age_public_key: age::x25519::Identity::generate().to_public().to_string(),
            s3: S3Config {
                enabled: true,
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                prefix: "backups/".to_string(),
                endpoint: "".to_string(),
                storage_class: S3StorageClass {
                    manifest: "STANDARD".to_string(),
                    backup_data: vec!["DEEP_ARCHIVE".to_string()],
                },
                retry: S3RetryConfig::default(),
                upload_speed_limit_kbps: 0,
            },
            tasks: vec![Task {
                name: "test".to_string(),
                description: "test task".to_string(),
                subvolume: PathBuf::from("/mnt/data"),
                snapshot_dir: PathBuf::from("/mnt/snapshots"),
                enabled: true,
            }],
        }
    }

    #[test]
    fn test_valid_config() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_base_dir() {
        let mut config = valid_config();
        config.base_dir = PathBuf::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_age_public_key() {
        let mut config = valid_config();
        config.age_public_key = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_age_public_key_format() {
        let mut config = valid_config();
        config.age_public_key = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_tasks() {
        let mut config = valid_config();
        config.tasks = vec![];
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_duplicate_task_names() {
        let mut config = valid_config();
        config.tasks = vec![
            Task {
                name: "test".to_string(),
                description: "task 1".to_string(),
                subvolume: PathBuf::from("/mnt/data1"),
                snapshot_dir: PathBuf::from("/mnt/snapshots1"),
                enabled: true,
            },
            Task {
                name: "test".to_string(),
                description: "task 2".to_string(),
                subvolume: PathBuf::from("/mnt/data2"),
                snapshot_dir: PathBuf::from("/mnt/snapshots2"),
                enabled: true,
            },
        ];
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_duplicate_subvolume_rejected() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        config.tasks.push(second);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_subvolume_sanitize_collision_rejected() {
        let mut config = valid_config();
        config.tasks[0].subvolume = PathBuf::from("/mnt/data/movies");
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        second.subvolume = PathBuf::from("/mnt/data_movies");
        config.tasks.push(second);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_disabled_task_does_not_collide_on_subvolume() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        second.enabled = false;
        config.tasks.push(second);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_task_name() {
        let mut config = valid_config();
        config.tasks[0].name = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_subvolume() {
        let mut config = valid_config();
        config.tasks[0].subvolume = PathBuf::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_snapshot_dir() {
        let mut config = valid_config();
        config.tasks[0].snapshot_dir = PathBuf::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_enabled_empty_bucket() {
        let mut config = valid_config();
        config.s3.enabled = true;
        config.s3.bucket = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_enabled_empty_region() {
        let mut config = valid_config();
        config.s3.enabled = true;
        config.s3.region = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_retry_max_attempts_zero() {
        let mut config = valid_config();
        config.s3.enabled = true;
        config.s3.retry.max_attempts = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_enabled_empty_backup_data_classes() {
        let mut config = valid_config();
        config.s3.storage_class.backup_data = vec![];
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_storage_class_name() {
        let mut config = valid_config();
        config.s3.storage_class.backup_data = vec!["DEEP_FREEZE".to_string()];
        assert!(config.validate().is_err());

        let mut config = valid_config();
        config.s3.storage_class.manifest = "bogus".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_disabled_no_validation() {
        let mut config = valid_config();
        config.s3.enabled = false;
        config.s3.bucket = String::new();
        config.s3.region = String::new();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_default_s3_retry_config() {
        let retry = S3RetryConfig::default();
        assert_eq!(retry.max_attempts, 8);
    }
}
