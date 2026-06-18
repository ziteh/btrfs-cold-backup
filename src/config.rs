use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3RetryConfig {
    /// Maximum number of retry attempts for S3 uploads
    pub max_attempts: u32,
}

impl Default for S3RetryConfig {
    fn default() -> Self {
        Self { max_attempts: 8 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3Config {
    /// Whether upload is enabled
    pub enabled: bool,
    /// Bucket name
    pub bucket: String,
    /// Region
    pub region: String,
    /// Prefix for backup keys
    pub prefix: String,
    /// Endpoint URL (for S3-compatible)
    #[serde(default)]
    pub endpoint: String,
    /// Storage class
    #[serde(default)]
    pub storage_class: S3StorageClass,
    /// Retry config
    #[serde(default)]
    pub retry: S3RetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3StorageClass {
    /// Storage class for manifest files
    #[serde(default = "default_manifest_class")]
    pub manifest: String,
    /// Storage class for backup data parts by level
    #[serde(default)]
    pub backup_data: Vec<String>,
}

fn default_manifest_class() -> String {
    "STANDARD".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique name for the backup task
    pub name: String,
    /// Description of the backup task
    pub description: String,
    /// Path to the btrfs subvolume to back up
    pub subvolume: PathBuf,
    /// Directory to store snapshots
    pub snapshot_dir: PathBuf,
    /// Whether this task is enabled
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
        Ok(serde_yaml::from_slice(&bytes)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.base_dir.as_os_str().is_empty() {
            return Err(anyhow!("base_dir cannot be empty"));
        }
        if self.age_public_key.is_empty() {
            return Err(anyhow!("age_public_key cannot be empty"));
        }
        if !self.age_public_key.starts_with("age1") {
            return Err(anyhow!("age_public_key must start with 'age1'"));
        }
        if self.tasks.is_empty() {
            return Err(anyhow!("at least one task must be defined"));
        }

        let mut task_names = HashSet::new();
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
            base_dir: PathBuf::from("/var/lib/bcr"),
            age_public_key:
                "age1qypmyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpq"
                    .to_string(),
            s3: S3Config {
                enabled: true,
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                prefix: "backups/".to_string(),
                endpoint: "".to_string(),
                storage_class: S3StorageClass::default(),
                retry: S3RetryConfig::default(),
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
