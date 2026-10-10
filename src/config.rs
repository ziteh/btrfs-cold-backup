use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::layout::sanitize_subvolume_path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub base_dir: PathBuf,
    pub age_public_key: String,
    #[serde(default)]
    pub s3: S3Config,
    /// Unset disables the check.
    #[serde(default)]
    pub oversize_guard: Option<OversizeGuard>,
    pub tasks: Vec<Task>,
}

/// Holds back an incremental whose size is out of line with recent ones, e.g. because
/// ransomware encrypted the data, before it is uploaded. Sizes are compared per day since
/// the parent, so a cumulative incremental isn't mistaken for an outlier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OversizeGuard {
    /// How many of the newest incrementals, of any level, set the usual size per day.
    pub history: usize,
    /// How many times the usual size per day an incremental may reach.
    pub rate_factor: f64,
    /// Incrementals up to this size always pass, so a small dataset's normal swings don't
    /// trip the check.
    pub min_size_gib: f64,
    /// While fewer than `history` incrementals exist, the share of the chain's level 0 size
    /// an incremental may reach instead.
    pub fallback_level0_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub subvolume: PathBuf,
    pub snapshot_dir: PathBuf,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    /// Empty for AWS; set for an S3-compatible service.
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub storage_class: StorageClasses,
    #[serde(default)]
    pub retry: S3RetryConfig,
    /// Kilobit/s; 0 means unlimited.
    #[serde(default)]
    pub upload_speed_limit_kbps: u64,
    #[serde(default)]
    pub restore: RestoreConfig,
}

/// The S3 storage classes bcb supports. INTELLIGENT_TIERING is left out because its archive
/// tiers need a different retrieval flow, and REDUCED_REDUNDANCY because it is deprecated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageClass {
    #[default]
    Standard,
    StandardIa,
    OnezoneIa,
    GlacierIr,
    Glacier,
    DeepArchive,
}

impl StorageClass {
    /// Whether objects of this class must be retrieved before they can be downloaded.
    pub fn is_archive(self) -> bool {
        matches!(self, Self::Glacier | Self::DeepArchive)
    }

    /// The name S3 uses for this class.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::StandardIa => "STANDARD_IA",
            Self::OnezoneIa => "ONEZONE_IA",
            Self::GlacierIr => "GLACIER_IR",
            Self::Glacier => "GLACIER",
            Self::DeepArchive => "DEEP_ARCHIVE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageClasses {
    #[serde(default)]
    pub manifest: StorageClass,
    /// Indexed by backup level. A level without an entry is an error rather than a silent
    /// default, since the storage class directly decides the storage cost.
    #[serde(default)]
    pub backup_data: Vec<StorageClass>,
}

impl StorageClasses {
    pub fn for_level(&self, level: u32) -> Result<StorageClass> {
        self.backup_data
            .get(level as usize)
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "no storage class configured for level {level} \
                 (s3.storage_class.backup_data has {} entries)",
                    self.backup_data.len()
                )
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3RetryConfig {
    pub max_attempts: u32,
}

impl Default for S3RetryConfig {
    fn default() -> Self {
        Self { max_attempts: 8 }
    }
}

/// How a restore paces archive retrievals to the download bandwidth.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RestoreConfig {
    /// Expected download speed in kilobit/s, used only to pace retrievals, never to limit the
    /// download. 0 disables pacing: every part is requested at once.
    #[serde(default)]
    pub expected_download_kbps: u64,

    /// How many days of download to keep requested ahead. Unset means a default for the
    /// retrieval tier.
    #[serde(default)]
    pub lead_days: Option<f64>,

    /// Maximum concurrent part status checks.
    #[serde(default = "default_status_check_concurrency")]
    pub status_check_concurrency: u32,
}

fn default_status_check_concurrency() -> u32 {
    16
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("failed to read config {path:?}"))?;
        let config: Self = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("failed to parse config {path:?}"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.base_dir.as_os_str().is_empty() {
            return Err(anyhow!("base_dir cannot be empty"));
        }
        crate::crypto::parse_recipient(&self.age_public_key)
            .map_err(|e| anyhow!("age_public_key: {e}"))?;
        self.validate_tasks()?;
        self.validate_s3()?;
        self.validate_oversize_guard()
    }

    fn validate_oversize_guard(&self) -> Result<()> {
        let Some(guard) = &self.oversize_guard else {
            return Ok(());
        };
        if guard.history == 0 {
            return Err(anyhow!("oversize_guard.history must be > 0"));
        }
        for (name, value) in [
            ("rate_factor", guard.rate_factor),
            ("min_size_gib", guard.min_size_gib),
            ("fallback_level0_ratio", guard.fallback_level0_ratio),
        ] {
            if !(value.is_finite() && value > 0.0) {
                return Err(anyhow!("oversize_guard.{name} must be > 0, got {value}"));
            }
        }
        Ok(())
    }

    fn validate_tasks(&self) -> Result<()> {
        if self.tasks.is_empty() {
            return Err(anyhow!("at least one task must be defined"));
        }
        let mut names = HashSet::new();
        // Keyed by sanitized path because state files and S3 keys are too: distinct paths
        // like /mnt/data/movies and /mnt/data_movies collide after sanitization and would
        // silently overwrite each other's backups.
        let mut subvolumes: HashMap<String, &str> = HashMap::new();
        // Snapshot selection only looks at names, so two tasks sharing a snapshot_dir would
        // each back up the other's snapshots. Collecting components ignores trailing slashes.
        let mut snapshot_dirs: HashMap<PathBuf, &str> = HashMap::new();
        for task in &self.tasks {
            validate_task_name(&task.name)?;
            if !names.insert(&task.name) {
                return Err(anyhow!("duplicate task name: {}", task.name));
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
            if !task.enabled {
                continue;
            }
            let sanitized = sanitize_subvolume_path(&task.subvolume)
                .map_err(|e| anyhow!("task '{}': {e}", task.name))?;
            if let Some(other) = subvolumes.insert(sanitized, &task.name) {
                return Err(anyhow!(
                    "tasks '{other}' and '{}' back up the same subvolume (identical after \
                     path sanitization)",
                    task.name
                ));
            }
            let snapshot_dir: PathBuf = task.snapshot_dir.components().collect();
            if let Some(other) = snapshot_dirs.insert(snapshot_dir, &task.name) {
                return Err(anyhow!(
                    "tasks '{other}' and '{}' share snapshot_dir {:?}",
                    task.name,
                    task.snapshot_dir
                ));
            }
        }
        Ok(())
    }

    fn validate_s3(&self) -> Result<()> {
        let s3 = &self.s3;
        if s3.bucket.is_empty() {
            return Err(anyhow!("s3.bucket cannot be empty"));
        }
        if s3.region.is_empty() {
            return Err(anyhow!("s3.region cannot be empty"));
        }
        // Keys are built as `{prefix}manifests/...`; without the trailing slash the prefix
        // would glue onto the next component, and a leading slash creates an empty-named
        // top-level "directory" on S3.
        let prefix = &s3.prefix;
        if !prefix.is_empty() && (!prefix.ends_with('/') || prefix.starts_with('/')) {
            return Err(anyhow!(
                "s3.prefix must be empty or end with '/' and not start with '/', got {prefix:?}"
            ));
        }
        if s3.retry.max_attempts == 0 {
            return Err(anyhow!("s3.retry.max_attempts must be > 0"));
        }
        if s3.storage_class.backup_data.is_empty() {
            return Err(anyhow!(
                "s3.storage_class.backup_data must define a storage class per backup level"
            ));
        }
        // Manifests are read on every backup and at the start of a restore.
        if s3.storage_class.manifest.is_archive() {
            return Err(anyhow!(
                "s3.storage_class.manifest cannot be {}: manifests must be readable without \
                 an archive retrieval",
                s3.storage_class.manifest.as_str()
            ));
        }
        Ok(())
    }
}

/// The name becomes a path component under base_dir, so a separator in it would write outside
/// base_dir. Rejecting it here beats sanitizing at each use site: this fires at `bcb check`,
/// the other is something every new use site must remember.
fn validate_task_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("task name cannot be empty"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(anyhow!(
            "task '{name}': name may only contain letters, digits, '.', '_' and '-'"
        ));
    }
    // The only two names the charset above still lets escape base_dir.
    if name == "." || name == ".." {
        return Err(anyhow!("task name cannot be '.' or '..'"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            base_dir: PathBuf::from("/var/lib/bcb"),
            age_public_key: age::x25519::Identity::generate().to_public().to_string(),
            s3: S3Config {
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                prefix: "backups/".to_string(),
                storage_class: StorageClasses {
                    manifest: StorageClass::Standard,
                    backup_data: vec![StorageClass::DeepArchive],
                },
                ..Default::default()
            },
            oversize_guard: None,
            tasks: vec![Task {
                name: "test".to_string(),
                description: "test task".to_string(),
                subvolume: PathBuf::from("/mnt/data"),
                snapshot_dir: PathBuf::from("/mnt/snapshots"),
                enabled: true,
            }],
        }
    }

    fn assert_invalid(config: &Config, needle: &str) {
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains(needle), "unexpected error: {err}");
    }

    #[test]
    fn valid_config_should_pass_validation() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn example_yaml_should_parse_and_validate() {
        let yaml = r#"
base_dir: /var/lib/bcb/
age_public_key: KEY
s3:
  bucket: b
  prefix: p/
  region: us-east-1
  storage_class:
    manifest: GLACIER_IR
    backup_data: [DEEP_ARCHIVE, STANDARD_IA]
tasks:
  - name: t
    subvolume: /mnt/data
    snapshot_dir: /mnt/.snapshots
"#;
        let key = age::x25519::Identity::generate().to_public().to_string();
        let config: Config = serde_yaml::from_str(&yaml.replace("KEY", &key)).unwrap();
        config.validate().unwrap();
        assert_eq!(config.s3.storage_class.manifest, StorageClass::GlacierIr);
        assert_eq!(
            config.s3.storage_class.for_level(1).unwrap(),
            StorageClass::StandardIa
        );
        assert!(config.tasks[0].enabled);
    }

    #[test]
    fn unsupported_storage_class_should_fail_to_parse() {
        for class in [
            "DEEP_FREEZE",
            "bogus",
            "INTELLIGENT_TIERING",
            "REDUCED_REDUNDANCY",
        ] {
            assert!(
                serde_yaml::from_str::<StorageClass>(class).is_err(),
                "{class} should be rejected"
            );
        }
    }

    #[test]
    fn task_name_with_path_separator_dots_or_whitespace_should_be_rejected() {
        for name in ["../escape", "sub/dir", "..", "has space", ""] {
            let mut config = valid_config();
            config.tasks[0].name = name.to_string();
            assert!(
                config.validate().is_err(),
                "task name {name:?} should be rejected"
            );
        }
    }

    #[test]
    fn each_empty_required_field_should_be_rejected() {
        let mut config = valid_config();
        config.base_dir = PathBuf::new();
        assert_invalid(&config, "base_dir");

        let mut config = valid_config();
        config.tasks = vec![];
        assert_invalid(&config, "at least one task");

        let mut config = valid_config();
        config.tasks[0].subvolume = PathBuf::new();
        assert_invalid(&config, "subvolume cannot be empty");

        let mut config = valid_config();
        config.tasks[0].snapshot_dir = PathBuf::new();
        assert_invalid(&config, "snapshot_dir cannot be empty");

        let mut config = valid_config();
        config.s3.bucket = String::new();
        assert_invalid(&config, "s3.bucket");

        let mut config = valid_config();
        config.s3.region = String::new();
        assert_invalid(&config, "s3.region");
    }

    #[test]
    fn empty_or_malformed_age_public_key_should_be_rejected() {
        for key in ["", "invalid"] {
            let mut config = valid_config();
            config.age_public_key = key.to_string();
            assert_invalid(&config, "age_public_key");
        }
    }

    #[test]
    fn duplicate_task_names_should_be_rejected() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.subvolume = PathBuf::from("/mnt/data2");
        second.snapshot_dir = PathBuf::from("/mnt/snapshots2");
        config.tasks.push(second);
        assert_invalid(&config, "duplicate task name");
    }

    #[test]
    fn duplicate_subvolume_should_be_rejected() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        config.tasks.push(second);
        assert_invalid(&config, "same subvolume");
    }

    #[test]
    fn subvolumes_sanitizing_to_the_same_name_should_be_rejected() {
        let mut config = valid_config();
        config.tasks[0].subvolume = PathBuf::from("/mnt/data/movies");
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        second.subvolume = PathBuf::from("/mnt/data_movies");
        config.tasks.push(second);
        assert_invalid(&config, "same subvolume");
    }

    #[test]
    fn snapshot_dir_shared_up_to_trailing_slash_should_be_rejected() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        second.subvolume = PathBuf::from("/mnt/other");
        second.snapshot_dir = PathBuf::from("/mnt/snapshots/");
        config.tasks.push(second);
        assert_invalid(&config, "share snapshot_dir");
    }

    #[test]
    fn disabled_task_should_not_collide_on_subvolume() {
        let mut config = valid_config();
        let mut second = config.tasks[0].clone();
        second.name = "other".to_string();
        second.enabled = false;
        config.tasks.push(second);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn zero_retry_max_attempts_should_be_rejected() {
        let mut config = valid_config();
        config.s3.retry.max_attempts = 0;
        assert_invalid(&config, "max_attempts");
    }

    #[test]
    fn empty_backup_data_storage_classes_should_be_rejected() {
        let mut config = valid_config();
        config.s3.storage_class.backup_data = vec![];
        assert_invalid(&config, "backup_data");
    }

    #[test]
    fn s3_prefix_should_be_empty_or_relative_and_end_with_slash() {
        for ok in ["", "backups/", "a/b/"] {
            let mut config = valid_config();
            config.s3.prefix = ok.to_string();
            assert!(
                config.validate().is_ok(),
                "prefix {ok:?} should be accepted"
            );
        }
        for bad in ["backups", "/backups/", "/"] {
            let mut config = valid_config();
            config.s3.prefix = bad.to_string();
            assert!(
                config.validate().is_err(),
                "prefix {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn manifest_storage_class_should_reject_archive_tiers_but_allow_glacier_ir() {
        for class in [StorageClass::Glacier, StorageClass::DeepArchive] {
            let mut config = valid_config();
            config.s3.storage_class.manifest = class;
            assert_invalid(&config, "manifest cannot be");
        }
        let mut config = valid_config();
        config.s3.storage_class.manifest = StorageClass::GlacierIr;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn for_level_should_fail_for_a_level_without_storage_class() {
        let classes = StorageClasses {
            manifest: StorageClass::Standard,
            backup_data: vec![StorageClass::DeepArchive],
        };
        assert_eq!(classes.for_level(0).unwrap(), StorageClass::DeepArchive);
        let err = classes.for_level(1).unwrap_err().to_string();
        assert!(err.contains("level 1"), "unexpected error: {err}");
    }

    #[test]
    fn oversize_guard_should_parse_and_reject_zero_history_or_rate_factor() {
        let yaml = "history: 5\nrate_factor: 10\nmin_size_gib: 20\nfallback_level0_ratio: 0.1\n";
        let guard: OversizeGuard = serde_yaml::from_str(yaml).unwrap();
        let mut config = valid_config();
        config.oversize_guard = Some(guard.clone());
        assert!(config.validate().is_ok());

        config.oversize_guard = Some(OversizeGuard {
            history: 0,
            ..guard.clone()
        });
        assert_invalid(&config, "oversize_guard.history");
        config.oversize_guard = Some(OversizeGuard {
            rate_factor: 0.0,
            ..guard
        });
        assert_invalid(&config, "oversize_guard.rate_factor");
    }
}
