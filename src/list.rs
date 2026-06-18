use anyhow::{Result, anyhow};

use crate::{Config, RemoteBackend, Task, util::sanitize_subvolume_path};

pub struct BackupEntry {
    pub level: u32,
    pub date: String,
}

fn parse_entry(key: &str) -> Option<BackupEntry> {
    // key format: {prefix}manifests/{sanitized_subvol}/bcr_level{N}_{YYYYMMDD}/task_manifest.yaml
    let dir = key.split('/').find(|s| s.starts_with("bcr_level"))?;
    let parts: Vec<&str> = dir.split('_').collect();
    let [_, level_str, date, ..] = parts.as_slice() else {
        return None;
    };
    let level: u32 = level_str.strip_prefix("level")?.parse().ok()?;
    Some(BackupEntry {
        level,
        date: date.to_string(),
    })
}

pub async fn list_backups<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    remote: &R,
) -> Result<Vec<BackupEntry>> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let prefix = format!("{}manifests/{}/", config.s3.prefix, sanitized);
    let keys = remote.list_with_prefix(&prefix).await?;

    let mut entries: Vec<BackupEntry> = keys
        .iter()
        .filter(|k| k.ends_with("task_manifest.yaml"))
        .filter_map(|k| parse_entry(k))
        .collect();

    entries.sort_by(|a, b| a.date.cmp(&b.date).then(a.level.cmp(&b.level)));

    if entries.is_empty() {
        return Err(anyhow!("no backups found for task '{}'", task.name));
    }

    Ok(entries)
}
