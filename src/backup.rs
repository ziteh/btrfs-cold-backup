use anyhow::{Result, anyhow};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, encrypt_file},
    manifest::{Backup, Last, PartInfo},
    util::{s3_backup_key, s3_last_manifest_key, s3_manifest_key, sanitize_subvolume_path},
};

fn snapshot_timestamp(name: &str) -> Result<String> {
    let parts: Vec<&str> = name.split('_').collect();
    let [_, _, date, time, ..] = parts.as_slice() else {
        return Err(anyhow!("invalid snapshot name: {name}"));
    };
    let iso_date = match date.len() {
        8 => format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]),
        10 => date.to_string(),
        _ => return Err(anyhow!("unrecognized date in snapshot name: {name}")),
    };
    Ok(format!("{}T{}:00Z", iso_date, time.replace('-', ":")))
}

pub async fn backup_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
) -> Result<Backup> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);

    let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
    let level_prefix = format!("bcr_level{level}_");
    let snapshot_name = snapshots
        .iter()
        .filter(|s| s.starts_with(&level_prefix))
        .last()
        .ok_or_else(|| anyhow!("no level {level} snapshot found in {:?}", task.snapshot_dir))?
        .clone();
    let snapshot_path = task.snapshot_dir.join(&snapshot_name);
    let timestamp = snapshot_timestamp(&snapshot_name)?;

    // Get parent snapshot if this is an incremental backup
    let parent = if level == 0 {
        None
    } else {
        if remote.head(&last_key).await?.is_none() {
            return Err(anyhow!(
                "no previous backup; level {level} requires a prior level 0 backup"
            ));
        }
        let last: Last = serde_yaml::from_slice(&remote.download(&last_key).await?)?;
        if !snapshots.contains(&last.snapshot_name) {
            return Err(anyhow!(
                "parent snapshot '{}' not found locally",
                last.snapshot_name
            ));
        }
        Some(last.snapshot_name)
    };

    // Get the btrfs send stream
    let mut stream = match &parent {
        Some(parent_name) => {
            let parent_path = task.snapshot_dir.join(parent_name.as_str());
            btrfs.send_incremental(&parent_path, &snapshot_path).await?
        }
        None => btrfs.send_full(&snapshot_path).await?,
    };

    tokio::fs::create_dir_all(&config.base_dir).await?;

    // Encrypt data
    let plain_path = config.base_dir.join(format!("{snapshot_name}.plain"));
    let enc_path = config.base_dir.join(format!("{snapshot_name}.age"));
    {
        let mut f = tokio::fs::File::create(&plain_path).await?;
        tokio::io::copy(&mut stream, &mut f).await?;
    }
    encrypt_file(&plain_path, &enc_path, &config.age_public_key).await?;
    let _ = tokio::fs::remove_file(&plain_path).await;
    let enc_size = tokio::fs::metadata(&enc_path).await?.len();
    let hash = blake3_file(&enc_path).await?;

    // Upload backup to remote
    let storage_class = config
        .s3
        .storage_class
        .backup_data
        .get(level as usize)
        .map(String::as_str)
        .unwrap_or("STANDARD");
    let part_key = s3_backup_key(&config.s3.prefix, &sanitized, level, &timestamp, 1)?;
    remote
        .upload_file(&part_key, &enc_path, storage_class)
        .await?;
    let _ = tokio::fs::remove_file(&enc_path).await;

    let backup = Backup {
        timestamp: timestamp.clone(),
        level,
        snapshot_name: snapshot_name.clone(),
        parent_snapshot: parent,
        total_size_bytes: enc_size,
        sha256: hash.clone(),
        parts: vec![PartInfo {
            part_number: 1,
            size_bytes: enc_size,
            sha256: hash,
        }],
    };

    // Upload manifest to remote
    let manifest_class = &config.s3.storage_class.manifest;
    let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp)?;
    remote
        .upload_bytes(
            &manifest_key,
            serde_yaml::to_string(&backup)?.into_bytes(),
            manifest_class,
        )
        .await?;

    let last = Last {
        timestamp,
        level,
        snapshot_name,
    };
    remote
        .upload_bytes(
            &last_key,
            serde_yaml::to_string(&last)?.into_bytes(),
            manifest_class,
        )
        .await?;

    Ok(backup)
}
