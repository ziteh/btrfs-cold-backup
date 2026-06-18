use anyhow::{Result, anyhow};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    // Pipe btrfs send stream directly into split, hashing as data flows through.
    // This avoids writing a full plain file — only the split parts land on disk.
    let part_prefix = config.base_dir.join(format!("{snapshot_name}.part-"));
    let part_prefix_str = part_prefix
        .to_str()
        .ok_or_else(|| anyhow!("invalid part prefix path"))?;
    let mut split_child = tokio::process::Command::new("split")
        .args(["-b", "3G", "-d", "-a", "6", "-", part_prefix_str])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let stream_hash = {
        let mut split_stdin = split_child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open split stdin"))?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 65536];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            split_stdin.write_all(&buf[..n]).await?;
        }
        hasher.finalize().to_hex().to_string()
    };

    let split_out = split_child.wait_with_output().await?;
    if !split_out.status.success() {
        let stderr = String::from_utf8_lossy(&split_out.stderr);
        return Err(anyhow!("split failed: {}", stderr.trim()));
    }

    // Collect and sort part files
    let part_prefix_name = format!("{snapshot_name}.part-");
    let mut part_files: Vec<PathBuf> = Vec::new();
    let mut read_dir = tokio::fs::read_dir(&config.base_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(&part_prefix_name)
        {
            part_files.push(entry.path());
        }
    }
    part_files.sort();
    if part_files.is_empty() {
        return Err(anyhow!("split produced no parts"));
    }

    // Encrypt, hash, and upload each part
    let storage_class = config
        .s3
        .storage_class
        .backup_data
        .get(level as usize)
        .map(String::as_str)
        .unwrap_or("STANDARD");

    let mut parts_info: Vec<PartInfo> = Vec::new();
    let mut total_enc_size: u64 = 0;

    for (idx, part_file) in part_files.iter().enumerate() {
        let part_number = (idx + 1) as u32;
        let enc_path = config
            .base_dir
            .join(format!("{snapshot_name}.part-{part_number}.age"));

        encrypt_file(part_file, &enc_path, &config.age_public_key).await?;
        let _ = tokio::fs::remove_file(part_file).await;

        let enc_size = tokio::fs::metadata(&enc_path).await?.len();
        let hash = blake3_file(&enc_path).await?;

        let part_key = s3_backup_key(
            &config.s3.prefix,
            &sanitized,
            level,
            &timestamp,
            part_number,
        )?;
        remote
            .upload_file(&part_key, &enc_path, storage_class)
            .await?;
        let _ = tokio::fs::remove_file(&enc_path).await;

        total_enc_size += enc_size;
        parts_info.push(PartInfo {
            part_number,
            size_bytes: enc_size,
            sha256: hash,
        });
    }

    let backup = Backup {
        timestamp: timestamp.clone(),
        level,
        snapshot_name: snapshot_name.clone(),
        parent_snapshot: parent,
        total_size_bytes: total_enc_size,
        sha256: stream_hash,
        parts: parts_info,
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
