use anyhow::{Result, anyhow};
use std::path::Path;

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_file},
    manifest::Backup,
    util::{s3_backup_key, s3_manifest_key, sanitize_subvolume_path},
};

fn date_to_timestamp(date: &str) -> Result<String> {
    if date.len() == 8 && date.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!(
            "{}-{}-{}T00:00:00Z",
            &date[0..4],
            &date[4..6],
            &date[6..8]
        ))
    } else {
        Err(anyhow!("date must be YYYYMMDD, got: {date}"))
    }
}

pub async fn restore_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    date: &str,
    target_dir: &Path,
    private_key_str: &str,
    _btrfs: &B,
    remote: &R,
) -> Result<()> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let timestamp = date_to_timestamp(date)?;

    // Download and parse the task manifest
    let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp)?;
    let backup: Backup = serde_yaml::from_slice(&remote.download(&manifest_key).await?)?;

    tokio::fs::create_dir_all(&config.base_dir).await?;

    // Download, verify, and decrypt each part
    let mut dec_paths = Vec::new();
    for part in &backup.parts {
        let part_key = s3_backup_key(
            &config.s3.prefix,
            &sanitized,
            level,
            &timestamp,
            part.part_number,
        )?;
        let part_bytes = remote.download(&part_key).await?;

        let enc_path = config
            .base_dir
            .join(format!("restore.part-{}.age", part.part_number));
        tokio::fs::write(&enc_path, &part_bytes).await?;

        let actual_hash = blake3_file(&enc_path).await?;
        if actual_hash != part.sha256 {
            let _ = tokio::fs::remove_file(&enc_path).await;
            return Err(anyhow!(
                "BLAKE3 mismatch for part {}: expected {}, got {}",
                part.part_number,
                part.sha256,
                actual_hash
            ));
        }

        let dec_path = config
            .base_dir
            .join(format!("restore.part-{}.btrfs", part.part_number));
        decrypt_file(&enc_path, &dec_path, private_key_str).await?;
        let _ = tokio::fs::remove_file(&enc_path).await;
        dec_paths.push(dec_path);
    }

    // Pipe decrypted parts sequentially into btrfs receive
    tokio::fs::create_dir_all(target_dir).await?;
    let target_str = target_dir
        .to_str()
        .ok_or_else(|| anyhow!("target_dir contains invalid UTF-8"))?;

    use tokio::process::Command;
    let mut child = Command::new("btrfs")
        .args(["receive", target_str])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
        for dec_path in &dec_paths {
            let mut f = tokio::fs::File::open(dec_path).await?;
            tokio::io::copy(&mut f, &mut stdin).await?;
            let _ = tokio::fs::remove_file(dec_path).await;
        }
    }

    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("btrfs receive failed: {}", stderr.trim()));
    }

    Ok(())
}
