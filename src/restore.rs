use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_file},
    manifest::Backup,
    util::{s3_backup_key, s3_manifest_key, sanitize_subvolume_path},
};

fn date_to_timestamp(date: &str) -> Result<String> {
    if date.len() == 12 && date.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!(
            "{}-{}-{}T{}:{}:00Z",
            &date[0..4],
            &date[4..6],
            &date[6..8],
            &date[8..10],
            &date[10..12]
        ))
    } else if date.len() == 8 && date.chars().all(|c| c.is_ascii_digit()) {
        Ok(format!(
            "{}-{}-{}T00:00:00Z",
            &date[0..4],
            &date[4..6],
            &date[6..8]
        ))
    } else {
        Err(anyhow!(
            "date must be YYYYMMDD or YYYYMMDDHHMM, got: {date}"
        ))
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

    let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp)?;
    let backup: Backup = serde_yaml::from_slice(&remote.download(&manifest_key).await?)?;

    // Unique temp dir per invocation; prevents concurrent restores from colliding.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let restore_dir = config
        .base_dir
        .join(format!("restore-{}-{}", std::process::id(), nonce));
    tokio::fs::create_dir_all(&restore_dir).await?;

    let result: Result<()> = async {
        // Phase 1: download, verify per-part BLAKE3, decrypt
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

            let enc_path = restore_dir.join(format!("part-{}.age", part.part_number));
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

            let dec_path = restore_dir.join(format!("part-{}.btrfs", part.part_number));
            decrypt_file(&enc_path, &dec_path, private_key_str).await?;
            let _ = tokio::fs::remove_file(&enc_path).await;
            dec_paths.push(dec_path);
        }

        // Phase 2: pipe decrypted parts into btrfs receive; compute stream BLAKE3 in-flight
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

        let stream_hash = {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
            let mut hasher = blake3::Hasher::new();
            let mut buf = vec![0u8; 65536];
            for dec_path in &dec_paths {
                let mut f = tokio::fs::File::open(dec_path).await?;
                loop {
                    let n = f.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    stdin.write_all(&buf[..n]).await?;
                }
                let _ = tokio::fs::remove_file(dec_path).await;
            }
            hasher.finalize().to_hex().to_string()
        };

        let output = child.wait_with_output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("btrfs receive failed: {}", stderr.trim()));
        }

        // Phase 3: verify full-stream BLAKE3 against manifest
        if stream_hash != backup.sha256 {
            return Err(anyhow!(
                "stream BLAKE3 mismatch: expected {}, got {}",
                backup.sha256,
                stream_hash
            ));
        }

        Ok(())
    }
    .await;

    let _ = tokio::fs::remove_dir_all(&restore_dir).await;
    result
}
