use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_file},
    manifest::Backup,
    util::{compact_to_iso, s3_backup_key, s3_manifest_key, sanitize_subvolume_path},
};

async fn download_and_decrypt_parts<R: RemoteBackend>(
    backup: &Backup,
    sanitized: &str,
    restore_dir: &Path,
    private_key_str: &str,
    prefix: &str,
    remote: &R,
) -> Result<Vec<std::path::PathBuf>> {
    let mut dec_paths = Vec::new();
    for part in &backup.parts {
        let part_key = s3_backup_key(
            prefix,
            sanitized,
            backup.level,
            &backup.timestamp,
            part.part_number,
        )?;
        let part_bytes = remote.download(&part_key).await?;

        let enc_path = restore_dir.join(format!("part-{}.age", part.part_number));
        tokio::fs::write(&enc_path, &part_bytes).await?;

        let actual_hash = blake3_file(&enc_path).await?;
        if actual_hash != part.blake3 {
            let _ = tokio::fs::remove_file(&enc_path).await;
            return Err(anyhow!(
                "BLAKE3 mismatch for part {}: expected {}, got {}",
                part.part_number,
                part.blake3,
                actual_hash
            ));
        }

        let dec_path = restore_dir.join(format!("part-{}.btrfs", part.part_number));
        decrypt_file(&enc_path, &dec_path, private_key_str).await?;
        let _ = tokio::fs::remove_file(&enc_path).await;
        dec_paths.push(dec_path);
    }
    Ok(dec_paths)
}

async fn pipe_to_btrfs_receive(
    dec_paths: &[std::path::PathBuf],
    target_dir: &Path,
) -> Result<String> {
    tokio::fs::create_dir_all(target_dir).await?;
    let target_str = target_dir
        .to_str()
        .ok_or_else(|| anyhow!("target_dir contains invalid UTF-8"))?;

    let mut child = tokio::process::Command::new("btrfs")
        .args(["receive", target_str])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stream_hash = {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 65536];
        for dec_path in dec_paths {
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

    Ok(stream_hash)
}

pub async fn restore_task<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    date: &str,
    target_dir: &Path,
    private_key_str: &str,
    remote: &R,
) -> Result<()> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let timestamp = compact_to_iso(date)?;

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
        let dec_paths = download_and_decrypt_parts(
            &backup,
            &sanitized,
            &restore_dir,
            private_key_str,
            &config.s3.prefix,
            remote,
        )
        .await?;

        let stream_hash = pipe_to_btrfs_receive(&dec_paths, target_dir).await?;

        if stream_hash != backup.blake3 {
            return Err(anyhow!(
                "stream BLAKE3 mismatch: expected {}, got {}",
                backup.blake3,
                stream_hash
            ));
        }

        Ok(())
    }
    .await;

    let _ = tokio::fs::remove_dir_all(&restore_dir).await;
    result
}
