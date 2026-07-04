use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_file},
    manifest::Backup,
    remote::RestoreStatus,
    util::{IO_BUF_SIZE, compact_to_iso, s3_backup_key, s3_manifest_key, sanitize_subvolume_path},
};

/// Retrieval tier and lifetime for archived (Glacier / Deep Archive) objects.
pub struct RetrievalOptions {
    pub days: i32,
    pub tier: String,
}

/// Ensures every key is downloadable, requesting archive retrievals where needed.
///
/// Archive retrievals take hours, so this does not wait: if any key is not yet ready it
/// returns an error telling the operator to re-run once the retrievals complete.
async fn ensure_retrievable<R: RemoteBackend>(
    remote: &R,
    keys: &[String],
    opts: &RetrievalOptions,
) -> Result<()> {
    let mut requested = 0usize;
    let mut in_progress = 0usize;
    for key in keys {
        match remote.restore_status(key).await? {
            RestoreStatus::Ready => {}
            RestoreStatus::InProgress => in_progress += 1,
            RestoreStatus::NotRequested => {
                remote.request_restore(key, opts.days, &opts.tier).await?;
                requested += 1;
            }
        }
    }
    if requested > 0 || in_progress > 0 {
        return Err(anyhow!(
            "{requested} archive retrieval(s) initiated, {in_progress} still in progress; \
             archived objects take hours to become downloadable — re-run this restore once \
             the retrievals complete"
        ));
    }
    Ok(())
}

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
            &part.index,
        )?;

        let enc_path = restore_dir.join(format!("part-{}.age", part.index));
        remote.download_to_file(&part_key, &enc_path).await?;

        let actual_hash = blake3_file(&enc_path).await?;
        if actual_hash != part.blake3 {
            let _ = tokio::fs::remove_file(&enc_path).await;
            return Err(anyhow!(
                "BLAKE3 mismatch for part {}: expected {}, got {}",
                part.index,
                part.blake3,
                actual_hash
            ));
        }

        let dec_path = restore_dir.join(format!("part-{}.btrfs", part.index));
        decrypt_file(&enc_path, &dec_path, private_key_str).await?;
        let _ = tokio::fs::remove_file(&enc_path).await;
        dec_paths.push(dec_path);
    }
    Ok(dec_paths)
}

async fn pipe_to_btrfs_receive(dec_paths: &[std::path::PathBuf], target_dir: &Path) -> Result<()> {
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

    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
        let mut buf = vec![0u8; IO_BUF_SIZE];
        for dec_path in dec_paths {
            let mut f = tokio::fs::File::open(dec_path).await?;
            loop {
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                stdin.write_all(&buf[..n]).await?;
            }
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

/// Verifies the whole-stream BLAKE3 across the decrypted parts in order, BEFORE anything is
/// piped to btrfs receive — a corrupt stream must never be written to the target.
async fn verify_stream_hash(dec_paths: &[std::path::PathBuf], expected: &str) -> Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; IO_BUF_SIZE];
    for dec_path in dec_paths {
        let mut f = tokio::fs::File::open(dec_path).await?;
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }
    let actual = hasher.finalize().to_hex().to_string();
    if actual != expected {
        return Err(anyhow!(
            "stream BLAKE3 mismatch: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn restore_task<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    date: &str,
    target_dir: &Path,
    private_key_str: &str,
    retrieval: &RetrievalOptions,
    remote: &R,
) -> Result<()> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let timestamp = compact_to_iso(date)?;

    let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp)?;
    ensure_retrievable(remote, std::slice::from_ref(&manifest_key), retrieval).await?;
    let backup: Backup = serde_yaml::from_slice(&remote.download(&manifest_key).await?)?;

    let part_keys = backup
        .parts
        .iter()
        .map(|p| s3_backup_key(&config.s3.prefix, &sanitized, level, &timestamp, &p.index))
        .collect::<Result<Vec<_>>>()?;
    ensure_retrievable(remote, &part_keys, retrieval).await?;

    // Unique temp dir per invocation; prevents concurrent restores from colliding.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
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

        verify_stream_hash(&dec_paths, &backup.blake3).await?;

        pipe_to_btrfs_receive(&dec_paths, target_dir).await?;

        let snapshot_path = target_dir.join(&backup.snapshot_name);
        let verify = tokio::process::Command::new("btrfs")
            .args(["subvolume", "show"])
            .arg(&snapshot_path)
            .output()
            .await?;
        if !verify.status.success() {
            return Err(anyhow!(
                "snapshot '{}' not found after restore",
                backup.snapshot_name
            ));
        }

        Ok(())
    }
    .await;

    if let Err(e) = tokio::fs::remove_dir_all(&restore_dir).await {
        tracing::warn!("failed to clean up restore dir {:?}: {e}", restore_dir);
    }
    result
}
