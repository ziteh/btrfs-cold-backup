use anyhow::{Result, anyhow};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, encrypt_file},
    manifest::{self, Backup, CompletedPart, Last, PartInfo},
    util::{
        backup_output_dir, s3_backup_key, s3_last_manifest_key, s3_manifest_key,
        sanitize_subvolume_path, state_path,
    },
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

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub async fn backup_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
) -> Result<Backup> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;

    // Load or create resume state
    let sp = state_path(&config.base_dir, &sanitized);
    tokio::fs::create_dir_all(sp.parent().unwrap()).await?;

    let mut state = match manifest::read_state(&sp)? {
        Some(s) if s.task_name == task.name && s.level == level => {
            tracing::info!("resuming from existing backup state");
            s
        }
        Some(_) => {
            tracing::info!("existing state is for a different task/level, starting fresh");
            manifest::BackupState::default()
        }
        None => manifest::BackupState::default(),
    };

    // Phase 0: determine snapshot, timestamp, parent, output_dir
    let snapshot_name: String;
    let timestamp: String;
    let parent: Option<String>;
    let output_dir: PathBuf;

    if !state.snapshot_name.is_empty() {
        snapshot_name = state.snapshot_name.clone();
        timestamp = state.timestamp.clone();
        parent = state.parent_snapshot.clone();
        output_dir = PathBuf::from(&state.output_dir);
    } else {
        let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);
        let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
        let level_prefix = format!("bcb_level{level}_");
        let sn = snapshots
            .iter()
            .rfind(|s| s.starts_with(&level_prefix))
            .ok_or_else(|| anyhow!("no level {level} snapshot found in {:?}", task.snapshot_dir))?
            .clone();
        let ts = snapshot_timestamp(&sn)?;
        let par = if level == 0 {
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
        let od = backup_output_dir(&config.base_dir, &sanitized, level, &ts)?;

        state.task_name = task.name.clone();
        state.level = level;
        state.snapshot_name = sn.clone();
        state.parent_snapshot = par.clone();
        state.timestamp = ts.clone();
        state.output_dir = od.to_string_lossy().to_string();
        state.last_updated = now_unix();
        manifest::write_state(&sp, &state)?;

        snapshot_name = sn;
        timestamp = ts;
        parent = par;
        output_dir = od;
    }

    // Phase 1: btrfs send + split (skip if already done)
    let stream_hash = if let Some(ref h) = state.stream_hash {
        tracing::info!("btrfs send already done, skipping");
        h.clone()
    } else {
        if output_dir.exists() {
            tokio::fs::remove_dir_all(&output_dir).await?;
        }
        tokio::fs::create_dir_all(&output_dir).await?;

        let snapshot_path = task.snapshot_dir.join(&snapshot_name);
        let mut stream = match &parent {
            Some(p) => {
                btrfs
                    .send_incremental(&task.snapshot_dir.join(p.as_str()), &snapshot_path)
                    .await?
            }
            None => btrfs.send_full(&snapshot_path).await?,
        };

        let part_prefix = output_dir.join("snapshot.part-");
        let part_prefix_str = part_prefix
            .to_str()
            .ok_or_else(|| anyhow!("invalid part prefix path"))?;
        let mut split_child = tokio::process::Command::new("split")
            .args(["-b", "3G", "-d", "-a", "6", "-", part_prefix_str])
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let hash = {
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
            return Err(anyhow!(
                "split failed: {}",
                String::from_utf8_lossy(&split_out.stderr).trim()
            ));
        }

        let mut total_parts = 0u32;
        let mut count_dir = tokio::fs::read_dir(&output_dir).await?;
        while let Some(entry) = count_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("snapshot.part-") && !name.ends_with(".age") {
                total_parts += 1;
            }
        }

        state.total_parts = Some(total_parts);
        state.stream_hash = Some(hash.clone());
        state.last_updated = now_unix();
        manifest::write_state(&sp, &state)?;
        hash
    };

    // Phase 2: collect part suffixes from output_dir
    let mut part_suffixes: BTreeSet<String> = BTreeSet::new();
    if output_dir.exists() {
        let mut read_dir = tokio::fs::read_dir(&output_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            let base = name.strip_suffix(".age").unwrap_or(name.as_str());
            if let Some(suffix) = base.strip_prefix("snapshot.part-") {
                part_suffixes.insert(suffix.to_string());
            }
        }
    }

    if let Some(total) = state.total_parts {
        let accounted = state.parts_completed.len() + part_suffixes.len();
        if accounted < total as usize {
            return Err(anyhow!(
                "expected {total} parts but only found {accounted} \
                 ({} on disk, {} already uploaded); \
                 output_dir may have been cleared — restore missing parts and re-run",
                part_suffixes.len(),
                state.parts_completed.len()
            ));
        }
    }

    if part_suffixes.is_empty() && state.parts_completed.is_empty() {
        if state.stream_hash.is_some() {
            state.stream_hash = None;
            state.total_parts = None;
            state.last_updated = now_unix();
            manifest::write_state(&sp, &state)?;
            return Err(anyhow!(
                "output_dir vanished after split; state cleared — re-run to retry from split"
            ));
        }
        return Err(anyhow!("no snapshot parts found in {:?}", output_dir));
    }

    // Phase 3: encrypt + upload each part
    let storage_class = config
        .s3
        .storage_class
        .backup_data
        .get(level as usize)
        .map(String::as_str)
        .unwrap_or("STANDARD");

    let mut parts_info: Vec<PartInfo> = Vec::new();
    let mut total_enc_size: u64 = 0;

    // Include already-completed parts from state
    for (&part_number, completed) in &state.parts_completed {
        total_enc_size += completed.size_bytes;
        parts_info.push(PartInfo {
            part_number,
            size_bytes: completed.size_bytes,
            sha256: completed.hash.clone(),
        });
    }

    // Process remaining parts still on disk
    for suffix in &part_suffixes {
        let part_number: u32 = suffix
            .parse::<u32>()
            .map_err(|_| anyhow!("invalid part suffix: {suffix}"))?
            + 1;

        if state.parts_completed.contains_key(&part_number) {
            continue;
        }

        let raw_path = output_dir.join(format!("snapshot.part-{suffix}"));
        let enc_path = output_dir.join(format!("snapshot.part-{suffix}.age"));

        if enc_path.exists() && raw_path.exists() {
            // enc_path is from a crashed partial run; discard it and re-encrypt
            tokio::fs::remove_file(&enc_path).await?;
        }
        if !enc_path.exists() {
            encrypt_file(&raw_path, &enc_path, &config.age_public_key).await?;
            let _ = tokio::fs::remove_file(&raw_path).await;
        }

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

        state.parts_completed.insert(
            part_number,
            CompletedPart {
                size_bytes: enc_size,
                hash: hash.clone(),
            },
        );
        state.last_updated = now_unix();
        manifest::write_state(&sp, &state)?;

        let _ = tokio::fs::remove_file(&enc_path).await;

        total_enc_size += enc_size;
        parts_info.push(PartInfo {
            part_number,
            size_bytes: enc_size,
            sha256: hash,
        });
    }

    parts_info.sort_by_key(|p| p.part_number);

    let backup = Backup {
        timestamp: timestamp.clone(),
        level,
        snapshot_name: snapshot_name.clone(),
        parent_snapshot: parent,
        total_size_bytes: total_enc_size,
        sha256: stream_hash,
        parts: parts_info,
    };

    // Phase 4: upload task manifest
    if !state.manifest_uploaded {
        let manifest_class = &config.s3.storage_class.manifest;
        let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp)?;
        remote
            .upload_bytes(
                &manifest_key,
                serde_yaml::to_string(&backup)?.into_bytes(),
                manifest_class,
            )
            .await?;

        state.manifest_uploaded = true;
        state.last_updated = now_unix();
        manifest::write_state(&sp, &state)?;
    }

    // Phase 5: upload last manifest (always — authoritative pointer)
    let last = Last {
        timestamp,
        level,
        snapshot_name,
    };
    let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);
    let manifest_class = &config.s3.storage_class.manifest;
    remote
        .upload_bytes(
            &last_key,
            serde_yaml::to_string(&last)?.into_bytes(),
            manifest_class,
        )
        .await?;

    // Phase 6: cleanup
    let _ = tokio::fs::remove_dir_all(&output_dir).await;
    let _ = tokio::fs::remove_file(&sp).await;

    Ok(backup)
}
