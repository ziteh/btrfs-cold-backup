use anyhow::{Result, anyhow};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, encrypt_file},
    manifest::{Backup, BackupState, CompletedPart, Last, LastEntry, PartInfo, read_state},
    util::{
        backup_output_dir, s3_backup_key, s3_last_manifest_key, s3_manifest_key,
        sanitize_subvolume_path, state_path,
    },
};

// parts: ["bcb", "level{N}", "{date}", "{time}"]
fn snapshot_timestamp(name: &str) -> Result<String> {
    let parts: Vec<&str> = name.split('_').collect();
    let [_, _, date, time, ..] = parts.as_slice() else {
        return Err(anyhow!("invalid snapshot name: {name}"));
    };
    let iso_date = match date.len() {
        8 => format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8]), // YYYYMMDD
        10 => date.to_string(),                                          // YYYY-MM-DD
        _ => return Err(anyhow!("unrecognized date in snapshot name: {name}")),
    };
    Ok(format!("{}T{}:00Z", iso_date, time.replace('-', ":")))
}

struct PersistentState {
    inner: BackupState,
    path: PathBuf,
}

impl PersistentState {
    fn load_or_init(path: PathBuf, task_name: &str, level: u32) -> Result<Self> {
        let inner = match read_state(&path)? {
            Some(s) if s.task_name == task_name && s.level == level => {
                tracing::info!("resuming from existing backup state");
                s
            }
            Some(_) => {
                tracing::info!("existing state is for a different task/level, starting fresh");
                BackupState::default()
            }
            None => BackupState::default(),
        };
        Ok(Self { inner, path })
    }

    fn save(&mut self) -> Result<()> {
        self.inner.save(&self.path)
    }
}

struct BackupPlan {
    snapshot_name: String,
    timestamp: String,
    parent_snapshot: Option<String>,
    output_dir: PathBuf,
}

async fn init_backup_plan<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
    ps: &mut PersistentState,
) -> Result<BackupPlan> {
    if !ps.inner.snapshot_name.is_empty() {
        return Ok(BackupPlan {
            snapshot_name: ps.inner.snapshot_name.clone(),
            timestamp: ps.inner.timestamp.clone(),
            parent_snapshot: ps.inner.parent_snapshot.clone(),
            output_dir: ps.inner.output_dir.clone(),
        });
    }

    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
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
    } else if remote.head(&last_key).await?.is_none() {
        return Err(anyhow!(
            "no previous backup; level {level} requires a prior level 0 backup"
        ));
    } else {
        let last: Last = serde_yaml::from_slice(&remote.download(&last_key).await?)?;
        let parent_entry = last.get(&(level - 1)).ok_or_else(|| {
            anyhow!(
                "no level {} entry in last manifest; run a level {} backup first",
                level - 1,
                level - 1
            )
        })?;
        if !snapshots.contains(&parent_entry.snapshot_name) {
            return Err(anyhow!(
                "parent snapshot '{}' not found locally",
                parent_entry.snapshot_name
            ));
        }
        Some(parent_entry.snapshot_name.clone())
    };
    let od = backup_output_dir(&config.base_dir, &sanitized, level, &ts)?;

    ps.inner.task_name = task.name.clone();
    ps.inner.level = level;
    ps.inner.snapshot_name = sn.clone();
    ps.inner.parent_snapshot = par.clone();
    ps.inner.timestamp = ts.clone();
    ps.inner.output_dir = od.clone();
    ps.save()?;

    Ok(BackupPlan {
        snapshot_name: sn,
        timestamp: ts,
        parent_snapshot: par,
        output_dir: od,
    })
}

// Test-only pause point: parks here, with both the btrfs-send and split children already
// spawned and alive, so the VM e2e suite can SIGKILL bcb at a deterministic point to exercise
// orphaned-child-process handling (see util::die_with_parent) instead of racing on sleep timing.
#[cfg(debug_assertions)]
async fn test_pause_during_send() {
    if std::env::var("BCB_TEST_PAUSE_DURING_SEND").is_ok() {
        use std::io::Write;
        eprintln!("BCB_TEST_PAUSE_REACHED:during_send");
        let _ = std::io::stderr().flush();
        std::future::pending::<()>().await;
    }
}

#[cfg(not(debug_assertions))]
async fn test_pause_during_send() {}

async fn send_and_split<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    snapshot_name: &str,
    parent: &Option<String>,
    output_dir: &Path,
    ps: &mut PersistentState,
) -> Result<String> {
    if let Some(ref h) = ps.inner.stream_hash {
        tracing::info!("btrfs send already done, skipping");
        return Ok(h.clone());
    }

    if output_dir.exists() {
        tokio::fs::remove_dir_all(output_dir).await?;
    }
    tokio::fs::create_dir_all(output_dir).await?;

    let snapshot_path = task.snapshot_dir.join(snapshot_name);
    let mut stream = match parent {
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
    let mut split_cmd = tokio::process::Command::new("split");
    split_cmd
        .args(["-b", "3G", "-d", "-a", "6", "-", part_prefix_str])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        split_cmd.pre_exec(crate::util::die_with_parent);
    }
    let mut split_child = split_cmd.spawn()?;
    test_pause_during_send().await;

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
    let mut count_dir = tokio::fs::read_dir(output_dir).await?;
    while let Some(entry) = count_dir.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("snapshot.part-") && !name.ends_with(".age") {
            total_parts += 1;
        }
    }

    ps.inner.total_parts = Some(total_parts);
    ps.inner.stream_hash = Some(hash.clone());
    ps.save()?;

    Ok(hash)
}

async fn collect_disk_parts(output_dir: &Path) -> Result<BTreeSet<String>> {
    let mut part_suffixes = BTreeSet::new();
    if !output_dir.exists() {
        return Ok(part_suffixes);
    }
    let mut read_dir = tokio::fs::read_dir(output_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        let base = name.strip_suffix(".age").unwrap_or(name.as_str());
        if let Some(suffix) = base.strip_prefix("snapshot.part-") {
            part_suffixes.insert(suffix.to_string());
        }
    }
    Ok(part_suffixes)
}

fn validate_part_coverage(part_suffixes: &BTreeSet<String>, state: &BackupState) -> Result<()> {
    let Some(total) = state.total_parts else {
        return Ok(());
    };
    let mut all: BTreeSet<&str> = part_suffixes.iter().map(String::as_str).collect();
    for k in state.parts_completed.keys() {
        all.insert(k.as_str());
    }
    let found = all.len();
    if found < total as usize {
        return Err(anyhow!(
            "expected {total} parts but only found {found} \
             ({} on disk, {} already uploaded); \
             output_dir may have been cleared — restore missing parts and re-run",
            part_suffixes.len(),
            state.parts_completed.len()
        ));
    }
    Ok(())
}

async fn encrypt_and_upload_part<R: RemoteBackend>(
    remote: &R,
    suffix: &str,
    output_dir: &Path,
    sanitized: &str,
    level: u32,
    timestamp: &str,
    config: &Config,
) -> Result<CompletedPart> {
    let raw_path = output_dir.join(format!("snapshot.part-{suffix}"));
    let enc_path = output_dir.join(format!("snapshot.part-{suffix}.age"));

    if enc_path.exists() && raw_path.exists() {
        // coexisting raw+enc means encryption was interrupted; enc may be truncated
        tokio::fs::remove_file(&enc_path).await?;
    }
    if !enc_path.exists() {
        encrypt_file(&raw_path, &enc_path, &config.age_public_key).await?;
        let _ = tokio::fs::remove_file(&raw_path).await;
    }

    let enc_size = tokio::fs::metadata(&enc_path).await?.len();
    let hash = blake3_file(&enc_path).await?;

    let storage_class = config
        .s3
        .storage_class
        .backup_data
        .get(&level)
        .map(String::as_str)
        .unwrap_or("STANDARD");
    let part_key = s3_backup_key(&config.s3.prefix, sanitized, level, timestamp, suffix)?;
    remote
        .upload_file(&part_key, &enc_path, storage_class)
        .await?;
    let _ = tokio::fs::remove_file(&enc_path).await;

    Ok(CompletedPart {
        size_bytes: enc_size,
        hash,
    })
}

pub async fn backup_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
) -> Result<Backup> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let sp = state_path(&config.base_dir, &sanitized);
    tokio::fs::create_dir_all(sp.parent().unwrap()).await?;

    let mut ps = PersistentState::load_or_init(sp, &task.name, level)?;

    let plan = init_backup_plan(config, task, level, btrfs, remote, &mut ps).await?;

    let stream_hash = send_and_split(
        btrfs,
        task,
        &plan.snapshot_name,
        &plan.parent_snapshot,
        &plan.output_dir,
        &mut ps,
    )
    .await?;

    let part_suffixes = collect_disk_parts(&plan.output_dir).await?;

    if part_suffixes.is_empty() && ps.inner.parts_completed.is_empty() {
        if ps.inner.stream_hash.is_some() {
            ps.inner.stream_hash = None;
            ps.inner.total_parts = None;
            ps.save()?;
            return Err(anyhow!(
                "output_dir vanished after split; state cleared — re-run to retry from split"
            ));
        }
        return Err(anyhow!("no snapshot parts found in {:?}", plan.output_dir));
    }

    validate_part_coverage(&part_suffixes, &ps.inner)?;

    for suffix in &part_suffixes {
        if ps.inner.parts_completed.contains_key(suffix.as_str()) {
            continue;
        }
        let completed = encrypt_and_upload_part(
            remote,
            suffix,
            &plan.output_dir,
            &sanitized,
            level,
            &plan.timestamp,
            config,
        )
        .await?;
        ps.inner.parts_completed.insert(suffix.clone(), completed);
        ps.save()?;
    }

    let mut parts_info: Vec<PartInfo> = ps
        .inner
        .parts_completed
        .iter()
        .map(|(idx, c)| PartInfo {
            index: idx.clone(),
            size_bytes: c.size_bytes,
            blake3: c.hash.clone(),
        })
        .collect();
    parts_info.sort_by(|a, b| a.index.cmp(&b.index));
    let total_enc_size: u64 = parts_info.iter().map(|p| p.size_bytes).sum();

    let backup = Backup {
        timestamp: plan.timestamp.clone(),
        level,
        snapshot_name: plan.snapshot_name.clone(),
        parent_snapshot: plan.parent_snapshot.clone(),
        total_size_bytes: total_enc_size,
        blake3: stream_hash,
        parts: parts_info,
    };

    if !ps.inner.manifest_uploaded {
        let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &plan.timestamp)?;
        remote
            .upload_bytes(
                &manifest_key,
                serde_yaml::to_string(&backup)?.into_bytes(),
                &config.s3.storage_class.manifest,
            )
            .await?;
        ps.inner.manifest_uploaded = true;
        ps.save()?;
    }

    // Merge current level's entry into the per-level map and re-upload.
    let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);
    let mut last: Last = match remote.head(&last_key).await? {
        Some(_) => serde_yaml::from_slice(&remote.download(&last_key).await?).unwrap_or_default(),
        None => Last::default(),
    };
    last.insert(
        level,
        LastEntry {
            timestamp: plan.timestamp,
            snapshot_name: plan.snapshot_name,
        },
    );
    remote
        .upload_bytes(
            &last_key,
            serde_yaml::to_string(&last)?.into_bytes(),
            &config.s3.storage_class.manifest,
        )
        .await?;

    let _ = tokio::fs::remove_dir_all(&plan.output_dir).await;
    let _ = tokio::fs::remove_file(&ps.path).await;

    Ok(backup)
}
