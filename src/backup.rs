use anyhow::{Result, anyhow};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, encrypt_file},
    manifest::{
        Backup, BackupState, CompletedPart, Last, LastEntry, PartInfo, SystemInfo, read_state,
    },
    util::{
        IO_BUF_SIZE, SNAPSHOT_PART_PREFIX, backup_output_dir, s3_backup_key, s3_last_manifest_key,
        s3_manifest_key, sanitize_subvolume_path, state_path,
    },
};

// Parses btrbk's snapshot naming (the optional _<n> is btrbk's same-minute collision
// counter); the expected format is spelled out in the error message below.
fn snapshot_timestamp(name: &str) -> Result<(String, Option<String>)> {
    let err = || {
        anyhow!(
            "invalid snapshot name '{name}': expected bcb_level<N>.<YYYYMMDD>T<hhmm> \
             (btrbk snapshot_name bcb_level<N> with timestamp_format long)"
        )
    };
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());

    let (prefix, ts) = name.split_once('.').ok_or_else(err)?;
    if !prefix.starts_with("bcb_level") {
        return Err(err());
    }
    let (date, rest) = ts.split_once('T').ok_or_else(err)?;
    let mut segments = rest.split('_');
    let time = segments.next().ok_or_else(err)?;
    let collision: Vec<&str> = segments.collect();
    if date.len() != 8
        || !all_digits(date)
        || time.len() != 4
        || !all_digits(time)
        || !collision.iter().all(|s| all_digits(s))
    {
        return Err(err());
    }
    let iso = format!(
        "{}-{}-{}T{}:{}:00Z",
        &date[0..4],
        &date[4..6],
        &date[6..8],
        &time[0..2],
        &time[2..4]
    );
    // The collision suffix is threaded into the S3 key so a second same-minute snapshot
    // gets its own backup instead of silently colliding with the first one's manifest key.
    let suffix = (!collision.is_empty()).then(|| collision.join("_"));
    Ok((iso, suffix))
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
    collision_suffix: Option<String>,
    parent_snapshot: Option<String>,
    output_dir: PathBuf,
}

/// Determines the incremental parent for a level > 0 backup from the last manifest;
/// level 0 has none.
async fn resolve_parent<R: RemoteBackend>(
    remote: &R,
    last_key: &str,
    snapshots: &[String],
    level: u32,
    snapshot_name: &str,
    timestamp: &str,
) -> Result<Option<String>> {
    if level == 0 {
        return Ok(None);
    }
    if !remote.exists(last_key).await? {
        return Err(anyhow!(
            "no previous backup; level {level} requires a prior level 0 backup"
        ));
    }
    let last: Last = serde_yaml::from_slice(&remote.download(last_key).await?)?;
    let parent = last.get(&(level - 1)).ok_or_else(|| {
        anyhow!(
            "no level {} entry in last manifest; run a level {} backup first",
            level - 1,
            level - 1
        )
    })?;
    if !snapshots.contains(&parent.snapshot_name) {
        return Err(anyhow!(
            "parent snapshot '{}' not found locally",
            parent.snapshot_name
        ));
    }
    if timestamp <= parent.timestamp.as_str() {
        return Err(anyhow!(
            "snapshot '{snapshot_name}' ({timestamp}) is not newer than its parent '{}' ({}); \
             create a fresh level {level} snapshot before backing up",
            parent.snapshot_name,
            parent.timestamp
        ));
    }
    Ok(Some(parent.snapshot_name.clone()))
}

/// Returns None when the newest matching snapshot is already backed up (skip).
async fn init_backup_plan<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
    state: &mut PersistentState,
) -> Result<Option<BackupPlan>> {
    if !state.inner.snapshot_name.is_empty() {
        return Ok(Some(BackupPlan {
            snapshot_name: state.inner.snapshot_name.clone(),
            timestamp: state.inner.timestamp.clone(),
            collision_suffix: state.inner.collision_suffix.clone(),
            parent_snapshot: state.inner.parent_snapshot.clone(),
            output_dir: state.inner.output_dir.clone(),
        }));
    }

    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
    let level_prefix = format!("bcb_level{level}.");
    let snapshot_name = snapshots
        .iter()
        .rfind(|s| s.starts_with(&level_prefix))
        .ok_or_else(|| anyhow!("no level {level} snapshot found in {:?}", task.snapshot_dir))?
        .clone();
    let (timestamp, collision_suffix) = snapshot_timestamp(&snapshot_name)?;

    // A manifest already on remote means this snapshot is fully backed up: skip rather than
    // redo it — overwriting archived objects incurs minimum-storage-duration charges, and a
    // skip keeps re-runs idempotent so one completed task never blocks the others.
    let manifest_key = s3_manifest_key(
        &config.s3.prefix,
        &sanitized,
        level,
        &timestamp,
        collision_suffix.as_deref(),
    )?;
    if remote.exists(&manifest_key).await? {
        tracing::info!(
            "snapshot '{snapshot_name}' is already backed up at level {level}, skipping"
        );
        return Ok(None);
    }

    let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);
    let parent_snapshot = resolve_parent(
        remote,
        &last_key,
        &snapshots,
        level,
        &snapshot_name,
        &timestamp,
    )
    .await?;
    let output_dir = backup_output_dir(
        &config.base_dir,
        &sanitized,
        level,
        &timestamp,
        collision_suffix.as_deref(),
    )?;

    state.inner.task_name = task.name.clone();
    state.inner.level = level;
    state.inner.snapshot_name = snapshot_name.clone();
    state.inner.parent_snapshot = parent_snapshot.clone();
    state.inner.timestamp = timestamp.clone();
    state.inner.output_dir = output_dir.clone();
    state.inner.collision_suffix = collision_suffix.clone();
    state.save()?;

    Ok(Some(BackupPlan {
        snapshot_name,
        timestamp,
        collision_suffix,
        parent_snapshot,
        output_dir,
    }))
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

// Test-only override so multi-part backups can be exercised without multi-GiB fixtures.
#[cfg(debug_assertions)]
fn split_part_size() -> String {
    std::env::var("BCB_TEST_SPLIT_SIZE").unwrap_or_else(|_| "3G".to_string())
}

#[cfg(not(debug_assertions))]
fn split_part_size() -> String {
    "3G".to_string()
}

async fn send_and_split<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    snapshot_name: &str,
    parent: &Option<String>,
    output_dir: &Path,
    state: &mut PersistentState,
) -> Result<String> {
    if let Some(ref h) = state.inner.stream_hash {
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

    let part_prefix = output_dir.join(SNAPSHOT_PART_PREFIX);
    let part_prefix_str = part_prefix
        .to_str()
        .ok_or_else(|| anyhow!("invalid part prefix path"))?;
    let mut split_cmd = tokio::process::Command::new("split");
    split_cmd
        .args([
            "-b",
            &split_part_size(),
            "-d",
            "-a",
            "6",
            "-",
            part_prefix_str,
        ])
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
        let mut buf = vec![0u8; IO_BUF_SIZE];
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

    let total_parts = collect_disk_parts(output_dir).await?.len() as u32;

    state.inner.total_parts = Some(total_parts);
    state.inner.stream_hash = Some(hash.clone());
    state.save()?;

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
        if let Some(suffix) = base.strip_prefix(SNAPSHOT_PART_PREFIX) {
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

#[allow(clippy::too_many_arguments)]
async fn encrypt_and_upload_part<R: RemoteBackend>(
    remote: &R,
    part_index: &str,
    output_dir: &Path,
    sanitized: &str,
    level: u32,
    timestamp: &str,
    collision_suffix: Option<&str>,
    config: &Config,
) -> Result<CompletedPart> {
    let raw_path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{part_index}"));
    let enc_path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{part_index}.age"));

    if enc_path.exists() && raw_path.exists() {
        // coexisting raw+enc means encryption was interrupted; enc may be truncated
        tokio::fs::remove_file(&enc_path).await?;
    }
    if !enc_path.exists() {
        encrypt_file(&raw_path, &enc_path, &config.age_public_key).await?;
        if let Err(e) = tokio::fs::remove_file(&raw_path).await {
            tracing::warn!("failed to remove raw part {:?}: {e}", raw_path);
        }
    }

    let enc_size = tokio::fs::metadata(&enc_path).await?.len();
    let hash = blake3_file(&enc_path).await?;

    let storage_class = config
        .s3
        .storage_class
        .backup_data
        .get(level as usize)
        .map(String::as_str)
        .ok_or_else(|| {
            anyhow!(
                "no storage class configured for level {level} \
                 (s3.storage_class.backup_data has {} entries)",
                config.s3.storage_class.backup_data.len()
            )
        })?;
    let part_key = s3_backup_key(
        &config.s3.prefix,
        sanitized,
        level,
        timestamp,
        collision_suffix,
        part_index,
    )?;
    remote
        .upload_file(
            &part_key,
            &enc_path,
            storage_class,
            &format!("bcb-backup-level={level}"),
        )
        .await?;
    if let Err(e) = tokio::fs::remove_file(&enc_path).await {
        tracing::warn!("failed to remove uploaded part {:?}: {e}", enc_path);
    }

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
) -> Result<Option<Backup>> {
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let state_file = state_path(&config.base_dir, &sanitized);
    tokio::fs::create_dir_all(state_file.parent().unwrap()).await?;

    let mut state = PersistentState::load_or_init(state_file, &task.name, level)?;

    let Some(plan) = init_backup_plan(config, task, level, btrfs, remote, &mut state).await? else {
        return Ok(None);
    };

    let stream_hash = send_and_split(
        btrfs,
        task,
        &plan.snapshot_name,
        &plan.parent_snapshot,
        &plan.output_dir,
        &mut state,
    )
    .await?;

    let part_suffixes = collect_disk_parts(&plan.output_dir).await?;

    if part_suffixes.is_empty() && state.inner.parts_completed.is_empty() {
        if state.inner.stream_hash.is_some() {
            state.inner.stream_hash = None;
            state.inner.total_parts = None;
            state.save()?;
            return Err(anyhow!(
                "output_dir vanished after split; state cleared — re-run to retry from split"
            ));
        }
        return Err(anyhow!("no snapshot parts found in {:?}", plan.output_dir));
    }

    validate_part_coverage(&part_suffixes, &state.inner)?;

    for suffix in &part_suffixes {
        if state.inner.parts_completed.contains_key(suffix.as_str()) {
            continue;
        }
        let completed = encrypt_and_upload_part(
            remote,
            suffix,
            &plan.output_dir,
            &sanitized,
            level,
            &plan.timestamp,
            plan.collision_suffix.as_deref(),
            config,
        )
        .await?;
        state
            .inner
            .parts_completed
            .insert(suffix.clone(), completed);
        state.save()?;
    }

    let mut parts_info: Vec<PartInfo> = state
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
        created_at: crate::util::now_unix(),
        level,
        snapshot_name: plan.snapshot_name.clone(),
        parent_snapshot: plan.parent_snapshot.clone(),
        system: SystemInfo {
            os: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            btrfs_version: crate::btrfs::btrfs_version().await,
            bcb_version: env!("CARGO_PKG_VERSION").to_string(),
        },
        age_public_key: config.age_public_key.clone(),
        total_size_bytes: total_enc_size,
        blake3: stream_hash,
        parts: parts_info,
    };

    if !state.inner.manifest_uploaded {
        let manifest_key = s3_manifest_key(
            &config.s3.prefix,
            &sanitized,
            level,
            &plan.timestamp,
            plan.collision_suffix.as_deref(),
        )?;
        remote
            .upload_bytes(
                &manifest_key,
                serde_yaml::to_string(&backup)?.into_bytes(),
                &config.s3.storage_class.manifest,
                "bcb-backup-level=manifest",
            )
            .await?;
        state.inner.manifest_uploaded = true;
        state.save()?;
    }

    // Merge current level's entry into the per-level map and re-upload.
    let last_key = s3_last_manifest_key(&config.s3.prefix, &sanitized);
    let mut last: Last = if remote.exists(&last_key).await? {
        // A corrupted last manifest must not be silently reset: dropping other levels'
        // entries would break subsequent incremental backups without warning.
        serde_yaml::from_slice(&remote.download(&last_key).await?).map_err(|e| {
            anyhow!("failed to parse last manifest {last_key}: {e}; repair or delete it manually")
        })?
    } else {
        Last::default()
    };
    // A fresh level-N backup invalidates every deeper level's incremental chain (they were
    // built on the level-N snapshot this run just superseded); drop them so the next run of
    // a deeper level fails loudly asking for a fresh parent, instead of silently chaining
    // onto a since-superseded snapshot that may no longer even be retained.
    last.retain(|&existing_level, _| existing_level <= level);
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
            "bcb-backup-level=manifest",
        )
        .await?;

    if let Err(e) = tokio::fs::remove_dir_all(&plan.output_dir).await {
        tracing::warn!("failed to clean up output dir {:?}: {e}", plan.output_dir);
    }
    if let Err(e) = tokio::fs::remove_file(&state.path).await {
        tracing::warn!("failed to remove state file {:?}: {e}", state.path);
    }

    Ok(Some(backup))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_timestamp_valid_name() {
        let (ts, suffix) = snapshot_timestamp("bcb_level1.20260602T1430").unwrap();
        assert_eq!(ts, "2026-06-02T14:30:00Z");
        assert!(suffix.is_none());
    }

    #[test]
    fn test_snapshot_timestamp_collision_suffix() {
        let (ts, suffix) = snapshot_timestamp("bcb_level0.20260602T1430_1").unwrap();
        assert_eq!(ts, "2026-06-02T14:30:00Z");
        assert_eq!(suffix.as_deref(), Some("1"));
    }

    #[test]
    fn test_snapshot_timestamp_invalid_names() {
        assert!(snapshot_timestamp("bcb_level0").is_err());
        // btrbk timestamp_format short (date only) is not supported
        assert!(snapshot_timestamp("bcb_level0.20260602").is_err());
        // underscore instead of btrbk's T separator
        assert!(snapshot_timestamp("bcb_level0.20260602_1430").is_err());
        assert!(snapshot_timestamp("bcb_level0_2026-06-02_14-30").is_err());
        assert!(snapshot_timestamp("bcb_level0.2026-06-02T1430").is_err());
        assert!(snapshot_timestamp("bcb_level0.20260602T14x0").is_err());
        assert!(snapshot_timestamp("unrelated.20260602T1430").is_err());
    }

    #[test]
    fn test_validate_part_coverage_counts_disk_and_uploaded() {
        let mut state = BackupState {
            total_parts: Some(2),
            ..Default::default()
        };
        state.parts_completed.insert(
            "000000".to_string(),
            crate::manifest::CompletedPart {
                size_bytes: 1,
                hash: String::new(),
            },
        );

        let mut on_disk = BTreeSet::new();
        assert!(validate_part_coverage(&on_disk, &state).is_err());

        on_disk.insert("000001".to_string());
        assert!(validate_part_coverage(&on_disk, &state).is_ok());

        // a part both on disk and uploaded must not be double-counted
        on_disk.insert("000000".to_string());
        state.total_parts = Some(3);
        assert!(validate_part_coverage(&on_disk, &state).is_err());
    }

    #[test]
    fn test_validate_part_coverage_no_total_is_ok() {
        let state = BackupState::default();
        assert!(validate_part_coverage(&BTreeSet::new(), &state).is_ok());
    }
}
