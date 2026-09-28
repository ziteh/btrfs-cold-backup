use anyhow::{Result, anyhow};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    crypto::{blake3_file, encrypt_file},
    manifest::{
        Backup, FORMAT_VERSION, LatestBackup, LatestManifest, PartInfo, SystemInfo, parse_backup,
        parse_latest,
    },
    state::{BackupState, UploadedPart, read_backup_state},
    util::{
        IO_BUF_SIZE, SNAPSHOT_PART_PREFIX, available_space_bytes, backup_output_dir, fsync_dir,
        s3_backup_key, s3_latest_manifest_key, s3_manifest_key, sanitize_subvolume_path,
        state_path,
    },
};

// Parses btrbk's snapshot naming; the expected format is spelled out in the error message below.
fn snapshot_timestamp(name: &str) -> Result<String> {
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
    let (date, time) = ts.split_once('T').ok_or_else(err)?;

    // btrbk appends a counter when it takes two snapshots of the same subvolume within one
    // minute. bcb identifies a backup by its minute, so it cannot store both. Refusing is
    // safe: the backup is skipped this cycle only, and the next snapshot sorts after this
    // one, so the following cycle picks it up without intervention.
    if let Some((_, counter)) = time.split_once('_') {
        return Err(anyhow!(
            "snapshot '{name}' is a btrbk same-minute snapshot (trailing '_{counter}'), \
             which bcb cannot back up because it identifies backups down to the minute. \
             Delete or rename it; the next scheduled run will back up normally either way."
        ));
    }

    if date.len() != 8 || !all_digits(date) || time.len() != 4 || !all_digits(time) {
        return Err(err());
    }
    Ok(format!(
        "{}-{}-{}T{}:{}",
        &date[0..4],
        &date[4..6],
        &date[6..8],
        &time[0..2],
        &time[2..4]
    ))
}

struct PersistentState {
    inner: BackupState,
    path: PathBuf,
}

impl PersistentState {
    /// Refuses to replace another level's unfinished backup: starting fresh would silently
    /// discard its uploaded parts and leave that level without a backup.
    fn load_or_init(path: PathBuf, task_name: &str, level: u32) -> Result<Self> {
        let inner = match read_backup_state(&path)? {
            Some(s) if s.level == level => {
                tracing::info!("resuming from existing backup state");
                s
            }
            Some(s) => {
                return Err(anyhow!(
                    "an unfinished level {} backup exists (state {path:?}, parts in {:?}); \
                     finish it with `bcb backup --level {} --task {task_name}`, or to abandon \
                     it delete both paths",
                    s.level,
                    s.output_dir,
                    s.level
                ));
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
    source_uuid: String,
    snapshot_uuid: String,
    output_dir: PathBuf,
    manifest_key: String,
}

/// Determines the incremental parent for a level > 0 backup from the latest manifest;
/// level 0 has none. The parent is matched by UUID, not just name, so a same-named snapshot
/// of another subvolume can't silently become the base of an incremental.
#[allow(clippy::too_many_arguments)]
async fn resolve_parent<B: BtrfsOps, R: RemoteBackend>(
    btrfs: &B,
    remote: &R,
    task: &Task,
    latest_key: &str,
    snapshots: &[String],
    level: u32,
    snapshot_name: &str,
    timestamp: &str,
    source_uuid: &str,
) -> Result<Option<String>> {
    if level == 0 {
        return Ok(None);
    }
    if !remote.exists(latest_key).await? {
        return Err(anyhow!(
            "no previous backup; level {level} requires a prior level 0 backup"
        ));
    }
    let latest = parse_latest(&remote.download(latest_key).await?)
        .map_err(|e| anyhow!("failed to parse latest manifest {latest_key}: {e}"))?;
    if latest.source_uuid != source_uuid {
        return Err(anyhow!(
            "latest manifest {latest_key} belongs to subvolume UUID {}, but {:?} is \
             {source_uuid}; if the source was replaced, start a new chain with a level 0 backup",
            latest.source_uuid,
            task.subvolume
        ));
    }
    let parent = latest.levels.get(&(level - 1)).ok_or_else(|| {
        anyhow!(
            "no level {} entry in latest manifest; run a level {} backup first",
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
    let local_parent = btrfs
        .subvolume_uuids(&task.snapshot_dir.join(&parent.snapshot_name))
        .await?;
    if local_parent.uuid != parent.snapshot_uuid {
        return Err(anyhow!(
            "local snapshot '{}' has UUID {}, but the backed-up parent of that name has {}; \
             it is a different snapshot and cannot serve as the incremental base",
            parent.snapshot_name,
            local_parent.uuid,
            parent.snapshot_uuid
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
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;

    if !state.inner.snapshot_name.is_empty() {
        let timestamp = state.inner.timestamp.clone();
        return Ok(Some(BackupPlan {
            manifest_key: s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp),
            snapshot_name: state.inner.snapshot_name.clone(),
            timestamp,
            parent_snapshot: state.inner.parent_snapshot.clone(),
            source_uuid: state.inner.source_uuid.clone(),
            snapshot_uuid: state.inner.snapshot_uuid.clone(),
            output_dir: state.inner.output_dir.clone(),
        }));
    }

    let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
    let level_prefix = format!("bcb_level{level}.");
    let snapshot_name = snapshots
        .iter()
        .rfind(|s| s.starts_with(&level_prefix))
        .ok_or_else(|| anyhow!("no level {level} snapshot found in {:?}", task.snapshot_dir))?
        .clone();
    let timestamp = snapshot_timestamp(&snapshot_name)?;

    let source = btrfs.subvolume_uuids(&task.subvolume).await?;
    let snapshot = btrfs
        .subvolume_uuids(&task.snapshot_dir.join(&snapshot_name))
        .await?;
    if snapshot.parent_uuid.as_deref() != Some(source.uuid.as_str()) {
        return Err(anyhow!(
            "snapshot '{snapshot_name}' was not taken from {:?} (its Parent UUID is {}, the \
             subvolume's UUID is {}); check that snapshot_dir holds this subvolume's snapshots",
            task.subvolume,
            snapshot.parent_uuid.as_deref().unwrap_or("-"),
            source.uuid
        ));
    }

    // A manifest already on remote means this snapshot is fully backed up: skip rather than
    // redo it — overwriting archived objects incurs minimum-storage-duration charges, and a
    // skip keeps re-runs idempotent so one completed task never blocks the others.
    let manifest_key = s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp);
    if remote.exists(&manifest_key).await? {
        let existing = parse_backup(&remote.download(&manifest_key).await?)
            .map_err(|e| anyhow!("failed to parse manifest {manifest_key}: {e}"))?;
        if existing.source_uuid != source.uuid || existing.snapshot_uuid != snapshot.uuid {
            return Err(anyhow!(
                "manifest {manifest_key} records a different snapshot (subvolume UUID {}, \
                 snapshot UUID {}) than local '{snapshot_name}' (subvolume UUID {}, snapshot \
                 UUID {}); s3.bucket/s3.prefix may be wrong, or another host writes to the \
                 same location",
                existing.source_uuid,
                existing.snapshot_uuid,
                source.uuid,
                snapshot.uuid
            ));
        }
        tracing::info!(
            "snapshot '{snapshot_name}' is already backed up at level {level}, skipping"
        );
        return Ok(None);
    }

    let latest_key = s3_latest_manifest_key(&config.s3.prefix, &sanitized);
    let parent_snapshot = resolve_parent(
        btrfs,
        remote,
        task,
        &latest_key,
        &snapshots,
        level,
        &snapshot_name,
        &timestamp,
        &source.uuid,
    )
    .await?;
    let output_dir = backup_output_dir(&config.base_dir, &sanitized, level, &timestamp);

    state.inner.level = level;
    state.inner.snapshot_name = snapshot_name.clone();
    state.inner.parent_snapshot = parent_snapshot.clone();
    state.inner.source_uuid = source.uuid.clone();
    state.inner.snapshot_uuid = snapshot.uuid.clone();
    state.inner.timestamp = timestamp.clone();
    state.inner.output_dir = output_dir.clone();
    state.save()?;

    Ok(Some(BackupPlan {
        snapshot_name,
        timestamp,
        parent_snapshot,
        source_uuid: source.uuid,
        snapshot_uuid: snapshot.uuid,
        output_dir,
        manifest_key,
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

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

/// Estimates a snapshot's on-disk size via `du`, as an upper bound for the space a `btrfs
/// send` of it (full or incremental) could require locally: exactly right for a full send,
/// and a safe overestimate for an incremental one.
async fn snapshot_size_bytes(path: &Path) -> Result<u64> {
    let output = tokio::process::Command::new("du")
        .args(["-sb"])
        .arg(path)
        .output()
        .await
        .map_err(|e| anyhow!("failed to run du on {:?}: {e}", path))?;
    if !output.status.success() {
        return Err(anyhow!(
            "du failed for {:?}: {}",
            path,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let size_str = stdout
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("unexpected du output for {:?}: {stdout:?}", path))?;
    size_str
        .parse::<u64>()
        .map_err(|e| anyhow!("failed to parse du output '{size_str}' for {:?}: {e}", path))
}

fn ensure_enough_space(available: u64, required: u64, snapshot_path: &Path) -> Result<()> {
    if available < required {
        return Err(anyhow!(
            "insufficient free space for base_dir: {:.1} GiB available, ~{:.1} GiB needed \
             (estimated from the size of {:?}); free up space or point base_dir at a larger \
             filesystem before retrying",
            available as f64 / GIB,
            required as f64 / GIB,
            snapshot_path,
        ));
    }
    Ok(())
}

/// Refuses to start a potentially hours-long send/upload when base_dir clearly doesn't have
/// room for it, rather than failing partway through with ENOSPC. The size estimate is a
/// best-effort heuristic (`du` can fail for reasons unrelated to disk space, e.g. permissions
/// or an unsupported flag on a non-Linux `du`), so a failure to estimate only skips the
/// check — it must never block a backup that might otherwise have succeeded.
async fn check_available_space(snapshot_path: &Path, output_dir: &Path) -> Result<()> {
    let required = match snapshot_size_bytes(snapshot_path).await {
        Ok(size) => size,
        Err(e) => {
            tracing::warn!("could not estimate snapshot size, skipping free-space check: {e}");
            return Ok(());
        }
    };
    // output_dir was just created under base_dir, so it's on the same filesystem.
    let available = available_space_bytes(output_dir)?;
    ensure_enough_space(available, required, snapshot_path)
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
    check_available_space(&snapshot_path, output_dir).await?;

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

    // The state saved below claims these parts exist; after a power loss they must too.
    let parts = collect_disk_parts(output_dir).await?;
    for suffix in &parts {
        let part = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{suffix}"));
        tokio::fs::File::open(&part).await?.sync_all().await?;
    }
    fsync_dir(output_dir)?;
    let total_parts = parts.len() as u32;

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

async fn encrypt_and_upload_part<R: RemoteBackend>(
    remote: &R,
    part_index: &str,
    output_dir: &Path,
    sanitized: &str,
    level: u32,
    timestamp: &str,
    config: &Config,
) -> Result<UploadedPart> {
    let raw_path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{part_index}"));
    let enc_path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{part_index}.age"));

    if enc_path.exists() && raw_path.exists() {
        // coexisting raw+enc means encryption was interrupted; enc may be truncated
        tokio::fs::remove_file(&enc_path).await?;
    }
    if !enc_path.exists() {
        encrypt_file(&raw_path, &enc_path, &config.age_public_key).await?;
        // The .age entry must be durable before the raw part it replaces disappears.
        fsync_dir(output_dir)?;
        if let Err(e) = tokio::fs::remove_file(&raw_path).await {
            tracing::warn!("failed to remove raw part {:?}: {e}", raw_path);
        }
    }

    let enc_size = tokio::fs::metadata(&enc_path).await?.len();
    let blake3 = blake3_file(&enc_path).await?;

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
    let part_key = s3_backup_key(&config.s3.prefix, sanitized, level, timestamp, part_index);
    remote
        .upload_file(
            &part_key,
            &enc_path,
            storage_class,
            &format!("bcb-backup-level={level}"),
        )
        .await?;

    Ok(UploadedPart {
        size_bytes: enc_size,
        blake3,
    })
}

/// Distinguishes "the split produced nothing" from "the output dir disappeared after a split
/// that had already succeeded": the latter is recoverable by re-running the send, but only
/// once the stream hash and part count recorded from the vanished run are cleared.
fn ensure_parts_exist(
    part_suffixes: &BTreeSet<String>,
    state: &mut PersistentState,
    output_dir: &Path,
) -> Result<()> {
    if !part_suffixes.is_empty() || !state.inner.parts_completed.is_empty() {
        return Ok(());
    }
    if state.inner.stream_hash.is_some() {
        state.inner.stream_hash = None;
        state.inner.total_parts = None;
        state.save()?;
        return Err(anyhow!(
            "output_dir vanished after split; state cleared — re-run to retry from split"
        ));
    }
    Err(anyhow!("no snapshot parts found in {output_dir:?}"))
}

/// Assembles the manifest describing the backup that was just uploaded.
async fn build_manifest(
    config: &Config,
    plan: &BackupPlan,
    level: u32,
    stream_hash: String,
    state: &BackupState,
) -> Backup {
    let mut parts: Vec<PartInfo> = state
        .parts_completed
        .iter()
        .map(|(index, part)| PartInfo {
            index: index.clone(),
            size_bytes: part.size_bytes,
            blake3: part.blake3.clone(),
        })
        .collect();
    parts.sort_by(|a, b| a.index.cmp(&b.index));

    Backup {
        format_version: FORMAT_VERSION.to_string(),
        timestamp: plan.timestamp.clone(),
        created_at: crate::util::now_unix(),
        level,
        snapshot_name: plan.snapshot_name.clone(),
        parent_snapshot: plan.parent_snapshot.clone(),
        source_uuid: plan.source_uuid.clone(),
        snapshot_uuid: plan.snapshot_uuid.clone(),
        system: SystemInfo {
            os: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            btrfs_version: crate::btrfs::btrfs_version().await,
            bcb_version: env!("CARGO_PKG_VERSION").to_string(),
        },
        age_public_key: config.age_public_key.clone(),
        total_size_bytes: parts.iter().map(|p| p.size_bytes).sum(),
        blake3: stream_hash,
        parts,
    }
}

/// Records this backup as the newest of its level, and drops every deeper level's entry:
/// those were built on the level-N snapshot this run just superseded, so leaving them would
/// let a later deeper-level backup chain silently onto a stale parent instead of failing
/// loudly and asking for a fresh one.
async fn publish_latest<R: RemoteBackend>(
    config: &Config,
    remote: &R,
    sanitized: &str,
    level: u32,
    plan: &BackupPlan,
) -> Result<()> {
    let latest_key = s3_latest_manifest_key(&config.s3.prefix, sanitized);
    let existing = if remote.exists(&latest_key).await? {
        // A corrupted latest manifest must not be silently reset: dropping other levels'
        // entries would break subsequent incremental backups without warning.
        Some(parse_latest(&remote.download(&latest_key).await?).map_err(|e| {
            anyhow!("failed to parse latest manifest {latest_key}: {e}; repair or delete it manually")
        })?)
    } else {
        None
    };

    // resolve_parent rejects a foreign latest manifest for level > 0, so a mismatch here is
    // a level 0 backup of a replaced source, which starts a new chain.
    let mut latest = match existing {
        Some(l) if l.source_uuid == plan.source_uuid => l,
        Some(l) => {
            tracing::warn!(
                "latest manifest {latest_key} was for subvolume UUID {}, replacing it with a \
                 new chain for {}",
                l.source_uuid,
                plan.source_uuid
            );
            LatestManifest::new(plan.source_uuid.clone())
        }
        None => LatestManifest::new(plan.source_uuid.clone()),
    };

    latest
        .levels
        .retain(|&existing_level, _| existing_level <= level);
    latest.levels.insert(
        level,
        LatestBackup {
            timestamp: plan.timestamp.clone(),
            snapshot_name: plan.snapshot_name.clone(),
            snapshot_uuid: plan.snapshot_uuid.clone(),
        },
    );

    remote
        .upload_bytes(
            &latest_key,
            serde_yaml::to_string(&latest)?.into_bytes(),
            &config.s3.storage_class.manifest,
            "bcb-backup-level=manifest",
        )
        .await?;
    Ok(())
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

    ensure_parts_exist(&part_suffixes, &mut state, &plan.output_dir)?;

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
            config,
        )
        .await?;
        state
            .inner
            .parts_completed
            .insert(suffix.clone(), completed);
        state.save()?;
        // Deleted only once the state records the upload: gone any earlier, a crash would
        // leave a part that is neither on disk nor marked complete. A crash after the save
        // leaves the file for the output_dir cleanup below.
        let enc_path = plan
            .output_dir
            .join(format!("{SNAPSHOT_PART_PREFIX}{suffix}.age"));
        if let Err(e) = tokio::fs::remove_file(&enc_path).await {
            tracing::warn!("failed to remove uploaded part {:?}: {e}", enc_path);
        }
    }

    let backup = build_manifest(config, &plan, level, stream_hash, &state.inner).await;

    if !state.inner.manifest_uploaded {
        remote
            .upload_bytes(
                &plan.manifest_key,
                serde_yaml::to_string(&backup)?.into_bytes(),
                &config.s3.storage_class.manifest,
                "bcb-backup-level=manifest",
            )
            .await?;
        state.inner.manifest_uploaded = true;
        state.save()?;
    }

    publish_latest(config, remote, &sanitized, level, &plan).await?;

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
        let ts = snapshot_timestamp("bcb_level1.20260602T1430").unwrap();
        assert_eq!(ts, "2026-06-02T14:30");
    }

    #[test]
    fn test_snapshot_timestamp_same_minute_snapshot_error_is_specific() {
        let err = snapshot_timestamp("bcb_level0.20260602T1430_1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("same-minute"), "unhelpful message: {err}");
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
            UploadedPart {
                size_bytes: 1,
                blake3: String::new(),
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

    #[test]
    fn test_ensure_enough_space_sufficient_is_ok() {
        assert!(ensure_enough_space(100, 50, Path::new("/snap")).is_ok());
    }

    #[test]
    fn test_ensure_enough_space_exactly_equal_is_ok() {
        assert!(ensure_enough_space(50, 50, Path::new("/snap")).is_ok());
    }

    #[test]
    fn test_ensure_enough_space_insufficient_is_err() {
        let err = ensure_enough_space(10, 50, Path::new("/snap")).unwrap_err();
        assert!(err.to_string().contains("insufficient free space"));
    }
}
