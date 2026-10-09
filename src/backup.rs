use age::x25519;
use anyhow::{Context, Result, anyhow};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::{
    BtrfsOps, Config, RemoteBackend, Task,
    btrfs::ensure_snapshot_of,
    crypto::blake3_file,
    manifest::{
        Backup, FORMAT_VERSION, LatestBackup, LatestManifest, PartInfo, SystemInfo, parse_backup,
        parse_latest,
    },
    state::{BackupState, PartRecord, read_state},
    util::{
        IO_BUF_SIZE, SNAPSHOT_PART_PREFIX, available_space_bytes, backup_output_dir,
        backup_state_path, fsync_dir, s3_backup_key, s3_latest_manifest_key, s3_manifest_key,
        sanitize_subvolume_path,
    },
    verify::check_parts,
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
        let inner = match read_state::<BackupState>(&path)? {
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
    prefix: &str,
    sanitized: &str,
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
    // Restoring this incremental needs every ancestor's parts. Lifecycle expiration removes
    // parts but not manifests, so each part is HEADed; any failure, even a network error,
    // fails the backup rather than letting a broken chain grow.
    for (&l, entry) in latest.levels.range(..level) {
        let manifest_key = s3_manifest_key(prefix, sanitized, l, &entry.timestamp);
        let ancestor = parse_backup(&remote.download(&manifest_key).await?)
            .map_err(|e| anyhow!("failed to parse manifest {manifest_key}: {e}"))?;
        let issues = check_parts(
            remote,
            prefix,
            sanitized,
            l,
            &entry.timestamp,
            &ancestor,
            |_, _| None,
        )
        .await;
        if let Some(first) = issues.first() {
            return Err(anyhow!(
                "level {l} backup {manifest_key} is incomplete ({} problem(s), first: {first}), \
                 so a level {level} backup on top of it could never be restored; its parts may \
                 have been deleted by an S3 lifecycle expiration rule. Run a level 0 backup to \
                 start a new chain",
                issues.len()
            ));
        }
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
    ensure_snapshot_of(&snapshot_name, &snapshot, &task.subvolume, &source.uuid)?;

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
        &config.s3.prefix,
        &sanitized,
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

// Test-only pause point: parks here, with the btrfs-send child already spawned and alive, so
// the VM e2e suite can SIGKILL bcb at a deterministic point to exercise
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

const SPLIT_PART_SIZE: u64 = 3 * 1024 * 1024 * 1024;

// Test-only override (in bytes) so multi-part backups can be exercised without multi-GiB
// fixtures.
#[cfg(debug_assertions)]
fn split_part_size() -> u64 {
    // A value that doesn't parse must not silently fall back to full-size parts, or a test
    // meant to exercise multiple parts would quietly run with one.
    std::env::var("BCB_TEST_SPLIT_SIZE")
        .ok()
        .map(|v| v.parse().expect("BCB_TEST_SPLIT_SIZE must be a byte count"))
        .unwrap_or(SPLIT_PART_SIZE)
}

#[cfg(not(debug_assertions))]
fn split_part_size() -> u64 {
    SPLIT_PART_SIZE
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

/// The send stream adds per-file metadata that `du` doesn't count, and age adds a tag per
/// chunk and a header per part, so the estimate gets a margin on top.
fn required_space(snapshot_size: u64) -> u64 {
    snapshot_size + snapshot_size / 100
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
        Ok(size) => required_space(size),
        Err(e) => {
            tracing::warn!("could not estimate snapshot size, skipping free-space check: {e}");
            return Ok(());
        }
    };
    // output_dir was just created under base_dir, so it's on the same filesystem.
    let available = available_space_bytes(output_dir)?;
    ensure_enough_space(available, required, snapshot_path)
}

async fn send_and_encrypt<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    snapshot_name: &str,
    parent: &Option<String>,
    output_dir: &Path,
    public_key: &str,
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

    let result = send_into_parts(
        btrfs,
        task,
        snapshot_name,
        parent,
        output_dir,
        public_key,
        state,
    )
    .await;
    // Without a recorded stream hash the next run starts over anyway; left behind, the parts
    // would take space from the tasks that run after this one.
    if result.is_err()
        && let Err(e) = tokio::fs::remove_dir_all(output_dir).await
    {
        tracing::warn!("failed to clean up output dir {:?}: {e}", output_dir);
    }
    result
}

async fn send_into_parts<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    snapshot_name: &str,
    parent: &Option<String>,
    output_dir: &Path,
    public_key: &str,
    state: &mut PersistentState,
) -> Result<String> {
    let recipient: x25519::Recipient = public_key
        .parse()
        .map_err(|e| anyhow!("invalid age public key: {e}"))?;

    let snapshot_path = task.snapshot_dir.join(snapshot_name);
    check_available_space(&snapshot_path, output_dir).await?;

    let stream = match parent {
        Some(p) => {
            btrfs
                .send_incremental(&task.snapshot_dir.join(p.as_str()), &snapshot_path)
                .await?
        }
        None => btrfs.send_full(&snapshot_path).await?,
    };
    test_pause_during_send().await;

    let stream = tokio_util::io::SyncIoBridge::new(stream);
    let dir = output_dir.to_path_buf();
    let part_size = split_part_size();
    let (hash, parts) = tokio::task::spawn_blocking(move || {
        encrypt_into_parts(stream, &dir, part_size, &recipient)
    })
    .await??;

    state.inner.parts = parts;
    state.inner.stream_hash = Some(hash.clone());
    state.save()?;

    Ok(hash)
}

/// Passes writes through to `inner`, hashing and counting the bytes written.
struct HashingWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
    written: u64,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Cuts `stream` into parts of `part_size` plaintext bytes, each written straight to an
/// encrypted `.age` file in `output_dir`, so plaintext never touches the disk. Returns the
/// BLAKE3 of the whole plaintext stream and each part's ciphertext size and BLAKE3.
fn encrypt_into_parts(
    mut stream: impl Read,
    output_dir: &Path,
    part_size: u64,
    recipient: &x25519::Recipient,
) -> Result<(String, BTreeMap<String, PartRecord>)> {
    let mut stream_hasher = blake3::Hasher::new();
    let mut parts = BTreeMap::new();
    let mut buf = vec![0u8; IO_BUF_SIZE];
    loop {
        let mut part_stream = (&mut stream).take(part_size);
        let mut n = part_stream.read(&mut buf)?;
        // Checked before creating the file, so a stream ending on a part boundary doesn't
        // produce an empty trailing part.
        if n == 0 {
            break;
        }

        let index = format!("{:06}", parts.len());
        let path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{index}.age"));
        let write_err = || format!("failed to write {path:?}");
        let file = std::fs::File::create(&path).with_context(write_err)?;
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
                .map_err(|e| anyhow!("failed to create encryptor: {e}"))?;
        let mut writer = encryptor
            .wrap_output(HashingWriter {
                inner: file,
                hasher: blake3::Hasher::new(),
                written: 0,
            })
            .with_context(write_err)?;
        while n > 0 {
            stream_hasher.update(&buf[..n]);
            writer.write_all(&buf[..n]).with_context(write_err)?;
            n = part_stream.read(&mut buf)?;
        }
        let out = writer.finish().with_context(write_err)?;
        // The state saved after this claims the part exists; after a power loss it must too.
        out.inner.sync_all().with_context(write_err)?;

        parts.insert(
            index,
            PartRecord {
                size_bytes: out.written,
                blake3: out.hasher.finalize().to_hex().to_string(),
            },
        );
    }
    fsync_dir(output_dir)?;
    Ok((stream_hasher.finalize().to_hex().to_string(), parts))
}

/// Checks that every part not yet uploaded is on disk exactly as the send stage wrote it.
/// If nothing was uploaded yet, a bad part only costs a fresh send, so the state is reset
/// for the next run to redo it.
async fn verify_pending_parts(state: &mut PersistentState, output_dir: &Path) -> Result<()> {
    let mut bad = Vec::new();
    for (index, record) in &state.inner.parts {
        if state.inner.uploaded_parts.contains(index) {
            continue;
        }
        let path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{index}.age"));
        let intact = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len() == record.size_bytes && blake3_file(&path).await? == record.blake3,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(e.into()),
        };
        if !intact {
            bad.push(index.clone());
        }
    }
    if bad.is_empty() {
        return Ok(());
    }

    if state.inner.uploaded_parts.is_empty() {
        state.inner.stream_hash = None;
        state.inner.parts.clear();
        state.save()?;
        return Err(anyhow!(
            "parts {bad:?} in {output_dir:?} are missing or damaged; state cleared, re-run \
             to redo the send"
        ));
    }
    Err(anyhow!(
        "parts {bad:?} in {output_dir:?} are missing or damaged, and others were already \
         uploaded; to start over delete the state file {:?} and {output_dir:?}, then re-run",
        state.path
    ))
}

async fn upload_part<R: RemoteBackend>(
    remote: &R,
    part_index: &str,
    output_dir: &Path,
    sanitized: &str,
    level: u32,
    timestamp: &str,
    config: &Config,
) -> Result<()> {
    let enc_path = output_dir.join(format!("{SNAPSHOT_PART_PREFIX}{part_index}.age"));
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
        .await
}

/// Assembles the manifest describing the backup that was just uploaded.
async fn build_manifest(
    config: &Config,
    plan: &BackupPlan,
    level: u32,
    stream_hash: String,
    state: &BackupState,
) -> Backup {
    let parts: Vec<PartInfo> = state
        .parts
        .iter()
        .map(|(index, part)| PartInfo {
            index: index.clone(),
            size_bytes: part.size_bytes,
            blake3: part.blake3.clone(),
        })
        .collect();

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
    let state_file = backup_state_path(&config.base_dir, &sanitized);
    tokio::fs::create_dir_all(state_file.parent().unwrap()).await?;

    let mut state = PersistentState::load_or_init(state_file, &task.name, level)?;

    let Some(plan) = init_backup_plan(config, task, level, btrfs, remote, &mut state).await? else {
        return Ok(None);
    };

    let stream_hash = send_and_encrypt(
        btrfs,
        task,
        &plan.snapshot_name,
        &plan.parent_snapshot,
        &plan.output_dir,
        &config.age_public_key,
        &mut state,
    )
    .await?;

    verify_pending_parts(&mut state, &plan.output_dir).await?;

    let pending: Vec<String> = state
        .inner
        .parts
        .keys()
        .filter(|i| !state.inner.uploaded_parts.contains(*i))
        .cloned()
        .collect();
    for index in pending {
        upload_part(
            remote,
            &index,
            &plan.output_dir,
            &sanitized,
            level,
            &plan.timestamp,
            config,
        )
        .await?;
        state.inner.uploaded_parts.insert(index.clone());
        state.save()?;
        // Deleted only once the state records the upload: gone any earlier, a crash would
        // leave a part that is neither on disk nor marked uploaded. A crash after the save
        // leaves the file for the output_dir cleanup below.
        let enc_path = plan
            .output_dir
            .join(format!("{SNAPSHOT_PART_PREFIX}{index}.age"));
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
    fn test_ensure_enough_space_sufficient_is_ok() {
        assert!(ensure_enough_space(100, 50, Path::new("/snap")).is_ok());
    }

    #[test]
    fn test_ensure_enough_space_exactly_equal_is_ok() {
        assert!(ensure_enough_space(50, 50, Path::new("/snap")).is_ok());
    }

    #[test]
    fn test_required_space_exceeds_snapshot_size() {
        assert!(required_space(1 << 30) > 1 << 30);
    }

    #[test]
    fn test_ensure_enough_space_insufficient_is_err() {
        let err = ensure_enough_space(10, 50, Path::new("/snap")).unwrap_err();
        assert!(err.to_string().contains("insufficient free space"));
    }

    fn decrypt(bytes: &[u8], identity: &x25519::Identity) -> Vec<u8> {
        let decryptor = age::Decryptor::new(bytes).unwrap();
        let mut reader = decryptor
            .decrypt(std::iter::once(identity as &dyn age::Identity))
            .unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn test_encrypt_into_parts_round_trips_and_records_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let identity = x25519::Identity::generate();
        let data: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();

        let (stream_hash, parts) =
            encrypt_into_parts(&data[..], dir.path(), 4096, &identity.to_public()).unwrap();

        assert_eq!(stream_hash, blake3::hash(&data).to_hex().to_string());
        assert_eq!(
            parts.keys().collect::<Vec<_>>(),
            ["000000", "000001", "000002"]
        );
        let mut joined = Vec::new();
        for (index, record) in &parts {
            let bytes = std::fs::read(
                dir.path()
                    .join(format!("{SNAPSHOT_PART_PREFIX}{index}.age")),
            )
            .unwrap();
            assert_eq!(record.size_bytes, bytes.len() as u64);
            assert_eq!(record.blake3, blake3::hash(&bytes).to_hex().to_string());
            joined.extend(decrypt(&bytes, &identity));
        }
        assert_eq!(joined, data);
        // No plaintext part files: every file in the dir is a recorded .age part.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), parts.len());
    }

    #[test]
    fn test_encrypt_into_parts_stream_on_part_boundary_has_no_empty_part() {
        let dir = tempfile::tempdir().unwrap();
        let identity = x25519::Identity::generate();
        let data = vec![7u8; 8192];

        let (_, parts) =
            encrypt_into_parts(&data[..], dir.path(), 4096, &identity.to_public()).unwrap();

        assert_eq!(parts.len(), 2);
    }
}
