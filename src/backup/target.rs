//! Choosing what a new backup sends: the newest snapshot of the level, checked against the
//! subvolume it must come from and, for an incremental, against the chain it extends.

use anyhow::{Result, anyhow};

use crate::{
    btrfs::{BtrfsOps, ensure_snapshot_of},
    config::Task,
    layout::TaskLayout,
    manifest::{LatestEntry, LatestManifest, fetch_latest, fetch_manifest},
    remote::RemoteBackend,
    state::BackupTarget,
    timestamp::{BackupId, Timestamp, snapshot_prefix},
    verify::check_parts,
};

/// The target of a new level `level` backup of `task`, or None when its snapshot is already
/// backed up.
pub(super) async fn resolve_target<B: BtrfsOps, R: RemoteBackend>(
    btrfs: &B,
    remote: &R,
    layout: &TaskLayout,
    task: &Task,
    level: u32,
) -> Result<Option<BackupTarget>> {
    let snapshots = btrfs.list_snapshots(&task.snapshot_dir).await?;
    let snapshot_name = snapshots
        .iter()
        .rfind(|s| s.starts_with(&snapshot_prefix(level)))
        .ok_or_else(|| anyhow!("no level {level} snapshot found in {:?}", task.snapshot_dir))?
        .clone();
    let id = BackupId {
        level,
        timestamp: Timestamp::from_snapshot_name(&snapshot_name)?,
    };

    let source = btrfs.subvolume_uuids(&task.subvolume).await?;
    let snapshot = btrfs
        .subvolume_uuids(&task.snapshot_dir.join(&snapshot_name))
        .await?;
    ensure_snapshot_of(&snapshot_name, &snapshot, &task.subvolume, &source.uuid)?;

    let mut target = BackupTarget {
        id,
        snapshot_name,
        parent_snapshot: None,
        source_uuid: source.uuid,
        snapshot_uuid: snapshot.uuid,
    };
    if is_backed_up(remote, layout, &target).await? {
        tracing::info!(
            "snapshot '{}' is already backed up at level {level}, skipping",
            target.snapshot_name
        );
        return Ok(None);
    }
    if level > 0 {
        let parent = resolve_parent(btrfs, remote, layout, task, &snapshots, &target).await?;
        target.parent_snapshot = Some(parent);
    }
    Ok(Some(target))
}

/// A manifest at the target's key means it is fully backed up. Skipping it rather than
/// redoing it avoids overwriting archived objects, which incurs minimum-storage-duration
/// charges, and keeps re-runs idempotent.
async fn is_backed_up<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    target: &BackupTarget,
) -> Result<bool> {
    let key = layout.manifest_key(&target.id);
    if remote.head_metadata(&key).await?.is_none() {
        return Ok(false);
    }
    let existing = fetch_manifest(remote, &key).await?;
    if existing.source_uuid != target.source_uuid || existing.snapshot_uuid != target.snapshot_uuid
    {
        return Err(anyhow!(
            "manifest {key} records a different snapshot (subvolume UUID {}, snapshot UUID {}) \
             than local '{}' (subvolume UUID {}, snapshot UUID {}); s3.bucket/s3.prefix may be \
             wrong, or another host writes to the same location",
            existing.source_uuid,
            existing.snapshot_uuid,
            target.snapshot_name,
            target.source_uuid,
            target.snapshot_uuid
        ));
    }
    Ok(true)
}

/// The snapshot an incremental target is sent relative to: the newest backup of any lower
/// level, from the latest manifest, so a level may be skipped. It is matched by UUID, not just name, so a same-named
/// snapshot of another subvolume can't silently become the base of an incremental.
async fn resolve_parent<B: BtrfsOps, R: RemoteBackend>(
    btrfs: &B,
    remote: &R,
    layout: &TaskLayout,
    task: &Task,
    snapshots: &[String],
    target: &BackupTarget,
) -> Result<String> {
    let level = target.id.level;
    let latest_key = layout.latest_key();
    let latest = fetch_latest(remote, &latest_key).await?.ok_or_else(|| {
        anyhow!("no previous backup; level {level} requires a prior level 0 backup")
    })?;
    if latest.source_uuid != target.source_uuid {
        return Err(anyhow!(
            "latest manifest {latest_key} belongs to subvolume UUID {}, but {:?} is {}; if the \
             source was replaced, start a new chain with a level 0 backup",
            latest.source_uuid,
            task.subvolume,
            target.source_uuid
        ));
    }
    let parent = parent_entry(&latest, level)?;
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
    if target.id.timestamp <= parent.timestamp {
        return Err(anyhow!(
            "snapshot '{}' ({}) is not newer than its parent '{}' ({}); create a fresh level \
             {level} snapshot before backing up",
            target.snapshot_name,
            target.id.timestamp,
            parent.snapshot_name,
            parent.timestamp
        ));
    }
    ensure_chain_restorable(remote, layout, &latest, level).await?;
    Ok(parent.snapshot_name.clone())
}

/// The latest manifest's entry a new level `level` incremental builds on. Publishing a level
/// drops every deeper entry, so the entries below `level` form one chain and the deepest of
/// them is its newest backup.
pub(super) fn parent_entry(latest: &LatestManifest, level: u32) -> Result<&LatestEntry> {
    latest
        .levels
        .range(..level)
        .next_back()
        .map(|(_, entry)| entry)
        .ok_or_else(|| {
            anyhow!("no backup below level {level} in latest manifest; run a level 0 backup first")
        })
}

/// Restoring a level `level` incremental needs every part of each backup below it. S3
/// lifecycle expiration removes parts but not manifests, so every part is HEADed; any failure,
/// even a network error, fails the backup rather than letting a broken chain grow.
async fn ensure_chain_restorable<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    latest: &LatestManifest,
    level: u32,
) -> Result<()> {
    for (&ancestor_level, entry) in latest.levels.range(..level) {
        let id = BackupId {
            level: ancestor_level,
            timestamp: entry.timestamp.clone(),
        };
        let key = layout.manifest_key(&id);
        let ancestor = fetch_manifest(remote, &key).await?;
        let issues = check_parts(remote, layout, &ancestor, None).await?;
        if let Some(first) = issues.first() {
            return Err(anyhow!(
                "level {ancestor_level} backup {key} is incomplete ({} problem(s), first: {}), \
                 so a level {level} backup on top of it could never be restored; its parts may \
                 have been deleted by an S3 lifecycle expiration rule. Run a level 0 backup to \
                 start a new chain",
                issues.len(),
                first.description
            ));
        }
    }
    Ok(())
}
