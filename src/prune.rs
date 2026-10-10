//! `bcb prune-snapshots`: deletes the local snapshots of a task that no backup needs.
//!
//! btrbk only creates the snapshots: it doesn't know which one an unfinished backup sends or
//! which one the next incremental builds on, so bcb decides what to keep. When what S3 records
//! doesn't match the local snapshots, nothing is deleted: a wrong bucket or prefix must not
//! cost the snapshots a weeks-long level 0 would be needed to replace.

use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    btrfs::BtrfsOps,
    config::{Config, Task},
    layout::TaskLayout,
    manifest::fetch_latest,
    remote::RemoteBackend,
    state::{BackupState, StateFile},
    timestamp::BackupId,
};

/// Deletes, or with `dry_run` only lists, the snapshots of `task` no backup needs. Returns
/// their names.
///
/// Kept: the snapshot of each latest manifest entry, which the next incremental may build
/// on; the target and parent of each unfinished backup; and the newest snapshot of each level,
/// which the next backup of that level sends. Snapshots not named by bcb's scheme or not taken
/// from the task's subvolume are never touched.
pub async fn prune_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    btrfs: &B,
    remote: &R,
    dry_run: bool,
) -> Result<Vec<String>> {
    let layout = TaskLayout::new(config, task)?;
    let legacy = layout.legacy_backup_state_path();
    if legacy.exists() {
        return Err(anyhow!(
            "{legacy:?} is the state of an unfinished backup from an older bcb; finish or \
             abandon it before pruning"
        ));
    }

    let snapshots: BTreeMap<String, BackupId> = btrfs
        .list_snapshots(&task.snapshot_dir)
        .await?
        .into_iter()
        .filter_map(|name| {
            let id = BackupId::from_snapshot_name(&name).ok()?;
            Some((name, id))
        })
        .collect();

    let latest_key = layout.latest_key();
    let Some(latest) = fetch_latest(remote, &latest_key).await? else {
        tracing::warn!(
            "no latest manifest at {latest_key}; keeping every snapshot of task '{}' until its \
             first backup completes",
            task.name
        );
        return Ok(Vec::new());
    };
    let source = btrfs.subvolume_uuids(&task.subvolume).await?;
    if latest.source_uuid != source.uuid {
        return Err(anyhow!(
            "latest manifest {latest_key} belongs to subvolume UUID {}, but {:?} is {}; check \
             s3.bucket and s3.prefix",
            latest.source_uuid,
            task.subvolume,
            source.uuid
        ));
    }

    let mut keep = BTreeSet::new();
    for entry in latest.levels.values() {
        let name = &entry.snapshot_name;
        let local_uuid = if snapshots.contains_key(name) {
            Some(
                btrfs
                    .subvolume_uuids(&task.snapshot_dir.join(name))
                    .await?
                    .uuid,
            )
        } else {
            None
        };
        if local_uuid.as_deref() != Some(entry.snapshot_uuid.as_str()) {
            return Err(anyhow!(
                "latest manifest {latest_key} records snapshot '{name}' (UUID {}), which is not \
                 in {:?}; the local snapshots don't match S3",
                entry.snapshot_uuid,
                task.snapshot_dir
            ));
        }
        keep.insert(name.clone());
    }

    let levels: BTreeSet<u32> = snapshots.values().map(|id| id.level).collect();
    for &level in &levels {
        if let Some(state) = StateFile::<BackupState>::load(layout.backup_state_path(level))? {
            let target = state.data.target;
            keep.insert(target.snapshot_name);
            keep.extend(target.parent_snapshot);
        }
        // Names of one level differ only in their timestamp, so the last sorts newest.
        if let Some((name, _)) = snapshots.iter().rfind(|(_, id)| id.level == level) {
            keep.insert(name.clone());
        }
    }

    let mut pruned = Vec::new();
    for name in snapshots.keys().filter(|name| !keep.contains(*name)) {
        let path = task.snapshot_dir.join(name);
        if btrfs.subvolume_uuids(&path).await?.parent_uuid.as_deref() != Some(&source.uuid) {
            tracing::warn!(
                "leaving {path:?} alone: it was not taken from {:?}",
                task.subvolume
            );
            continue;
        }
        if !dry_run {
            btrfs.delete_subvolume(&path).await?;
            tracing::info!("deleted snapshot {path:?}");
        }
        pruned.push(name.clone());
    }
    Ok(pruned)
}
