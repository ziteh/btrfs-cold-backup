//! `bcb backup`: sends a snapshot, uploads it encrypted in parts, then publishes its manifest.
//!
//! Every step records its progress in a state file per task and level, so an interrupted
//! backup resumes on the next run of its level: a finished send isn't redone, and uploaded
//! parts aren't sent again. Other levels may run in between.

mod send;
mod target;
mod upload;

use anyhow::{Context, Result, anyhow};

use crate::{
    btrfs::BtrfsOps,
    config::{Config, Task},
    layout::TaskLayout,
    manifest::{BackupManifest, fetch_latest},
    remote::RemoteBackend,
    state::{BackupState, StateFile},
};

/// Backs up the newest level `level` snapshot of `task`, or resumes the task's unfinished
/// backup of that level. Returns None when the snapshot is already backed up.
pub async fn backup_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    btrfs: &B,
    remote: &R,
) -> Result<Option<BackupManifest>> {
    // Looked up first, so a missing entry fails at once instead of after a send that can take
    // hours.
    let part_class = config.s3.storage_class.for_level(level)?;
    let manifest_class = config.s3.storage_class.manifest;
    let layout = TaskLayout::new(config, task)?;

    let mut state = match load_unfinished(remote, &layout, level).await? {
        Some(state) => state,
        None => match target::resolve_target(btrfs, remote, &layout, task, level).await? {
            Some(target) => {
                StateFile::create(layout.backup_state_path(level), BackupState::new(target))?
            }
            None => return Ok(None),
        },
    };
    let parts_dir = layout.backup_parts_dir(&state.data.target.id);

    if state.data.sent.is_none() {
        let sent = send::send_and_encrypt(
            btrfs,
            task,
            &state.data.target,
            &parts_dir,
            &config.age_public_key,
        )
        .await?;
        state.data.sent = Some(sent);
        state.save()?;
    }
    upload::verify_local_parts(&mut state, &parts_dir).await?;
    upload::upload_pending_parts(remote, &layout, &mut state, &parts_dir, part_class).await?;

    let sent = state.data.sent.as_ref().expect("the send finished above");
    let manifest = upload::build_manifest(config, &state.data.target, sent).await;
    if !state.data.manifest_uploaded {
        upload::publish_manifest(remote, &layout, &mut state, &manifest, manifest_class).await?;
    }
    upload::publish_latest(remote, &layout, &state.data.target, manifest_class).await?;

    if let Err(e) = tokio::fs::remove_dir_all(&parts_dir).await {
        tracing::warn!("failed to clean up {parts_dir:?}: {e}");
    }
    state.remove();
    Ok(Some(manifest))
}

/// The task's unfinished backup of `level`, if any. One whose parent another backup has
/// superseded since it started is abandoned: committing it would chain the latest manifest
/// onto a stale parent, and a fresh backup against the new parent restores the same snapshot.
async fn load_unfinished<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    level: u32,
) -> Result<Option<StateFile<BackupState>>> {
    let legacy = layout.legacy_backup_state_path();
    if legacy.exists() {
        return Err(anyhow!(
            "{legacy:?} is the state of an unfinished backup from an older bcb, which this \
             version cannot resume; finish it with that version, or to abandon it delete it and \
             its parts directory next to it"
        ));
    }
    let Some(state) = StateFile::<BackupState>::load(layout.backup_state_path(level))? else {
        return Ok(None);
    };
    let target = &state.data.target;
    if let Some(parent) = &target.parent_snapshot {
        let latest_key = layout.latest_key();
        let latest = fetch_latest(remote, &latest_key).await?.ok_or_else(|| {
            anyhow!(
                "latest manifest {latest_key} is missing, but the unfinished {} builds on it; \
                 check s3.bucket and s3.prefix",
                target.id
            )
        })?;
        let current = target::parent_entry(&latest, level)?;
        if current.snapshot_name != *parent {
            let uploaded: Vec<String> = state
                .data
                .uploaded_parts
                .iter()
                .map(|index| layout.part_key(&target.id, index))
                .collect();
            tracing::warn!(
                "abandoning the unfinished {}: its parent '{parent}' was superseded by '{}'; \
                 parts it already uploaded stay on S3 until lifecycle expiration: {uploaded:?}",
                target.id,
                current.snapshot_name
            );
            let parts_dir = layout.backup_parts_dir(&target.id);
            if parts_dir.exists() {
                tokio::fs::remove_dir_all(&parts_dir)
                    .await
                    .with_context(|| format!("failed to remove {parts_dir:?}"))?;
            }
            state.remove();
            return Ok(None);
        }
    }
    tracing::info!("resuming the unfinished {}", target.id);
    Ok(Some(state))
}
