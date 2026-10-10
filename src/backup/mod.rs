//! `bcb backup`: sends a snapshot, uploads it encrypted in parts, then publishes its manifest.
//!
//! Every step records its progress in the task's state file, so an interrupted backup
//! resumes on the next run: a finished send isn't redone, and uploaded parts aren't sent
//! again.

mod send;
mod target;
mod upload;

use anyhow::{Result, anyhow};

use crate::{
    btrfs::BtrfsOps,
    config::{Config, Task},
    layout::TaskLayout,
    manifest::BackupManifest,
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

    let mut state = match load_unfinished(&layout, task, level)? {
        Some(state) => state,
        None => match target::resolve_target(btrfs, remote, &layout, task, level).await? {
            Some(target) => {
                StateFile::create(layout.backup_state_path(), BackupState::new(target))?
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

/// The task's unfinished backup of `level`, if any. An unfinished backup of another level is
/// an error rather than discarded: its state is the only record of which parts already
/// reached S3.
fn load_unfinished(
    layout: &TaskLayout,
    task: &Task,
    level: u32,
) -> Result<Option<StateFile<BackupState>>> {
    let Some(state) = StateFile::<BackupState>::load(layout.backup_state_path())? else {
        return Ok(None);
    };
    let id = &state.data.target.id;
    if id.level != level {
        return Err(anyhow!(
            "an unfinished level {0} backup exists (state {1:?}, parts in {2:?}); finish it \
             with `bcb backup --level {0} --task {3}`, or to abandon it delete both paths",
            id.level,
            state.path(),
            layout.backup_parts_dir(id),
            task.name
        ));
    }
    tracing::info!("resuming the unfinished {id}");
    Ok(Some(state))
}
