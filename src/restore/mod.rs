//! `bcb restore`: downloads a backup's parts and receives the snapshot they hold.
//!
//! Archived parts must be retrieved before they can be downloaded, which takes hours to days,
//! so a restore is driven by repeated runs. Each run requests the next retrievals (see
//! [`pacing`]), downloads whatever is retrievable now, and returns; once every part is
//! downloaded, a run verifies them and receives the snapshot. Progress is kept in the work
//! dir, so an interrupted run loses at most one range chunk.

mod download;
mod pacing;
mod receive;

use age::x25519;
use anyhow::{Context, Result, anyhow};
use std::path::{Path, PathBuf};

use crate::{
    btrfs::BtrfsOps,
    config::{Config, Task},
    crypto::ensure_key_pair,
    fsutil::{available_space_bytes, device_of, write_atomic},
    layout::{TaskLayout, part_file_name},
    manifest::BackupManifest,
    remote::{RemoteBackend, RetrievalStatus, RetrievalTier},
    state::{PartDownloadState, RestoreState, StateFile},
    timestamp::{BackupId, Timestamp},
    util::GIB,
};

/// Retrieval tier and lifetime for archived objects.
pub struct RetrievalOptions {
    /// Days a retrieved copy stays downloadable.
    pub days: i32,
    pub tier: RetrievalTier,
}

pub struct RestoreRequest {
    pub level: u32,
    pub timestamp: Timestamp,
    pub target_dir: PathBuf,
    pub identity: x25519::Identity,
    pub retrieval: RetrievalOptions,
}

/// The outcome of one run: the restore finished (the snapshot was received and verified), or
/// it made what progress it could and must be run again later.
#[derive(Debug, Clone, Copy)]
pub struct RestoreProgress {
    pub completed: bool,
    pub parts_downloaded: usize,
    pub parts_total: usize,
    pub bytes_remaining: u64,
}

/// Runs one step of restoring `request`'s backup of `task`; see the module docs.
pub async fn restore_task<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    task: &Task,
    request: &RestoreRequest,
    btrfs: &B,
    remote: &R,
) -> Result<RestoreProgress> {
    let buffer_target = pacing::retrieval_buffer_target(&config.s3.restore, &request.retrieval)?;
    let layout = TaskLayout::new(config, task)?;
    let id = BackupId {
        level: request.level,
        timestamp: request.timestamp.clone(),
    };
    let work_dir = layout.restore_work_dir(&id);
    tokio::fs::create_dir_all(&work_dir)
        .await
        .with_context(|| format!("failed to create {work_dir:?}"))?;

    let manifest = load_manifest(
        remote,
        &layout.manifest_key(&id),
        &work_dir.join("manifest.yaml"),
        &request.retrieval,
    )
    .await?;
    let state = load_state(work_dir.join("state.yaml"), &manifest)?;
    preflight(&manifest, &state.data, request, &work_dir)?;

    let mut run = RestoreRun {
        config,
        remote,
        btrfs,
        request,
        layout,
        id,
        manifest,
        buffer_target,
        work_dir,
        state,
    };
    let pending = run.progress();
    pacing::log_schedule(
        &config.s3.restore,
        &request.retrieval,
        buffer_target,
        pending.parts_total - pending.parts_downloaded,
        pending.bytes_remaining,
    );
    run.run().await.inspect_err(|e| {
        tracing::error!("restore failed: {e:#}");
        tracing::info!(
            "restore state preserved at {:?} for resumption",
            run.state.path()
        );
    })
}

/// The manifest, from the copy cached in the work dir if there is one. A paced restore can
/// span weeks while a retrieved copy of an archived manifest only lives `--restore-days`;
/// without the cache, every expiry would stall the restore on a fresh manifest retrieval even
/// with part copies sitting ready.
async fn load_manifest<R: RemoteBackend>(
    remote: &R,
    key: &str,
    cache_path: &Path,
    retrieval: &RetrievalOptions,
) -> Result<BackupManifest> {
    if let Ok(bytes) = tokio::fs::read(cache_path).await {
        return BackupManifest::parse(&bytes)
            .with_context(|| format!("failed to parse cached manifest {cache_path:?}"));
    }

    match remote.retrieval_status(key).await?.status {
        RetrievalStatus::Ready => {}
        RetrievalStatus::NotRequested => {
            tracing::warn!("manifest is archived, requesting retrieval");
            remote
                .request_retrieval(key, retrieval.days, retrieval.tier)
                .await?;
            return Err(anyhow!(
                "manifest archived, retrieval requested; re-run restore once retrieval completes"
            ));
        }
        RetrievalStatus::InProgress => {
            return Err(anyhow!(
                "manifest retrieval already in progress; re-run restore once it completes"
            ));
        }
    }

    let bytes = remote.download(key).await?;
    // Parsed before caching, so a corrupt download is never kept.
    let manifest =
        BackupManifest::parse(&bytes).with_context(|| format!("failed to parse manifest {key}"))?;
    write_atomic(cache_path, &bytes)?;
    Ok(manifest)
}

/// The state at `path` if it belongs to this exact manifest; otherwise a fresh one.
fn load_state(path: PathBuf, manifest: &BackupManifest) -> Result<StateFile<RestoreState>> {
    if let Some(state) = StateFile::<RestoreState>::load(path.clone())?
        && state.data.manifest_blake3 == manifest.blake3
    {
        tracing::info!("resuming from existing restore state");
        return Ok(state);
    }
    tracing::info!("starting fresh restore");
    let fresh = RestoreState {
        manifest_blake3: manifest.blake3.clone(),
        parts: manifest
            .parts
            .iter()
            .map(|p| (p.index.clone(), PartDownloadState::NotDownloaded))
            .collect(),
        receive_completed: false,
    };
    StateFile::create(path, fresh)
}

/// Checks what would otherwise only fail after retrievals were paid for, or deep into the
/// restore: the key, the parent snapshot an incremental needs, and local space.
fn preflight(
    manifest: &BackupManifest,
    state: &RestoreState,
    request: &RestoreRequest,
    work_dir: &Path,
) -> Result<()> {
    ensure_key_pair(&manifest.age_public_key, &request.identity)
        .context("the backup was encrypted to another key; no retrieval was requested")?;

    let target_dir = &request.target_dir;
    if let Some(parent) = &manifest.parent_snapshot
        && !target_dir.join(parent).exists()
    {
        return Err(anyhow!(
            "parent snapshot '{parent}' not found in {target_dir:?}; restore the level {} \
             backup it was built on first",
            request.level.saturating_sub(1)
        ));
    }

    let required = required_restore_space(
        pending_bytes(manifest, state),
        manifest.total_size_bytes,
        device_of(target_dir)? == device_of(work_dir)?,
    );
    ensure_restore_space(available_space_bytes(work_dir)?, required, work_dir)
}

fn pending_bytes(manifest: &BackupManifest, state: &RestoreState) -> u64 {
    manifest
        .parts
        .iter()
        .filter(|p| state.parts.get(&p.index) != Some(&PartDownloadState::Downloaded))
        .map(|p| p.size_bytes)
        .sum()
}

/// Space the work dir's filesystem needs for the rest of a restore: the parts still to be
/// downloaded (kept encrypted until the receive), plus the received snapshot when the target
/// is on the same filesystem.
fn required_restore_space(pending_part_bytes: u64, stream_bytes: u64, target_same_fs: bool) -> u64 {
    pending_part_bytes + if target_same_fs { stream_bytes } else { 0 }
}

fn ensure_restore_space(available: u64, required: u64, work_dir: &Path) -> Result<()> {
    if available < required {
        return Err(anyhow!(
            "insufficient free space for the restore work dir {work_dir:?}: {:.1} GiB \
             available, ~{:.1} GiB needed; free up space or point base_dir at a larger \
             filesystem before retrying",
            available as f64 / GIB,
            required as f64 / GIB,
        ));
    }
    Ok(())
}

/// One run of a restore, with what its stages share.
struct RestoreRun<'a, B: BtrfsOps, R: RemoteBackend> {
    config: &'a Config,
    remote: &'a R,
    btrfs: &'a B,
    request: &'a RestoreRequest,
    layout: TaskLayout,
    id: BackupId,
    manifest: BackupManifest,
    buffer_target: u64,
    work_dir: PathBuf,
    state: StateFile<RestoreState>,
}

impl<B: BtrfsOps, R: RemoteBackend> RestoreRun<'_, B, R> {
    async fn run(&mut self) -> Result<RestoreProgress> {
        // Retrieval status is checked live: since the last run a retrieved copy may have
        // expired, or a requested one become ready.
        let live = self.fetch_live_statuses().await?;
        let live_bytes = |status: RetrievalStatus| -> u64 {
            self.manifest
                .parts
                .iter()
                .filter(|p| live.get(&p.index).is_some_and(|r| r.status == status))
                .map(|p| p.size_bytes)
                .sum()
        };
        let ready_bytes = live_bytes(RetrievalStatus::Ready);
        let in_progress_bytes = live_bytes(RetrievalStatus::InProgress);

        // Requested before downloading: requests are instant while draining the ready parts
        // can take days, and AWS prepares the next ones meanwhile. Requesting afterwards would
        // serialize each cycle into download-then-wait.
        let unrequested: Vec<pacing::Unrequested> = self
            .manifest
            .parts
            .iter()
            .filter(|p| {
                live.get(&p.index)
                    .is_some_and(|r| r.status == RetrievalStatus::NotRequested)
            })
            .map(|p| pacing::Unrequested {
                index: &p.index,
                size_bytes: p.size_bytes,
                partially_downloaded: matches!(
                    self.state.data.parts.get(&p.index),
                    Some(PartDownloadState::PartiallyDownloaded { .. })
                ),
            })
            .collect();
        let selected = pacing::select_retrievals(
            &unrequested,
            ready_bytes,
            in_progress_bytes,
            self.buffer_target,
        );
        for index in selected {
            tracing::info!("requesting retrieval for part {index}");
            let retrieval = &self.request.retrieval;
            self.remote
                .request_retrieval(&self.part_key(&index), retrieval.days, retrieval.tier)
                .await?;
        }

        let parts_failed = self.download_ready_parts(&live).await?;

        let progress = self.progress();
        if progress.parts_downloaded < progress.parts_total {
            tracing::info!(
                "waiting for retrievals: {}/{} parts downloaded, {parts_failed} download \
                 failures this run, {ready_bytes} bytes ready, {in_progress_bytes} bytes in \
                 progress",
                progress.parts_downloaded,
                progress.parts_total,
            );
            return Ok(progress);
        }
        if !self.finalize().await? {
            return Ok(self.progress());
        }
        Ok(RestoreProgress {
            completed: true,
            ..progress
        })
    }

    fn progress(&self) -> RestoreProgress {
        let parts_total = self.manifest.parts.len();
        RestoreProgress {
            completed: false,
            parts_downloaded: parts_total - self.state.data.pending_parts().count(),
            parts_total,
            bytes_remaining: pending_bytes(&self.manifest, &self.state.data),
        }
    }

    fn part_key(&self, index: &str) -> String {
        self.layout.part_key(&self.id, index)
    }

    fn part_path(&self, index: &str) -> PathBuf {
        self.work_dir.join(part_file_name(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_restore_space_counts_the_receive_only_on_the_same_filesystem() {
        assert_eq!(required_restore_space(10, 100, false), 10);
        assert_eq!(required_restore_space(10, 100, true), 110);
        assert_eq!(required_restore_space(0, 100, false), 0);
    }

    #[test]
    fn ensure_restore_space_boundaries() {
        let dir = Path::new("/work");
        assert!(ensure_restore_space(50, 50, dir).is_ok());
        assert!(ensure_restore_space(51, 50, dir).is_ok());
        let err = ensure_restore_space(49, 50, dir).unwrap_err().to_string();
        assert!(
            err.contains("insufficient free space"),
            "unexpected error: {err}"
        );
    }
}
