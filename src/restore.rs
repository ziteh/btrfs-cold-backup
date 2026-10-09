use age::x25519;
use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{
    Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_to_writer},
    manifest::{Backup, parse_backup},
    remote::{RestoreStatus, RestoreStatusDetail, RestoreTier},
    state::{PartDownloadState, RestorePartState, RestoreState, read_state},
    util::{
        BYTES_PER_KBPS, GIB, available_space_bytes, compact_to_iso, restore_state_path,
        restore_work_dir, s3_backup_key, s3_manifest_key, sanitize_subvolume_path, write_atomic,
    },
};

// Test-only override so chunked-download resume can be exercised without a multi-MiB fixture.
#[cfg(debug_assertions)]
fn part_chunk_size() -> usize {
    std::env::var("BCB_TEST_PART_CHUNK_SIZE")
        .ok()
        .map(|v| {
            v.parse()
                .expect("BCB_TEST_PART_CHUNK_SIZE must be a byte count")
        })
        .unwrap_or(64 * 1024 * 1024)
}

#[cfg(not(debug_assertions))]
fn part_chunk_size() -> usize {
    64 * 1024 * 1024
}

// Test-only pause point: parks here right after a chunk's progress is saved (so the on-disk
// state and file both reflect exactly `downloaded` bytes) so the VM e2e suite can SIGKILL bcb
// at a deterministic point to exercise resuming a partially-downloaded part, instead of
// racing on sleep timing.
#[cfg(debug_assertions)]
async fn test_pause_after_downloaded_bytes(downloaded: u64) {
    if let Ok(target) = std::env::var("BCB_TEST_PAUSE_AFTER_DOWNLOADED_BYTES")
        && target.parse() == Ok(downloaded)
    {
        use std::io::Write;
        eprintln!("BCB_TEST_PAUSE_REACHED:downloaded_bytes:{downloaded}");
        let _ = std::io::stderr().flush();
        std::future::pending::<()>().await;
    }
}

#[cfg(not(debug_assertions))]
async fn test_pause_after_downloaded_bytes(_downloaded: u64) {}

/// Retrieval tier and lifetime for archived (Glacier / Deep Archive) objects.
pub struct RetrievalOptions {
    pub days: i32,
    pub tier: RestoreTier,
}

/// Outcome of one `restore_task` invocation: either the restore fully finished (the
/// snapshot was received and verified), or this run made partial progress (downloaded
/// whatever was ready, maybe requested another retrieval wave) and must be re-run later.
#[derive(Debug, Clone, Copy)]
pub struct RestoreProgress {
    pub completed: bool,
    pub parts_downloaded: usize,
    pub parts_total: usize,
    pub bytes_remaining: u64,
}

/// Calculates recommended lead_days based on retrieval tier (AWS SLA + safety margin).
///
/// AWS does not expose an API to query current retrieval latency — these are static
/// estimates from AWS's published documentation, taking the larger of the Glacier and
/// Deep Archive SLA for each tier (since a single retrieval tier can back either storage
/// class) with a 1.5x safety margin. Override via `restore.lead_days` if these drift from
/// AWS's current published values.
fn default_lead_days(tier: RestoreTier) -> f64 {
    match tier {
        RestoreTier::Expedited => 0.05, // Glacier only, ~1-5min SLA
        RestoreTier::Standard => 0.75,  // max(Glacier ~5h, Deep Archive ~12h) * 1.5
        RestoreTier::Bulk => 3.0,       // max(Glacier ~12h, Deep Archive ~48h) * 1.5
    }
}

/// Calculates the daily download budget in bytes from expected kbps.
fn daily_budget_bytes(expected_download_kbps: u64) -> u64 {
    if expected_download_kbps == 0 {
        return 0;
    }
    (expected_download_kbps as f64 * BYTES_PER_KBPS * 86400.0) as u64
}

/// Decrypts `enc_paths` in order into `out`, reassembling the send stream. Blocking.
fn decrypt_parts(
    enc_paths: &[PathBuf],
    identity: &x25519::Identity,
    out: &mut impl std::io::Write,
) -> Result<()> {
    for path in enc_paths {
        decrypt_to_writer(path, identity, out)?;
    }
    Ok(())
}

async fn stream_hash(enc_paths: Vec<PathBuf>, identity: x25519::Identity) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        decrypt_parts(&enc_paths, &identity, &mut hasher)?;
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await?
}

/// Decrypts the parts straight into `btrfs receive`, so no plaintext touches the disk.
async fn pipe_to_btrfs_receive(
    enc_paths: Vec<PathBuf>,
    identity: x25519::Identity,
    target_dir: &Path,
) -> Result<()> {
    tokio::fs::create_dir_all(target_dir).await?;
    let target_dir = target_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut child = std::process::Command::new("btrfs")
            .arg("receive")
            .arg(&target_dir)
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
        let written = decrypt_parts(&enc_paths, &identity, &mut stdin);
        // Closing stdin is the end of the stream for receive.
        drop(stdin);

        // Checked first: a receive that died makes the write fail too, and its stderr says why.
        let output = child.wait_with_output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("btrfs receive failed: {}", stderr.trim()));
        }
        written
    })
    .await?
}

/// Downloads a single part in `chunk_size` ranges, persisting progress to `RestoreState`
/// after every chunk so a crash mid-part only loses the in-flight chunk, not the whole part.
/// The part is left `PartiallyDownloaded` at its full size; only the caller's hash check
/// makes it `Downloaded`.
async fn download_part_with_resume<R: RemoteBackend>(
    remote: &R,
    key: &str,
    chunk_size: u64,
    target_path: &Path,
    state: &mut RestoreState,
    part_index: &str,
    state_path: &Path,
) -> Result<()> {
    let part = state
        .parts
        .get(part_index)
        .ok_or_else(|| anyhow!("unknown part index {part_index}"))?;
    let part_size = part.size_bytes;
    let resumed_from = match part.download_state {
        PartDownloadState::PartiallyDownloaded { downloaded_bytes } => downloaded_bytes,
        _ => 0,
    };

    // The on-disk file's actual length is the source of truth for how many bytes are
    // genuinely present: a crash mid-chunk-write can leave the file longer than the last
    // confirmed-and-saved offset. Truncate back to the confirmed offset (or start over if
    // the file is missing/shorter than expected) before resuming with Range appends.
    let mut downloaded = if resumed_from > 0 {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .open(target_path)
            .await
        {
            Ok(f) => match f.metadata().await {
                Ok(meta) if meta.len() >= resumed_from => {
                    f.set_len(resumed_from).await?;
                    resumed_from
                }
                _ => 0,
            },
            Err(_) => 0,
        }
    } else {
        0
    };

    while downloaded < part_size {
        let chunk_end = std::cmp::min(downloaded + chunk_size - 1, part_size - 1);
        remote
            .download_range_to_file(key, downloaded, chunk_end, target_path)
            .await?;
        // A body cut short without an error would otherwise shift every later chunk.
        let len = tokio::fs::metadata(target_path).await?.len();
        if len != chunk_end + 1 {
            return Err(anyhow!(
                "range {downloaded}-{chunk_end} of {key} left {target_path:?} at {len} bytes, \
                 expected {}",
                chunk_end + 1
            ));
        }

        downloaded = chunk_end + 1;
        if let Some(part) = state.parts.get_mut(part_index) {
            part.download_state = PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: downloaded,
            };
        }
        state.save(state_path)?;
        test_pause_after_downloaded_bytes(downloaded).await;
    }
    Ok(())
}

/// Fetches the live S3 retrieval status of every part that still needs downloading, bounded
/// to `restore.status_check_concurrency` concurrent HeadObject calls so a 1000-part restore
/// doesn't burst 1000 simultaneous requests.
///
/// The returned map is what this run acts on; the persisted per-part states are also
/// refreshed as an informational cache (a part with byte progress keeps its
/// `PartiallyDownloaded` offset — the live status alone decides whether it can be resumed
/// now). Parts whose status check failed are absent from the map and skipped this run; if
/// every check failed, e.g. with the network down, this fails instead so the run exits 1
/// rather than reporting progress it never makes.
async fn fetch_live_statuses<R: RemoteBackend>(
    remote: &R,
    keys_by_index: &HashMap<String, String>,
    state: &RestoreState,
    config: &Config,
) -> Result<HashMap<String, RestoreStatusDetail>> {
    let concurrency = (config.s3.restore.status_check_concurrency as usize).max(1);

    let parts_to_check: Vec<_> = state
        .parts
        .iter()
        .filter(|(_, p)| !matches!(p.download_state, PartDownloadState::Downloaded))
        .map(|(idx, _)| idx.clone())
        .collect();

    let results: Vec<_> = futures_util::stream::iter(parts_to_check)
        .map(|index| {
            let key = keys_by_index.get(&index).cloned();
            async move {
                let key = match key {
                    Some(k) => k,
                    None => return (index, Err(anyhow!("no S3 key for part"))),
                };

                match remote.restore_status_detail(&key).await {
                    Ok(detail) => (index, Ok(detail)),
                    Err(e) => {
                        tracing::warn!("failed to check status for part {}: {}", index, e);
                        (index, Err(e))
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut live = HashMap::new();
    let mut first_failure = None;
    for (index, detail) in results {
        match detail {
            Ok(detail) => {
                live.insert(index, detail);
            }
            Err(e) => {
                first_failure.get_or_insert(e);
            }
        }
    }
    if live.is_empty()
        && let Some(e) = first_failure
    {
        return Err(e.context("every part status check failed this run"));
    }

    Ok(live)
}

/// Fetches the manifest from S3 once and caches it in the work dir. A paced restore can span
/// weeks while the manifest's retrieved copy only lives `--restore-days`; without the cache
/// every expiry would stall the whole restore on a fresh manifest retrieval (hours to days on
/// archive tiers) even when part copies are sitting ready.
async fn load_or_fetch_manifest<R: RemoteBackend>(
    remote: &R,
    manifest_key: &str,
    cache_path: &Path,
    retrieval: &RetrievalOptions,
) -> Result<Backup> {
    if let Ok(bytes) = tokio::fs::read(cache_path).await {
        return parse_backup(&bytes);
    }

    // The manifest may itself be archived. Both NotRequested (needs a retrieval kicked off)
    // and InProgress (already requested, not yet Ready) must bail out here — downloading an
    // archived-and-not-yet-restored object fails at S3.
    match remote.restore_status_detail(manifest_key).await?.status {
        RestoreStatus::Ready => {}
        RestoreStatus::NotRequested => {
            tracing::warn!("manifest is archived, requesting retrieval");
            remote
                .request_restore(manifest_key, retrieval.days, retrieval.tier)
                .await?;
            return Err(anyhow!(
                "manifest archived, retrieval requested; re-run restore task once retrieval completes"
            ));
        }
        RestoreStatus::InProgress => {
            return Err(anyhow!(
                "manifest retrieval already in progress; re-run restore task once it completes"
            ));
        }
    }

    let bytes = remote.download(manifest_key).await?;
    // Parse before caching so a corrupt download is never persisted.
    let backup = parse_backup(&bytes)?;
    write_atomic(cache_path, &bytes)?;
    Ok(backup)
}

/// Resumes the state file only if it belongs to this exact manifest; otherwise starts fresh.
fn load_or_init_state(state_path: &Path, backup: &Backup) -> Result<RestoreState> {
    if let Some(state) = read_state::<RestoreState>(state_path)?
        && state.manifest_blake3 == backup.blake3
    {
        tracing::info!("resuming from existing restore state");
        return Ok(state);
    }

    tracing::info!("starting fresh restore");
    let mut state = RestoreState {
        manifest_blake3: backup.blake3.clone(),
        parts: backup
            .parts
            .iter()
            .map(|p| {
                (
                    p.index.clone(),
                    RestorePartState {
                        size_bytes: p.size_bytes,
                        blake3: p.blake3.clone(),
                        download_state: PartDownloadState::NotDownloaded,
                    },
                )
            })
            .collect(),
        receive_completed: false,
        last_updated: 0,
    };
    state.save(state_path)?;
    Ok(state)
}

/// Computes how many bytes of retrieval may be in flight at once, and rejects a
/// `--restore-days` too short for the configured pacing before any S3 side effect happens.
fn retrieval_buffer_target(config: &Config, retrieval: &RetrievalOptions) -> Result<u64> {
    let lead_days = config
        .s3
        .restore
        .lead_days
        .unwrap_or_else(|| default_lead_days(retrieval.tier));
    let daily_budget = daily_budget_bytes(config.s3.restore.expected_download_kbps);

    // With pacing enabled, a wave of retrieved copies is drained over ~lead_days of
    // downloading; a copy expiring before its turn wastes per-GB retrieval fees on a
    // re-request.
    if daily_budget > 0 && (retrieval.days as f64) < lead_days + 1.0 {
        return Err(anyhow!(
            "retrieved copies would expire before the paced download reaches them: \
             --restore-days is {} but pacing needs at least {} (lead_days {} plus one day \
             of margin); raise --restore-days or lower restore.lead_days",
            retrieval.days,
            (lead_days + 1.0).ceil() as i64,
            lead_days
        ));
    }

    // expected_download_kbps == 0 means "no pacing estimate given" — request every remaining
    // part immediately rather than use a target of 0 bytes, which no part could ever fit
    // under, silently never requesting any retrieval at all.
    Ok(if daily_budget == 0 {
        u64::MAX
    } else {
        (lead_days * daily_budget as f64) as u64
    })
}

/// One invocation of the restore pipeline, holding the context its four phases share.
struct RestoreRun<'a, R: RemoteBackend> {
    config: &'a Config,
    remote: &'a R,
    retrieval: &'a RetrievalOptions,
    target_dir: &'a Path,
    identity: x25519::Identity,
    backup: Backup,
    part_keys: HashMap<String, String>,
    buffer_target: u64,
    work_dir: PathBuf,
    state_path: PathBuf,
    state: RestoreState,
}

impl<R: RemoteBackend> RestoreRun<'_, R> {
    async fn run(&mut self) -> Result<RestoreProgress> {
        // Live S3 retrieval status of every part still needing download; the persisted
        // per-part states alone can't be trusted for "downloadable now" (a retrieved copy
        // may have expired since the last run, or a partial download's copy may still be
        // valid).
        let live =
            fetch_live_statuses(self.remote, &self.part_keys, &self.state, self.config).await?;

        let live_bytes = |status: RestoreStatus| -> u64 {
            self.state
                .parts
                .iter()
                .filter(|(index, _)| {
                    live.get(*index)
                        .is_some_and(|detail| detail.status == status)
                })
                .map(|(_, p)| p.size_bytes)
                .sum()
        };
        let ready_bytes = live_bytes(RestoreStatus::Ready);
        let in_progress_bytes = live_bytes(RestoreStatus::InProgress);

        self.request_retrievals(&live, ready_bytes, in_progress_bytes)
            .await?;
        let parts_failed = self.download_ready_parts(&live).await?;

        let progress = self.progress();
        if progress.parts_downloaded < progress.parts_total {
            tracing::info!(
                "waiting for retrievals: {}/{} parts downloaded, {} download failures this run, {} bytes ready, {} bytes in progress",
                progress.parts_downloaded,
                progress.parts_total,
                parts_failed,
                ready_bytes,
                in_progress_bytes,
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
        let pending = || {
            self.state
                .parts
                .values()
                .filter(|p| !matches!(p.download_state, PartDownloadState::Downloaded))
        };
        RestoreProgress {
            completed: false,
            parts_downloaded: self.state.parts.len() - pending().count(),
            parts_total: self.state.parts.len(),
            bytes_remaining: pending().map(|p| p.size_bytes).sum(),
        }
    }

    /// Requests retrievals BEFORE downloading: requests are instant while draining the ready
    /// wave can take days, and AWS prepares the next wave in parallel — requesting afterwards
    /// would serialize each cycle into download-then-wait.
    async fn request_retrievals(
        &mut self,
        live: &HashMap<String, RestoreStatusDetail>,
        ready_bytes: u64,
        in_progress_bytes: u64,
    ) -> Result<()> {
        // Ready bytes are excluded from the budget gate because this run is about to drain
        // them; what counts against the lead buffer is what will still be arriving after this
        // run: in-progress retrievals plus the requests made here. Partially downloaded parts
        // whose copy expired are re-requested unconditionally (their bytes are already
        // invested; the resume offset stays valid since the object is immutable).
        let mut bytes_requested = 0u64;
        // Parts skipped purely because the budget was full, in case nothing else this run
        // ends up ready, in progress, or requested (see the stall-avoidance fallback below).
        let mut skipped_by_budget: Vec<String> = Vec::new();

        for part_info in &self.backup.parts {
            let Some(detail) = live.get(&part_info.index) else {
                continue;
            };
            if detail.status != RestoreStatus::NotRequested {
                continue;
            }
            let Some(part) = self.state.parts.get(&part_info.index) else {
                continue;
            };
            if matches!(part.download_state, PartDownloadState::Downloaded) {
                continue;
            }
            let partially_downloaded = matches!(
                part.download_state,
                PartDownloadState::PartiallyDownloaded { .. }
            );
            if !partially_downloaded
                && in_progress_bytes + bytes_requested + part.size_bytes > self.buffer_target
            {
                skipped_by_budget.push(part_info.index.clone());
                continue;
            }

            let Some(key) = self.part_keys.get(&part_info.index) else {
                continue;
            };
            tracing::info!("requesting retrieval for part {}", part_info.index);
            self.remote
                .request_restore(key, self.retrieval.days, self.retrieval.tier)
                .await?;
            if !partially_downloaded {
                bytes_requested += part.size_bytes;
            }
        }

        // If the budget is smaller than every remaining part, the loop above skips all of
        // them every run: nothing is ever ready, in progress, or requested, so the restore
        // would report "in progress" forever without ever advancing. Force through the
        // smallest skipped part regardless of budget so each run makes some progress.
        if bytes_requested == 0 && ready_bytes == 0 && in_progress_bytes == 0 {
            let smallest = skipped_by_budget
                .iter()
                .filter_map(|index| self.state.parts.get(index).map(|p| (index, p.size_bytes)))
                .min_by_key(|(_, size)| *size);
            if let Some((index, _)) = smallest
                && let Some(key) = self.part_keys.get(index)
            {
                tracing::warn!(
                    "restore budget is smaller than any remaining part; forcing retrieval of \
                     part {index} to avoid stalling forever"
                );
                self.remote
                    .request_restore(key, self.retrieval.days, self.retrieval.tier)
                    .await?;
            }
        }

        Ok(())
    }

    /// Downloads every part whose retrieved copy is live right now — including parts left
    /// half-downloaded by an interrupted run — earliest expiry first, so copies about to
    /// lapse are saved before the others. Returns how many parts failed to download.
    ///
    /// A remote failure skips just that part, but fails the run when no part this run
    /// tried got through, so a lasting outage exits 1 instead of reporting progress
    /// forever. A local error (disk full, permissions) fails the run at once: it would hit
    /// every other part too.
    async fn download_ready_parts(
        &mut self,
        live: &HashMap<String, RestoreStatusDetail>,
    ) -> Result<usize> {
        let mut downloadable: Vec<(String, i64)> = self
            .state
            .parts
            .iter()
            .filter(|(_, p)| !matches!(p.download_state, PartDownloadState::Downloaded))
            .filter_map(|(idx, _)| {
                let detail = live.get(idx)?;
                if detail.status == RestoreStatus::Ready {
                    Some((idx.clone(), detail.expires_at.unwrap_or(i64::MAX)))
                } else {
                    None
                }
            })
            .collect();
        downloadable.sort_by_key(|(_, expires_at)| *expires_at);

        let download_started_at = std::time::Instant::now();
        let mut downloaded_bytes = 0u64;
        let mut parts_failed = 0usize;
        let mut parts_succeeded = 0usize;
        let mut first_failure = None;
        for (part_idx, _) in downloadable {
            let (expected_blake3, size_bytes) = match self.state.parts.get(&part_idx) {
                Some(part) => (part.blake3.clone(), part.size_bytes),
                None => continue,
            };
            let key = match self.part_keys.get(&part_idx) {
                Some(k) => k.clone(),
                None => continue,
            };

            let enc_path = self.work_dir.join(format!("part-{part_idx}.age"));
            tracing::info!("downloading part {}", part_idx);
            // Draining a wave can take hours to days, so a copy that was Ready at the
            // status check may expire before its turn, and transient network failures can
            // hit any part. Neither should abandon the parts that are still downloadable;
            // the failed part's progress is already persisted chunk-by-chunk and the next
            // run picks it up again.
            if let Err(e) = download_part_with_resume(
                self.remote,
                &key,
                part_chunk_size() as u64,
                &enc_path,
                &mut self.state,
                &part_idx,
                &self.state_path,
            )
            .await
            {
                if e.chain().any(|c| c.is::<std::io::Error>()) {
                    return Err(e.context(format!("download of part {part_idx} failed")));
                }
                tracing::warn!(
                    "download of part {} failed, continuing with remaining parts: {}",
                    part_idx,
                    e
                );
                parts_failed += 1;
                first_failure.get_or_insert(e);
                continue;
            }

            let actual_hash = blake3_file(&enc_path).await?;
            let part = self
                .state
                .parts
                .get_mut(&part_idx)
                .ok_or_else(|| anyhow!("unknown part index {part_idx}"))?;
            if actual_hash != expected_blake3 {
                part.download_state = PartDownloadState::NotDownloaded;
                let _ = tokio::fs::remove_file(&enc_path).await;
                self.state.save(&self.state_path)?;
                return Err(anyhow!(
                    "BLAKE3 mismatch for part {}: expected {}, got {}",
                    part_idx,
                    expected_blake3,
                    actual_hash
                ));
            }
            part.download_state = PartDownloadState::Downloaded;
            self.state.save(&self.state_path)?;
            downloaded_bytes += size_bytes;
            parts_succeeded += 1;
        }

        if parts_succeeded == 0
            && let Some(e) = first_failure
        {
            return Err(e.context(format!(
                "all {parts_failed} part download(s) attempted this run failed"
            )));
        }

        if downloaded_bytes > 0 {
            let elapsed_secs = download_started_at.elapsed().as_secs_f64().max(0.001);
            let measured_kbps = (downloaded_bytes as f64 / elapsed_secs) / BYTES_PER_KBPS;
            tracing::info!(
                "download speed this run: {} bytes in {:.1}s, ~{:.1} kbps (compare with expected_download_kbps)",
                downloaded_bytes,
                elapsed_secs,
                measured_kbps
            );
        }

        Ok(parts_failed)
    }

    /// All parts are downloaded: verify them, pipe the decrypted stream into `btrfs receive`
    /// and clean up. Every run re-verifies from the `.age` files, since those are all that is
    /// kept on disk. Returns false if a part was damaged on disk since its download; it is
    /// reset for the next run to download again.
    async fn finalize(&mut self) -> Result<bool> {
        if !self.state.receive_completed {
            let enc_paths: Vec<PathBuf> = self
                .backup
                .parts
                .iter()
                .map(|p| self.work_dir.join(format!("part-{}.age", p.index)))
                .collect();

            // A power loss or bit rot after the download check must cost one part's
            // download, not fail the restore.
            let mut damaged = Vec::new();
            for (part, path) in self.backup.parts.iter().zip(&enc_paths) {
                let intact = match blake3_file(path).await {
                    Ok(hash) => hash == part.blake3,
                    Err(e)
                        if e.downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        false
                    }
                    Err(e) => return Err(e),
                };
                if !intact {
                    damaged.push(part.index.clone());
                }
            }
            if !damaged.is_empty() {
                for index in &damaged {
                    if let Some(part) = self.state.parts.get_mut(index) {
                        part.download_state = PartDownloadState::NotDownloaded;
                    }
                }
                self.state.save(&self.state_path)?;
                tracing::warn!(
                    "parts {damaged:?} no longer match their BLAKE3 on disk; they will be \
                     downloaded again"
                );
                return Ok(false);
            }

            // Every part checked out, so a mismatch here is a bug on the backup side, which
            // downloading again cannot fix.
            tracing::info!("verifying stream hash");
            let actual = stream_hash(enc_paths.clone(), self.identity.clone()).await?;
            if actual != self.backup.blake3 {
                return Err(anyhow!(
                    "stream BLAKE3 mismatch: expected {}, got {actual}",
                    self.backup.blake3
                ));
            }

            tracing::info!("piping to btrfs receive");
            pipe_to_btrfs_receive(enc_paths, self.identity.clone(), self.target_dir).await?;

            let snapshot_path = self.target_dir.join(&self.backup.snapshot_name);
            let verify = tokio::process::Command::new("btrfs")
                .args(["subvolume", "show"])
                .arg(&snapshot_path)
                .output()
                .await?;
            if !verify.status.success() {
                return Err(anyhow!(
                    "snapshot '{}' not found after restore",
                    self.backup.snapshot_name
                ));
            }

            self.state.receive_completed = true;
            self.state.save(&self.state_path)?;
            tracing::info!("restore completed successfully");
        }

        // Only clean up once the receive is fully verified.
        if let Err(e) = tokio::fs::remove_dir_all(&self.work_dir).await {
            tracing::warn!(
                "failed to clean up restore work dir {:?}: {e}",
                self.work_dir
            );
        }

        Ok(true)
    }
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

/// The device of `path`, or of its nearest existing ancestor if it doesn't exist yet.
fn device_of(path: &Path) -> Result<u64> {
    use std::os::unix::fs::MetadataExt;
    let path = std::path::absolute(path)?;
    let existing = path
        .ancestors()
        .find(|p| p.exists())
        .ok_or_else(|| anyhow!("no existing ancestor of {path:?}"))?;
    Ok(std::fs::metadata(existing)?.dev())
}

/// Checks what would otherwise only fail after retrievals were paid for, or deep into a
/// restore: the key, the parent snapshot an incremental needs, and local space.
fn preflight(
    backup: &Backup,
    state: &RestoreState,
    identity: &x25519::Identity,
    level: u32,
    target_dir: &Path,
    work_dir: &Path,
) -> Result<()> {
    let public_key = identity.to_public().to_string();
    if public_key != backup.age_public_key {
        return Err(anyhow!(
            "the private key belongs to {public_key}, but this backup was encrypted to {}; \
             no retrieval was requested",
            backup.age_public_key
        ));
    }

    if let Some(parent) = &backup.parent_snapshot
        && !target_dir.join(parent).exists()
    {
        return Err(anyhow!(
            "parent snapshot '{parent}' not found in {target_dir:?}; restore the level {} \
             backup it was built on first",
            level.saturating_sub(1)
        ));
    }

    let pending_part_bytes = state
        .parts
        .values()
        .filter(|p| !matches!(p.download_state, PartDownloadState::Downloaded))
        .map(|p| p.size_bytes)
        .sum();
    let required = required_restore_space(
        pending_part_bytes,
        backup.total_size_bytes,
        device_of(target_dir)? == device_of(work_dir)?,
    );
    ensure_restore_space(available_space_bytes(work_dir)?, required, work_dir)
}

// The argument count is this function's public shape, as called from main.
#[allow(clippy::too_many_arguments)]
pub async fn restore_task<R: RemoteBackend>(
    config: &Config,
    task: &Task,
    level: u32,
    date: &str,
    target_dir: &Path,
    private_key_str: &str,
    retrieval: &RetrievalOptions,
    remote: &R,
) -> Result<RestoreProgress> {
    let identity: x25519::Identity = private_key_str
        .parse()
        .map_err(|e| anyhow!("invalid age private key: {e}"))?;
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let timestamp = compact_to_iso(date)?;
    let buffer_target = retrieval_buffer_target(config, retrieval)?;

    let work_dir = restore_work_dir(&config.base_dir, &sanitized, level, &timestamp);
    tokio::fs::create_dir_all(&work_dir).await?;

    let backup = load_or_fetch_manifest(
        remote,
        &s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp),
        &work_dir.join("manifest.yaml"),
        retrieval,
    )
    .await?;

    let state_path = restore_state_path(&work_dir);
    let state = load_or_init_state(&state_path, &backup)?;
    preflight(&backup, &state, &identity, level, target_dir, &work_dir)?;

    let daily_budget = daily_budget_bytes(config.s3.restore.expected_download_kbps);
    let remaining = || {
        state
            .parts
            .values()
            .filter(|p| !matches!(p.download_state, PartDownloadState::Downloaded))
    };
    let remaining_bytes: u64 = remaining().map(|p| p.size_bytes).sum();
    let remaining_parts = remaining().count();
    if daily_budget > 0 {
        let estimated_days = (remaining_bytes as f64 / daily_budget as f64).ceil() as u64;
        tracing::info!(
            "restore schedule: buffer_target {} bytes, daily_budget {} bytes/day, {} bytes remaining, ~{} day(s) to complete at this pace",
            buffer_target,
            daily_budget,
            remaining_bytes,
            estimated_days
        );
    } else if remaining_parts > 1 {
        tracing::warn!(
            "restore schedule: no pacing configured (s3.restore.expected_download_kbps is 0), \
             so all {remaining_parts} remaining parts ({remaining_bytes} bytes) are requested \
             for retrieval at once; parts not downloaded within --restore-days ({} days) \
             expire and must be retrieved, and paid for, again. Set expected_download_kbps \
             to pace retrievals to your bandwidth",
            retrieval.days
        );
    } else {
        tracing::info!(
            "restore schedule: no pacing configured (expected_download_kbps is 0), all {} remaining bytes will be requested for retrieval without throttling",
            remaining_bytes
        );
    }

    let part_keys = backup
        .parts
        .iter()
        .map(|p| {
            let key = s3_backup_key(&config.s3.prefix, &sanitized, level, &timestamp, &p.index);
            (p.index.clone(), key)
        })
        .collect();

    let mut run = RestoreRun {
        config,
        remote,
        retrieval,
        target_dir,
        identity,
        backup,
        part_keys,
        buffer_target,
        work_dir,
        state_path,
        state,
    };

    run.run().await.inspect_err(|e| {
        tracing::error!("restore failed: {}", e);
        tracing::info!(
            "restore state preserved at {:?} for resumption",
            run.state_path
        );
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::MockRemoteBackend;
    use crate::state::RestorePartState;
    use std::collections::BTreeMap;

    fn state_with_part(index: &str, size: u64) -> RestoreState {
        let mut parts = BTreeMap::new();
        parts.insert(
            index.to_string(),
            RestorePartState {
                size_bytes: size,
                blake3: String::new(),
                download_state: PartDownloadState::NotDownloaded,
            },
        );
        RestoreState {
            parts,
            ..Default::default()
        }
    }

    fn append_range(path: &Path, start: u64, end: u64) -> Result<()> {
        use std::io::Write;
        let mut file = if start == 0 {
            std::fs::File::create(path)?
        } else {
            std::fs::OpenOptions::new().append(true).open(path)?
        };
        let bytes: Vec<u8> = (start..=end).map(|i| i as u8).collect();
        file.write_all(&bytes)?;
        Ok(())
    }

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

    #[tokio::test]
    async fn download_part_resumes_from_saved_offset() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let state_path = dir.path().join("state.yaml");
        let mut state = state_with_part("000000", 25);

        // First run, chunk size 10: ranges 0-9 and 10-19 succeed, 20-24 fails.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, _, _| *start < 20)
            .returning(|_, start, end, path| append_range(path, start, end));
        remote
            .expect_download_range_to_file()
            .withf(|_, start, _, _| *start == 20)
            .returning(|_, _, _, _| Err(anyhow!("connection lost")));

        download_part_with_resume(
            &remote,
            "key",
            10,
            &target,
            &mut state,
            "000000",
            &state_path,
        )
        .await
        .expect_err("third chunk must fail");

        assert_eq!(
            state.parts["000000"].download_state,
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 20
            }
        );

        // Second run: must resume with a single range request starting at the saved
        // offset, never re-downloading the first two chunks.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, end, _| (*start, *end) == (20, 24))
            .times(1)
            .returning(|_, start, end, path| append_range(path, start, end));

        download_part_with_resume(
            &remote,
            "key",
            10,
            &target,
            &mut state,
            "000000",
            &state_path,
        )
        .await
        .expect("resumed download should succeed");

        assert_eq!(
            state.parts["000000"].download_state,
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 25
            }
        );
        let content = std::fs::read(&target).unwrap();
        let expected: Vec<u8> = (0u64..25).map(|i| i as u8).collect();
        assert_eq!(content, expected);
    }

    #[tokio::test]
    async fn download_part_fails_when_a_range_comes_back_short() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let state_path = dir.path().join("state.yaml");
        let mut state = state_with_part("000000", 25);

        // The second range ends a byte early without an error.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .returning(|_, start, end, path| {
                append_range(path, start, if start == 10 { end - 1 } else { end })
            });

        download_part_with_resume(
            &remote,
            "key",
            10,
            &target,
            &mut state,
            "000000",
            &state_path,
        )
        .await
        .expect_err("a short range must fail the part");

        assert_eq!(
            state.parts["000000"].download_state,
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 10
            }
        );
    }

    #[tokio::test]
    async fn download_part_restarts_when_file_shorter_than_saved_offset() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let state_path = dir.path().join("state.yaml");
        let mut state = state_with_part("000000", 10);
        state.parts.get_mut("000000").unwrap().download_state =
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 8,
            };
        // On-disk file is shorter than the saved offset (e.g. truncated by the OS):
        // the download must start over from 0 rather than appending at the wrong offset.
        std::fs::write(&target, [0u8; 3]).unwrap();

        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, end, _| (*start, *end) == (0, 9))
            .times(1)
            .returning(|_, start, end, path| append_range(path, start, end));

        download_part_with_resume(
            &remote,
            "key",
            10,
            &target,
            &mut state,
            "000000",
            &state_path,
        )
        .await
        .expect("restarted download should succeed");

        let content = std::fs::read(&target).unwrap();
        assert_eq!(content.len(), 10);
    }
}
