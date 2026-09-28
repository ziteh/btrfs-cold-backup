use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    Config, RemoteBackend, Task,
    crypto::{blake3_file, decrypt_file},
    manifest::{Backup, parse_backup},
    remote::{RestoreStatus, RestoreStatusDetail},
    state::{PartDownloadState, RestorePartState, RestoreState, read_state},
    util::{
        BYTES_PER_KBPS, IO_BUF_SIZE, compact_to_iso, s3_backup_key, s3_manifest_key,
        sanitize_subvolume_path,
    },
};

// Test-only override so chunked-download resume can be exercised without a multi-MiB fixture.
#[cfg(debug_assertions)]
fn part_chunk_size() -> usize {
    std::env::var("BCB_TEST_PART_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
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
    pub tier: String,
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
fn default_lead_days(tier: &str) -> f64 {
    match tier {
        "Expedited" => 0.05, // Glacier only, ~1-5min SLA
        "Standard" => 0.75,  // max(Glacier ~5h, Deep Archive ~12h) * 1.5
        "Bulk" => 3.0,       // max(Glacier ~12h, Deep Archive ~48h) * 1.5
        _ => 1.0,
    }
}

/// Calculates the daily download budget in bytes from expected kbps.
fn daily_budget_bytes(expected_download_kbps: u64) -> u64 {
    if expected_download_kbps == 0 {
        return 0;
    }
    (expected_download_kbps as f64 * BYTES_PER_KBPS * 86400.0) as u64
}

/// Returns the restoration working directory (deterministic, not random).
fn restore_work_dir(base_dir: &Path, task_name: &str, level: u32, timestamp: &str) -> PathBuf {
    base_dir.join(format!(
        "restore-{}-level{}-{}",
        task_name, level, timestamp
    ))
}

/// Returns the restoration state file path.
fn restore_state_file(work_dir: &Path) -> PathBuf {
    work_dir.join("restore-state.yaml")
}

async fn pipe_to_btrfs_receive(dec_paths: &[std::path::PathBuf], target_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(target_dir).await?;
    let target_str = target_dir
        .to_str()
        .ok_or_else(|| anyhow!("target_dir contains invalid UTF-8"))?;

    let mut child = tokio::process::Command::new("btrfs")
        .args(["receive", target_str])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
        let mut buf = vec![0u8; IO_BUF_SIZE];
        for dec_path in dec_paths {
            let mut f = tokio::fs::File::open(dec_path).await?;
            loop {
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                stdin.write_all(&buf[..n]).await?;
            }
            // Deliberately not deleted here: if btrfs receive or the subvolume verification
            // that follows fails, a retry needs these files still present. The caller deletes
            // the whole work dir only once the receive is fully verified.
        }
    }

    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("btrfs receive failed: {}", stderr.trim()));
    }

    Ok(())
}

/// Verifies the whole-stream BLAKE3 across the decrypted parts in order.
async fn verify_stream_hash(dec_paths: &[std::path::PathBuf], expected: &str) -> Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; IO_BUF_SIZE];
    for dec_path in dec_paths {
        let mut f = tokio::fs::File::open(dec_path).await?;
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }
    let actual = hasher.finalize().to_hex().to_string();
    if actual != expected {
        return Err(anyhow!(
            "stream BLAKE3 mismatch: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

/// Downloads a single part in `chunk_size` ranges, persisting progress to `RestoreState`
/// after every chunk so a crash mid-part only loses the in-flight chunk, not the whole part.
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

        downloaded = chunk_end + 1;
        if let Some(part) = state.parts.get_mut(part_index) {
            part.download_state = PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: downloaded,
            };
        }
        state.save(state_path)?;
        test_pause_after_downloaded_bytes(downloaded).await;
    }

    if let Some(part) = state.parts.get_mut(part_index) {
        part.download_state = PartDownloadState::Downloaded;
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
/// now). Parts whose status check failed are absent from the map and skipped this run.
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
                    None => return (index, None),
                };

                match remote.restore_status_detail(&key).await {
                    Ok(detail) => (index, Some(detail)),
                    Err(e) => {
                        tracing::warn!("failed to check status for part {}: {}", index, e);
                        (index, None)
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut live = HashMap::new();
    for (index, detail) in results {
        if let Some(detail) = detail {
            live.insert(index, detail);
        }
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
                .request_restore(manifest_key, retrieval.days, &retrieval.tier)
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
    let tmp = cache_path.with_extension("yaml.tmp");
    tokio::fs::write(&tmp, &bytes).await?;
    tokio::fs::rename(&tmp, cache_path).await?;
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
        stream_verified: false,
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
        .unwrap_or_else(|| default_lead_days(&retrieval.tier));
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
    private_key: &'a str,
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

        let parts_total = self.state.parts.len();
        let parts_downloaded = self
            .state
            .parts
            .values()
            .filter(|p| matches!(p.download_state, PartDownloadState::Downloaded))
            .count();

        if parts_downloaded < parts_total {
            let bytes_remaining: u64 = self
                .state
                .parts
                .values()
                .filter(|p| !matches!(p.download_state, PartDownloadState::Downloaded))
                .map(|p| p.size_bytes)
                .sum();
            tracing::info!(
                "waiting for retrievals: {}/{} parts downloaded, {} download failures this run, {} bytes ready, {} bytes in progress",
                parts_downloaded,
                parts_total,
                parts_failed,
                ready_bytes,
                in_progress_bytes,
            );
            return Ok(RestoreProgress {
                completed: false,
                parts_downloaded,
                parts_total,
                bytes_remaining,
            });
        }

        self.finalize().await?;
        Ok(RestoreProgress {
            completed: true,
            parts_downloaded,
            parts_total,
            bytes_remaining: 0,
        })
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
                .request_restore(key, self.retrieval.days, &self.retrieval.tier)
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
                    .request_restore(key, self.retrieval.days, &self.retrieval.tier)
                    .await?;
            }
        }

        Ok(())
    }

    /// Downloads every part whose retrieved copy is live right now — including parts left
    /// half-downloaded by an interrupted run — earliest expiry first, so copies about to
    /// lapse are saved before the others. Returns how many parts failed to download.
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
                tracing::warn!(
                    "download of part {} failed, continuing with remaining parts: {}",
                    part_idx,
                    e
                );
                parts_failed += 1;
                continue;
            }

            let actual_hash = blake3_file(&enc_path).await?;
            if actual_hash != expected_blake3 {
                let _ = tokio::fs::remove_file(&enc_path).await;
                return Err(anyhow!(
                    "BLAKE3 mismatch for part {}: expected {}, got {}",
                    part_idx,
                    expected_blake3,
                    actual_hash
                ));
            }

            let dec_path = self.work_dir.join(format!("part-{part_idx}.btrfs"));
            decrypt_file(&enc_path, &dec_path, self.private_key).await?;
            let _ = tokio::fs::remove_file(&enc_path).await;

            if let Some(part) = self.state.parts.get_mut(&part_idx) {
                part.download_state = PartDownloadState::Downloaded;
            }
            self.state.save(&self.state_path)?;
            downloaded_bytes += size_bytes;
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

    /// All parts are downloaded: verify the reassembled stream, pipe it into `btrfs receive`
    /// and clean up. `stream_verified` and `receive_completed` are two separate flags (not
    /// one "did the final step" flag) so a crash between the hash check and the receive step
    /// doesn't cause a resumed run to skip re-attempting receive — that would report success
    /// without ever writing the subvolume.
    async fn finalize(&mut self) -> Result<()> {
        let mut dec_paths = Vec::new();
        for part in &self.backup.parts {
            let dec_path = self.work_dir.join(format!("part-{}.btrfs", part.index));
            match tokio::fs::metadata(&dec_path).await {
                Ok(_) => dec_paths.push(dec_path),
                Err(_) => return Err(anyhow!("decrypted part {} not found", part.index)),
            }
        }

        if !self.state.stream_verified {
            tracing::info!("verifying stream hash");
            verify_stream_hash(&dec_paths, &self.backup.blake3).await?;
            self.state.stream_verified = true;
            self.state.save(&self.state_path)?;
        }

        if !self.state.receive_completed {
            tracing::info!("piping to btrfs receive");
            pipe_to_btrfs_receive(&dec_paths, self.target_dir).await?;

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

        Ok(())
    }
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
    let sanitized = sanitize_subvolume_path(&task.subvolume)?;
    let timestamp = compact_to_iso(date)?;
    let buffer_target = retrieval_buffer_target(config, retrieval)?;

    let work_dir = restore_work_dir(&config.base_dir, &task.name, level, &timestamp);
    tokio::fs::create_dir_all(&work_dir).await?;

    let backup = load_or_fetch_manifest(
        remote,
        &s3_manifest_key(&config.s3.prefix, &sanitized, level, &timestamp),
        &work_dir.join("manifest.yaml"),
        retrieval,
    )
    .await?;

    let state_path = restore_state_file(&work_dir);
    let state = load_or_init_state(&state_path, &backup)?;

    let daily_budget = daily_budget_bytes(config.s3.restore.expected_download_kbps);
    let remaining_bytes: u64 = state
        .parts
        .values()
        .filter(|p| !matches!(p.download_state, PartDownloadState::Downloaded))
        .map(|p| p.size_bytes)
        .sum();
    if daily_budget > 0 {
        let estimated_days = (remaining_bytes as f64 / daily_budget as f64).ceil() as u64;
        tracing::info!(
            "restore schedule: buffer_target {} bytes, daily_budget {} bytes/day, {} bytes remaining, ~{} day(s) to complete at this pace",
            buffer_target,
            daily_budget,
            remaining_bytes,
            estimated_days
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
        private_key: private_key_str,
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
            PartDownloadState::Downloaded
        );
        let content = std::fs::read(&target).unwrap();
        let expected: Vec<u8> = (0u64..25).map(|i| i as u8).collect();
        assert_eq!(content, expected);
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
