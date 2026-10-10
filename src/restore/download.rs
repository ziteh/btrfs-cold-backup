//! Checking which parts are retrievable, and downloading them with resumable range requests.

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::path::Path;

use crate::{
    btrfs::BtrfsOps,
    crypto::blake3_file,
    remote::{RemoteBackend, RemoteError, Retrieval, RetrievalStatus},
    state::{PartDownloadState, RestoreState, StateFile},
    test_hooks,
    util::BYTES_PER_KBPS,
};

use super::RestoreRun;

/// Bytes per range request. Progress is saved after each, so it is also the most a crash can
/// cost of a part's download.
const RANGE_CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Whether `e` came from S3 or the network rather than from this machine. A remote failure
/// skips just that part, which the next run retries; a local one (disk full, permissions)
/// would hit every other part too, so it fails the run.
fn is_remote(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<RemoteError>())
}

impl<B: BtrfsOps, R: RemoteBackend> RestoreRun<'_, B, R> {
    /// The live retrieval status of every pending part, with bounded concurrency so a
    /// thousand-part restore doesn't burst a thousand requests.
    ///
    /// Parts whose check failed are left out and skipped this run. If every check failed,
    /// e.g. with the network down, this fails instead, so the run exits 1 rather than
    /// reporting progress it never makes.
    pub(super) async fn fetch_live_statuses(&self) -> Result<HashMap<String, Retrieval>> {
        let concurrency = self.config.s3.restore.status_check_concurrency.max(1) as usize;
        let results: Vec<(String, Result<Retrieval>)> =
            futures_util::stream::iter(self.state.data.pending_parts().cloned())
                .map(|index| async move {
                    let result = self.remote.retrieval_status(&self.part_key(&index)).await;
                    (index, result)
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

        let mut live = HashMap::new();
        let mut first_failure = None;
        for (index, result) in results {
            match result {
                Ok(retrieval) => {
                    live.insert(index, retrieval);
                }
                Err(e) => {
                    tracing::warn!("failed to check the status of part {index}: {e:#}");
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

    /// Downloads every part whose retrieved copy is live, including parts left half-done by
    /// an interrupted run, earliest expiry first so copies about to lapse are saved first.
    /// Returns how many parts failed remotely; those are skipped, unless no part this run
    /// tried got through, which fails the run so a lasting outage doesn't report progress
    /// forever.
    pub(super) async fn download_ready_parts(
        &mut self,
        live: &HashMap<String, Retrieval>,
    ) -> Result<usize> {
        let mut ready: Vec<(String, u64, String, i64)> = self
            .manifest
            .parts
            .iter()
            .filter_map(|part| {
                let retrieval = live.get(&part.index)?;
                (retrieval.status == RetrievalStatus::Ready).then(|| {
                    (
                        part.index.clone(),
                        part.size_bytes,
                        part.blake3.clone(),
                        retrieval.expires_at.unwrap_or(i64::MAX),
                    )
                })
            })
            .collect();
        ready.sort_by_key(|(_, _, _, expires_at)| *expires_at);

        let chunk_size = test_hooks::size_override("BCB_TEST_PART_CHUNK_SIZE", RANGE_CHUNK_SIZE);
        let started_at = std::time::Instant::now();
        let mut downloaded_bytes = 0u64;
        let mut succeeded = 0usize;
        let mut failed = 0usize;
        let mut first_failure = None;
        for (index, size_bytes, blake3, _) in ready {
            tracing::info!("downloading part {index}");
            let path = self.part_path(&index);
            // A copy that was ready at the status check may expire before its turn, since
            // draining a wave can take days; that, like a network failure, must not abandon
            // the parts still downloadable. Progress is already saved chunk by chunk.
            if let Err(e) = download_part(
                self.remote,
                &self.part_key(&index),
                size_bytes,
                chunk_size,
                &path,
                &mut self.state,
                &index,
            )
            .await
            {
                if !is_remote(&e) {
                    return Err(e.context(format!("download of part {index} failed")));
                }
                tracing::warn!("download of part {index} failed, continuing with the rest: {e:#}");
                failed += 1;
                first_failure.get_or_insert(e);
                continue;
            }

            let actual = blake3_file(&path).await?;
            if actual != blake3 {
                self.state
                    .data
                    .parts
                    .insert(index.clone(), PartDownloadState::NotDownloaded);
                let _ = tokio::fs::remove_file(&path).await;
                self.state.save()?;
                return Err(anyhow!(
                    "BLAKE3 mismatch for part {index}: expected {blake3}, got {actual}"
                ));
            }
            self.state
                .data
                .parts
                .insert(index, PartDownloadState::Downloaded);
            self.state.save()?;
            downloaded_bytes += size_bytes;
            succeeded += 1;
        }

        if succeeded == 0
            && let Some(e) = first_failure
        {
            return Err(e.context(format!(
                "all {failed} part download(s) attempted this run failed"
            )));
        }
        if downloaded_bytes > 0 {
            let elapsed_secs = started_at.elapsed().as_secs_f64().max(0.001);
            tracing::info!(
                "download speed this run: {downloaded_bytes} bytes in {elapsed_secs:.1}s, \
                 ~{:.1} kbps (compare with expected_download_kbps)",
                downloaded_bytes as f64 / elapsed_secs / BYTES_PER_KBPS
            );
        }
        Ok(failed)
    }
}

/// Downloads part `index` of `size_bytes` to `path` in ranges of `chunk_size`, saving the
/// byte offset after each so a crash only loses the range in flight. The part is left
/// `PartiallyDownloaded` at its full size; only the caller's hash check makes it `Downloaded`.
async fn download_part<R: RemoteBackend>(
    remote: &R,
    key: &str,
    size_bytes: u64,
    chunk_size: u64,
    path: &Path,
    state: &mut StateFile<RestoreState>,
    index: &str,
) -> Result<()> {
    let mut downloaded = resume_offset(state.data.parts.get(index), path).await?;
    while downloaded < size_bytes {
        let end = (downloaded + chunk_size).min(size_bytes) - 1;
        remote
            .download_range_to_file(key, downloaded, end, path)
            .await?;
        // A body cut short without an error would otherwise shift every later chunk.
        let len = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("failed to inspect {path:?}"))?
            .len();
        if len != end + 1 {
            return Err(RemoteError::new(format!(
                "range {downloaded}-{end} of {key} left {path:?} at {len} bytes, expected {}",
                end + 1
            ))
            .into());
        }

        downloaded = end + 1;
        state.data.parts.insert(
            index.to_string(),
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: downloaded,
            },
        );
        state.save()?;
        test_hooks::pause_at(
            "BCB_TEST_PAUSE_AFTER_DOWNLOADED_BYTES",
            "downloaded_bytes",
            downloaded,
        )
        .await;
    }
    Ok(())
}

/// Where to resume a part's download. The file, not the state, decides how many bytes are
/// really there: a crash mid-chunk can leave the file longer than the last saved offset, so
/// it is truncated back to that offset, and a file shorter than it (or gone) starts over.
async fn resume_offset(state: Option<&PartDownloadState>, path: &Path) -> Result<u64> {
    let Some(&PartDownloadState::PartiallyDownloaded { downloaded_bytes }) = state else {
        return Ok(0);
    };
    let Ok(file) = tokio::fs::OpenOptions::new().write(true).open(path).await else {
        return Ok(0);
    };
    match file.metadata().await {
        Ok(meta) if meta.len() >= downloaded_bytes => {
            file.set_len(downloaded_bytes)
                .await
                .with_context(|| format!("failed to truncate {path:?}"))?;
            Ok(downloaded_bytes)
        }
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::MockRemoteBackend;

    fn state_file(dir: &Path, download_state: PartDownloadState) -> StateFile<RestoreState> {
        StateFile::create(
            dir.join("state.yaml"),
            RestoreState {
                manifest_blake3: String::new(),
                parts: [("000000".to_string(), download_state)].into(),
                receive_completed: false,
            },
        )
        .unwrap()
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
    async fn download_part_should_resume_from_saved_offset() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let mut state = state_file(dir.path(), PartDownloadState::NotDownloaded);

        // First run: ranges 0-9 and 10-19 succeed, 20-24 fails.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, _, _| *start < 20)
            .returning(|_, start, end, path| append_range(path, start, end));
        remote
            .expect_download_range_to_file()
            .withf(|_, start, _, _| *start == 20)
            .returning(|_, _, _, _| Err(RemoteError::new("connection lost").into()));
        download_part(&remote, "key", 25, 10, &target, &mut state, "000000")
            .await
            .expect_err("third chunk must fail");
        assert_eq!(
            state.data.parts["000000"],
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 20
            }
        );

        // Second run: a single range request from the saved offset.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, end, _| (*start, *end) == (20, 24))
            .times(1)
            .returning(|_, start, end, path| append_range(path, start, end));
        download_part(&remote, "key", 25, 10, &target, &mut state, "000000")
            .await
            .expect("resumed download should succeed");

        assert_eq!(
            state.data.parts["000000"],
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 25
            }
        );
        let expected: Vec<u8> = (0u64..25).map(|i| i as u8).collect();
        assert_eq!(std::fs::read(&target).unwrap(), expected);
    }

    #[tokio::test]
    async fn download_part_should_fail_as_remote_error_when_range_comes_back_short() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let mut state = state_file(dir.path(), PartDownloadState::NotDownloaded);

        // The second range ends a byte early without an error.
        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .returning(|_, start, end, path| {
                append_range(path, start, if start == 10 { end - 1 } else { end })
            });

        let err = download_part(&remote, "key", 25, 10, &target, &mut state, "000000")
            .await
            .expect_err("a short range must fail the part");
        assert!(is_remote(&err));
        assert_eq!(
            state.data.parts["000000"],
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 10
            }
        );
    }

    #[tokio::test]
    async fn download_part_should_restart_when_file_is_shorter_than_saved_offset() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("part.age");
        let mut state = state_file(
            dir.path(),
            PartDownloadState::PartiallyDownloaded {
                downloaded_bytes: 8,
            },
        );
        std::fs::write(&target, [0u8; 3]).unwrap();

        let mut remote = MockRemoteBackend::new();
        remote
            .expect_download_range_to_file()
            .withf(|_, start, end, _| (*start, *end) == (0, 9))
            .times(1)
            .returning(|_, start, end, path| append_range(path, start, end));
        download_part(&remote, "key", 10, 10, &target, &mut state, "000000")
            .await
            .expect("restarted download should succeed");

        assert_eq!(std::fs::read(&target).unwrap().len(), 10);
    }
}
