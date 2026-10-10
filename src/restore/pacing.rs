//! Pacing archive retrievals to the download bandwidth.
//!
//! A retrieved copy of an archived part is downloadable only for `--restore-days`, and each
//! retrieval is paid per GB. Requesting every part at once wastes retrievals when the download
//! can't finish before the copies expire, so with `expected_download_kbps` set, a restore only
//! keeps `lead_days` worth of download requested ahead (the buffer target): each run requests
//! more as the earlier requests are downloaded.

use anyhow::{Result, anyhow};

use crate::{config::RestoreConfig, remote::RetrievalTier, util::BYTES_PER_KBPS};

use super::RetrievalOptions;

/// Default lead time for a retrieval tier: the larger of the Glacier and Deep Archive
/// retrieval times AWS documents for it (a tier can serve either class), with a 1.5x margin.
/// AWS offers no API for current retrieval latency; `restore.lead_days` overrides these if
/// AWS's published times change.
fn default_lead_days(tier: RetrievalTier) -> f64 {
    match tier {
        RetrievalTier::Expedited => 0.05, // Glacier only, ~1-5 min
        RetrievalTier::Standard => 0.75,  // max(Glacier ~5h, Deep Archive ~12h) * 1.5
        RetrievalTier::Bulk => 3.0,       // max(Glacier ~12h, Deep Archive ~48h) * 1.5
    }
}

/// Bytes a day of downloading at `expected_download_kbps` brings in; 0 when pacing is off.
fn daily_budget_bytes(expected_download_kbps: u64) -> u64 {
    (expected_download_kbps as f64 * BYTES_PER_KBPS * 86400.0) as u64
}

/// How many bytes of retrieval may be in flight at once. Rejects a `--restore-days` too short
/// for the pacing before any S3 call is made.
pub(super) fn retrieval_buffer_target(
    config: &RestoreConfig,
    retrieval: &RetrievalOptions,
) -> Result<u64> {
    let daily_budget = daily_budget_bytes(config.expected_download_kbps);
    if daily_budget == 0 {
        return Ok(u64::MAX);
    }
    let lead_days = config
        .lead_days
        .unwrap_or_else(|| default_lead_days(retrieval.tier));
    // A wave of retrieved copies is drained over about lead_days of downloading; a copy that
    // expires before its turn costs another retrieval.
    if (retrieval.days as f64) < lead_days + 1.0 {
        return Err(anyhow!(
            "retrieved copies would expire before the paced download reaches them: \
             --restore-days is {} but pacing needs at least {} (lead_days {lead_days} plus one \
             day of margin); raise --restore-days or lower restore.lead_days",
            retrieval.days,
            (lead_days + 1.0).ceil() as i64,
        ));
    }
    Ok((lead_days * daily_budget as f64) as u64)
}

/// Logs how long the rest of the restore should take, or warns that it isn't paced.
pub(super) fn log_schedule(
    config: &RestoreConfig,
    retrieval: &RetrievalOptions,
    buffer_target: u64,
    pending_parts: usize,
    pending_bytes: u64,
) {
    let daily_budget = daily_budget_bytes(config.expected_download_kbps);
    if daily_budget > 0 {
        tracing::info!(
            "restore schedule: buffer_target {buffer_target} bytes, daily_budget {daily_budget} \
             bytes/day, {pending_bytes} bytes remaining, ~{} day(s) to complete at this pace",
            pending_bytes.div_ceil(daily_budget)
        );
    } else if pending_parts > 1 {
        tracing::warn!(
            "restore schedule: no pacing configured (s3.restore.expected_download_kbps is 0), \
             so all {pending_parts} remaining parts ({pending_bytes} bytes) are requested for \
             retrieval at once; parts not downloaded within --restore-days ({} days) expire and \
             must be retrieved, and paid for, again. Set expected_download_kbps to pace \
             retrievals to your bandwidth",
            retrieval.days
        );
    }
}

/// A pending part that is archived with no retrieval requested.
pub(super) struct Unrequested<'a> {
    pub index: &'a str,
    pub size_bytes: u64,
    /// Part of it was downloaded before its retrieved copy expired.
    pub partially_downloaded: bool,
}

/// Picks which of `parts` to request retrieval for this run.
///
/// Bytes already retrievable don't count against `buffer_target`, since this run is about to
/// download them; what counts is what will still be arriving afterwards: retrievals in
/// progress plus the ones requested now. Partially downloaded parts are requested regardless
/// of the budget, since their bytes are already invested and the resume offset stays valid
/// (objects are immutable).
pub(super) fn select_retrievals(
    parts: &[Unrequested],
    ready_bytes: u64,
    in_progress_bytes: u64,
    buffer_target: u64,
) -> Vec<String> {
    let mut selected = Vec::new();
    let mut requested_bytes = 0u64;
    let mut over_budget: Vec<&Unrequested> = Vec::new();
    for part in parts {
        if part.partially_downloaded {
            selected.push(part.index.to_string());
        } else if in_progress_bytes + requested_bytes + part.size_bytes > buffer_target {
            over_budget.push(part);
        } else {
            selected.push(part.index.to_string());
            requested_bytes += part.size_bytes;
        }
    }

    // With a budget smaller than every remaining part, nothing would ever be ready, in
    // progress or requested, and the restore would report "in progress" forever. The smallest
    // part goes through regardless so every run advances.
    if requested_bytes == 0
        && ready_bytes == 0
        && in_progress_bytes == 0
        && let Some(smallest) = over_budget.iter().min_by_key(|p| p.size_bytes)
    {
        tracing::warn!(
            "restore budget is smaller than any remaining part; requesting part {} anyway so \
             the restore advances",
            smallest.index
        );
        selected.push(smallest.index.to_string());
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(index: &str, size_bytes: u64) -> Unrequested<'_> {
        Unrequested {
            index,
            size_bytes,
            partially_downloaded: false,
        }
    }

    #[test]
    fn select_retrievals_should_fill_the_budget_in_order() {
        let parts = [part("0", 10), part("1", 10), part("2", 10)];
        assert_eq!(select_retrievals(&parts, 0, 0, 25), ["0", "1"]);
    }

    #[test]
    fn select_retrievals_should_count_in_progress_but_not_ready_bytes() {
        let parts = [part("0", 10), part("1", 10)];
        assert_eq!(select_retrievals(&parts, 1000, 10, 25), ["0"]);
        assert!(select_retrievals(&parts, 0, 20, 25).is_empty());
    }

    #[test]
    fn select_retrievals_should_request_partial_downloads_regardless_of_budget() {
        let parts = [
            Unrequested {
                partially_downloaded: true,
                ..part("0", 100)
            },
            part("1", 100),
        ];
        assert_eq!(select_retrievals(&parts, 0, 50, 60), ["0"]);
    }

    #[test]
    fn select_retrievals_should_force_smallest_part_when_nothing_else_moves() {
        let parts = [part("0", 30), part("1", 20), part("2", 40)];
        assert_eq!(select_retrievals(&parts, 0, 0, 5), ["1"]);
    }

    #[test]
    fn select_retrievals_should_not_force_while_something_is_in_flight() {
        let parts = [part("0", 30)];
        assert!(select_retrievals(&parts, 0, 1, 5).is_empty());
        assert!(select_retrievals(&parts, 1, 0, 5).is_empty());
    }

    #[test]
    fn buffer_target_should_be_unbounded_without_pacing() {
        let retrieval = RetrievalOptions {
            days: 1,
            tier: RetrievalTier::Bulk,
        };
        assert_eq!(
            retrieval_buffer_target(&RestoreConfig::default(), &retrieval).unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn buffer_target_should_reject_restore_days_within_lead_window() {
        let config = RestoreConfig {
            expected_download_kbps: 8,
            lead_days: Some(2.0),
            ..Default::default()
        };
        let retrieval = |days| RetrievalOptions {
            days,
            tier: RetrievalTier::Standard,
        };
        let err = retrieval_buffer_target(&config, &retrieval(2))
            .unwrap_err()
            .to_string();
        assert!(err.contains("would expire"), "unexpected error: {err}");
        // 8 kbps is 1000 bytes/s, so two lead days hold 172.8 MB.
        assert_eq!(
            retrieval_buffer_target(&config, &retrieval(3)).unwrap(),
            2 * 86_400_000
        );
    }
}
