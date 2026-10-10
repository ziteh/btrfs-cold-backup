//! The oversize guard: holds back an incremental whose size is out of line, before any of it
//! is uploaded. Ransomware that encrypts the data makes the next incremental about as large
//! as what it encrypted; uploading that would take weeks on a slow uplink and bill for it.

use anyhow::{Result, anyhow};

use crate::{
    config::OversizeGuard,
    layout::TaskLayout,
    list::list_backups,
    manifest::{BackupManifest, fetch_latest, fetch_manifest},
    remote::RemoteBackend,
    state::BackupTarget,
    timestamp::{BackupId, Timestamp},
    util::GIB,
};

/// An incremental held back by the oversize guard. Its state and parts are kept, so allowing
/// it resumes the upload without a new send.
#[derive(Debug, thiserror::Error)]
#[error(
    "{id} is {size_gib:.1} GiB, over the oversize guard's limit of {limit_gib:.1} GiB ({basis}); \
     its parts are kept but not uploaded. If the change is legitimate, upload it with \
     `bcb backup --level {level} --task <name> --allow-oversize`",
    level = id.level
)]
pub struct Oversize {
    pub id: BackupId,
    pub size_gib: f64,
    pub limit_gib: f64,
    pub basis: String,
}

/// Fails with [`Oversize`] if the incremental `target`, `size_bytes` large, exceeds the
/// guard's limit: `rate_factor` times the median size per day of the newest `history`
/// incrementals, or with fewer of them, `fallback_level0_ratio` of the chain's level 0.
pub(super) async fn check<R: RemoteBackend>(
    guard: &OversizeGuard,
    remote: &R,
    layout: &TaskLayout,
    target: &BackupTarget,
    size_bytes: u64,
) -> Result<()> {
    let size_gib = size_bytes as f64 / GIB;
    if size_gib <= guard.min_size_gib {
        return Ok(());
    }

    let mut recent: Vec<BackupId> = list_backups(remote, layout)
        .await?
        .into_iter()
        .filter(|id| id.level > 0 && *id != target.id)
        .collect();
    let (limit_gib, basis) = if recent.len() >= guard.history {
        let mut rates = Vec::with_capacity(guard.history);
        for id in recent.split_off(recent.len() - guard.history) {
            let backup = fetch_manifest(remote, &layout.manifest_key(&id)).await?;
            rates.push(gib_per_day(&backup)?);
        }
        let median = median(&mut rates);
        let days = days_since_parent(&target.id.timestamp, target.parent_snapshot.as_deref())?;
        (
            median * guard.rate_factor * days,
            format!(
                "{:.2} GiB/day over {days:.1} day(s) since its parent, {} times the median of \
                 the {} newest incrementals",
                median, guard.rate_factor, guard.history
            ),
        )
    } else {
        let latest_key = layout.latest_key();
        let level0 = fetch_latest(remote, &latest_key)
            .await?
            .and_then(|latest| latest.levels.get(&0).cloned())
            .ok_or_else(|| anyhow!("no level 0 entry in latest manifest {latest_key}"))?;
        let id = BackupId {
            level: 0,
            timestamp: level0.timestamp,
        };
        let level0_gib = fetch_manifest(remote, &layout.manifest_key(&id))
            .await?
            .total_size_bytes as f64
            / GIB;
        (
            level0_gib * guard.fallback_level0_ratio,
            format!(
                "{} of the {level0_gib:.1} GiB level 0, while fewer than {} incrementals exist",
                guard.fallback_level0_ratio, guard.history
            ),
        )
    };

    if size_gib > limit_gib {
        return Err(Oversize {
            id: target.id.clone(),
            size_gib,
            limit_gib,
            basis,
        }
        .into());
    }
    Ok(())
}

fn gib_per_day(backup: &BackupManifest) -> Result<f64> {
    let days = days_since_parent(&backup.timestamp, backup.parent_snapshot.as_deref())?;
    Ok(backup.total_size_bytes as f64 / GIB / days)
}

/// Days from a backup's parent to it, from the snapshots' times rather than when the backups
/// ran, which can be weeks apart for a long level 0. At least one, so backups taken close
/// together don't inflate the rate.
fn days_since_parent(timestamp: &Timestamp, parent_snapshot: Option<&str>) -> Result<f64> {
    let parent = parent_snapshot.ok_or_else(|| anyhow!("incremental has no parent snapshot"))?;
    let days = timestamp.days_since(&Timestamp::from_snapshot_name(parent)?)?;
    Ok(days.max(1.0))
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn test_days_since_parent_is_at_least_one() {
        let ts = |s: &str| s.parse::<Timestamp>().unwrap();
        assert_eq!(
            days_since_parent(&ts("2026-01-15T01:00"), Some("bcb_level0.20260101T0100")).unwrap(),
            14.0
        );
        assert_eq!(
            days_since_parent(&ts("2026-01-01T02:00"), Some("bcb_level0.20260101T0100")).unwrap(),
            1.0
        );
    }
}
