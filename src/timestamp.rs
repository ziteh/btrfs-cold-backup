//! How backups are named: a snapshot's timestamp and level identify a backup everywhere, in
//! snapshot names, `--date`, manifests, S3 keys and local paths.
//!
//! Timestamps have minute precision because btrbk's `timestamp_format long` does. When btrbk
//! takes two snapshots of a subvolume within one minute it appends a counter (`_<n>`) to the
//! second, which therefore has no timestamp of its own; bcb refuses such snapshots rather than
//! confuse them with the first.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Name prefix of the snapshots bcb backs up; the level follows it.
pub const SNAPSHOT_PREFIX: &str = "bcb_level";

/// Prefix of the names of `level`'s snapshots: `bcb_level<N>.`
pub fn snapshot_prefix(level: u32) -> String {
    format!("{SNAPSHOT_PREFIX}{level}.")
}

/// The local time a snapshot was taken, with no zone, as `YYYY-MM-DDTHH:MM`. Comparing two
/// timestamps as strings compares them chronologically.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Timestamp(String);

fn all_digits(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_digit())
}

impl Timestamp {
    /// `date` is `YYYYMMDD`, `time` is `HHMM`; both already checked to be digits.
    fn from_digits(date: &str, time: &str) -> Self {
        Self(format!(
            "{}-{}-{}T{}:{}",
            &date[0..4],
            &date[4..6],
            &date[6..8],
            &time[0..2],
            &time[2..4]
        ))
    }

    /// Parses a btrbk snapshot name, `bcb_level<N>.<YYYYMMDD>T<hhmm>`.
    pub fn from_snapshot_name(name: &str) -> Result<Self> {
        let invalid = || {
            anyhow!(
                "invalid snapshot name '{name}': expected {SNAPSHOT_PREFIX}<N>.<YYYYMMDD>T<hhmm> \
                 (btrbk snapshot_name {SNAPSHOT_PREFIX}<N> with timestamp_format long)"
            )
        };
        let (prefix, ts) = name.split_once('.').ok_or_else(invalid)?;
        if !prefix.starts_with(SNAPSHOT_PREFIX) {
            return Err(invalid());
        }
        let (date, time) = ts.split_once('T').ok_or_else(invalid)?;
        // Refusing is safe: only this cycle's backup is skipped, and the next snapshot sorts
        // after this one, so the following cycle picks it up without intervention.
        if let Some((_, counter)) = time.split_once('_') {
            return Err(anyhow!(
                "snapshot '{name}' is a btrbk same-minute snapshot (trailing '_{counter}'), \
                 which bcb cannot back up because it identifies backups down to the minute. \
                 Delete or rename it; the next scheduled run will back up normally either way."
            ));
        }
        if !all_digits(date, 8) || !all_digits(time, 4) {
            return Err(invalid());
        }
        Ok(Self::from_digits(date, time))
    }

    /// Parses `--date`: `YYYYMMDDHHMM`, or `YYYYMMDD` for midnight.
    pub fn from_cli_date(s: &str) -> Result<Self> {
        if let Some((_, counter)) = s.split_once('_')
            && !counter.is_empty()
            && all_digits(counter, counter.len())
        {
            return Err(anyhow!(
                "'{s}' names a btrbk same-minute snapshot, which bcb does not back up, \
                 so no backup exists under that date"
            ));
        }
        if all_digits(s, 12) {
            Ok(Self::from_digits(&s[0..8], &s[8..12]))
        } else if all_digits(s, 8) {
            Ok(Self::from_digits(s, "0000"))
        } else {
            Err(anyhow!("date must be YYYYMMDD or YYYYMMDDHHMM, got: {s}"))
        }
    }

    /// `YYYYMMDDHHMM`, for names where `-`, `T` and `:` don't belong.
    pub fn compact(&self) -> String {
        self.0.chars().filter(char::is_ascii_digit).collect()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Days from `earlier` to this timestamp. Both are read as UTC: they share the host's
    /// zone, which bcb requires to have no DST, so the zone cancels out.
    pub fn days_since(&self, earlier: &Timestamp) -> Result<f64> {
        let at = |t: &Timestamp| {
            humantime::parse_rfc3339_weak(&format!("{}:00", t.0))
                .map_err(|e| anyhow!("invalid timestamp {t}: {e}"))
        };
        let seconds = match at(self)?.duration_since(at(earlier)?) {
            Ok(d) => d.as_secs_f64(),
            Err(e) => -e.duration().as_secs_f64(),
        };
        Ok(seconds / 86400.0)
    }
}

impl FromStr for Timestamp {
    type Err = anyhow::Error;

    /// Parses the `YYYY-MM-DDTHH:MM` form timestamps are stored in.
    fn from_str(s: &str) -> Result<Self> {
        let valid = s.len() == 16
            && s.bytes().enumerate().all(|(i, b)| match i {
                4 | 7 => b == b'-',
                10 => b == b'T',
                13 => b == b':',
                _ => b.is_ascii_digit(),
            });
        if !valid {
            return Err(anyhow!(
                "invalid timestamp {s:?}: expected YYYY-MM-DDTHH:MM"
            ));
        }
        Ok(Self(s.to_string()))
    }
}

impl TryFrom<String> for Timestamp {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl From<Timestamp> for String {
    fn from(t: Timestamp) -> Self {
        t.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One backup of a task: the level and timestamp of the snapshot it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupId {
    pub level: u32,
    pub timestamp: Timestamp,
}

impl BackupId {
    /// The name of the S3 "directory" and local directory holding this backup:
    /// `bcb_level<N>_<YYYYMMDDHHMM>`.
    pub fn dir_name(&self) -> String {
        format!(
            "{SNAPSHOT_PREFIX}{}_{}",
            self.level,
            self.timestamp.compact()
        )
    }

    /// The backup a btrbk snapshot name, `bcb_level<N>.<YYYYMMDD>T<hhmm>`, would become.
    pub fn from_snapshot_name(name: &str) -> Result<Self> {
        let timestamp = Timestamp::from_snapshot_name(name)?;
        let level = name[SNAPSHOT_PREFIX.len()..]
            .split_once('.')
            .and_then(|(level, _)| level.parse().ok())
            .ok_or_else(|| anyhow!("invalid level in snapshot name '{name}'"))?;
        Ok(Self { level, timestamp })
    }

    /// Inverse of [`BackupId::dir_name`].
    pub fn parse_dir_name(dir: &str) -> Option<Self> {
        let rest = dir.strip_prefix(SNAPSHOT_PREFIX)?;
        let (level, compact) = rest.split_once('_')?;
        if !all_digits(compact, 12) {
            return None;
        }
        Some(Self {
            level: level.parse().ok()?,
            timestamp: Timestamp::from_cli_date(compact).ok()?,
        })
    }
}

impl fmt::Display for BackupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "level {} backup of {}", self.level, self.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn test_days_since() {
        assert_eq!(
            ts("2026-03-01T12:00")
                .days_since(&ts("2026-02-27T00:00"))
                .unwrap(),
            2.5
        );
        assert_eq!(
            ts("2026-01-01T00:00")
                .days_since(&ts("2026-01-02T00:00"))
                .unwrap(),
            -1.0
        );
        assert!(
            ts("2026-13-01T00:00")
                .days_since(&ts("2026-01-02T00:00"))
                .is_err()
        );
    }

    #[test]
    fn test_backup_id_from_snapshot_name() {
        let id = BackupId::from_snapshot_name("bcb_level12.20260602T1430").unwrap();
        assert_eq!(id.level, 12);
        assert_eq!(id.timestamp, ts("2026-06-02T14:30"));
        assert!(BackupId::from_snapshot_name("bcb_levelX.20260602T1430").is_err());
        assert!(BackupId::from_snapshot_name("bcb_level1.20260602T1430_1").is_err());
        assert!(BackupId::from_snapshot_name("other.20260602T1430").is_err());
    }

    #[test]
    fn test_from_snapshot_name_valid() {
        let t = Timestamp::from_snapshot_name("bcb_level1.20260602T1430").unwrap();
        assert_eq!(t, ts("2026-06-02T14:30"));
    }

    #[test]
    fn test_from_snapshot_name_same_minute_error_is_specific() {
        let err = Timestamp::from_snapshot_name("bcb_level0.20260602T1430_1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("same-minute"), "unhelpful message: {err}");
    }

    #[test]
    fn test_from_snapshot_name_invalid() {
        for name in [
            "bcb_level0",
            // btrbk timestamp_format short (date only).
            "bcb_level0.20260602",
            "bcb_level0.20260602_1430",
            "bcb_level0_2026-06-02_14-30",
            "bcb_level0.2026-06-02T1430",
            "bcb_level0.20260602T14x0",
            "unrelated.20260602T1430",
        ] {
            assert!(Timestamp::from_snapshot_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn test_from_cli_date() {
        assert_eq!(
            Timestamp::from_cli_date("202606021430").unwrap(),
            ts("2026-06-02T14:30")
        );
        assert_eq!(
            Timestamp::from_cli_date("20260602").unwrap(),
            ts("2026-06-02T00:00")
        );
        assert!(Timestamp::from_cli_date("invalid").is_err());
        assert!(Timestamp::from_cli_date("2026060214").is_err());
        let err = Timestamp::from_cli_date("202606021430_1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("same-minute"), "unhelpful message: {err}");
    }

    #[test]
    fn test_parse_rejects_malformed() {
        assert!("2026-06-02T14:30".parse::<Timestamp>().is_ok());
        for bad in ["2026-06-02 14:30", "2026-06-02T14:3", "202606021430", ""] {
            assert!(bad.parse::<Timestamp>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_serde_is_the_plain_string() {
        let t = ts("2026-06-02T14:30");
        assert_eq!(
            serde_yaml::to_string(&t).unwrap().trim(),
            "2026-06-02T14:30"
        );
        assert!(serde_yaml::from_str::<Timestamp>("not a timestamp").is_err());
    }

    #[test]
    fn test_ordering_is_chronological() {
        assert!(ts("2025-12-31T23:59") < ts("2026-01-01T00:00"));
        assert!(ts("2026-06-25T08:00") < ts("2026-06-25T20:00"));
    }

    #[test]
    fn test_dir_name_round_trip() {
        let id = BackupId {
            level: 3,
            timestamp: ts("2026-06-02T14:30"),
        };
        assert_eq!(id.dir_name(), "bcb_level3_202606021430");
        assert_eq!(BackupId::parse_dir_name(&id.dir_name()), Some(id));
    }

    #[test]
    fn test_parse_dir_name_invalid() {
        for dir in [
            "bcb",
            "bcb_levelX_202606021430",
            "bcb_level0_notadate",
            "bcb_level0_202606021430_1",
            "bcb_level0_20260602",
        ] {
            assert!(BackupId::parse_dir_name(dir).is_none(), "{dir}");
        }
    }
}
