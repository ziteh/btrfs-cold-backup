# Keep one freshness threshold for all levels

## Context and Problem Statement

`bcb verify --max-age-days` flags a task whose newest backup, of any level, is older than the threshold. Should each level get its own threshold, so that a lower level that stopped completing is flagged even while higher levels keep running?

## Considered Options

- One threshold, on the newest backup of any level
- One threshold per level

## Decision Outcome

Chosen option: "One threshold", because with a state per level, incrementals keep completing while a long level 0 uploads, so a threshold of about one incremental period already catches a schedule that stopped, including a host that is down and so sends no failure alert. A lower level that stops completing while higher ones run is either failing, which alerts on every run, or still progressing slowly, which a wrapper can report when the next level is due.

This should be revisited if `verify` runs on a separate host to watch backups independently of the backed-up host's alerts.

### Consequences

- Good, because the CLI and config stay simple.
- Bad, because `verify` doesn't flag a lower level that keeps failing while higher levels succeed; that relies on the failure alerts of the runs themselves. If no backup at the same or a lower level succeeds before a level's lifecycle expiration, every chain built on it becomes unrestorable at once (see the README's Retention).
