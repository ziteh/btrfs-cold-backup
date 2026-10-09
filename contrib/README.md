# Contrib

Ready-to-copy templates for running `bcb` on a schedule via systemd + btrbk, as described in
[../docs/periodic-backup-example.md](../docs/periodic-backup-example.md). Lines needing your own
values are marked `# EDIT:`.

```bash
sudo install -m 755 contrib/bcb-run contrib/bcb-notify /usr/local/sbin/
sudo install -m 644 contrib/btrbk/bcb-level0.conf contrib/btrbk/bcb-level1.conf /etc/btrbk/
sudo install -m 644 contrib/systemd/*.service contrib/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bcb-backup@0.timer bcb-backup@1.timer
```

Edit the `# EDIT:` lines in `contrib/btrbk/*.conf` (subvolume path), `contrib/bcb-notify`
(where failures get sent), and `contrib/systemd/*.timer` (schedule) before enabling.

`bcb-run` holds a lock for the whole run. A level 0 run waits for any run in progress; a higher level finding the lock held logs a skip and exits 0, so a multi-day level 0 doesn't turn every overlapping incremental into a failure alert. Both timers can be enabled from the start. A run that keeps getting skipped shows up in `bcb verify --max-age-days`. The lock covers `bcb-run` only: a timer firing during a manual `bcb restore` or `bcb verify` still fails on bcb's own lock and alerts, so pause the timers for long manual runs.

## `snapshot_create` for incremental levels (level 1+)

`bcb-level1.conf` ships with `snapshot_create always`. `onchange` is the alternative if the
subvolume can go long stretches without changing:

| Aspect | `always` (default here) | `onchange` |
| --- | --- | --- |
| When nothing changed | Creates a snapshot anyway; bcb still uploads a near-empty incremental (S3 storage-class minimum object size/duration means the extra cost is negligible — a fraction of a cent per year even weekly) | btrbk creates nothing; bcb idempotently skips the upload |
| Backup freshness monitoring | `bcb verify --max-age-days` stays meaningful: every scheduled run produces a new backup, so "no new backup" reliably means the schedule broke | `bcb verify --max-age-days` can false-positive on legitimately unchanged data — rely on a separate liveness signal (e.g. the dead man's switch in [../docs/periodic-backup-example.md](../docs/periodic-backup-example.md)) instead, since `bcb-run` still exits 0 on a skip |
| Local recovery points | Fixed cadence: `snapshot_preserve 12m` keeps one per month regardless of whether data changed | Can leave gaps: no new snapshot at all when nothing changed, so local recovery-point granularity isn't guaranteed |
| Chain length over time | Grows by one entry per schedule tick, even when empty | Grows only when there's real data, so restore chains stay shorter |
| Depends on | Nothing beyond bcb/btrbk's normal behavior | btrbk's change detection (btrfs generation/transid based, not just mtime — reliable, but bcb has no way to cross-check it) |

Pick `onchange` if the subvolume is often unchanged and you don't rely on `--max-age-days`;
otherwise `always` keeps monitoring and retention simpler.
