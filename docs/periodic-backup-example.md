# Example: Periodic Backups

A NAS backing up `/mnt/data` with a level 0 full backup on Jan 1 / Jul 1 and a level 1 incremental every Sunday, using the configuration from the [README](../README.md#configuration).

Ready-to-copy versions of everything below live in [../contrib/](../contrib/README.md).

One-time setup:

```bash
sudo install -m 755 target/release/bcb /usr/local/bin/bcb

# Prints the public key (put it in the config). Keep the private key OFF this
# machine (paper, password manager); backups only ever need the public key.
bcb genkey --output ./bcb_private.key

sudo bcb check
```

AWS credentials go in root's `~/.aws/credentials` (bcb uses the AWS SDK default chain). The IAM policy needs `PutObject`, `PutObjectTagging`, `GetObject`, `HeadObject`, `ListBucket`, `ListBucketMultipartUploads`, `AbortMultipartUpload`, `DeleteObject`, `RestoreObject`, and `HeadBucket` on the bucket. `PutObjectTagging` is easy to miss since every upload sets its tag inline via `PutObject`, but AWS checks it as a separate permission — without it, `bcb check` fails at the write probe rather than passing and only failing later during a real backup.

Snapshots come from btrbk, one [config][btrbk-conf] per level so each can be created and pruned independently ([contrib/btrbk/](../contrib/btrbk/), level 1 identical to level 0 except `snapshot_name bcb_level1`).

`snapshot_preserve_min latest` guarantees the newest snapshot per level always survives pruning — exactly the parent the next incremental needs — while `snapshot_preserve 12m` additionally keeps a year of monthly snapshots locally. The shared `lockfile` keeps the two levels' btrbk invocations from overlapping (a level 0 upload can run for hours into the next timer), and `transaction_log` records every snapshot create/delete for audits.

For incremental levels, `snapshot_create onchange` is worth considering instead of `always`: if the subvolume has not changed since the last snapshot, btrbk creates nothing and bcb idempotently skips the upload.

The scheduled job snapshots, backs up, then prunes ([contrib/bcb-run](../contrib/bcb-run)). On failure the script exits non-zero before pruning, and re-running is safe: an interrupted backup resumes, a completed one is skipped.

The systemd service ([contrib/systemd/bcb-backup@.service](../contrib/systemd/bcb-backup@.service)) wires that script to `OnFailure=`, which only fires when the unit actually ran and failed — it can't catch a timer that never fired at all (disabled, machine off past `Persistent=true`'s catch-up window, or a full disk that keeps `bcb` from starting). A silently-stopped backup schedule reports no failure and no error, so also set up a positive liveness signal — e.g. a dead man's switch (`curl` a healthchecks.io-style URL at the end of a successful `bcb-run`) that alerts you when it *stops* hearing from the job, rather than only reacting to failures it's told about.

The failure path ([contrib/systemd/bcb-notify-failure@.service](../contrib/systemd/bcb-notify-failure@.service) + [contrib/bcb-notify](../contrib/bcb-notify)) forwards the failed unit's recent log to wherever you want to hear about it (mail, ntfy, a webhook).

Timers ([contrib/systemd/bcb-backup@0.timer](../contrib/systemd/bcb-backup@0.timer), [contrib/systemd/bcb-backup@1.timer](../contrib/systemd/bcb-backup@1.timer)):

```bash
sudo systemctl enable --now bcb-backup@0.timer bcb-backup@1.timer
```

`Persistent=true` catches up on schedules missed while powered off. Runs are logged in `journalctl -u 'bcb-backup@*'` and in `<base_dir>/log/` (kept to the last 30 daily files); failure notifications land in `journalctl -u 'bcb-notify-failure@*'`.

Notes:

- Each level 1 is the cumulative delta since the last level 0 and grows over the half year; if that gets large, add a monthly level 1 and weekly level 2 instead.
- Set bucket lifecycle expirations filtered on the `bcb-backup-level` tag (see [README](../README.md#retention)) to at least 13 months so the current and previous chains always survive.
- Drill restores: `bcb list` regularly, and walk a full restore on a test machine at least yearly to prove the private key and procedure work. See [restore-drill-runbook.md](restore-drill-runbook.md) for a runbook that exercises the real Glacier retrieval path (which no automated test does).
- Run `bcb verify` regularly (e.g. a monthly systemd timer, wired into the same `OnFailure=` alerting as the backup jobs): it audits every backup's incremental chain, part existence and size, and storage class against S3 via HeadObject/ListObjectsV2 only, so it never triggers an archive retrieval or its cost. Pass `--max-age-days` to also flag a task whose newest backup is older than expected — the one check that can catch a schedule that silently stopped running. It complements, not replaces, an actual restore drill: it cannot detect data corrupted in a way that preserves size, nor verify the private key still works.

[btrbk-conf]: https://digint.ch/btrbk/doc/btrbk.conf.5.html
