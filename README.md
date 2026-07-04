# Btrfs Cold Backup

`bcb` backs up btrfs subvolumes to object storage services.

Features:

- Specifically designed for [AWS S3 Glacier Deep Archive][s3-gda] cold storage, optimal storage costs.
- Full and incremental multi-level backups.
- [Age][age] is used for end-to-end encryption (E2EE).
- [BLAKE3][b3] hash is used for integrity verification.

> For ZFS: [zrb](https://github.com/ziteh/zfs-remote-backup)

## Usage

**Build:**

```bash
cargo fmt && cargo clippy && cargo build && cargo test

# release
cargo build --release
```

**Execute:**

```bash
cd target/release
./bcb --help
```

## Configuration

```yaml
# /etc/bcb/config.yaml
base_dir: /var/lib/bcb/
age_public_key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
s3:
  bucket: my-backup-bucket
  prefix: btrfs-backups/
  endpoint: "" # Leave empty for AWS S3, or specify custom endpoint for S3-compatible services
  region: us-east-1
  storage_class:
    manifest: STANDARD
    backup_data: # One entry per backup level; a level without an entry is an error
      - DEEP_ARCHIVE # Level 0 (full backup)
      - DEEP_ARCHIVE # Level 1
      - GLACIER      # Level 2
tasks:
  - name: example_task
    description: Example backup task
    subvolume: /mnt/data
    snapshot_dir: /mnt/.snapshots
    enabled: true
```

`base_dir` is bcb's local working directory: it holds the lock file, resume state, daily log files, and the temporary split/encrypted parts during backup and restore. Its filesystem needs free space roughly equal to one full backup.

## Snapshots

`bcb` does not create snapshots itself; it expects snapshots to already exist in each task's `snapshot_dir`, created by [btrbk][btrbk].

Snapshots must follow btrbk's naming with `snapshot_name bcb_level<N>` and `timestamp_format long` (btrbk's default):

```text
bcb_level<N>.<YYYYMMDD>T<hhmm>
```

For example `bcb_level1.20261231T2359` (btrbk's same-minute collision suffix `_<n>` is also accepted).

For each level, `bcb backup --level N` picks the newest matching snapshot. A level-N (N > 0) backup requires the level-(N-1) parent snapshot recorded in the last backup to still exist locally, and the level-N snapshot must be newer than that parent.

Each task backs up one subvolume; tasks run sequentially and one failing task does not block the others. Re-running a backup is idempotent: a snapshot whose backup already completed is skipped, and an interrupted backup resumes where it left off.

## Example: Periodic Backups

A NAS backing up `/mnt/data` with a level 0 full backup on Jan 1 / Jul 1 and a level 1 incremental every Sunday, using the configuration above.

One-time setup:

```bash
sudo install -m 755 target/release/bcb /usr/local/bin/bcb

# Prints the public key (put it in the config). Keep the private key OFF this
# machine (paper, password manager); backups only ever need the public key.
bcb genkey --output ./bcb_private.key

sudo bcb check
```

AWS credentials go in root's `~/.aws/credentials` (bcb uses the AWS SDK default chain). The IAM policy needs `PutObject`, `GetObject`, `HeadObject`, `ListBucket`, `ListBucketMultipartUploads`, `AbortMultipartUpload`, `DeleteObject`, `RestoreObject`, and `HeadBucket` on the bucket.

Snapshots come from btrbk, one [config][btrbk-conf] per level so each can be created and pruned independently. `/etc/btrbk/bcb-level0.conf` (level 1 is identical except `snapshot_name bcb_level1`):

```txt
timestamp_format        long
snapshot_create         always
snapshot_preserve_min   latest
snapshot_preserve       12m
snapshot_dir            .snapshots
lockfile                /run/lock/btrbk-bcb.lock
transaction_log         /var/log/btrbk.log

volume /mnt
  subvolume data
    snapshot_name bcb_level0
```

`snapshot_preserve_min latest` guarantees the newest snapshot per level always survives pruning — exactly the parent the next incremental needs — while `snapshot_preserve 12m` additionally keeps a year of monthly snapshots locally. The shared `lockfile` keeps the two levels' btrbk invocations from overlapping (a level 0 upload can run for hours into the next timer), and `transaction_log` records every snapshot create/delete for audits.

For incremental levels, `snapshot_create onchange` is worth considering instead of `always`: if the subvolume has not changed since the last snapshot, btrbk creates nothing and bcb idempotently skips the upload.

The scheduled job snapshots, backs up, then prunes. `/usr/local/sbin/bcb-run`:

```bash
#!/usr/bin/env bash
set -euo pipefail
LEVEL="$1"
CONF="/etc/btrbk/bcb-level${LEVEL}.conf"

btrbk -c "$CONF" snapshot
bcb backup --level "$LEVEL"

# Prune only after a successful backup, so a failed run keeps the previous snapshot
# still referenced as parent by the last backup manifest
btrbk -c "$CONF" prune
```

On failure the script exits non-zero before pruning, and re-running is safe: an interrupted backup resumes, a completed one is skipped.

```ini
; /etc/systemd/system/bcb-backup@.service
[Unit]
Description=bcb backup level %i

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/bcb-run %i
```

```ini
; /etc/systemd/system/bcb-backup@0.timer
[Timer]
OnCalendar=*-01,07-01 01:00
Persistent=true

[Install]
WantedBy=timers.target
```

```ini
; /etc/systemd/system/bcb-backup@1.timer
[Timer]
OnCalendar=Sun *-*-* 03:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl enable --now bcb-backup@0.timer bcb-backup@1.timer
```

`Persistent=true` catches up on schedules missed while powered off. Runs are logged in `journalctl -u 'bcb-backup@*'` and in `<base_dir>/log/`.

Notes:

- Each level 1 is the cumulative delta since the last level 0 and grows over the half year; if that gets large, add a monthly level 1 and weekly level 2 instead.
- Set bucket lifecycle expirations filtered on the `bcb-backup-level` tag (see Retention below) to at least 13 months so the current and previous chains always survive.
- Drill restores: `bcb list` regularly, and walk a full restore on a test machine at least yearly to prove the private key and procedure work.

## Restoring

Objects in S3 `GLACIER` (including `DEEP_ARCHIVE`) storage classes cannot be downloaded directly; they must be retrieved first, which takes hours. `bcb restore` handles this in two phases:

1. Initiates the retrievals and exits with a message.
2. Re-run the same command once the retrievals complete to perform the actual restore. `--restore-tier` and `--restore-days` control the retrieval speed/cost and how long the retrieved copies stay downloadable.

To restore an incremental backup, restore each level in order onto the same target (level 0 first, then level 1, and so on); each incremental needs its parent present on the target.

## Retention

`bcb` never deletes anything from S3; use bucket lifecycle rules for retention.

Every uploaded object carries a `bcb-backup-level` tag (`0`, `1`, ... for backup parts, `manifest` for manifests), so one lifecycle rule per level covers all subvolumes — filter on the tag, optionally combined with the bucket prefix. Leave `manifest`-tagged objects unexpired (they are tiny and needed to restore anything).

Be careful that lifecycle rules must not expire a backup that later incremental levels still depend on (deleting a level-0 breaks every chain built on it), and a level's expiration must respect its storage class minimum duration (e.g. 180 days for `DEEP_ARCHIVE`).

## Testing

**Unit and integration:**

```bash
cargo test
```

**E2E:**

Requires Docker and [Multipass](https://multipass.run). The script creates and setup the VM on first run.

```bash
bash scripts/run-vm-tests.sh
```

## TODO

- [ ] CI (GitHub Actions: test, clippy, fmt)
- [ ] Failure alerting (e.g. systemd OnFailure= notification unit)
- [ ] Release packaging
- [ ] Parallel part encryption/upload (worker pool)
- [ ] Protect parent snapshots of incremental chains from deletion (no btrfs equivalent of `zfs hold`; likely needs a check against btrbk retention)
- [ ] `restore --dry-run` and a `--force` passthrough for `btrfs receive`
- [ ] Resumable restore: keep downloaded parts across runs so an interrupted restore only fetches (and re-requests archive retrieval for) the missing parts
- [ ] Add btrbk to VM for E2E testing

[s3-gda]: https://aws.amazon.com/s3/storage-classes/glacier/
[age]: https://github.com/filosottile/age
[b3]: https://github.com/BLAKE3-team/BLAKE3
[btrbk]: https://github.com/digint/btrbk
[btrbk-conf]: https://digint.ch/btrbk/doc/btrbk.conf.5.html
