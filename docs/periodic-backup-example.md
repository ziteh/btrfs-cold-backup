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

AWS credentials go in root's `~/.aws/credentials` (bcb uses the AWS SDK default chain). The IAM policy is [contrib/aws/bcb-backup-policy.json](../contrib/aws/bcb-backup-policy.json), with `BUCKET_NAME` replaced. It lacks `s3:RestoreObject`, so stolen host credentials cannot run up retrieval costs; restore uses [contrib/aws/bcb-restore-policy.json](../contrib/aws/bcb-restore-policy.json) instead, with credentials kept offline alongside the private key. `PutObjectTagging` is easy to miss since every upload sets its tag inline via `PutObject`, but AWS checks it as a separate permission — without it, `bcb check` fails at the write probe rather than passing and only failing later during a real backup.

Optional hardening for the credentials, since anyone holding them can call S3 directly, not just through bcb:

- Keep `~/.aws/credentials` readable by root only (`chmod 600`), with no other profiles on the host.
- Encrypt them at rest with [systemd-creds][systemd-creds] (bound to the TPM2 where available) so a copied disk or image does not leak them, and let the service decrypt them at runtime:

  ```bash
  sudo systemd-creds encrypt --name=aws /root/.aws/credentials /etc/credstore.encrypted/bcb-aws
  ```

  ```ini
  # systemctl edit bcb-backup@.service
  [Service]
  LoadCredentialEncrypted=aws:/etc/credstore.encrypted/bcb-aws
  Environment=AWS_SHARED_CREDENTIALS_FILE=%d/aws
  ```

  Then remove the plaintext file. Interactive `sudo bcb ...` runs need the same variable pointed at a decrypted copy.
- Restrict the bcb IAM user to your own source IP with a bucket policy, so credentials copied off the host are useless elsewhere. Scope it with `aws:PrincipalArn` so console access from other places is unaffected; with a dynamic IPv4 address, use an IPv6 prefix (requires S3's dual-stack endpoint) or route through a host with a fixed address.

  ```json
  {
    "Sid": "BcbFromHomeOnly",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": ["arn:aws:s3:::BUCKET_NAME", "arn:aws:s3:::BUCKET_NAME/*"],
    "Condition": {
      "ArnEquals": { "aws:PrincipalArn": "arn:aws:iam::ACCOUNT_ID:user/bcb" },
      "NotIpAddress": { "aws:SourceIp": ["203.0.113.10/32"] }
    }
  }
  ```

Snapshots come from btrbk, one [config][btrbk-conf] per level so each level's snapshots are created independently ([contrib/btrbk/](../contrib/btrbk/), level 1 identical to level 0 except `snapshot_name bcb_level1`).

btrbk only creates them: `bcb prune-snapshots` deletes them, because only bcb knows which ones backups still need (the parent the next incremental builds on, the target and parent of an unfinished backup, and the newest of each level). It keeps no local history beyond that; for local recovery points, run a separate btrbk instance with its own `snapshot_name` and `snapshot_dir`. The shared `lockfile` keeps the two levels' btrbk invocations from overlapping (a level 0 upload can run for hours into the next timer), and `transaction_log` records every snapshot create/delete for audits.

For incremental levels, `snapshot_create onchange` is worth considering instead of `always`: if the subvolume has not changed since the last snapshot, btrbk creates nothing and bcb idempotently skips the upload.

The scheduled job snapshots, backs up, then runs `bcb prune-snapshots` ([contrib/bcb-run](../contrib/bcb-run)). On failure the script exits non-zero before pruning, and re-running is safe: an interrupted backup resumes, a completed one is skipped.

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
- Drill restores: `bcb list` regularly, and walk a full restore on a test machine at least yearly to prove the private key and procedure work. [aws-drill.md](aws-drill.md) exercises the real Glacier retrieval path, which the VM tests cannot.
- A large level 0 over a slow uplink can take weeks. Each level keeps its own progress, so instead of skipping incrementals for that long, a wrapper can make the level 0 yield: `systemctl stop bcb-backup@0`, run the incremental, then `systemctl start bcb-backup@0`, which resumes it. Restart the level 0 from a `trap` so a failing incremental can't leave it stopped. A level 0 stopped while still sending (before its first upload) restarts the send, so schedule incrementals at a different time of day than the level 0 rather than at the same minute.
- Run `bcb verify` regularly (e.g. a monthly systemd timer, wired into the same `OnFailure=` alerting as the backup jobs): it audits every backup's incremental chain, part existence and size, and storage class against S3 via HeadObject/ListObjectsV2 only, so it never triggers an archive retrieval or its cost. Pass `--max-age-days` to also flag a task whose newest backup is older than expected — the one check that can catch a schedule that silently stopped running. It complements, not replaces, an actual restore drill: it cannot detect data corrupted in a way that preserves size, nor verify the private key still works.

[btrbk-conf]: https://digint.ch/btrbk/doc/btrbk.conf.5.html
[systemd-creds]: https://www.freedesktop.org/software/systemd/man/latest/systemd-creds.html
