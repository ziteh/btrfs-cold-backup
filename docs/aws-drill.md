# Real AWS Drill

The VM test suite runs against SeaweedFS, which never enforces Glacier's access restrictions or retrieval scheduling, and every VM test stores parts as `STANDARD`. [scripts/aws-drill.sh](../scripts/aws-drill.sh) covers what only real AWS can show:

- IAM: [contrib/aws/bcb-backup-policy.json](../contrib/aws/bcb-backup-policy.json) is enough for every command but restore, which it is denied; [contrib/aws/bcb-restore-policy.json](../contrib/aws/bcb-restore-policy.json) is enough for restore; `bcb check` fails without `s3:PutObjectTagging`.
- Storage classes and tags: parts land in the level's class (`DEEP_ARCHIVE`, `GLACIER`), manifests in `STANDARD`, each tagged `bcb-backup-level`, and a lifecycle rule filtering on that tag is accepted.
- Multipart: parts larger than one chunk; a backup killed mid-upload resumes and its orphaned multipart upload is aborted.
- Retrieval: `RestoreObject`, `x-amz-restore` parsing, no re-request of a retrieval in progress, and a level 0 and level 1 restore that match their snapshots byte for byte.
- Exit codes: a wrong private key and a cut network exit 1 with no retrieval requested; a pending retrieval exits 2.
- The release build, without the test hooks, against real S3.

It runs in its own Multipass VM (`bcb-aws`), separate from the VM tests, which reset their VM on every run.

## Running

```bash
export BCB_DRILL_BUCKET=<drill bucket>
task aws-drill
```

Like `bcb restore`, the drill is re-run until it exits 0:

| Exit code | Meaning                                                                                |
| --------- | -------------------------------------------------------------------------------------- |
| `0`       | Passed. The VM's test data is removed; S3 objects expire by lifecycle.                 |
| `1`       | An assertion failed. bcb's logs are under `/var/lib/bcb-aws-drill/logs/` in the VM.    |
| `2`       | Waiting on retrieval. Re-run later: Deep Archive's Standard tier takes up to 12 hours. |

The first run backs up, checks, and requests retrieval. Later runs finish the restores. `task aws-drill -- --new` abandons a waiting run and starts over.

Each run writes under its own prefix, `aws-drill/run-<UTC time>-<commit>/`, so runs never interfere and nothing needs deleting by hand.

Host requirements: `multipass`, `cargo-zigbuild`, and AWS CLI v2.

Optional environment variables:

- `BCB_DRILL_PROFILE`: profile with the backup policy. Default `bcb-drill`.
- `BCB_DRILL_NO_TAG_PROFILE`: profile with the backup policy minus `s3:PutObjectTagging`. Default `bcb-drill-no-tag`.
- `BCB_DRILL_RESTORE_PROFILE`: profile with the restore policy. Default `bcb-drill-restore`.

Credentials are exported from these profiles on the host and passed to the VM over stdin. They are never written to the VM.

## One-time AWS setup

Run with administrator credentials (e.g. `AWS_PROFILE=admin`), and AWS CLI v2 and `jq` on the host. The bucket must be dedicated to the drill: its lifecycle rules expire what the drill writes, so both scripts refuse a bucket that holds anything outside `aws-drill/` (the setup script also refuses one with other lifecycle rules).

```bash
bash scripts/aws-drill-setup.sh <drill bucket>
```

It creates the bucket in `us-east-1`, its lifecycle rules, and the IAM users `bcb-drill` (the backup policy), `bcb-drill-no-tag` (the same without `s3:PutObjectTagging`) and `bcb-drill-restore` (the restore policy). It then writes an access key for each to the host profiles and checks that each profile authenticates as its user. It is safe to re-run: it creates only what is missing, replaces a profile's key only if the profile doesn't work, and re-applies the policies and lifecycle rules from [contrib/aws/](../contrib/aws/).

Set an AWS Budgets alert by hand as a guard against a runaway cost.

The lifecycle configuration expires every drill run after 7 days, aborts incomplete multipart uploads after 1 day, and includes a rule filtered on the `bcb-backup-level` tag, like the production ones described in the [README](../README.md#retention).

## Cost

Each run stores about 250 MB for at most 7 days and retrieves it once. Deep Archive's 180-day and Glacier's 90-day minimum-storage charges apply to that early expiry. The total is a few cents at most.

## When a run fails

A failed run exits 1 at the first failed assertion and prints the tail of bcb's log. Re-running starts a fresh run. A mismatch with what [src/remote.rs](../src/remote.rs) and [src/restore/](../src/restore/) are meant to do is a bug: fix it and add a mocked test so it stays covered without AWS.
