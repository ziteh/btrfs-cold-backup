# Real AWS Restore Drill Runbook

The e2e and vm test suites run against a local S3-compatible server (SeaweedFS), which never
enforces Glacier's access restrictions or retrieval scheduling — every test task's
`storage_class.backup_data` is `STANDARD`. That leaves the following untested against real AWS:

- `request_restore` actually issuing `RestoreObject` with `GlacierJobParameters.tier`
- `restore_status_detail` parsing a real `x-amz-restore` response header
- AWS's `RestoreAlreadyInProgress` / `ObjectAlreadyInActiveTierError` error handling on repeat calls
- the retrieval-wave/budget scheduling in `restore.rs` behaving correctly under the real ~12h
  Standard-tier SLA

This is a one-off manual drill, not a script or CI job: it costs real (if tiny) money, needs a
~12 hour wait for the retrieval SLA, and only needs to be run occasionally — re-run this same
runbook for the periodic restore drills the README's "Drill restores" note recommends.

## Cost

With a ~15-20MB test dataset, DEEP_ARCHIVE storage cost, the 180-day minimum-storage early-deletion
fee, and Standard-tier retrieval cost are each well under $0.01 — negligible. The only real
financial risk is forgetting to delete the drill bucket afterward, so cleanup (last step below) is
not optional.

## Scope

- **A brand-new, disposable S3 bucket** (e.g. `bcb-drill-<date>`), fully separate from any
  production backup bucket, so it can be deleted wholesale afterward with no risk of touching
  real manifests/state.
- **IAM permissions**: the same set the README lists for production —
  `PutObject`, `PutObjectTagging`, `GetObject`, `HeadObject`, `ListBucket`,
  `ListBucketMultipartUploads`, `AbortMultipartUpload`, `DeleteObject`, `RestoreObject`,
  `HeadBucket` — scoped to the drill bucket only.
- **A throwaway test subvolume** on real hardware (not real data), so `btrfs send`/`receive` is
  exercised for real.
- **A dev build** (`cargo build`, not `--release`), so the test-only env vars that shrink part
  sizes are compiled in:
  - `BCB_TEST_SPLIT_SIZE` (see `split_part_size` in `src/backup.rs`) — set to a few MB so a
    15-20MB dataset splits into 2-3 parts instead of needing GB-scale data to trigger multipart.
  - `BCB_TEST_PART_CHUNK_SIZE` (see `part_chunk_size` in `src/restore.rs`) — same idea for
    download chunking.

  Multiple parts matter here because each is retrieved and expires independently — a single-part
  drill wouldn't exercise that scheduling at all.
- **Retrieval tier: Standard.** Deep Archive only supports Standard (~12h) and Bulk (~48h);
  Expedited isn't available for Deep Archive.

## Steps

1. **Set up the drill bucket, IAM policy, and config.** Use `storage_class.backup_data:
   [DEEP_ARCHIVE]` to match production. Test level 0 only — the incremental chain logic (level
   1/2 parent handling) is already covered by the mocked integration tests; this drill exists
   specifically to validate real Glacier access, not chain logic.

2. **Create the test subvolume and a `bcb_level0.<timestamp>` snapshot**, filled with identifiable
   random data for later comparison, e.g.:

   ```bash
   head -c 15M /dev/urandom > /mnt/data/drill-subvol/payload.bin
   ```

3. **`bcb check`** — confirms IAM permissions and config are correct. This only does
   HeadBucket/PutObject probes, no retrieval cost involved.

4. **`bcb backup --level 0`** — confirms all parts upload and the manifest is written.

5. **`bcb restore ...` immediately** — expect exit code 2 (retrieval in progress), and confirm
   from logs that `RestoreObject` was actually called.

6. **Wait ~12 hours** (Standard tier SLA). During the wait, call `bcb restore` again a few times
   and check that:
   - a repeat call handles `RestoreAlreadyInProgress` correctly (no duplicate retrieval request,
     no hard failure);
   - if there's more than one part, retrieval requests go out in scheduled waves rather than all
     at once.

7. **After the SLA window, call `bcb restore` again** — expect exit code 0 (`restore complete`).

8. **Compare results**: `diff -r` the restored subvolume against the original — they must be
   byte-identical. Cross-check with `bcb verify` or a manual blake3 hash comparison.

9. **Clean up**: delete the drill bucket, the IAM policy, and the local test subvolume. Note the
   180-day DEEP_ARCHIVE minimum-storage early-deletion fee applies (negligible at this data size,
   see Cost above) — this is expected, not a bug.

## What counts as success

- `diff -r` shows no differences between the restored and original subvolume.
- Exit codes follow the README's documented sequence: `backup` succeeds with 0, `restore`
  returns 2 while retrieval is pending, and 0 once it's ready.
- The observed behavior for `RestoreAlreadyInProgress` handling, wave scheduling, and
  expiry-date parsing matches what `src/remote.rs` and `src/restore.rs` are written to do. Any
  mismatch is a real bug — fix it and add a corresponding mock-based test case so it's covered
  going forward without needing another real-AWS drill.

After a successful run, update the README's "Drill restores" note with the date this was last
completed.
