# Leave local disk targets to btrbk

## Context and Problem Statement

bcb uploads to S3 Glacier Deep Archive as the last-resort offsite copy. A USB disk kept off the host would add a second, faster-to-restore copy, and offline backup.

Should bcb also write backups to a local disk, either btrfs-formatted or not?

## Considered Options

- Add a local disk target to bcb
- Use btrbk `send | receive` to a btrfs-formatted disk, outside bcb
- Use restic to a repository on the disk
- Use rsync to mirror a snapshot to the disk
- Use rclone to sync a snapshot to the disk

## Decision Outcome

Chosen option: "btrbk to a btrfs disk", because most of bcb exists for constraints of Deep Archive (part size, storage class per level, minimum storage duration, retrieval cost, retention delegated to lifecycle rules) that a local disk doesn't have. A received subvolume can be browsed and copied from directly, while a bcb stream needs its whole chain replayed. btrbk already creates the snapshots and handles incrementals and retention for local targets, and LUKS covers encryption. A local target in bcb would also require bcb to delete old backups itself, since a disk has no lifecycle rules.

Keeping the two copies in different tools and formats also means a bug in one, such as wrong chain selection, cannot corrupt both copies.

The file-level tools work on any filesystem but give up what btrfs already provides:

- restic keeps encrypted, deduplicated history, but each run walks the whole tree instead of sending only changed extents, and restoring needs restic and its repository password.
- rsync and rclone mirror only the latest state; history needs `--link-dest` or `--backup-dir` layouts, and changes are found by scanning the tree. rclone's strengths (cloud remotes, `crypt`) don't apply to a local disk.
- None of them preserve subvolume boundaries or reflinks, so the copy can be larger than the source.

This should be revisited if the disk must use a filesystem other than btrfs, e.g. to be read on another OS; restic is then the preferred fallback, since it is the only one of these that keeps history and encrypts without LUKS.

### Consequences

- Good, because bcb keeps one backend and its no-deletion guarantee.
- Good, because the local copy restores without bcb, age keys or chain replay.
- Good, because the two copies share no backup code or format, so a bug in either tool affects only its own copy.
- Bad, because there are two tools and two restore procedures to learn and drill.
- Bad, because the disk must be formatted btrfs.
- Neutral, because the local copy's schedule, retention and alerts are configured in btrbk and systemd, not in bcb.
