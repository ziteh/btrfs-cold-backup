# Btrfs Cold Backup

`bcb` backs up btrfs subvolumes to object storage services.

Features:

- Specifically designed for [AWS S3 Glacier Deep Archive][s3-gda] cold storage, optimal storage costs.
- Full and incremental multi-level backups.
- Age is used for end-to-end encryption (E2EE).
- BLAKE3 hash is used for integrity verification.

> For ZFS: [zrb](https://github.com/ziteh/zfs-remote-backup)

## Usage

```bash
# Build
cargo build --release
```

```bash
btrfs-cold-backup --help
```

## Testing

**Unit and integration:**

```bash
cargo test
```

**E2E:**

Requires Docker and [Multipass](https://multipass.run). The script creates and configures the VM on first run.

```bash
bash scripts/run-vm-tests.sh
```

[s3-gda]: https://aws.amazon.com/s3/storage-classes/glacier/
