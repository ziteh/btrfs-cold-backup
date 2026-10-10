//! The send stage: `btrfs send` the snapshot, cut the stream into parts and encrypt each one
//! straight to disk, so plaintext never touches it.

use age::x25519;
use anyhow::{Context, Result, anyhow};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;

use crate::{
    btrfs::BtrfsOps,
    config::Task,
    crypto::{encrypting_writer, parse_recipient},
    fsutil::{available_space_bytes, disk_usage_bytes, fsync_dir},
    layout::part_file_name,
    state::{BackupTarget, PartRecord, SentStream},
    test_hooks,
    util::{GIB, IO_BUF_SIZE},
};

/// Plaintext bytes per part.
const PART_SIZE: u64 = 3 * 1024 * 1024 * 1024;

/// Sends `target` into encrypted parts in `parts_dir`, replacing whatever an earlier
/// unfinished send left there. On failure the directory is removed: without a recorded
/// stream the next run starts over anyway, and the parts would take space from the tasks
/// that run after this one.
pub(super) async fn send_and_encrypt<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    target: &BackupTarget,
    parts_dir: &Path,
    public_key: &str,
) -> Result<SentStream> {
    if parts_dir.exists() {
        tokio::fs::remove_dir_all(parts_dir)
            .await
            .with_context(|| format!("failed to remove {parts_dir:?}"))?;
    }
    tokio::fs::create_dir_all(parts_dir)
        .await
        .with_context(|| format!("failed to create {parts_dir:?}"))?;

    let result = send_into_parts(btrfs, task, target, parts_dir, public_key).await;
    if result.is_err()
        && let Err(e) = tokio::fs::remove_dir_all(parts_dir).await
    {
        tracing::warn!("failed to clean up {parts_dir:?}: {e}");
    }
    result
}

async fn send_into_parts<B: BtrfsOps>(
    btrfs: &B,
    task: &Task,
    target: &BackupTarget,
    parts_dir: &Path,
    public_key: &str,
) -> Result<SentStream> {
    let recipient = parse_recipient(public_key)?;
    let snapshot_path = task.snapshot_dir.join(&target.snapshot_name);
    check_available_space(&snapshot_path, parts_dir).await?;

    let stream = match &target.parent_snapshot {
        Some(parent) => {
            btrfs
                .send_incremental(&task.snapshot_dir.join(parent), &snapshot_path)
                .await?
        }
        None => btrfs.send_full(&snapshot_path).await?,
    };
    // With the send process alive, so the VM tests can check it dies with bcb.
    test_hooks::pause_if_set("BCB_TEST_PAUSE_DURING_SEND", "during_send").await;

    let stream = tokio_util::io::SyncIoBridge::new(stream);
    let dir = parts_dir.to_path_buf();
    let part_size = test_hooks::size_override("BCB_TEST_SPLIT_SIZE", PART_SIZE);
    tokio::task::spawn_blocking(move || encrypt_into_parts(stream, &dir, part_size, &recipient))
        .await?
}

/// Refuses to start a potentially hours-long send when base_dir clearly lacks room for it,
/// rather than failing partway with ENOSPC. The estimate is best-effort (`du` can fail for
/// reasons unrelated to space, e.g. permissions), so failing to estimate only skips the
/// check: it must never block a backup that might have succeeded.
async fn check_available_space(snapshot_path: &Path, parts_dir: &Path) -> Result<()> {
    let snapshot_size = match disk_usage_bytes(snapshot_path).await {
        Ok(size) => size,
        Err(e) => {
            tracing::warn!("could not estimate snapshot size, skipping free-space check: {e:#}");
            return Ok(());
        }
    };
    let available = available_space_bytes(parts_dir)?;
    ensure_enough_space(available, required_space(snapshot_size), snapshot_path)
}

/// The space a send of a snapshot of `snapshot_size` bytes on disk may need: exactly the size
/// for a full send, an overestimate for an incremental one, plus a margin for what `du`
/// doesn't count (per-file metadata in the stream) and age's per-chunk tags and headers.
fn required_space(snapshot_size: u64) -> u64 {
    snapshot_size + snapshot_size / 100
}

fn ensure_enough_space(available: u64, required: u64, snapshot_path: &Path) -> Result<()> {
    if available < required {
        return Err(anyhow!(
            "insufficient free space for base_dir: {:.1} GiB available, ~{:.1} GiB needed \
             (estimated from the size of {snapshot_path:?}); free up space or point base_dir at \
             a larger filesystem before retrying",
            available as f64 / GIB,
            required as f64 / GIB,
        ));
    }
    Ok(())
}

/// Passes writes through to `inner`, hashing and counting the bytes written.
struct HashingWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
    written: u64,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Cuts `stream` into parts of `part_size` plaintext bytes, each written encrypted to its own
/// file in `parts_dir`. Records the BLAKE3 of the whole plaintext stream and each part's
/// ciphertext size and BLAKE3.
fn encrypt_into_parts(
    mut stream: impl Read,
    parts_dir: &Path,
    part_size: u64,
    recipient: &x25519::Recipient,
) -> Result<SentStream> {
    let mut stream_hasher = blake3::Hasher::new();
    let mut parts = BTreeMap::new();
    let mut buf = vec![0u8; IO_BUF_SIZE];
    loop {
        let mut part_stream = (&mut stream).take(part_size);
        let mut n = part_stream.read(&mut buf)?;
        // Checked before creating the file, so a stream ending on a part boundary doesn't
        // produce an empty trailing part.
        if n == 0 {
            break;
        }

        let index = format!("{:06}", parts.len());
        let path = parts_dir.join(part_file_name(&index));
        let write_err = || format!("failed to write {path:?}");
        let file = std::fs::File::create(&path).with_context(write_err)?;
        let mut writer = encrypting_writer(
            recipient,
            HashingWriter {
                inner: file,
                hasher: blake3::Hasher::new(),
                written: 0,
            },
        )
        .with_context(write_err)?;
        while n > 0 {
            stream_hasher.update(&buf[..n]);
            writer.write_all(&buf[..n]).with_context(write_err)?;
            n = part_stream.read(&mut buf)?;
        }
        let out = writer.finish().with_context(write_err)?;
        // The state saved after this claims the part exists; after a power loss it must too.
        out.inner.sync_all().with_context(write_err)?;

        parts.insert(
            index,
            PartRecord {
                size_bytes: out.written,
                blake3: out.hasher.finalize().to_hex().to_string(),
            },
        );
    }
    fsync_dir(parts_dir)?;
    Ok(SentStream {
        blake3: stream_hasher.finalize().to_hex().to_string(),
        parts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_enough_space_boundaries() {
        let snap = Path::new("/snap");
        assert!(ensure_enough_space(100, 50, snap).is_ok());
        assert!(ensure_enough_space(50, 50, snap).is_ok());
        let err = ensure_enough_space(10, 50, snap).unwrap_err();
        assert!(err.to_string().contains("insufficient free space"));
    }

    #[test]
    fn test_required_space_exceeds_snapshot_size() {
        assert!(required_space(1 << 30) > 1 << 30);
    }

    fn decrypt(bytes: &[u8], identity: &x25519::Identity) -> Vec<u8> {
        let decryptor = age::Decryptor::new(bytes).unwrap();
        let mut reader = decryptor
            .decrypt(std::iter::once(identity as &dyn age::Identity))
            .unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn test_encrypt_into_parts_round_trips_and_records_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let identity = x25519::Identity::generate();
        let data: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();

        let sent = encrypt_into_parts(&data[..], dir.path(), 4096, &identity.to_public()).unwrap();

        assert_eq!(sent.blake3, blake3::hash(&data).to_hex().to_string());
        assert_eq!(
            sent.parts.keys().collect::<Vec<_>>(),
            ["000000", "000001", "000002"]
        );
        let mut joined = Vec::new();
        for (index, record) in &sent.parts {
            let bytes = std::fs::read(dir.path().join(part_file_name(index))).unwrap();
            assert_eq!(record.size_bytes, bytes.len() as u64);
            assert_eq!(record.blake3, blake3::hash(&bytes).to_hex().to_string());
            joined.extend(decrypt(&bytes, &identity));
        }
        assert_eq!(joined, data);
        // No plaintext part files: every file in the dir is a recorded part.
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            sent.parts.len()
        );
    }

    #[test]
    fn test_encrypt_into_parts_stream_on_part_boundary_has_no_empty_part() {
        let dir = tempfile::tempdir().unwrap();
        let identity = x25519::Identity::generate();
        let data = vec![7u8; 8192];

        let sent = encrypt_into_parts(&data[..], dir.path(), 4096, &identity.to_public()).unwrap();

        assert_eq!(sent.parts.len(), 2);
    }
}
