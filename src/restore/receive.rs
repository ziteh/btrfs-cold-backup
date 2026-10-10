//! The last stage: verify every downloaded part and the stream they form, then feed the
//! decrypted stream to `btrfs receive`.

use age::x25519;
use anyhow::{Context, Result, anyhow};
use std::io::{self, Read};
use std::path::PathBuf;

use crate::{
    btrfs::BtrfsOps, crypto::blake3_file, manifest::BackupManifest, remote::RemoteBackend,
    state::PartDownloadState,
};

use super::RestoreRun;

/// The send stream, read by decrypting the parts in order. Nothing decrypted is written to
/// disk.
struct DecryptedParts {
    paths: std::vec::IntoIter<PathBuf>,
    identity: x25519::Identity,
    current: Option<Box<dyn Read + Send>>,
}

impl DecryptedParts {
    fn new(paths: Vec<PathBuf>, identity: x25519::Identity) -> Self {
        Self {
            paths: paths.into_iter(),
            identity,
            current: None,
        }
    }

    fn open(&self, path: &PathBuf) -> io::Result<Box<dyn Read + Send>> {
        let file = std::fs::File::open(path)?;
        let decryptor = age::Decryptor::new_buffered(io::BufReader::new(file))
            .map_err(|e| io::Error::other(format!("failed to read age header of {path:?}: {e}")))?;
        let reader = decryptor
            .decrypt(std::iter::once(&self.identity as &dyn age::Identity))
            .map_err(|e| io::Error::other(format!("failed to decrypt {path:?}: {e}")))?;
        Ok(Box::new(reader))
    }
}

impl Read for DecryptedParts {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.current.is_none() {
                let Some(path) = self.paths.next() else {
                    return Ok(0);
                };
                self.current = Some(self.open(&path)?);
            }
            let n = self.current.as_mut().expect("opened above").read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            self.current = None;
        }
    }
}

impl<B: BtrfsOps, R: RemoteBackend> RestoreRun<'_, B, R> {
    /// Receives the snapshot once every part is downloaded, then removes the work dir.
    /// Returns false if a part was damaged on disk since its download; it is reset for the
    /// next run to download again.
    pub(super) async fn finalize(&mut self) -> Result<bool> {
        if !self.state.data.receive_completed {
            let paths: Vec<PathBuf> = self
                .manifest
                .parts
                .iter()
                .map(|p| self.part_path(&p.index))
                .collect();

            // A power loss or bit rot after the download check must cost that part's
            // download, not fail the restore. Every run checks again, since the parts are all
            // that is kept on disk.
            let damaged = find_damaged_parts(&self.manifest, &paths).await?;
            if !damaged.is_empty() {
                for index in &damaged {
                    self.state
                        .data
                        .parts
                        .insert(index.clone(), PartDownloadState::NotDownloaded);
                }
                self.state.save()?;
                tracing::warn!(
                    "parts {damaged:?} no longer match their BLAKE3 on disk; they will be \
                     downloaded again"
                );
                return Ok(false);
            }

            // Every part checked out, so a mismatch here is a bug on the backup side, which
            // downloading again cannot fix.
            tracing::info!("verifying stream hash");
            verify_stream_hash(
                paths.clone(),
                self.request.identity.clone(),
                &self.manifest.blake3,
            )
            .await?;

            tracing::info!("receiving the snapshot");
            self.receive_into_target(paths).await?;
            self.state.data.receive_completed = true;
            self.state.save()?;
            tracing::info!("restore completed successfully");
        }

        if let Err(e) = tokio::fs::remove_dir_all(&self.work_dir).await {
            tracing::warn!(
                "failed to clean up restore work dir {:?}: {e}",
                self.work_dir
            );
        }
        Ok(true)
    }

    async fn receive_into_target(&self, paths: Vec<PathBuf>) -> Result<()> {
        let target_dir = &self.request.target_dir;
        let stream = DecryptedParts::new(paths, self.request.identity.clone());
        self.btrfs.receive(target_dir, Box::new(stream)).await?;
        let snapshot = target_dir.join(&self.manifest.snapshot_name);
        self.btrfs
            .subvolume_uuids(&snapshot)
            .await
            .with_context(|| format!("snapshot {snapshot:?} not found after btrfs receive"))?;
        Ok(())
    }
}

/// Indexes of the parts whose file is missing or doesn't match its manifest hash.
async fn find_damaged_parts(manifest: &BackupManifest, paths: &[PathBuf]) -> Result<Vec<String>> {
    let mut damaged = Vec::new();
    for (part, path) in manifest.parts.iter().zip(paths) {
        let intact = match tokio::fs::metadata(path).await {
            Ok(_) => blake3_file(path).await? == part.blake3,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(e).with_context(|| format!("failed to inspect {path:?}")),
        };
        if !intact {
            damaged.push(part.index.clone());
        }
    }
    Ok(damaged)
}

async fn verify_stream_hash(
    paths: Vec<PathBuf>,
    identity: x25519::Identity,
    expected: &str,
) -> Result<()> {
    let actual = tokio::task::spawn_blocking(move || -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        io::copy(&mut DecryptedParts::new(paths, identity), &mut hasher)?;
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await??;
    if actual != expected {
        return Err(anyhow!(
            "stream BLAKE3 mismatch: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn decrypted_parts_should_join_parts_in_index_order() {
        let dir = tempfile::tempdir().unwrap();
        let identity = x25519::Identity::generate();
        let mut paths = Vec::new();
        for (i, chunk) in [&b"first "[..], b"", b"second"].iter().enumerate() {
            let path = dir.path().join(format!("{i}.age"));
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = crate::crypto::encrypting_writer(&identity.to_public(), file).unwrap();
            writer.write_all(chunk).unwrap();
            writer.finish().unwrap();
            paths.push(path);
        }

        let mut out = Vec::new();
        DecryptedParts::new(paths, identity)
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, b"first second");
    }
}
