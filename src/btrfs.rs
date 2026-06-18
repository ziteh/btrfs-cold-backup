use anyhow::{Result, anyhow};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::process::{Child, ChildStdout, Command};

#[allow(async_fn_in_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait BtrfsOps: Send + Sync {
    async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>>;
    async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
    async fn send_incremental(
        &self,
        parent: &Path,
        snapshot: &Path,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
    async fn create_snapshot(
        &self,
        subvolume: &Path,
        snapshot_dir: &Path,
        name: &str,
    ) -> Result<PathBuf>;
    async fn delete_snapshot(&self, snapshot: &Path) -> Result<()>;
    async fn subvolume_exists(&self, path: &Path) -> Result<bool>;
}

// Keeps the child process alive while its stdout is being read.
struct BtrfsStream {
    _child: Child,
    stdout: Pin<Box<ChildStdout>>,
}

impl Unpin for BtrfsStream {}

impl AsyncRead for BtrfsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.stdout.as_mut().poll_read(cx, buf)
    }
}

pub struct BtrfsDriver;

impl BtrfsOps for BtrfsDriver {
    async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>> {
        let mut entries = tokio::fs::read_dir(snapshot_dir).await?;
        let mut names = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("bcr_level") && entry.file_type().await?.is_dir() {
                names.push(name_str.into_owned());
            }
        }

        names.sort();
        Ok(names)
    }

    async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let snapshot_str = snapshot
            .to_str()
            .ok_or_else(|| anyhow!("snapshot path is not valid UTF-8"))?;

        let mut child = Command::new("btrfs")
            .args(["send", snapshot_str])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture btrfs send stdout"))?;

        Ok(Box::new(BtrfsStream {
            _child: child,
            stdout: Box::pin(stdout),
        }))
    }

    async fn send_incremental(
        &self,
        parent: &Path,
        snapshot: &Path,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let parent_str = parent
            .to_str()
            .ok_or_else(|| anyhow!("parent path is not valid UTF-8"))?;

        let snapshot_str = snapshot
            .to_str()
            .ok_or_else(|| anyhow!("snapshot path is not valid UTF-8"))?;

        let mut child = Command::new("btrfs")
            .args(["send", "-p", parent_str, snapshot_str])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture btrfs send stdout"))?;

        Ok(Box::new(BtrfsStream {
            _child: child,
            stdout: Box::pin(stdout),
        }))
    }

    async fn create_snapshot(
        &self,
        subvolume: &Path,
        snapshot_dir: &Path,
        name: &str,
    ) -> Result<PathBuf> {
        let subvolume_str = subvolume
            .to_str()
            .ok_or_else(|| anyhow!("subvolume path is not valid UTF-8"))?;

        let dest = snapshot_dir.join(name);
        let dest_str = dest
            .to_str()
            .ok_or_else(|| anyhow!("snapshot destination path is not valid UTF-8"))?;

        let output = Command::new("btrfs")
            .args(["subvolume", "snapshot", "-r", subvolume_str, dest_str])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("btrfs snapshot failed: {}", stderr.trim()));
        }

        Ok(dest)
    }

    async fn delete_snapshot(&self, snapshot: &Path) -> Result<()> {
        let snapshot_str = snapshot
            .to_str()
            .ok_or_else(|| anyhow!("snapshot path is not valid UTF-8"))?;

        let output = Command::new("btrfs")
            .args(["subvolume", "delete", snapshot_str])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("btrfs subvolume delete failed: {}", stderr.trim()));
        }

        Ok(())
    }

    async fn subvolume_exists(&self, path: &Path) -> Result<bool> {
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("path is not valid UTF-8"))?;

        let output = Command::new("btrfs")
            .args(["subvolume", "show", path_str])
            .output()
            .await?;

        Ok(output.status.success())
    }
}
