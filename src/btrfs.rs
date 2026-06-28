use anyhow::{Result, anyhow};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::process::{ChildStdout, Command};
use tokio::sync::oneshot;

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

// Reads btrfs-send stdout; after EOF, blocks until the child exits and propagates
// a non-zero exit code as an io::Error.  The child is awaited in a spawned task
// so that poll_read can properly park and be woken rather than racing with try_wait.
// Dropping BtrfsStream drops _kill_tx, which closes the kill channel and signals
// the spawned task to SIGKILL the child — handles both early cancellation and a
// btrfs process that ignores EPIPE.
struct BtrfsStream {
    stdout: Pin<Box<ChildStdout>>,
    status_rx: oneshot::Receiver<io::Result<ExitStatus>>,
    _kill_tx: oneshot::Sender<()>,
    stdout_done: bool,
}

impl Unpin for BtrfsStream {}

impl BtrfsStream {
    fn poll_exit(
        rx: &mut oneshot::Receiver<io::Result<ExitStatus>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(Ok(status))) => {
                if status.success() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(io::Error::other(format!(
                        "btrfs send failed with {status}"
                    ))))
                }
            }
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(_)) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for BtrfsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.stdout_done {
            return Self::poll_exit(&mut this.status_rx, cx);
        }

        let filled_before = buf.filled().len();
        let result = this.stdout.as_mut().poll_read(cx, buf);
        match result {
            Poll::Ready(Ok(())) if buf.filled().len() == filled_before => {
                this.stdout_done = true;
                Self::poll_exit(&mut this.status_rx, cx)
            }
            other => other,
        }
    }
}

fn spawn_send(args: &[&str]) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
    let mut child = Command::new("btrfs")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture btrfs send stdout"))?;

    let (status_tx, status_rx) = oneshot::channel();
    let (kill_tx, kill_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::select! {
            result = child.wait() => { let _ = status_tx.send(result); }
            _ = kill_rx => {
                child.start_kill().ok();
                let _ = status_tx.send(child.wait().await);
            }
        }
    });

    Ok(Box::new(BtrfsStream {
        stdout: Box::pin(stdout),
        status_rx,
        _kill_tx: kill_tx,
        stdout_done: false,
    }))
}

pub struct BtrfsDriver;

impl BtrfsOps for BtrfsDriver {
    async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(snapshot_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("bcb_level") && entry.file_type().await?.is_dir() {
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
        spawn_send(&["send", snapshot_str])
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
        spawn_send(&["send", "-p", parent_str, snapshot_str])
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
