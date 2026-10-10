//! Everything bcb runs the `btrfs` command for, behind [`BtrfsOps`] so backup and restore can
//! be tested against a mock.

use anyhow::{Context as _, Result, anyhow};
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::process::{ChildStdout, Command};
use tokio::sync::oneshot;

use crate::timestamp::SNAPSHOT_PREFIX;

#[allow(async_fn_in_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait BtrfsOps: Send + Sync {
    /// Names of the bcb snapshots in `snapshot_dir`, sorted, which orders each level's
    /// snapshots oldest first since their names differ only in the timestamp.
    async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>>;
    async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
    async fn send_incremental(
        &self,
        parent: &Path,
        snapshot: &Path,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>>;
    async fn subvolume_uuids(&self, path: &Path) -> Result<SubvolumeUuids>;
    /// Runs `btrfs receive` into `target_dir`, feeding it `stream`.
    async fn receive(&self, target_dir: &Path, stream: Box<dyn Read + Send>) -> Result<()>;
    async fn delete_subvolume(&self, path: &Path) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubvolumeUuids {
    pub uuid: String,
    /// UUID of the subvolume this one was snapshotted from; None if it is not a snapshot.
    pub parent_uuid: Option<String>,
}

/// Fails unless `snapshot` was taken from the subvolume at `subvolume` with `source_uuid`.
pub(crate) fn ensure_snapshot_of(
    snapshot_name: &str,
    snapshot: &SubvolumeUuids,
    subvolume: &Path,
    source_uuid: &str,
) -> Result<()> {
    if snapshot.parent_uuid.as_deref() == Some(source_uuid) {
        return Ok(());
    }
    Err(anyhow!(
        "snapshot '{snapshot_name}' was not taken from {subvolume:?} (its Parent UUID is {}, \
         the subvolume's UUID is {source_uuid}); check that snapshot_dir holds this \
         subvolume's snapshots",
        snapshot.parent_uuid.as_deref().unwrap_or("-"),
    ))
}

/// Parses `btrfs subvolume show` output. `Received UUID:` and `Parent UUID:` lines are
/// distinguished from `UUID:` by matching each line's full label.
fn parse_subvolume_uuids(show_output: &str) -> Result<SubvolumeUuids> {
    let mut uuid = None;
    let mut parent_uuid = None;
    for line in show_output.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("UUID:") {
            uuid = Some(v.trim());
        } else if let Some(v) = line.strip_prefix("Parent UUID:") {
            parent_uuid = Some(v.trim());
        }
    }
    let uuid = match uuid {
        Some(u) if u != "-" => u.to_string(),
        _ => return Err(anyhow!("no UUID in btrfs subvolume show output")),
    };
    let parent_uuid = parent_uuid.filter(|p| *p != "-").map(str::to_string);
    Ok(SubvolumeUuids { uuid, parent_uuid })
}

/// The stdout of `btrfs send`. At EOF it waits for the process to exit and turns a failed
/// exit into a read error, so a stream cut short by a failing send can't pass for complete.
///
/// The process is awaited in a spawned task, so `poll_read` can park and be woken instead of
/// polling `try_wait`. Dropping the stream drops `_kill_tx`, which tells that task to kill the
/// process: this covers both a reader that stops early and a send that ignores EPIPE.
struct BtrfsStream {
    stdout: Pin<Box<ChildStdout>>,
    status_rx: oneshot::Receiver<io::Result<(ExitStatus, Vec<u8>)>>,
    _kill_tx: oneshot::Sender<()>,
    stdout_done: bool,
    // The oneshot panics if polled again after completing, and readers may hit EOF twice.
    exited: bool,
}

impl Unpin for BtrfsStream {}

impl BtrfsStream {
    fn poll_exit(
        rx: &mut oneshot::Receiver<io::Result<(ExitStatus, Vec<u8>)>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        match Pin::new(rx).poll(cx) {
            Poll::Ready(Ok(Ok((status, stderr)))) => {
                if status.success() {
                    Poll::Ready(Ok(()))
                } else {
                    let msg = if stderr.is_empty() {
                        format!("btrfs send failed with {status}")
                    } else {
                        format!(
                            "btrfs send failed with {status}: {}",
                            String::from_utf8_lossy(&stderr).trim()
                        )
                    };
                    Poll::Ready(Err(io::Error::other(msg)))
                }
            }
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            // Without the exit status a truncated stream is indistinguishable from a complete
            // one, so this must not read as EOF.
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::other(
                "btrfs send exit status was lost; the stream may be incomplete",
            ))),
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

        if this.exited {
            return Poll::Ready(Ok(()));
        }
        if !this.stdout_done {
            let filled_before = buf.filled().len();
            match this.stdout.as_mut().poll_read(cx, buf) {
                Poll::Ready(Ok(())) if buf.filled().len() == filled_before => {
                    this.stdout_done = true;
                }
                other => return other,
            }
        }
        let result = Self::poll_exit(&mut this.status_rx, cx);
        if result.is_ready() {
            this.exited = true;
        }
        result
    }
}

fn path_to_str<'a>(p: &'a Path, label: &str) -> Result<&'a str> {
    p.to_str()
        .ok_or_else(|| anyhow!("{label} path is not valid UTF-8"))
}

/// SIGKILL bypasses Drop, so kill_on_drop alone can't stop `btrfs send` from outliving a
/// killed bcb; PR_SET_PDEATHSIG asks the kernel to kill it when bcb dies.
#[cfg(target_os = "linux")]
fn die_with_parent() -> io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn spawn_send(args: &[&str]) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
    let mut cmd = Command::new("btrfs");
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(die_with_parent);
    }
    let mut child = cmd.spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture btrfs send stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture btrfs send stderr"))?;

    let (status_tx, status_rx) = oneshot::channel();
    let (kill_tx, kill_rx) = oneshot::channel::<()>();

    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).await.ok();
        buf
    });

    tokio::spawn(async move {
        tokio::select! {
            result = child.wait() => {
                let stderr_bytes = stderr_task.await.unwrap_or_default();
                let _ = status_tx.send(result.map(|s| (s, stderr_bytes)));
            }
            _ = kill_rx => {
                child.start_kill().ok();
                let result = child.wait().await.map(|s| (s, vec![]));
                let _ = status_tx.send(result);
            }
        }
    });

    Ok(Box::new(BtrfsStream {
        stdout: Box::pin(stdout),
        status_rx,
        _kill_tx: kill_tx,
        stdout_done: false,
        exited: false,
    }))
}

/// Reports the installed btrfs-progs version, or "unknown" — manifest provenance only,
/// never a hard failure.
pub async fn btrfs_version() -> String {
    match Command::new("btrfs").arg("--version").output().await {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => "unknown".to_string(),
    }
}

pub struct BtrfsDriver;

impl BtrfsOps for BtrfsDriver {
    async fn list_snapshots(&self, snapshot_dir: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(snapshot_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(SNAPSHOT_PREFIX) && entry.file_type().await?.is_dir() {
                names.push(name_str.into_owned());
            }
        }

        names.sort();
        Ok(names)
    }

    async fn send_full(&self, snapshot: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        spawn_send(&["send", path_to_str(snapshot, "snapshot")?])
    }

    async fn send_incremental(
        &self,
        parent: &Path,
        snapshot: &Path,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        spawn_send(&[
            "send",
            "-p",
            path_to_str(parent, "parent")?,
            path_to_str(snapshot, "snapshot")?,
        ])
    }

    async fn subvolume_uuids(&self, path: &Path) -> Result<SubvolumeUuids> {
        let output = Command::new("btrfs")
            .args(["subvolume", "show", path_to_str(path, "path")?])
            .output()
            .await?;
        if !output.status.success() {
            return Err(anyhow!(
                "{path:?} is not a btrfs subvolume: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        parse_subvolume_uuids(&String::from_utf8_lossy(&output.stdout))
            .map_err(|e| anyhow!("{path:?}: {e}"))
    }

    async fn delete_subvolume(&self, path: &Path) -> Result<()> {
        let output = Command::new("btrfs")
            .args(["subvolume", "delete", path_to_str(path, "path")?])
            .output()
            .await?;
        if !output.status.success() {
            return Err(anyhow!(
                "failed to delete subvolume {path:?}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(())
    }

    async fn receive(&self, target_dir: &Path, mut stream: Box<dyn Read + Send>) -> Result<()> {
        tokio::fs::create_dir_all(target_dir)
            .await
            .with_context(|| format!("failed to create {target_dir:?}"))?;
        let target_dir = target_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut child = std::process::Command::new("btrfs")
                .arg("receive")
                .arg(&target_dir)
                .stdin(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .context("failed to run btrfs receive")?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow!("failed to open btrfs receive stdin"))?;
            let written = io::copy(&mut stream, &mut stdin);
            // Closing stdin ends the stream for receive.
            drop(stdin);

            // Checked first: a receive that died makes the write fail too, and its stderr says
            // why.
            let output = child.wait_with_output()?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("btrfs receive failed: {}", stderr.trim()));
            }
            written.context("failed to feed the stream to btrfs receive")?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT_SHOW: &str = "\
/mnt/.snapshots/bcb_level0.20260602T1430
\tName: \t\t\tbcb_level0.20260602T1430
\tUUID: \t\t\t1c2d3e4f-0000-0000-0000-000000000002
\tParent UUID: \t\t1c2d3e4f-0000-0000-0000-000000000001
\tReceived UUID: \t\t1c2d3e4f-0000-0000-0000-000000000009
\tFlags: \t\t\treadonly
";

    #[test]
    fn test_parse_subvolume_uuids_snapshot() {
        let uuids = parse_subvolume_uuids(SNAPSHOT_SHOW).unwrap();
        assert_eq!(uuids.uuid, "1c2d3e4f-0000-0000-0000-000000000002");
        assert_eq!(
            uuids.parent_uuid.as_deref(),
            Some("1c2d3e4f-0000-0000-0000-000000000001")
        );
    }

    #[test]
    fn test_parse_subvolume_uuids_dash_parent_is_none() {
        let out = "\tUUID: \t\t\tabc\n\tParent UUID: \t\t-\n\tReceived UUID: \t\t-\n";
        let uuids = parse_subvolume_uuids(out).unwrap();
        assert_eq!(uuids.uuid, "abc");
        assert!(uuids.parent_uuid.is_none());
    }

    #[tokio::test]
    async fn test_read_fails_when_exit_status_is_lost() {
        let mut child = Command::new("true").stdout(Stdio::piped()).spawn().unwrap();
        let (status_tx, status_rx) = oneshot::channel();
        drop(status_tx);
        let (kill_tx, _kill_rx) = oneshot::channel();
        let mut stream = BtrfsStream {
            stdout: Box::pin(child.stdout.take().unwrap()),
            status_rx,
            _kill_tx: kill_tx,
            stdout_done: false,
            exited: false,
        };

        let mut buf = Vec::new();
        assert!(stream.read_to_end(&mut buf).await.is_err());
        child.wait().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_after_eof_stays_at_eof() {
        let mut child = Command::new("true").stdout(Stdio::piped()).spawn().unwrap();
        let (status_tx, status_rx) = oneshot::channel();
        let status = child.wait().await.unwrap();
        status_tx.send(Ok((status, Vec::new()))).unwrap();
        let (kill_tx, _kill_rx) = oneshot::channel();
        let mut stream = BtrfsStream {
            stdout: Box::pin(child.stdout.take().unwrap()),
            status_rx,
            _kill_tx: kill_tx,
            stdout_done: false,
            exited: false,
        };

        let mut buf = [0u8; 8];
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
    }

    #[test]
    fn test_parse_subvolume_uuids_missing_uuid_is_err() {
        assert!(parse_subvolume_uuids("\tParent UUID: \t\tabc\n").is_err());
        assert!(parse_subvolume_uuids("\tUUID: \t\t\t-\n").is_err());
    }
}
