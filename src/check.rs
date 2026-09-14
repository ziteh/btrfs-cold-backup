use anyhow::{Result, anyhow};
use std::path::Path;

use crate::{BtrfsOps, Config, RemoteBackend};

/// Verifies the external executables bcb shells out to are present on PATH.
pub fn check_binaries() -> Result<()> {
    for bin in ["btrfs", "split", "du"] {
        if !on_path(bin) {
            return Err(anyhow!("required executable '{bin}' not found on PATH"));
        }
    }
    Ok(())
}

fn on_path(bin: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&paths).any(|dir| is_executable(&dir.join(bin)))
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

pub async fn check<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
) -> Result<()> {
    config.validate()?;

    for task in &config.tasks {
        if !task.enabled {
            continue;
        }
        if !btrfs.subvolume_exists(&task.subvolume).await? {
            return Err(anyhow!(
                "task '{}': subvolume {:?} does not exist",
                task.name,
                task.subvolume
            ));
        }
        match tokio::fs::metadata(&task.snapshot_dir).await {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(anyhow!(
                    "task '{}': snapshot_dir {:?} is not a directory",
                    task.name,
                    task.snapshot_dir
                ));
            }
            Err(e) => {
                return Err(anyhow!(
                    "task '{}': cannot access snapshot_dir {:?}: {e}",
                    task.name,
                    task.snapshot_dir
                ));
            }
        }
    }

    remote.verify_credentials().await?;

    Ok(())
}
