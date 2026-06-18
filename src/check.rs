use anyhow::{Result, anyhow};

use crate::{BtrfsOps, Config, RemoteBackend};

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
    }

    if config.s3.enabled {
        remote.verify_credentials().await?;
    }

    Ok(())
}
