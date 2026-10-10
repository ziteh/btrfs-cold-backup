use std::path::{Path, PathBuf};
use std::process::Command;

use crate::common::*;

struct TmpfsMount(PathBuf);

impl TmpfsMount {
    fn new(dir: &Path, size: &str) -> Self {
        let out = Command::new("mount")
            .args(["-t", "tmpfs", "-o", &format!("size={size}"), "tmpfs"])
            .arg(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        Self(dir.to_path_buf())
    }
}

impl Drop for TmpfsMount {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(&self.0).output();
    }
}

/// Running out of space in base_dir must report ENOSPC and leave no parts behind to starve
/// the next task.
///
/// The free-space precheck estimates from `du`, which counts an empty file as zero bytes while
/// `btrfs send` spends a few hundred bytes of stream on each; many empty files therefore pass
/// the precheck and still overflow the small tmpfs base_dir.
#[tokio::test]
async fn backup_should_report_enospc_and_clean_up_when_base_dir_fills() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "disk-full", |subvol| {
        for i in 0..40_000 {
            std::fs::File::create(subvol.join(format!("f{i}"))).unwrap();
        }
    });
    let _tmpfs = TmpfsMount::new(&fixture.work_dir, "4M");

    let backup = Command::new(&env.bin_path)
        .args([
            "--config",
            fixture.config_path.to_str().unwrap(),
            "backup",
            "--level",
            "0",
            "--task",
            &fixture.task_name,
        ])
        .env("BCB_TEST_SPLIT_SIZE", (1024 * 1024).to_string())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&backup.stderr);
    assert!(!backup.status.success(), "backup should fail: {stderr}");
    assert!(
        stderr.contains("No space left on device"),
        "error must name ENOSPC: {stderr}"
    );

    let backup_dir = fixture.work_dir.join("backup");
    let state_path = backup_dir
        .join(btrfs_cold_backup::layout::sanitize_subvolume_path(&fixture.subvol).unwrap())
        .join("state-level0.yaml");
    let leftovers: Vec<_> = walk(&backup_dir)
        .into_iter()
        .filter(|p| p.is_file() && *p != state_path)
        .collect();
    assert!(
        leftovers.is_empty(),
        "parts left in base_dir: {leftovers:?}"
    );
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|e| {
            let p = e.path();
            let mut v = walk(&p);
            v.push(p);
            v
        })
        .collect()
}
