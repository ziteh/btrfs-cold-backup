use std::process::Command;

use crate::common::*;

/// A backup spanning multiple parts must round-trip: parts are encrypted and uploaded
/// individually, and restore must reassemble them in order before piping to btrfs receive.
/// Uses the debug-only BCB_TEST_SPLIT_SIZE override so a small fixture exercises the
/// multi-part path without multi-GiB test data.
#[tokio::test]
async fn multi_part_backup_and_restore_should_round_trip() {
    let Some(env) = vm_env() else { return };
    let fixture = setup_fixture(&env, "multi-part", |subvol| {
        let status = Command::new("dd")
            .args([
                "if=/dev/urandom",
                &format!("of={}", subvol.join("big.bin").to_str().unwrap()),
                "bs=1M",
                "count=10",
            ])
            .status()
            .unwrap();
        assert!(status.success());
    });

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
        .env("BCB_TEST_SPLIT_SIZE", (4 * 1024 * 1024).to_string())
        .output()
        .unwrap();
    assert!(
        backup.status.success(),
        "backup failed: {}",
        String::from_utf8_lossy(&backup.stderr)
    );

    // The test proves nothing unless the backup actually produced multiple parts.
    let prefix = data_prefix(&fixture);
    let s3 = raw_s3_client(&env).await;
    let resp = s3
        .list_objects_v2()
        .bucket(&env.s3_bucket)
        .prefix(&prefix)
        .send()
        .await
        .unwrap();
    let part_keys: Vec<&str> = resp
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .filter(|k| k.contains("snapshot.part-"))
        .collect();
    assert!(
        part_keys.len() >= 2,
        "expected a multi-part backup, got {} part(s)",
        part_keys.len()
    );

    // Lifecycle rules filter on this tag; verify it actually lands on the objects.
    let tagging = s3
        .get_object_tagging()
        .bucket(&env.s3_bucket)
        .key(part_keys[0])
        .send()
        .await
        .unwrap();
    assert!(
        tagging
            .tag_set()
            .iter()
            .any(|t| t.key() == "bcb-backup-level" && t.value() == "0"),
        "part object missing bcb-backup-level tag: {:?}",
        tagging.tag_set()
    );

    let restore = Command::new(&env.bin_path)
        .args([
            "--config",
            fixture.config_path.to_str().unwrap(),
            "restore",
            "--task",
            &fixture.task_name,
            "--level",
            "0",
            "--date",
            "20260601",
            "--target",
            fixture.restore_dir.to_str().unwrap(),
            "--private-key",
            fixture.private_key_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        restore.status.success(),
        "restore failed: {}",
        String::from_utf8_lossy(&restore.stderr)
    );

    let original = std::fs::read(fixture.subvol.join("big.bin")).unwrap();
    let recovered = std::fs::read(
        fixture
            .restore_dir
            .join("bcb_level0.20260601T0000")
            .join("big.bin"),
    )
    .unwrap();
    assert_eq!(original, recovered, "restored content must match source");
}
