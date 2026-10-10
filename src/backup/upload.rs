//! The upload stage: get every part onto S3, confirm they arrived, then publish the manifest
//! and the latest manifest that make the backup visible.

use anyhow::{Context, Result, anyhow};
use std::path::Path;

use crate::{
    config::{Config, StorageClass},
    crypto::blake3_file,
    layout::{TaskLayout, level_tag, manifest_tag, part_file_name},
    manifest::{
        BackupManifest, FORMAT_VERSION, LatestEntry, LatestManifest, ManifestPart, SystemInfo,
        fetch_latest,
    },
    remote::RemoteBackend,
    state::{BackupState, BackupTarget, SentStream, StateFile},
    util::now_unix,
    verify::check_parts,
};

/// Checks that every part not yet uploaded is on disk exactly as the send stage wrote it.
/// If nothing was uploaded yet, a bad part only costs a fresh send, so the send is forgotten
/// for the next run to redo it.
pub(super) async fn verify_local_parts(
    state: &mut StateFile<BackupState>,
    parts_dir: &Path,
) -> Result<()> {
    let sent = state
        .data
        .sent
        .as_ref()
        .context("the send has not finished")?;
    let mut bad = Vec::new();
    for (index, record) in &sent.parts {
        if state.data.uploaded_parts.contains(index) {
            continue;
        }
        let path = parts_dir.join(part_file_name(index));
        let intact = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len() == record.size_bytes && blake3_file(&path).await? == record.blake3,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(e).with_context(|| format!("failed to inspect {path:?}")),
        };
        if !intact {
            bad.push(index.clone());
        }
    }
    if bad.is_empty() {
        return Ok(());
    }

    if state.data.uploaded_parts.is_empty() {
        state.data.sent = None;
        state.save()?;
        return Err(anyhow!(
            "parts {bad:?} in {parts_dir:?} are missing or damaged; state cleared, re-run \
             to redo the send"
        ));
    }
    Err(anyhow!(
        "parts {bad:?} in {parts_dir:?} are missing or damaged, and others were already \
         uploaded; to start over delete the state file {:?} and {parts_dir:?}, then re-run",
        state.path()
    ))
}

/// Uploads each part not yet recorded as uploaded, recording each as soon as it is done.
pub(super) async fn upload_pending_parts<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    state: &mut StateFile<BackupState>,
    parts_dir: &Path,
    storage_class: StorageClass,
) -> Result<()> {
    let sent = state
        .data
        .sent
        .as_ref()
        .context("the send has not finished")?;
    let pending: Vec<String> = sent
        .parts
        .keys()
        .filter(|index| !state.data.uploaded_parts.contains(*index))
        .cloned()
        .collect();
    let id = state.data.target.id.clone();
    for index in pending {
        remote
            .upload_file(
                &layout.part_key(&id, &index),
                &parts_dir.join(part_file_name(&index)),
                storage_class,
                &level_tag(id.level),
            )
            .await?;
        state.data.uploaded_parts.insert(index);
        state.save()?;
    }
    Ok(())
}

pub(super) async fn build_manifest(
    config: &Config,
    target: &BackupTarget,
    sent: &SentStream,
) -> BackupManifest {
    let parts: Vec<ManifestPart> = sent
        .parts
        .iter()
        .map(|(index, part)| ManifestPart {
            index: index.clone(),
            size_bytes: part.size_bytes,
            blake3: part.blake3.clone(),
        })
        .collect();
    BackupManifest {
        format_version: FORMAT_VERSION.to_string(),
        timestamp: target.id.timestamp.clone(),
        created_at: now_unix(),
        level: target.id.level,
        snapshot_name: target.snapshot_name.clone(),
        parent_snapshot: target.parent_snapshot.clone(),
        source_uuid: target.source_uuid.clone(),
        snapshot_uuid: target.snapshot_uuid.clone(),
        system: SystemInfo {
            os: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            btrfs_version: crate::btrfs::btrfs_version().await,
            bcb_version: env!("CARGO_PKG_VERSION").to_string(),
        },
        age_public_key: config.age_public_key.clone(),
        total_size_bytes: parts.iter().map(|p| p.size_bytes).sum(),
        blake3: sent.blake3.clone(),
        parts,
    }
}

/// Uploads the manifest once every part is confirmed on S3.
///
/// A store can lose a part after acknowledging its upload, e.g. on power loss, and a manifest
/// promises a restorable backup, so every part is HEADed first. A part found missing or of
/// the wrong size is marked for upload again; the parts are kept on disk until the backup
/// completes, so the next run re-uploads just those. A HEAD that fails otherwise (e.g. the
/// network) also holds the manifest back but leaves the state alone, so an outage can't turn
/// into re-uploading every part.
pub(super) async fn publish_manifest<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    state: &mut StateFile<BackupState>,
    manifest: &BackupManifest,
    manifest_class: StorageClass,
) -> Result<()> {
    let issues = check_parts(remote, layout, manifest, None).await?;
    if let Some(first) = issues.first() {
        for issue in &issues {
            state.data.uploaded_parts.remove(&issue.index);
        }
        state.save()?;
        return Err(anyhow!(
            "{} part(s) of this backup are not on S3 as uploaded (first: {}); the manifest \
             was not written, re-run to upload those parts again",
            issues.len(),
            first.description
        ));
    }
    remote
        .upload_bytes(
            &layout.manifest_key(&state.data.target.id),
            serde_yaml::to_string(manifest)?.into_bytes(),
            manifest_class,
            &manifest_tag(),
        )
        .await?;
    state.data.manifest_uploaded = true;
    state.save()
}

/// Records the backup as the newest of its level, and drops every deeper level's entry: those
/// are older than this backup, and an incremental takes the deepest lower entry as its parent,
/// so leaving them would chain it onto a stale parent instead of this backup.
pub(super) async fn publish_latest<R: RemoteBackend>(
    remote: &R,
    layout: &TaskLayout,
    target: &BackupTarget,
    manifest_class: StorageClass,
) -> Result<()> {
    let latest_key = layout.latest_key();
    // A corrupt latest manifest must not be silently reset: dropping the other levels'
    // entries would break later incremental backups without warning.
    let existing = fetch_latest(remote, &latest_key)
        .await
        .map_err(|e| anyhow!("{e:#}; repair or delete it manually"))?;

    // An incremental's target was checked against the latest manifest's source, so a
    // mismatch here is a level 0 backup of a replaced source, which starts a new chain.
    let mut latest = match existing {
        Some(latest) if latest.source_uuid == target.source_uuid => latest,
        Some(latest) => {
            tracing::warn!(
                "latest manifest {latest_key} was for subvolume UUID {}, replacing it with a \
                 new chain for {}",
                latest.source_uuid,
                target.source_uuid
            );
            LatestManifest::new(target.source_uuid.clone())
        }
        None => LatestManifest::new(target.source_uuid.clone()),
    };

    let level = target.id.level;
    latest.levels.retain(|&l, _| l <= level);
    latest.levels.insert(
        level,
        LatestEntry {
            timestamp: target.id.timestamp.clone(),
            snapshot_name: target.snapshot_name.clone(),
            snapshot_uuid: target.snapshot_uuid.clone(),
        },
    );
    remote
        .upload_bytes(
            &latest_key,
            serde_yaml::to_string(&latest)?.into_bytes(),
            manifest_class,
            &manifest_tag(),
        )
        .await
}
