//! bcb backs up btrfs snapshots to S3 cold storage, encrypted with age. This library is the
//! `bcb` binary's own, not a public API.
//!
//! Terms used throughout:
//!
//! - **task**: one subvolume to back up, with the directory btrbk puts its snapshots in.
//! - **level**: 0 is a full backup; level N is an incremental relative to the newest backup
//!   of any lower level, as in Unix `dump`. Restoring it needs its chain of parents restored
//!   first.
//! - **backup id**: a backup's level and its snapshot's timestamp ([`timestamp::BackupId`]).
//! - **part**: one encrypted piece of a backup's send stream, uploaded as one S3 object.
//! - **multipart chunk**: the unit a part is uploaded in.
//! - **range chunk**: the unit a part is downloaded in, and the most a crash can cost.
//! - **manifest**: describes one backup and how to verify it; the **latest manifest** points
//!   at each level's newest backup, which the next incremental builds on.
//! - **retrieval**: S3's restore of an archived object to a temporary downloadable copy, not
//!   to be confused with `bcb restore`.
//! - **base_dir**: bcb's local working directory: lock, state files, logs and parts in
//!   transit.

pub mod backup;
pub mod btrfs;
pub mod check;
pub mod config;
pub mod crypto;
pub mod fsutil;
pub mod layout;
pub mod list;
pub mod lock;
pub mod manifest;
pub mod remote;
pub mod restore;
pub mod state;
mod test_hooks;
pub mod timestamp;
pub mod util;
pub mod verify;
