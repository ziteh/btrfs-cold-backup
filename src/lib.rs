//! bcb backs up btrfs snapshots to S3 cold storage, encrypted with age.
//!
//! This library is the `bcb` binary's own, not a public API.

pub mod backup;
pub mod btrfs;
pub mod check;
pub mod config;
pub mod crypto;
pub mod list;
pub mod lock;
pub mod manifest;
pub mod remote;
pub mod restore;
pub mod state;
pub mod util;
pub mod verify;
