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

// This is the binary's own library, not a public API: a name belongs here only if it is used
// across modules or by main/tests. Manifest types are reached through `manifest::` instead,
// since that module boundary is what marks them as the format stored on S3.
pub use btrfs::{BtrfsDriver, BtrfsOps, SubvolumeUuids};
pub use config::{Config, RestoreConfig, S3Config, S3RetryConfig, S3StorageClass, Task};
pub use crypto::{
    blake3_file, decrypt_file, encrypt_file, generate_key_pair, test_key_pair, write_private_key,
};
pub use lock::PidLock;
pub use remote::{ObjectMetadata, RemoteBackend, RestoreStatus, RestoreStatusDetail, S3Backend};
pub use restore::RetrievalOptions;
pub use state::{BackupState, PartDownloadState, PartRecord, RestorePartState, RestoreState};
pub use util::{sanitize_subvolume_path, state_path};
