use age::secrecy::ExposeSecret;
use anyhow::{Context, Result};
use btrfs_cold_backup::{
    btrfs::BtrfsDriver,
    check,
    commands::{cmd_backup, cmd_check, cmd_list, cmd_prune_snapshots, cmd_restore, cmd_verify},
    config::Config,
    crypto,
    fsutil::prepare_base_dir,
    lock::InstanceLock,
    remote::{RetrievalTier, S3Backend},
    restore::{RestoreRequest, RetrievalOptions},
    timestamp::Timestamp,
};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "bcb", about = "btrfs cold backup", version)]
struct Cli {
    #[arg(long, default_value = "/etc/bcb/config.yaml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate an age key pair; writes the private key to a file (mode 0600).
    Genkey {
        /// Output file for the age private key.
        #[arg(long)]
        output: PathBuf,
    },

    /// Validate config and connectivity.
    Check,

    /// Verify a private key matches the configured public key.
    TestKeys {
        /// Path to the age private key file.
        #[arg(long)]
        private_key: PathBuf,
    },

    /// List backups stored on S3.
    List {
        /// Limit to a single task by name (default: all enabled tasks).
        #[arg(long)]
        task: Option<String>,
        /// Filter by level.
        #[arg(long)]
        level: Option<u32>,
    },

    /// Audit backups on S3: chain completeness, part existence/size, storage class, and
    /// (optionally) freshness. Read-only and never triggers an archive retrieval.
    Verify {
        /// Limit to a single task by name (default: all enabled tasks).
        #[arg(long)]
        task: Option<String>,
        /// Flag the task if its newest backup is older than this many days.
        #[arg(long)]
        max_age_days: Option<u64>,
    },

    /// Delete the local snapshots no backup needs any more.
    PruneSnapshots {
        /// Limit to a single task by name (default: all enabled tasks).
        #[arg(long)]
        task: Option<String>,
        /// List what would be deleted without deleting it.
        #[arg(long)]
        dry_run: bool,
    },

    /// Run a backup.
    Backup {
        /// Backup level: 0 is full, N is incremental relative to the newest lower-level backup.
        #[arg(long)]
        level: u32,
        /// Limit to a single task by name (default: all enabled tasks).
        #[arg(long)]
        task: Option<String>,
        /// Upload an incremental the oversize guard held back, once the change it holds is
        /// confirmed legitimate.
        #[arg(long, requires = "task")]
        allow_oversize: bool,
    },

    /// Restore a backup.
    Restore {
        #[arg(long)]
        task: String,
        #[arg(long)]
        level: u32,
        /// Backup timestamp: YYYYMMDDHHMM (e.g. 202606021430) or YYYYMMDD for T00:00.
        #[arg(long)]
        date: String,
        #[arg(long)]
        target: PathBuf,
        /// Path to the age private key file.
        #[arg(long)]
        private_key: PathBuf,
        /// Days the retrieved copy of archived objects stays downloadable.
        #[arg(long, default_value_t = 3)]
        restore_days: i32,
        /// Archive retrieval tier.
        #[arg(long, value_enum, default_value_t = RetrievalTier::Standard)]
        restore_tier: RetrievalTier,
    },
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();
    // Genkey needs no config file, so handle it before the config is loaded.
    if let Commands::Genkey { output } = &cli.command {
        init_logging(None);
        cmd_genkey(output)?;
        return Ok(ExitCode::SUCCESS);
    }

    let config = Config::from_file(&cli.config)?;

    // Unattended commands log to a file as well as stderr, so a run leaves a trail.
    {
        let file_log_dir = matches!(
            cli.command,
            Commands::Backup { .. } | Commands::PruneSnapshots { .. } | Commands::Restore { .. }
        )
        .then(|| config.base_dir.join("log"));

        init_logging(file_log_dir);
    }

    // Needs no S3 or btrfs, so it runs before the S3 client is built.
    if let Commands::TestKeys { private_key } = &cli.command {
        cmd_test_keys(&config, private_key)?;
        return Ok(ExitCode::SUCCESS);
    }

    let remote = S3Backend::new(&config.s3).await?;
    let btrfs = BtrfsDriver;

    match cli.command {
        Commands::Genkey { .. } => unreachable!("handled above"),
        Commands::Check => {
            check::check_binaries()?;
            cmd_check(&config, &btrfs, &remote).await?;
        }
        Commands::TestKeys { .. } => unreachable!("handled above"),
        Commands::List { task, level } => {
            cmd_list(&config, &remote, task.as_deref(), level).await?
        }
        Commands::Verify { task, max_age_days } => {
            cmd_verify(&config, &remote, task.as_deref(), max_age_days).await?
        }
        Commands::Backup {
            level,
            task,
            allow_oversize,
        } => {
            let _lock = acquire_runtime(&config)?;
            return cmd_backup(
                &config,
                &btrfs,
                &remote,
                level,
                task.as_deref(),
                allow_oversize,
            )
            .await;
        }
        Commands::PruneSnapshots { task, dry_run } => {
            let _lock = acquire_runtime(&config)?;
            cmd_prune_snapshots(&config, &btrfs, &remote, task.as_deref(), dry_run).await?
        }
        Commands::Restore {
            task,
            level,
            date,
            target,
            private_key,
            restore_days,
            restore_tier,
        } => {
            let request = RestoreRequest {
                level,
                timestamp: Timestamp::from_cli_date(&date)?,
                target_dir: target,
                identity: read_private_key(&private_key)?,
                retrieval: RetrievalOptions {
                    days: restore_days,
                    tier: restore_tier,
                },
            };
            let _lock = acquire_runtime(&config)?;
            return cmd_restore(&config, &btrfs, &remote, &task, &request).await;
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_genkey(output: &Path) -> Result<()> {
    let pair = crypto::generate_key_pair();
    crypto::write_private_key(output, pair.private_key.expose_secret())?;
    println!("{}", pair.public_key);
    Ok(())
}

fn cmd_test_keys(config: &Config, private_key: &Path) -> Result<()> {
    crypto::ensure_key_pair(&config.age_public_key, &read_private_key(private_key)?)?;
    println!("OK: private key matches the configured public key");
    Ok(())
}

fn read_private_key(path: &Path) -> Result<age::x25519::Identity> {
    let key = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read private key {path:?}"))?;
    crypto::parse_identity(key.trim())
}

/// What backup, prune and restore need before touching anything: the external executables, a
/// prepared base_dir, and the lock that keeps other bcb instances out.
fn acquire_runtime(config: &Config) -> Result<InstanceLock> {
    check::check_binaries()?;
    prepare_base_dir(&config.base_dir)?;
    InstanceLock::acquire(&config.base_dir.join("bcb.lock"))
}

/// Stderr obeys RUST_LOG. With `file_log_dir`, INFO and above also go to a daily rolling file
/// there.
fn init_logging(file_log_dir: Option<PathBuf>) {
    use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    let file_layer = file_log_dir.and_then(|dir| match file_appender(&dir) {
        Ok(appender) => Some(
            tracing_subscriber::fmt::layer()
                .with_writer(appender)
                .with_ansi(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        ),
        Err(e) => {
            eprintln!("file logging disabled: {e:#}");
            None
        }
    });

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}

/// Daily log files in `dir`, capped so the directory doesn't grow forever under unattended
/// runs. The cap keeps logs well past the longest gap between the scheduled backups the
/// README describes.
fn file_appender(dir: &Path) -> Result<tracing_appender::rolling::RollingFileAppender> {
    const MAX_LOG_FILES: usize = 30;
    std::fs::create_dir_all(dir).with_context(|| format!("cannot create log directory {dir:?}"))?;
    tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("bcb.log")
        .max_log_files(MAX_LOG_FILES)
        .build(dir)
        .with_context(|| format!("cannot create log appender in {dir:?}"))
}
