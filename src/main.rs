use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, anyhow};
use btrfs_cold_backup::{
    backup::backup_task,
    btrfs::BtrfsDriver,
    check,
    config::{Config, Task},
    crypto,
    fsutil::prepare_base_dir,
    layout::TaskLayout,
    list::list_backups,
    lock::InstanceLock,
    prune::prune_task,
    remote::{RetrievalTier, S3Backend},
    restore::{RestoreRequest, RetrievalOptions, restore_task},
    timestamp::Timestamp,
    verify::verify_task,
};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

// Exit code of a restore that made progress but isn't done.
const EXIT_RESTORE_IN_PROGRESS: i32 = 2;

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
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Genkey needs no config file, so handle it before the config is loaded.
    if let Commands::Genkey { output } = &cli.command {
        init_logging(None);
        return cmd_genkey(output);
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

    match cli.command {
        Commands::Genkey { .. } => unreachable!("handled above"),
        Commands::Check => cmd_check(&config).await,
        Commands::TestKeys { private_key } => cmd_test_keys(&config, &private_key),
        Commands::List { task, level } => cmd_list(&config, task.as_deref(), level).await,
        Commands::Verify { task, max_age_days } => {
            cmd_verify(&config, task.as_deref(), max_age_days).await
        }
        Commands::Backup { level, task } => cmd_backup(&config, level, task.as_deref()).await,
        Commands::PruneSnapshots { task, dry_run } => {
            cmd_prune_snapshots(&config, task.as_deref(), dry_run).await
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
            cmd_restore(&config, &task, &request).await
        }
    }
}

fn cmd_genkey(output: &Path) -> Result<()> {
    let pair = crypto::generate_key_pair();
    crypto::write_private_key(output, pair.private_key.expose_secret())?;
    println!("{}", pair.public_key);
    Ok(())
}

async fn cmd_check(config: &Config) -> Result<()> {
    check::check_binaries()?;
    let remote = S3Backend::new(&config.s3).await?;
    check::check(config, &BtrfsDriver, &remote).await?;
    println!("OK");
    Ok(())
}

fn cmd_test_keys(config: &Config, private_key: &Path) -> Result<()> {
    crypto::ensure_key_pair(&config.age_public_key, &read_private_key(private_key)?)?;
    println!("OK: private key matches the configured public key");
    Ok(())
}

async fn cmd_list(config: &Config, task: Option<&str>, level: Option<u32>) -> Result<()> {
    let remote = S3Backend::new(&config.s3).await?;
    let mut failed = Vec::new();
    for t in enabled_tasks(&config.tasks, task) {
        let ids = match TaskLayout::new(config, t) {
            Ok(layout) => list_backups(&remote, &layout).await,
            Err(e) => Err(e),
        };
        match ids {
            Ok(ids) => {
                let shown: Vec<_> = ids
                    .into_iter()
                    .filter(|id| level.is_none_or(|l| l == id.level))
                    .collect();
                if shown.is_empty() {
                    println!("{}: no backups found", t.name);
                }
                for id in shown {
                    println!("{}\tlevel{}\t{}", t.name, id.level, id.timestamp);
                }
            }
            Err(e) => {
                eprintln!("{}: list failed: {e:#}", t.name);
                failed.push(t.name.clone());
            }
        }
    }
    aggregate_failures("list", &failed)
}

async fn cmd_verify(config: &Config, task: Option<&str>, max_age_days: Option<u64>) -> Result<()> {
    let remote = S3Backend::new(&config.s3).await?;
    let mut failed = Vec::new();
    for t in enabled_tasks(&config.tasks, task) {
        match verify_task(config, t, &remote, max_age_days).await {
            Ok(issues) if issues.is_empty() => println!("{}: OK", t.name),
            Ok(issues) => {
                for issue in &issues {
                    match issue.level {
                        Some(level) => {
                            println!("{}: level {level}: {}", t.name, issue.description)
                        }
                        None => println!("{}: {}", t.name, issue.description),
                    }
                }
                failed.push(t.name.clone());
            }
            Err(e) => {
                eprintln!("{}: verify failed: {e:#}", t.name);
                failed.push(t.name.clone());
            }
        }
    }
    aggregate_failures("verify", &failed)
}

async fn cmd_backup(config: &Config, level: u32, task: Option<&str>) -> Result<()> {
    let (remote, _lock) = acquire_runtime(config).await?;
    let tasks = enabled_tasks(&config.tasks, task);
    if tasks.is_empty() {
        return Err(anyhow!("no matching enabled tasks found"));
    }

    // Outcomes are logged too, so the log file records them.
    let mut failed = Vec::new();
    for t in tasks {
        tracing::info!(task = %t.name, level, "backup started");
        match backup_task(config, t, level, &BtrfsDriver, &remote).await {
            Ok(Some(backup)) => {
                tracing::info!(task = %t.name, level, timestamp = %backup.timestamp, "backup completed");
                println!(
                    "{}: level {level} backup completed ({})",
                    t.name, backup.timestamp
                );
            }
            Ok(None) => {
                tracing::info!(task = %t.name, level, "already backed up, skipped");
                println!("{}: level {level} already backed up, skipped", t.name);
            }
            Err(e) => {
                tracing::error!(task = %t.name, level, error = format!("{e:#}"), "backup failed");
                eprintln!("{}: level {level} backup failed: {e:#}", t.name);
                failed.push(t.name.clone());
            }
        }
    }
    aggregate_failures("backup", &failed)
}

async fn cmd_prune_snapshots(config: &Config, task: Option<&str>, dry_run: bool) -> Result<()> {
    let (remote, _lock) = acquire_runtime(config).await?;
    let tasks = enabled_tasks(&config.tasks, task);
    if tasks.is_empty() {
        return Err(anyhow!("no matching enabled tasks found"));
    }

    let verb = if dry_run { "would delete" } else { "deleted" };
    let mut failed = Vec::new();
    for t in tasks {
        match prune_task(config, t, &BtrfsDriver, &remote, dry_run).await {
            Ok(pruned) => {
                for name in pruned {
                    println!("{}: {verb} {name}", t.name);
                }
            }
            Err(e) => {
                tracing::error!(task = %t.name, error = format!("{e:#}"), "prune failed");
                eprintln!("{}: prune failed: {e:#}", t.name);
                failed.push(t.name.clone());
            }
        }
    }
    aggregate_failures("prune-snapshots", &failed)
}

async fn cmd_restore(config: &Config, task: &str, request: &RestoreRequest) -> Result<()> {
    let (remote, _lock) = acquire_runtime(config).await?;
    let t = config
        .tasks
        .iter()
        .find(|t| t.name == task)
        .ok_or_else(|| anyhow!("task '{task}' not found in config"))?;
    let level = request.level;

    tracing::info!(task = %t.name, level, timestamp = %request.timestamp, "restore started");
    let progress = match restore_task(config, t, request, &BtrfsDriver, &remote).await {
        Ok(progress) => progress,
        Err(e) => {
            tracing::error!(task = %t.name, level, error = format!("{e:#}"), "restore failed");
            return Err(e);
        }
    };
    if progress.completed {
        tracing::info!(task = %t.name, level, "restore completed");
        println!("restore complete");
        return Ok(());
    }
    tracing::info!(
        task = %t.name,
        level,
        parts_downloaded = progress.parts_downloaded,
        parts_total = progress.parts_total,
        bytes_remaining = progress.bytes_remaining,
        "restore in progress"
    );
    println!(
        "restore in progress: {}/{} parts downloaded, {} bytes remaining; re-run once further \
         retrievals complete",
        progress.parts_downloaded, progress.parts_total, progress.bytes_remaining
    );
    std::process::exit(EXIT_RESTORE_IN_PROGRESS);
}

fn read_private_key(path: &Path) -> Result<age::x25519::Identity> {
    let key = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read private key {path:?}"))?;
    crypto::parse_identity(key.trim())
}

fn enabled_tasks<'a>(tasks: &'a [Task], name: Option<&str>) -> Vec<&'a Task> {
    tasks
        .iter()
        .filter(|t| t.enabled)
        .filter(|t| name.is_none_or(|n| t.name == n))
        .collect()
}

/// The aggregate error of a command that ran every selected task, or Ok if none failed.
/// List, verify, prune and backup run every task to completion rather than stopping at the first
/// failure: one failing task must never keep the others from being attempted.
fn aggregate_failures(verb: &str, failed: &[String]) -> Result<()> {
    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{verb} failed for task(s): {}", failed.join(", ")))
    }
}

/// What backup, prune and restore need before touching anything: the external executables, a
/// prepared base_dir, and the lock that keeps other bcb instances out.
async fn acquire_runtime(config: &Config) -> Result<(S3Backend, InstanceLock)> {
    check::check_binaries()?;
    prepare_base_dir(&config.base_dir)?;
    let lock = InstanceLock::acquire(&config.base_dir.join("bcb.lock"))?;
    let remote = S3Backend::new(&config.s3).await?;
    Ok((remote, lock))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn task(name: &str, enabled: bool) -> Task {
        Task {
            name: name.to_string(),
            description: String::new(),
            subvolume: PathBuf::from(format!("/mnt/{name}")),
            snapshot_dir: PathBuf::from(format!("/mnt/.snapshots/{name}")),
            enabled,
        }
    }

    #[test]
    fn test_enabled_tasks_no_filter_returns_all_enabled() {
        let tasks = vec![task("a", true), task("b", true), task("c", false)];
        let selected = enabled_tasks(&tasks, None);
        assert_eq!(
            selected.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn test_enabled_tasks_by_name_ignores_disabled() {
        let tasks = vec![task("a", true), task("b", false)];
        assert!(enabled_tasks(&tasks, Some("b")).is_empty());
        assert_eq!(enabled_tasks(&tasks, Some("a")).len(), 1);
    }

    #[test]
    fn test_enabled_tasks_unknown_name_is_empty() {
        let tasks = vec![task("a", true)];
        assert!(enabled_tasks(&tasks, Some("nonexistent")).is_empty());
    }

    #[test]
    fn test_aggregate_failures_empty_is_ok() {
        assert!(aggregate_failures("backup", &[]).is_ok());
    }

    #[test]
    fn test_aggregate_failures_names_every_failed_task() {
        let failed = vec!["a".to_string(), "b".to_string()];
        let err = aggregate_failures("backup", &failed).unwrap_err();
        assert_eq!(err.to_string(), "backup failed for task(s): a, b");
    }
}
