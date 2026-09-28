use age::secrecy::ExposeSecret;
use anyhow::{Result, anyhow};
use btrfs_cold_backup::{BtrfsDriver, Config, PidLock, S3Backend, Task, write_private_key};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    /// Generate an age key pair; writes the private key to a file (mode 0600)
    Genkey {
        /// Output file for the age private key
        #[arg(long)]
        output: PathBuf,
    },

    /// Validate config and connectivity
    Check,

    /// Verify an age key pair can encrypt and decrypt together
    TestKeys {
        /// Path to the age private key file
        #[arg(long)]
        private_key: PathBuf,
    },

    /// List backups stored on S3
    List {
        /// Limit to a single task by name (default: all enabled tasks)
        #[arg(long)]
        task: Option<String>,
        /// Filter by level
        #[arg(long)]
        level: Option<u32>,
    },

    /// Audit backups on S3: chain completeness, part existence/size, storage class, and
    /// (optionally) freshness. Read-only and never triggers an archive retrieval.
    Verify {
        /// Limit to a single task by name (default: all enabled tasks)
        #[arg(long)]
        task: Option<String>,
        /// Flag the task if its newest backup is older than this many days
        #[arg(long)]
        max_age_days: Option<u64>,
    },

    /// Run a backup
    Backup {
        #[arg(long)]
        level: u32,
        /// Limit to a single task by name (default: all enabled tasks)
        #[arg(long)]
        task: Option<String>,
    },

    /// Restore a backup
    Restore {
        #[arg(long)]
        task: String,
        #[arg(long)]
        level: u32,
        /// Backup timestamp: YYYYMMDDHHMM (e.g. 202606021430) or YYYYMMDD for T00:00
        #[arg(long)]
        date: String,
        #[arg(long)]
        target: PathBuf,
        /// Path to the age private key file
        #[arg(long)]
        private_key: PathBuf,
        /// Days the retrieved copy of archived objects stays downloadable
        #[arg(long, default_value_t = 3)]
        restore_days: i32,
        /// Archive retrieval tier: Standard, Bulk, or Expedited
        #[arg(long, default_value = "Standard")]
        restore_tier: String,
    },
}

fn select_tasks<'a>(tasks: &'a [Task], name: Option<&str>) -> Vec<&'a Task> {
    tasks
        .iter()
        .filter(|t| t.enabled)
        .filter(|t| name.is_none_or(|n| t.name == n))
        .collect()
}

/// Turns the list of task names a command failed for into its aggregate error, or `Ok(())`
/// if none did. Shared by List/Verify/Backup, which each run every selected task to
/// completion and collect failures here rather than stopping at the first one — one failing
/// task must never prevent the others from being attempted.
fn aggregate_failures(verb: &str, failed: &[String]) -> Result<()> {
    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{verb} failed for task(s): {}", failed.join(", ")))
    }
}

async fn acquire_runtime(config: &Config) -> Result<(BtrfsDriver, S3Backend, PidLock)> {
    // Up front, so a missing executable fails the run before any work starts.
    btrfs_cold_backup::check::check_binaries()?;

    btrfs_cold_backup::util::prepare_base_dir(&config.base_dir)
        .map_err(|e| anyhow!("failed to prepare base_dir: {e}"))?;

    let lock_path = config.base_dir.join("bcb.lock");
    let lock = PidLock::acquire(&lock_path)?;
    let btrfs = BtrfsDriver;
    let remote = S3Backend::new(&config.s3).await?;
    Ok((btrfs, remote, lock))
}

// Stderr obeys RUST_LOG as before; backup/restore additionally append INFO+ to a daily
// rolling file under base_dir so unattended cron runs leave a trail to debug from.
fn init_logging(file_log_dir: Option<std::path::PathBuf>) {
    use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    let file_layer = file_log_dir.and_then(|dir| {
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("cannot create log directory {dir:?}: {e}; file logging disabled");
            return None;
        }
        // Capped so unattended cron runs don't let the log directory grow forever; 30 daily
        // files covers a month, comfortably past the longest gap between scheduled backups
        // described in the README (level 0 twice a year, level 1 weekly).
        let appender = match tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("bcb.log")
            .max_log_files(30)
            .build(&dir)
        {
            Ok(appender) => appender,
            Err(e) => {
                eprintln!("cannot create log appender in {dir:?}: {e}; file logging disabled");
                return None;
            }
        };
        Some(
            tracing_subscriber::fmt::layer()
                .with_writer(appender)
                .with_ansi(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
    });

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let file_log_dir = match &cli.command {
        // Errors loading the config are swallowed here on purpose; the command arm reloads
        // it and reports them properly.
        Commands::Backup { .. } | Commands::Restore { .. } => Config::from_file(&cli.config)
            .ok()
            .map(|c| c.base_dir.join("log")),
        _ => None,
    };
    init_logging(file_log_dir);

    match cli.command {
        Commands::Genkey { output } => {
            let pair = btrfs_cold_backup::generate_key_pair();
            write_private_key(&output, pair.private_key.expose_secret())?;
            println!("{}", pair.public_key);
        }

        Commands::Check => {
            let config = Config::from_file(&cli.config)?;
            btrfs_cold_backup::check::check_binaries()?;
            let btrfs = BtrfsDriver;
            let remote = S3Backend::new(&config.s3).await?;
            btrfs_cold_backup::check::check(&config, &btrfs, &remote).await?;
            println!("OK");
        }

        Commands::TestKeys { private_key } => {
            let config = Config::from_file(&cli.config)?;
            let key_str = std::fs::read_to_string(&private_key)
                .map_err(|e| anyhow!("failed to read private key {:?}: {e}", private_key))?;

            btrfs_cold_backup::test_key_pair(&config.age_public_key, key_str.trim()).await?;
            println!("OK: private key matches the configured public key");
        }

        Commands::List { task, level } => {
            let config = Config::from_file(&cli.config)?;
            let remote = S3Backend::new(&config.s3).await?;

            let mut failed: Vec<String> = Vec::new();
            for t in select_tasks(&config.tasks, task.as_deref()) {
                match btrfs_cold_backup::list::list_backups(&config, t, &remote).await {
                    Ok(entries) => {
                        let mut shown = 0usize;
                        for entry in entries {
                            if level.is_none_or(|l| l == entry.level) {
                                println!("{}\tlevel{}\t{}", t.name, entry.level, entry.timestamp);
                                shown += 1;
                            }
                        }
                        if shown == 0 {
                            println!("{}: no backups found", t.name);
                        }
                    }
                    Err(e) => {
                        eprintln!("{}: list failed: {e:#}", t.name);
                        failed.push(t.name.clone());
                    }
                }
            }
            aggregate_failures("list", &failed)?;
        }

        Commands::Verify { task, max_age_days } => {
            let config = Config::from_file(&cli.config)?;
            let remote = S3Backend::new(&config.s3).await?;

            let mut failed: Vec<String> = Vec::new();
            for t in select_tasks(&config.tasks, task.as_deref()) {
                match btrfs_cold_backup::verify::verify_task(&config, t, &remote, max_age_days)
                    .await
                {
                    Ok(issues) if issues.is_empty() => {
                        println!("{}: OK", t.name);
                    }
                    Ok(issues) => {
                        for issue in &issues {
                            println!("{}: level {}: {}", t.name, issue.level, issue.description);
                        }
                        failed.push(t.name.clone());
                    }
                    Err(e) => {
                        eprintln!("{}: verify failed: {e:#}", t.name);
                        failed.push(t.name.clone());
                    }
                }
            }
            aggregate_failures("verify", &failed)?;
        }

        Commands::Backup { level, task } => {
            let config = Config::from_file(&cli.config)?;
            let (btrfs, remote, _lock) = acquire_runtime(&config).await?;

            let tasks = select_tasks(&config.tasks, task.as_deref());
            if tasks.is_empty() {
                return Err(anyhow!("no matching enabled tasks found"));
            }

            // One failing task must not block the others; run them all and report at the end.
            // Outcomes go to tracing as well so the daily log file records them.
            let mut failed: Vec<String> = Vec::new();
            for t in tasks {
                tracing::info!(task = %t.name, level, "backup started");
                match btrfs_cold_backup::backup::backup_task(&config, t, level, &btrfs, &remote)
                    .await
                {
                    Ok(Some(backup)) => {
                        tracing::info!(task = %t.name, level, timestamp = %backup.timestamp, "backup completed");
                        println!(
                            "{}: level {} backup completed ({})",
                            t.name, backup.level, backup.timestamp
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
            aggregate_failures("backup", &failed)?;
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
            let config = Config::from_file(&cli.config)?;
            let (_btrfs, remote, _lock) = acquire_runtime(&config).await?;

            let t = config
                .tasks
                .iter()
                .find(|t| t.name == task)
                .ok_or_else(|| anyhow!("task '{task}' not found in config"))?;

            let key_str = std::fs::read_to_string(&private_key)
                .map_err(|e| anyhow!("failed to read private key {:?}: {e}", private_key))?;

            let retrieval = btrfs_cold_backup::restore::RetrievalOptions {
                days: restore_days,
                tier: restore_tier,
            };

            tracing::info!(task = %t.name, level, date, "restore started");
            let result = btrfs_cold_backup::restore::restore_task(
                &config,
                t,
                level,
                &date,
                &target,
                key_str.trim(),
                &retrieval,
                &remote,
            )
            .await;
            match &result {
                Ok(p) if p.completed => {
                    tracing::info!(task = %t.name, level, "restore completed")
                }
                Ok(p) => {
                    tracing::info!(
                        task = %t.name,
                        level,
                        parts_downloaded = p.parts_downloaded,
                        parts_total = p.parts_total,
                        bytes_remaining = p.bytes_remaining,
                        "restore in progress"
                    )
                }
                Err(e) => {
                    tracing::error!(task = %t.name, level, error = format!("{e:#}"), "restore failed")
                }
            }
            let progress = result?;

            if progress.completed {
                println!("restore complete");
            } else {
                println!(
                    "restore in progress: {}/{} parts downloaded, {} bytes remaining; re-run once further retrievals complete",
                    progress.parts_downloaded, progress.parts_total, progress.bytes_remaining
                );
                // Distinct exit code so schedulers can tell "not done yet" from success (0)
                // and failure (1).
                std::process::exit(2);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
    fn test_select_tasks_no_filter_returns_all_enabled() {
        let tasks = vec![task("a", true), task("b", true), task("c", false)];
        let selected = select_tasks(&tasks, None);
        assert_eq!(
            selected.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn test_select_tasks_by_name_ignores_disabled() {
        let tasks = vec![task("a", true), task("b", false)];
        assert!(select_tasks(&tasks, Some("b")).is_empty());
        assert_eq!(select_tasks(&tasks, Some("a")).len(), 1);
    }

    #[test]
    fn test_select_tasks_unknown_name_is_empty() {
        let tasks = vec![task("a", true)];
        assert!(select_tasks(&tasks, Some("nonexistent")).is_empty());
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
