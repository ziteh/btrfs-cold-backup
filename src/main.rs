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
    },
}

fn select_tasks<'a>(tasks: &'a [Task], name: Option<&str>) -> Vec<&'a Task> {
    tasks
        .iter()
        .filter(|t| t.enabled)
        .filter(|t| name.is_none_or(|n| t.name == n))
        .collect()
}

async fn acquire_runtime(config: &Config) -> Result<(BtrfsDriver, S3Backend, PidLock)> {
    std::fs::create_dir_all(&config.base_dir)
        .map_err(|e| anyhow!("failed to create base_dir: {e}"))?;

    let lock_path = config.base_dir.join("bcb.lock");
    PidLock::remove_stale(&lock_path)?;

    let lock = PidLock::acquire(&lock_path)?;
    let btrfs = BtrfsDriver;
    let remote = S3Backend::new(&config.s3).await?;
    Ok((btrfs, remote, lock))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match cli.command {
        Commands::Genkey { output } => {
            let pair = btrfs_cold_backup::generate_key_pair();
            write_private_key(&output, pair.private_key.expose_secret())?;
            println!("{}", pair.public_key);
        }

        Commands::Check => {
            let config = Config::from_file(&cli.config)?;
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

            for t in select_tasks(&config.tasks, task.as_deref()) {
                match btrfs_cold_backup::list::list_backups(&config, t, &remote).await {
                    Ok(entries) => {
                        for entry in entries {
                            if level.is_none_or(|l| l == entry.level) {
                                println!("{}\tlevel{}\t{}", t.name, entry.level, entry.timestamp);
                            }
                        }
                    }
                    Err(e) => tracing::warn!("{}: {e}", t.name),
                }
            }
        }

        Commands::Backup { level, task } => {
            let config = Config::from_file(&cli.config)?;
            let (btrfs, remote, _lock) = acquire_runtime(&config).await?;

            let tasks = select_tasks(&config.tasks, task.as_deref());
            if tasks.is_empty() {
                return Err(anyhow!("no matching enabled tasks found"));
            }

            for t in tasks {
                let backup =
                    btrfs_cold_backup::backup::backup_task(&config, t, level, &btrfs, &remote)
                        .await?;
                println!(
                    "{}: level {} backup completed ({})",
                    t.name, backup.level, backup.timestamp
                );
            }
        }

        Commands::Restore {
            task,
            level,
            date,
            target,
            private_key,
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

            btrfs_cold_backup::restore::restore_task(
                &config,
                t,
                level,
                &date,
                &target,
                key_str.trim(),
                &remote,
            )
            .await?;

            println!("restore complete");
        }
    }

    Ok(())
}
