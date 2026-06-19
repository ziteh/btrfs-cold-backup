use age::secrecy::ExposeSecret;
use anyhow::{Result, anyhow};
use btrfs_cold_backup::{BtrfsDriver, Config, S3Backend};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bcb", about = "btrfs cold backup")]
struct Cli {
    #[arg(long, default_value = "/etc/bcb/config.yaml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate an age key pair
    Genkey,

    /// Validate config and connectivity
    Check,

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
        /// Backup date in YYYYMMDD format
        #[arg(long)]
        date: String,
        #[arg(long)]
        target: PathBuf,
        /// Path to the age private key file
        #[arg(long)]
        private_key: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match cli.command {
        Commands::Genkey => {
            let pair = btrfs_cold_backup::keys::generate_key_pair();
            println!("Public key:  {}", pair.public_key);
            // TODO: don't print private key in production, save to file instead
            println!("Private key: {}", pair.private_key.expose_secret());
        }

        Commands::Check => {
            let config = Config::from_file(&cli.config)?;
            let btrfs = BtrfsDriver;
            let remote = S3Backend::new(&config.s3).await?;
            btrfs_cold_backup::check::check(&config, &btrfs, &remote).await?;
            println!("OK");
        }

        Commands::List { task, level } => {
            let config = Config::from_file(&cli.config)?;
            let remote = S3Backend::new(&config.s3).await?;

            let tasks: Vec<_> = config
                .tasks
                .iter()
                .filter(|t| t.enabled)
                .filter(|t| task.as_deref().map_or(true, |name| t.name == name))
                .collect();

            for t in tasks {
                match btrfs_cold_backup::list::list_backups(&config, t, &remote).await {
                    Ok(entries) => {
                        for entry in entries {
                            if level.map_or(true, |l| l == entry.level) {
                                println!("{}\tlevel{}\t{}", t.name, entry.level, entry.date);
                            }
                        }
                    }
                    Err(e) => tracing::warn!("{}: {e}", t.name),
                }
            }
        }

        Commands::Backup { level, task } => {
            let config = Config::from_file(&cli.config)?;
            let btrfs = BtrfsDriver;
            let remote = S3Backend::new(&config.s3).await?;

            let tasks: Vec<_> = config
                .tasks
                .iter()
                .filter(|t| t.enabled)
                .filter(|t| task.as_deref().map_or(true, |name| t.name == name))
                .collect();

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
            let btrfs = BtrfsDriver;
            let remote = S3Backend::new(&config.s3).await?;

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
                &btrfs,
                &remote,
            )
            .await?;

            println!("restore complete");
        }
    }

    Ok(())
}
