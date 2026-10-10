//! The bodies of the `bcb` subcommands that touch btrfs and S3. They take both as parameters
//! so tests can drive them with mocks; `main` supplies the real ones.

use anyhow::{Result, anyhow};
use std::process::ExitCode;

use crate::{
    backup::{Oversize, backup_task},
    btrfs::BtrfsOps,
    check,
    config::{Config, Task},
    layout::TaskLayout,
    list::list_backups,
    prune::prune_task,
    remote::RemoteBackend,
    restore::{RestoreRequest, restore_task},
    verify::verify_task,
};

// Exit code of a restore that made progress but isn't done.
pub const EXIT_RESTORE_IN_PROGRESS: u8 = 2;
// Exit code of a backup run whose only failures are incrementals the oversize guard held back.
pub const EXIT_BACKUP_OVERSIZE: u8 = 3;

pub async fn cmd_check<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
) -> Result<()> {
    check::check(config, btrfs, remote).await?;
    println!("OK");
    Ok(())
}

pub async fn cmd_list<R: RemoteBackend>(
    config: &Config,
    remote: &R,
    task: Option<&str>,
    level: Option<u32>,
) -> Result<()> {
    let mut failed = Vec::new();
    for t in enabled_tasks(&config.tasks, task) {
        let ids = match TaskLayout::new(config, t) {
            Ok(layout) => list_backups(remote, &layout).await,
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

pub async fn cmd_verify<R: RemoteBackend>(
    config: &Config,
    remote: &R,
    task: Option<&str>,
    max_age_days: Option<u64>,
) -> Result<()> {
    let mut failed = Vec::new();
    for t in enabled_tasks(&config.tasks, task) {
        match verify_task(config, t, remote, max_age_days).await {
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

pub async fn cmd_backup<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
    level: u32,
    task: Option<&str>,
    allow_oversize: bool,
) -> Result<ExitCode> {
    let tasks = enabled_tasks(&config.tasks, task);
    if tasks.is_empty() {
        return Err(anyhow!("no matching enabled tasks found"));
    }

    // Outcomes are logged too, so the log file records them.
    let mut failed = Vec::new();
    let mut held = Vec::new();
    for t in tasks {
        tracing::info!(task = %t.name, level, "backup started");
        match backup_task(config, t, level, btrfs, remote, allow_oversize).await {
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
            Err(e) if e.is::<Oversize>() => {
                tracing::error!(task = %t.name, level, error = format!("{e:#}"), "backup held back");
                eprintln!("{}: level {level} backup held back: {e:#}", t.name);
                held.push(t.name.clone());
            }
            Err(e) => {
                tracing::error!(task = %t.name, level, error = format!("{e:#}"), "backup failed");
                eprintln!("{}: level {level} backup failed: {e:#}", t.name);
                failed.push(t.name.clone());
            }
        }
    }
    // Other failures take precedence: they need attention whatever the guard held back.
    aggregate_failures("backup", &failed)?;
    if !held.is_empty() {
        eprintln!(
            "backup held back by the oversize guard for task(s): {}",
            held.join(", ")
        );
        return Ok(ExitCode::from(EXIT_BACKUP_OVERSIZE));
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn cmd_prune_snapshots<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
    task: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    let tasks = enabled_tasks(&config.tasks, task);
    if tasks.is_empty() {
        return Err(anyhow!("no matching enabled tasks found"));
    }

    let verb = if dry_run { "would delete" } else { "deleted" };
    let mut failed = Vec::new();
    for t in tasks {
        match prune_task(config, t, btrfs, remote, dry_run).await {
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

pub async fn cmd_restore<B: BtrfsOps, R: RemoteBackend>(
    config: &Config,
    btrfs: &B,
    remote: &R,
    task: &str,
    request: &RestoreRequest,
) -> Result<ExitCode> {
    let t = config
        .tasks
        .iter()
        .find(|t| t.name == task)
        .ok_or_else(|| anyhow!("task '{task}' not found in config"))?;
    let level = request.level;

    tracing::info!(task = %t.name, level, timestamp = %request.timestamp, "restore started");
    let progress = match restore_task(config, t, request, btrfs, remote).await {
        Ok(progress) => progress,
        Err(e) => {
            tracing::error!(task = %t.name, level, error = format!("{e:#}"), "restore failed");
            return Err(e);
        }
    };
    if progress.completed {
        tracing::info!(task = %t.name, level, "restore completed");
        println!("restore complete");
        return Ok(ExitCode::SUCCESS);
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
    Ok(ExitCode::from(EXIT_RESTORE_IN_PROGRESS))
}

pub(crate) fn enabled_tasks<'a>(tasks: &'a [Task], name: Option<&str>) -> Vec<&'a Task> {
    tasks
        .iter()
        .filter(|t| t.enabled)
        .filter(|t| name.is_none_or(|n| t.name == n))
        .collect()
}

/// The aggregate error of a command that ran every selected task, or Ok if none failed.
/// List, verify, prune and backup run every task to completion rather than stopping at the first
/// failure: one failing task must never keep the others from being attempted.
pub(crate) fn aggregate_failures(verb: &str, failed: &[String]) -> Result<()> {
    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{verb} failed for task(s): {}", failed.join(", ")))
    }
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
    fn enabled_tasks_without_filter_should_return_every_enabled_task() {
        let tasks = vec![task("a", true), task("b", true), task("c", false)];
        let selected = enabled_tasks(&tasks, None);
        assert_eq!(
            selected.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn enabled_tasks_by_name_should_not_select_disabled_task() {
        let tasks = vec![task("a", true), task("b", false)];
        assert!(enabled_tasks(&tasks, Some("b")).is_empty());
        assert_eq!(enabled_tasks(&tasks, Some("a")).len(), 1);
    }

    #[test]
    fn enabled_tasks_with_unknown_name_should_be_empty() {
        let tasks = vec![task("a", true)];
        assert!(enabled_tasks(&tasks, Some("nonexistent")).is_empty());
    }

    #[test]
    fn aggregate_failures_should_be_ok_without_failures() {
        assert!(aggregate_failures("backup", &[]).is_ok());
    }

    #[test]
    fn aggregate_failures_should_name_every_failed_task() {
        let failed = vec!["a".to_string(), "b".to_string()];
        let err = aggregate_failures("backup", &failed).unwrap_err();
        assert_eq!(err.to_string(), "backup failed for task(s): a, b");
    }
}
