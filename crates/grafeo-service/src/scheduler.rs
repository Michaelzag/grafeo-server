//! Backup scheduler — runs automatic backups on a cron schedule.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::backup::BackupService;
use crate::database::DatabaseManager;

/// Spawns a background task that runs backups on the given cron schedule.
///
/// Uses `ServiceState` (which is `Clone`) so the task can be `'static`.
/// Iterates all databases and backs up each one, then enforces the
/// retention policy.
pub fn start(
    schedule: String,
    service: crate::ServiceState,
    backup_dir: PathBuf,
    retention: Option<usize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let parsed = match cron::Schedule::from_str(&schedule) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    schedule = %schedule,
                    error = %e,
                    "Invalid backup schedule, scheduler not started"
                );
                return;
            }
        };

        tracing::info!(schedule = %schedule, "Backup scheduler started");

        loop {
            // cron::Schedule::upcoming returns chrono::DateTime<Utc> via the cron crate's
            // re-export of chrono.
            let next = match parsed.upcoming(chrono::Utc).next() {
                Some(dt) => dt,
                None => {
                    tracing::warn!("No upcoming schedule time, scheduler stopping");
                    return;
                }
            };

            let now = chrono::Utc::now();
            let wait = (next - now)
                .to_std()
                .unwrap_or(std::time::Duration::from_secs(60));

            tracing::debug!(next = %next, wait_secs = wait.as_secs(), "Next scheduled backup");
            tokio::time::sleep(wait).await;

            run_scheduled_backups(service.databases(), &backup_dir, retention).await;
        }
    })
}

async fn run_scheduled_backups(
    databases: &DatabaseManager,
    backup_dir: &Path,
    retention: Option<usize>,
) {
    let db_list = databases.list();
    tracing::info!(
        count = db_list.len(),
        "Running scheduled backup for all databases"
    );

    for db in &db_list {
        match BackupService::backup_database(databases, &db.name, backup_dir).await {
            Ok(entry) => {
                tracing::info!(
                    database = %db.name,
                    filename = %entry.filename,
                    size_bytes = entry.size_bytes,
                    "Scheduled backup completed"
                );

                if let Some(keep) = retention {
                    match BackupService::enforce_retention(&db.name, backup_dir, keep).await {
                        Ok(deleted) if !deleted.is_empty() => {
                            tracing::info!(
                                database = %db.name,
                                deleted_count = deleted.len(),
                                "Retention policy enforced"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                database = %db.name,
                                error = %e,
                                "Failed to enforce retention policy"
                            );
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    database = %db.name,
                    error = %e,
                    "Scheduled backup failed"
                );
            }
        }
    }
}
