//! Primary-replica replication for Grafeo databases.
//!
//! Replication is built on top of the CDC changefeed (`sync` feature).
//! A replica continuously polls the primary's `GET /db/{name}/changes`
//! endpoint and applies the returned events with `SyncService::apply()`.
//!
//! # Modes
//!
//! | Mode | Behavior |
//! |------|----------|
//! | `Standalone` | Default. No replication. Reads and writes allowed. |
//! | `Primary` | Announces itself as primary. Reads and writes allowed. |
//! | `Replica` | Polls primary continuously. **Writes are rejected (503).** |
//!
//! # Wire protocol
//!
//! The replica reuses the existing `GET /db/{name}/changes?since={epoch}&limit=500`
//! endpoint — no new protocol required. `SyncService::apply()` is used to
//! replay the returned `ChangeEventDto` entries.
//!
//! # Per-database epoch tracking
//!
//! `ReplicationState` holds a `DashMap<db_name, AtomicU64>` tracking the
//! last successfully applied epoch per database. The background task
//! (in `grafeo-http`) updates these after each successful batch.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Replication role for this server instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMode {
    /// No replication. Default mode.
    Standalone,
    /// Accepts reads and writes; advertises itself as a primary.
    Primary,
    /// Polls `primary_url` continuously; rejects local writes.
    Replica { primary_url: String },
}

impl ReplicationMode {
    /// Returns `true` when this instance should reject write operations.
    #[must_use]
    pub fn is_replica(&self) -> bool {
        matches!(self, Self::Replica { .. })
    }

    /// Returns the primary base URL if in `Replica` mode.
    #[must_use]
    pub fn primary_url(&self) -> Option<&str> {
        match self {
            Self::Replica { primary_url } => Some(primary_url.as_str()),
            _ => None,
        }
    }
}

/// Per-database replication progress.
#[derive(Debug, Clone, Serialize)]
pub struct DbReplicationStatus {
    /// The last CDC epoch successfully applied on this replica.
    pub last_applied_epoch: u64,
    /// Last error encountered, if any.
    pub last_error: Option<String>,
}

/// Overall replication status returned by the status endpoint.
#[derive(Debug, Serialize)]
pub struct ReplicationStatus {
    /// Replication role as a string: `"standalone"`, `"primary"`, `"replica"`.
    pub mode: String,
    /// Primary URL (replica mode only).
    pub primary_url: Option<String>,
    /// Per-database replication progress.
    pub databases: HashMap<String, DbReplicationStatus>,
}

/// Shared replication state updated by the background poll task.
///
/// Holds per-database last-applied epoch counters and the most recent error.
#[derive(Debug, Default)]
pub struct ReplicationState {
    /// Per-database last-applied epoch.
    pub epochs: DashMap<String, Arc<AtomicU64>>,
    /// Per-database last error.
    pub errors: DashMap<String, String>,
}

impl ReplicationState {
    /// Creates a new empty state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the last applied epoch for `db`, creating an entry if absent.
    pub fn last_epoch(&self, db: &str) -> u64 {
        self.epochs
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .load(Ordering::Relaxed)
    }

    /// Advances the stored epoch for `db` to `epoch` (if larger).
    pub fn advance_epoch(&self, db: &str, epoch: u64) {
        let entry = self
            .epochs
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        entry.fetch_max(epoch, Ordering::Relaxed);
    }

    /// Records an error for `db`.
    pub fn set_error(&self, db: &str, err: String) {
        self.errors.insert(db.to_string(), err);
    }

    /// Clears the error for `db`.
    pub fn clear_error(&self, db: &str) {
        self.errors.remove(db);
    }

    /// Returns the current status snapshot.
    #[must_use]
    pub fn status(&self, mode: &ReplicationMode) -> ReplicationStatus {
        let mut databases = HashMap::new();
        for entry in &self.epochs {
            let db = entry.key().clone();
            let last_applied_epoch = entry.value().load(Ordering::Relaxed);
            let last_error = self.errors.get(&db).map(|e| e.value().clone());
            databases.insert(
                db,
                DbReplicationStatus {
                    last_applied_epoch,
                    last_error,
                },
            );
        }
        ReplicationStatus {
            mode: match mode {
                ReplicationMode::Standalone => "standalone".to_string(),
                ReplicationMode::Primary => "primary".to_string(),
                ReplicationMode::Replica { .. } => "replica".to_string(),
            },
            primary_url: mode.primary_url().map(str::to_string),
            databases,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_is_not_replica() {
        assert!(!ReplicationMode::Standalone.is_replica());
        assert!(ReplicationMode::Standalone.primary_url().is_none());
    }

    #[test]
    fn primary_is_not_replica() {
        assert!(!ReplicationMode::Primary.is_replica());
        assert!(ReplicationMode::Primary.primary_url().is_none());
    }

    #[test]
    fn replica_mode_properties() {
        let mode = ReplicationMode::Replica {
            primary_url: "http://primary:7474".to_string(),
        };
        assert!(mode.is_replica());
        assert_eq!(mode.primary_url(), Some("http://primary:7474"));
    }

    #[test]
    fn replication_state_epoch_tracking() {
        let state = ReplicationState::new();
        assert_eq!(state.last_epoch("default"), 0);
        state.advance_epoch("default", 42);
        assert_eq!(state.last_epoch("default"), 42);
        // advance_epoch never goes backwards
        state.advance_epoch("default", 10);
        assert_eq!(state.last_epoch("default"), 42);
    }

    #[test]
    fn replication_state_error_tracking() {
        let state = ReplicationState::new();
        state.set_error("default", "connection refused".to_string());
        let _initial_status = state.status(&ReplicationMode::Replica {
            primary_url: "http://primary:7474".to_string(),
        });
        // Epoch 0 entry is created by set_error's status call if already tracked, or it may not
        // appear if advance_epoch was never called. Check error presence:
        state.advance_epoch("default", 0);
        let status = state.status(&ReplicationMode::Replica {
            primary_url: "http://primary:7474".to_string(),
        });
        assert_eq!(
            status.databases["default"].last_error.as_deref(),
            Some("connection refused")
        );
        state.clear_error("default");
        let status = state.status(&ReplicationMode::Replica {
            primary_url: "http://primary:7474".to_string(),
        });
        assert!(status.databases["default"].last_error.is_none());
    }

    #[test]
    fn status_mode_strings() {
        let state = ReplicationState::new();
        assert_eq!(
            state.status(&ReplicationMode::Standalone).mode,
            "standalone"
        );
        assert_eq!(state.status(&ReplicationMode::Primary).mode, "primary");
        assert_eq!(
            state
                .status(&ReplicationMode::Replica {
                    primary_url: "http://x".to_string()
                })
                .mode,
            "replica"
        );
    }
}
