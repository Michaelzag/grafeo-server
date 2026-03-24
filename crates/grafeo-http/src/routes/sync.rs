//! Sync endpoints for offline-first applications.
//!
//! # Endpoints
//!
//! - `GET /db/{name}/changes?since=<epoch>&limit=<n>` — pull changefeed
//! - `POST /db/{name}/sync` — push client changes with LWW conflict resolution
//!
//! Requires the `sync` feature (implies `cdc`).

use axum::extract::{Path, Query, State};
use axum::response::Json;
use serde::Deserialize;

use grafeo_service::sync::{ChangesResponse, SyncRequest, SyncResponse, SyncService};

use crate::error::ApiError;
use crate::state::AppState;

const MAX_LIMIT: usize = 10_000;
const DEFAULT_LIMIT: usize = 1_000;

/// Query parameters for the changefeed endpoint.
#[derive(Debug, Deserialize)]
pub struct ChangesQuery {
    /// Return events with epoch >= this value. Defaults to 0 (full history).
    #[serde(default)]
    pub since: u64,
    /// Maximum number of events per response. Defaults to 1 000, max 10 000.
    pub limit: Option<usize>,
}

/// Poll for change events since a given epoch.
///
/// Returns all mutations (create, update, delete) for nodes and edges in the
/// named database where the MVCC epoch is >= `since`.
///
/// Store `server_epoch` from the response and pass it as `since` on the next
/// request. If `changes.len() == limit`, more events may be available: poll
/// again using the epoch of the last returned event.
pub async fn db_changes(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let since = params.since;

    let result = tokio::task::spawn_blocking(move || {
        SyncService::pull(state.databases(), &name, since, limit)
    })
    .await
    .map_err(|e| ApiError::internal(e.to_string()))??;

    Ok(Json(result))
}

/// Apply a client changeset to the named database.
///
/// Accepts a JSON body with `{ client_id, last_seen_epoch, changes: [...] }`.
/// Changes are applied in order with last-write-wins (LWW) conflict
/// resolution: if the server has a more recent CDC timestamp for the target
/// entity, the client change is skipped and recorded in `conflicts`.
///
/// Returns `{ server_epoch, applied, skipped, conflicts, id_mappings }`.
/// The `id_mappings` array maps each create request (by index) to the
/// server-assigned entity ID.
pub async fn db_apply(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<SyncRequest>,
) -> Result<Json<SyncResponse>, ApiError> {
    let result =
        tokio::task::spawn_blocking(move || SyncService::apply(state.databases(), &name, request))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))??;

    Ok(Json(result))
}
