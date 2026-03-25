//! Replication status endpoint.
//!
//! `GET /admin/replication` returns the current replication mode and
//! per-database epoch lag.  Always available (returns `{mode: "standalone"}`
//! on instances that have not been configured for replication).

use axum::Json;
use axum::extract::State;

use grafeo_service::replication::ReplicationStatus;

use crate::AppState;
use crate::error::ApiError;

/// `GET /admin/replication`
///
/// Returns the replication mode and per-database status.
pub async fn get_replication_status(
    State(state): State<AppState>,
) -> Result<Json<ReplicationStatus>, ApiError> {
    let status = state
        .service()
        .replication_state()
        .status(state.service().replication_mode());
    Ok(Json(status))
}
