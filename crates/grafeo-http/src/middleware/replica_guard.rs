//! Middleware that rejects write operations when the server is in replica mode.
//!
//! POST, PUT, PATCH, and DELETE requests return `503 Service Unavailable`
//! with `{"error": "replica_mode", "message": "..."}` when this instance
//! is a read-only replica.  GET requests are always allowed.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::json;

use crate::AppState;

/// Axum middleware that rejects write requests on replicas.
///
/// Applied only when the `replication` feature is enabled. On non-replica
/// instances (Standalone, Primary) this is a zero-cost pass-through.
pub async fn replica_guard_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if state.service().is_replica()
        && matches!(
            *req.method(),
            Method::POST | Method::PUT | Method::PATCH | Method::DELETE
        )
    {
        let body = json!({
            "error": "replica_mode",
            "message": "This instance is a read-only replica. Write operations are not permitted."
        });
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("response builder with valid header is infallible");
    }

    next.run(req).await
}
