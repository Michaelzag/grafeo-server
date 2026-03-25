//! Async HTTP client for the Grafeo offline-first sync protocol.
//!
//! # Quick start
//!
//! ```no_run
//! use grafeo_sync::SyncClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), grafeo_sync::SyncError> {
//!     let client = SyncClient::new(
//!         "http://localhost:7474",
//!         "default",
//!         "my-device-id",
//!     )?;
//!
//!     // Pull changes since last known epoch (0 = full history)
//!     let pulled = client.pull(1_000).await?;
//!     println!("got {} events, server epoch = {}", pulled.changes.len(), pulled.server_epoch);
//!
//!     // Advance local epoch bookmark after applying pulled events
//!     client.advance_epoch(pulled.server_epoch);
//!
//!     // Push local changes
//!     use grafeo_service::sync::SyncChangeRequest;
//!     let resp = client.push(vec![]).await?;
//!     println!("applied={} skipped={}", resp.applied, resp.skipped);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Sync loop pattern
//!
//! ```no_run
//! # use grafeo_sync::SyncClient;
//! # use grafeo_service::sync::SyncChangeRequest;
//! # async fn example() -> Result<(), grafeo_sync::SyncError> {
//! let client = SyncClient::new("http://localhost:7474", "default", "device-1")?;
//! // Optionally resume from a saved epoch:
//! let client = client.with_epoch(42);
//!
//! loop {
//!     // Pull server changes, then push local ones
//!     let (pulled, pushed) = client.sync(vec![/* local pending changes */]).await?;
//!     client.advance_epoch(pulled.server_epoch);
//!     if !pushed.conflicts.is_empty() {
//!         // Handle conflicts...
//!     }
//!     tokio::time::sleep(std::time::Duration::from_secs(30)).await;
//! }
//! # }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use grafeo_service::sync::{ChangesResponse, SyncChangeRequest, SyncRequest, SyncResponse};
use url::Url;

pub use error::SyncError;

mod error;

/// Async HTTP client for the Grafeo sync protocol.
///
/// Wraps the `GET /db/{name}/changes` and `POST /db/{name}/sync` endpoints.
/// Thread-safe: cloning the struct shares the underlying HTTP client and epoch counter.
#[derive(Clone)]
pub struct SyncClient {
    http: reqwest::Client,
    changes_url: Url,
    sync_url: Url,
    /// Opaque identifier for this client/device.
    pub client_id: String,
    /// Last server epoch the client has processed. Updated by `advance_epoch()`.
    last_epoch: Arc<AtomicU64>,
}

impl SyncClient {
    /// Creates a new sync client.
    ///
    /// # Arguments
    ///
    /// * `base_url` — Root URL of the grafeo-server (e.g. `"http://localhost:7474"`).
    /// * `db_name` — Name of the database to sync (e.g. `"default"`).
    /// * `client_id` — Stable opaque identifier for this device/session.
    pub fn new(base_url: &str, db_name: &str, client_id: &str) -> Result<Self, SyncError> {
        let base = Url::parse(base_url).map_err(|e| SyncError::InvalidUrl(e.to_string()))?;

        let changes_url = base
            .join(&format!("db/{db_name}/changes"))
            .map_err(|e| SyncError::InvalidUrl(e.to_string()))?;

        let sync_url = base
            .join(&format!("db/{db_name}/sync"))
            .map_err(|e| SyncError::InvalidUrl(e.to_string()))?;

        Ok(Self {
            http: reqwest::Client::new(),
            changes_url,
            sync_url,
            client_id: client_id.to_string(),
            last_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Overrides the starting epoch (useful when resuming from a persisted bookmark).
    #[must_use]
    pub fn with_epoch(self, epoch: u64) -> Self {
        self.last_epoch.store(epoch, Ordering::Relaxed);
        self
    }

    /// Returns the last epoch the client has acknowledged.
    #[must_use]
    pub fn last_epoch(&self) -> u64 {
        self.last_epoch.load(Ordering::Relaxed)
    }

    /// Updates the acknowledged epoch.
    ///
    /// Call this after successfully applying a pull response to advance the
    /// cursor for the next poll. Only advances forward — a smaller value is ignored.
    pub fn advance_epoch(&self, epoch: u64) {
        self.last_epoch.fetch_max(epoch, Ordering::Relaxed);
    }

    /// Pulls change events from the server since `self.last_epoch()`.
    ///
    /// `limit` is capped at 10 000 by the server. If `response.changes.len() == limit`,
    /// there may be more events: call `advance_epoch(response.server_epoch)` and pull again.
    pub async fn pull(&self, limit: usize) -> Result<ChangesResponse, SyncError> {
        let since = self.last_epoch();

        let mut url = self.changes_url.clone();
        url.query_pairs_mut()
            .append_pair("since", &since.to_string())
            .append_pair("limit", &limit.to_string());

        let resp = self.http.get(url).send().await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(SyncError::ServerError { status, body });
        }

        Ok(resp.json::<ChangesResponse>().await?)
    }

    /// Pushes `changes` to the server and returns the server's response.
    ///
    /// `last_seen_epoch` in the request body is set to `self.last_epoch()`.
    pub async fn push(&self, changes: Vec<SyncChangeRequest>) -> Result<SyncResponse, SyncError> {
        let request = SyncRequest {
            client_id: self.client_id.clone(),
            last_seen_epoch: self.last_epoch(),
            changes,
            schema_version: None,
        };

        let resp = self
            .http
            .post(self.sync_url.clone())
            .json(&request)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(SyncError::ServerError { status, body });
        }

        Ok(resp.json::<SyncResponse>().await?)
    }

    /// Pulls server changes then pushes `local_changes` in a single round-trip pair.
    ///
    /// Returns `(pull_response, push_response)`.
    ///
    /// The caller is responsible for applying pulled events to the local database
    /// and for calling `advance_epoch(pulled.server_epoch)` after processing.
    pub async fn sync(
        &self,
        local_changes: Vec<SyncChangeRequest>,
    ) -> Result<(ChangesResponse, SyncResponse), SyncError> {
        let pulled = self.pull(1_000).await?;
        let pushed = self.push(local_changes).await?;
        Ok((pulled, pushed))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::Router;
    use axum::extract::{Path, Query};
    use axum::response::Json;
    use axum::routing::{get, post};
    use grafeo_service::sync::{ChangesResponse, SyncRequest, SyncResponse};
    use serde::Deserialize;
    use tokio::net::TcpListener;

    use super::*;

    // ---------------------------------------------------------------------------
    // Minimal mock HTTP server
    // ---------------------------------------------------------------------------

    #[derive(Deserialize)]
    struct SinceQuery {
        #[serde(default)]
        since: u64,
    }

    async fn mock_changes(
        Path(_name): Path<String>,
        Query(q): Query<SinceQuery>,
    ) -> Json<ChangesResponse> {
        Json(ChangesResponse {
            server_epoch: q.since + 10,
            changes: vec![],
        })
    }

    async fn mock_sync(
        Path(_name): Path<String>,
        Json(req): Json<SyncRequest>,
    ) -> Json<SyncResponse> {
        Json(SyncResponse {
            server_epoch: req.last_seen_epoch + 5,
            applied: req.changes.len(),
            skipped: 0,
            conflicts: vec![],
            id_mappings: vec![],
            schema_mismatch: false,
            server_schema_version: "abc123".to_string(),
        })
    }

    async fn mock_error(Path(_name): Path<String>) -> axum::http::StatusCode {
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    }

    async fn spawn_mock_server() -> String {
        let app = Router::new()
            .route("/db/{name}/changes", get(mock_changes))
            .route("/db/{name}/sync", post(mock_sync));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        format!("http://{addr}")
    }

    async fn spawn_error_server() -> String {
        let app = Router::new()
            .route("/db/{name}/changes", get(mock_error))
            .route("/db/{name}/sync", post(mock_error));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        format!("http://{addr}")
    }

    // ---------------------------------------------------------------------------
    // Constructor / epoch helpers (no server needed)
    // ---------------------------------------------------------------------------

    #[test]
    fn new_parses_valid_url() {
        let client = SyncClient::new("http://localhost:7474", "default", "dev-1");
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.client_id, "dev-1");
        assert_eq!(client.last_epoch(), 0);
    }

    #[test]
    fn new_rejects_invalid_url() {
        let result = SyncClient::new("not a url", "default", "dev-1");
        assert!(matches!(result, Err(SyncError::InvalidUrl(_))));
    }

    #[test]
    fn with_epoch_sets_starting_epoch() {
        let client = SyncClient::new("http://localhost:7474", "default", "dev-1")
            .unwrap()
            .with_epoch(42);
        assert_eq!(client.last_epoch(), 42);
    }

    #[test]
    fn advance_epoch_moves_forward() {
        let client = SyncClient::new("http://localhost:7474", "default", "dev-1").unwrap();
        client.advance_epoch(10);
        assert_eq!(client.last_epoch(), 10);
        client.advance_epoch(20);
        assert_eq!(client.last_epoch(), 20);
    }

    #[test]
    fn advance_epoch_does_not_go_backward() {
        let client = SyncClient::new("http://localhost:7474", "default", "dev-1").unwrap();
        client.advance_epoch(50);
        client.advance_epoch(10); // should be ignored
        assert_eq!(client.last_epoch(), 50);
    }

    #[test]
    fn clone_shares_epoch_counter() {
        let a = SyncClient::new("http://localhost:7474", "default", "dev-1").unwrap();
        let b = a.clone();
        a.advance_epoch(99);
        assert_eq!(b.last_epoch(), 99);
    }

    // ---------------------------------------------------------------------------
    // Pull
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn pull_returns_changes_response() {
        let base = spawn_mock_server().await;
        let client = SyncClient::new(&base, "default", "dev-1").unwrap();

        let resp = client.pull(100).await.unwrap();
        assert_eq!(resp.server_epoch, 10); // since=0, mock returns 0+10
        assert!(resp.changes.is_empty());
    }

    #[tokio::test]
    async fn pull_uses_last_epoch_as_since() {
        let base = spawn_mock_server().await;
        let client = SyncClient::new(&base, "default", "dev-1")
            .unwrap()
            .with_epoch(7);

        let resp = client.pull(100).await.unwrap();
        assert_eq!(resp.server_epoch, 17); // since=7, mock returns 7+10
    }

    #[tokio::test]
    async fn pull_returns_server_error() {
        let base = spawn_error_server().await;
        let client = SyncClient::new(&base, "default", "dev-1").unwrap();

        let err = client.pull(100).await.unwrap_err();
        assert!(matches!(err, SyncError::ServerError { status: 500, .. }));
    }

    // ---------------------------------------------------------------------------
    // Push
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn push_empty_changeset_succeeds() {
        let base = spawn_mock_server().await;
        let client = SyncClient::new(&base, "default", "dev-1").unwrap();

        let resp = client.push(vec![]).await.unwrap();
        assert_eq!(resp.applied, 0);
        assert_eq!(resp.skipped, 0);
        assert_eq!(resp.server_epoch, 5); // last_epoch=0, mock returns 0+5
    }

    #[tokio::test]
    async fn push_sends_client_id_and_epoch() {
        let base = spawn_mock_server().await;
        let client = SyncClient::new(&base, "default", "dev-1")
            .unwrap()
            .with_epoch(3);

        let resp = client.push(vec![]).await.unwrap();
        assert_eq!(resp.server_epoch, 8); // last_seen_epoch=3, mock returns 3+5
    }

    #[tokio::test]
    async fn push_returns_server_error() {
        let base = spawn_error_server().await;
        let client = SyncClient::new(&base, "default", "dev-1").unwrap();

        let err = client.push(vec![]).await.unwrap_err();
        assert!(matches!(err, SyncError::ServerError { status: 500, .. }));
    }

    // ---------------------------------------------------------------------------
    // Sync (pull + push)
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn sync_returns_both_responses() {
        let base = spawn_mock_server().await;
        let client = SyncClient::new(&base, "default", "dev-1").unwrap();

        let (pulled, pushed) = client.sync(vec![]).await.unwrap();
        assert_eq!(pulled.server_epoch, 10);
        assert_eq!(pushed.applied, 0);
    }
}
