//! W3C SPARQL 1.1 Protocol endpoints.
//!
//! Implements [SPARQL 1.1 Protocol](https://www.w3.org/TR/sparql11-protocol/) with
//! proper content-type negotiation for query and update operations.
//!
//! Supports:
//! - `GET /db/{name}/sparql?query=...` (query only)
//! - `POST /db/{name}/sparql` with `Content-Type: application/sparql-query`
//! - `POST /db/{name}/sparql` with `Content-Type: application/sparql-update`
//! - `POST /db/{name}/sparql` with `Content-Type: application/x-www-form-urlencoded`
//! - `POST /db/{name}/sparql` with `Content-Type: application/json` (backwards compat)
//!
//! Response format is negotiated via the `Accept` header:
//! - `application/sparql-results+json` for SELECT/ASK (default)
//! - `application/json` for Grafeo's native JSON format
//! - `text/turtle` for CONSTRUCT/DESCRIBE (future)

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;

use grafeo_service::query::QueryService;

use crate::encode::{convert_json_params, streaming_json_response};
use crate::encode_sparql::sparql_results_json_response;
use crate::error::ApiError;
use crate::state::AppState;
use crate::types::QueryRequest;

// ---------------------------------------------------------------------------
// Content types
// ---------------------------------------------------------------------------

const CT_SPARQL_QUERY: &str = "application/sparql-query";
const CT_SPARQL_UPDATE: &str = "application/sparql-update";
const CT_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
const CT_JSON: &str = "application/json";

const ACCEPT_SPARQL_JSON: &str = "application/sparql-results+json";
const ACCEPT_JSON: &str = "application/json";

// ---------------------------------------------------------------------------
// GET /db/{name}/sparql?query=...
// ---------------------------------------------------------------------------

/// Query parameters for SPARQL GET requests.
#[derive(serde::Deserialize)]
pub struct SparqlGetParams {
    /// URL-encoded SPARQL query string.
    pub query: String,
    /// Override default graph URI (repeatable, not yet enforced).
    #[serde(rename = "default-graph-uri")]
    pub default_graph_uri: Option<String>,
    /// Named graph URI (repeatable, not yet enforced).
    #[serde(rename = "named-graph-uri")]
    pub named_graph_uri: Option<String>,
}

/// `GET /db/{name}/sparql?query=...`
///
/// Executes a SPARQL query (read-only). Updates via GET return 400.
pub async fn sparql_get(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<SparqlGetParams>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let timeout = state.effective_timeout(None);

    let result = QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &params.query,
        Some("sparql"),
        None,
        timeout,
    )
    .await?;

    Ok(format_sparql_response(result, &headers))
}

// ---------------------------------------------------------------------------
// POST /db/{name}/sparql
// ---------------------------------------------------------------------------

/// `POST /db/{name}/sparql`
///
/// Content-Type dispatch:
/// - `application/sparql-query`: body is a SPARQL query string
/// - `application/sparql-update`: body is a SPARQL update string
/// - `application/x-www-form-urlencoded`: `query=...` or `update=...` parameter
/// - `application/json`: Grafeo JSON format (backwards compat)
pub async fn sparql_post(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CT_JSON);

    // Extract the base content type (strip charset and other parameters).
    let base_ct = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();

    let statement = match base_ct {
        CT_SPARQL_QUERY | CT_SPARQL_UPDATE => String::from_utf8(body.to_vec())
            .map_err(|_| ApiError::bad_request("request body is not valid UTF-8"))?,
        CT_FORM_URLENCODED => {
            let form: Vec<(String, String)> = serde_urlencoded::from_bytes(&body)
                .map_err(|e| ApiError::bad_request(format!("invalid form encoding: {e}")))?;

            form.iter()
                .find(|(k, _)| k == "query" || k == "update")
                .map(|(_, v)| v.clone())
                .ok_or_else(|| {
                    ApiError::bad_request("form body must contain 'query' or 'update' parameter")
                })?
        }
        CT_JSON => {
            // Backwards-compatible JSON body.
            let req: QueryRequest = serde_json::from_slice(&body)
                .map_err(|e| ApiError::bad_request(format!("invalid JSON: {e}")))?;

            let params = convert_json_params(req.params.as_ref())?;
            let timeout = state.effective_timeout(req.timeout_ms);

            let result = QueryService::execute(
                state.databases(),
                state.metrics(),
                &db_name,
                &req.query,
                Some("sparql"),
                params,
                timeout,
            )
            .await?;

            return Ok(format_sparql_response(result, &headers));
        }
        _ => {
            return Err(ApiError::bad_request(format!(
                "unsupported Content-Type: {base_ct}. Expected one of: \
                 application/sparql-query, application/sparql-update, \
                 application/x-www-form-urlencoded, application/json"
            )));
        }
    };

    let timeout = state.effective_timeout(None);

    let result = QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &statement,
        Some("sparql"),
        None,
        timeout,
    )
    .await?;

    Ok(format_sparql_response(result, &headers))
}

// ---------------------------------------------------------------------------
// Response format negotiation
// ---------------------------------------------------------------------------

/// Picks the response format based on the `Accept` header.
fn format_sparql_response(
    result: grafeo_engine::database::QueryResult,
    headers: &HeaderMap,
) -> Response {
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(ACCEPT_SPARQL_JSON);

    if accept.contains(ACCEPT_JSON) && !accept.contains(ACCEPT_SPARQL_JSON) {
        // Explicit request for Grafeo's native JSON format.
        streaming_json_response(result)
    } else {
        // Default: W3C SPARQL Results JSON.
        sparql_results_json_response(result)
    }
}
