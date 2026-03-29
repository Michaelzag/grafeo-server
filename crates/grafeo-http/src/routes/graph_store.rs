//! W3C SPARQL 1.1 Graph Store HTTP Protocol.
//!
//! Implements [Graph Store Protocol](https://www.w3.org/TR/sparql11-http-rdf-update/)
//! for RESTful CRUD on RDF named graphs.
//!
//! All operations are mapped to their SPARQL equivalents and dispatched
//! through the standard query service, so no direct grafeo-core dependency
//! is needed.
//!
//! Endpoints: `GET/PUT/POST/DELETE/HEAD /db/{name}/graph-store`
//!
//! Graph identification via query parameters:
//! - `?default` targets the default graph
//! - `?graph=<iri>` targets a named graph

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use grafeo_service::query::QueryService;

use crate::error::ApiError;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Graph identification parameters.
#[derive(serde::Deserialize, Default)]
pub struct GraphStoreParams {
    /// If present (any value or empty), targets the default graph.
    pub default: Option<String>,
    /// IRI of a named graph.
    pub graph: Option<String>,
}

enum GraphTarget {
    Default,
    Named(String),
}

fn resolve_target(params: &GraphStoreParams) -> Result<GraphTarget, ApiError> {
    if params.default.is_some() {
        Ok(GraphTarget::Default)
    } else if let Some(ref iri) = params.graph {
        if iri.is_empty() {
            return Err(ApiError::bad_request("?graph= parameter must not be empty"));
        }
        Ok(GraphTarget::Named(iri.clone()))
    } else {
        Err(ApiError::bad_request(
            "must specify ?default or ?graph=<iri>",
        ))
    }
}

// ---------------------------------------------------------------------------
// Content negotiation
// ---------------------------------------------------------------------------

const CT_TURTLE: &str = "text/turtle";
const CT_NTRIPLES: &str = "application/n-triples";

// ---------------------------------------------------------------------------
// GET /db/{name}/graph-store
// ---------------------------------------------------------------------------

/// Retrieve graph content as N-Triples.
///
/// Uses `CONSTRUCT { ?s ?p ?o } WHERE { ... }` under the hood.
pub async fn graph_store_get(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<GraphStoreParams>,
    _headers: HeaderMap,
) -> Result<Response, ApiError> {
    let target = resolve_target(&params)?;

    let sparql = match target {
        GraphTarget::Default => "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }".to_string(),
        GraphTarget::Named(ref iri) => {
            format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}")
        }
    };

    let timeout = state.effective_timeout(None);

    let result = QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &sparql,
        Some("sparql"),
        None,
        timeout,
    )
    .await?;

    // CONSTRUCT results come back as rows with columns [subject, predicate, object].
    // Serialize as N-Triples.
    let mut body = String::new();
    for row in &result.rows {
        if row.len() >= 3 {
            let s = value_to_nt_term(&row[0]);
            let p = value_to_nt_term(&row[1]);
            let o = value_to_nt_term(&row[2]);
            use std::fmt::Write;
            let _ = writeln!(body, "{s} {p} {o} .");
        }
    }

    // Return as N-Triples (most interoperable, no prefix management needed).
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", CT_NTRIPLES)
        .body(axum::body::Body::from(body))
        .expect("valid response"))
}

// ---------------------------------------------------------------------------
// HEAD /db/{name}/graph-store
// ---------------------------------------------------------------------------

/// Check if a graph exists (no body).
pub async fn graph_store_head(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<GraphStoreParams>,
) -> Result<Response, ApiError> {
    let target = resolve_target(&params)?;

    let sparql = match target {
        GraphTarget::Default => "ASK { ?s ?p ?o }".to_string(),
        GraphTarget::Named(ref iri) => {
            format!("ASK {{ GRAPH <{iri}> {{ ?s ?p ?o }} }}")
        }
    };

    let timeout = state.effective_timeout(None);

    // If the query succeeds, the graph exists (or is the default graph).
    let result = QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &sparql,
        Some("sparql"),
        None,
        timeout,
    )
    .await?;

    // For default graph, always 200. For named, check if ASK returned true.
    match target {
        GraphTarget::Default => Ok(StatusCode::OK.into_response()),
        GraphTarget::Named(_) => {
            // ASK returns a single boolean row. If true, graph has data.
            let has_data = result
                .rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| {
                    if let grafeo_common::Value::Bool(b) = v {
                        Some(*b)
                    } else {
                        None
                    }
                })
                .unwrap_or(false);

            if has_data {
                Ok(StatusCode::OK.into_response())
            } else {
                Ok(StatusCode::NOT_FOUND.into_response())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PUT /db/{name}/graph-store
// ---------------------------------------------------------------------------

/// Replace graph content.
///
/// Equivalent to: `DROP SILENT GRAPH <iri>; INSERT DATA { ... }`
pub async fn graph_store_put(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<GraphStoreParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let target = resolve_target(&params)?;
    let nt_triples = parse_body_to_ntriples(&headers, &body)?;

    // Drop existing content.
    let drop_sparql = match target {
        GraphTarget::Default => "DROP SILENT DEFAULT".to_string(),
        GraphTarget::Named(ref iri) => format!("DROP SILENT GRAPH <{iri}>"),
    };

    let timeout = state.effective_timeout(None);

    QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &drop_sparql,
        Some("sparql"),
        None,
        timeout,
    )
    .await?;

    // Insert new content.
    if !nt_triples.is_empty() {
        let insert_sparql = build_insert_data(&target, &nt_triples);
        QueryService::execute(
            state.databases(),
            state.metrics(),
            &db_name,
            &insert_sparql,
            Some("sparql"),
            None,
            timeout,
        )
        .await?;
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------------------
// POST /db/{name}/graph-store
// ---------------------------------------------------------------------------

/// Merge triples into a graph.
///
/// Without `?default` or `?graph=`, creates a new named graph and returns
/// `201 Created` with a `Location` header.
pub async fn graph_store_post(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<GraphStoreParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let nt_triples = parse_body_to_ntriples(&headers, &body)?;

    // No target params: create a new named graph.
    if params.default.is_none() && params.graph.is_none() {
        let graph_iri = format!("urn:grafeo:graph:{}", uuid::Uuid::new_v4());
        let create_sparql = format!("CREATE GRAPH <{graph_iri}>");
        let timeout = state.effective_timeout(None);

        QueryService::execute(
            state.databases(),
            state.metrics(),
            &db_name,
            &create_sparql,
            Some("sparql"),
            None,
            timeout,
        )
        .await?;

        if !nt_triples.is_empty() {
            let target = GraphTarget::Named(graph_iri.clone());
            let insert_sparql = build_insert_data(&target, &nt_triples);
            QueryService::execute(
                state.databases(),
                state.metrics(),
                &db_name,
                &insert_sparql,
                Some("sparql"),
                None,
                timeout,
            )
            .await?;
        }

        return Ok(Response::builder()
            .status(StatusCode::CREATED)
            .header(
                "location",
                format!("/api/db/{db_name}/graph-store?graph={graph_iri}"),
            )
            .body(axum::body::Body::empty())
            .expect("valid response"));
    }

    let target = resolve_target(&params)?;

    if !nt_triples.is_empty() {
        let insert_sparql = build_insert_data(&target, &nt_triples);
        let timeout = state.effective_timeout(None);
        QueryService::execute(
            state.databases(),
            state.metrics(),
            &db_name,
            &insert_sparql,
            Some("sparql"),
            None,
            timeout,
        )
        .await?;
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------------------
// DELETE /db/{name}/graph-store
// ---------------------------------------------------------------------------

/// Drop or clear a graph.
pub async fn graph_store_delete(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    Query(params): Query<GraphStoreParams>,
) -> Result<Response, ApiError> {
    let target = resolve_target(&params)?;

    let sparql = match target {
        GraphTarget::Default => "DROP DEFAULT".to_string(),
        GraphTarget::Named(ref iri) => format!("DROP GRAPH <{iri}>"),
    };

    let timeout = state.effective_timeout(None);

    let result = QueryService::execute(
        state.databases(),
        state.metrics(),
        &db_name,
        &sparql,
        Some("sparql"),
        None,
        timeout,
    )
    .await;

    match result {
        Ok(_) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(e) => {
            // If the graph didn't exist, return 404.
            let msg = e.to_string();
            if msg.contains("not found") || msg.contains("does not exist") {
                Ok(StatusCode::NOT_FOUND.into_response())
            } else {
                Err(ApiError::from(e))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Body parsing
// ---------------------------------------------------------------------------

/// Parses the request body into N-Triples lines (ready for INSERT DATA).
///
/// Accepts `text/turtle` (converted line by line) and `application/n-triples`.
/// For Turtle input, each triple is converted to its N-Triples representation.
fn parse_body_to_ntriples(headers: &HeaderMap, body: &Bytes) -> Result<Vec<String>, ApiError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CT_NTRIPLES);

    let base_ct = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();

    let text = std::str::from_utf8(body)
        .map_err(|_| ApiError::bad_request("request body is not valid UTF-8"))?;

    match base_ct {
        CT_NTRIPLES => {
            // N-Triples: each non-empty, non-comment line is a triple.
            Ok(text
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(String::from)
                .collect())
        }
        CT_TURTLE => {
            // Full Turtle parsing requires grafeo-core >= 0.5.30.
            Err(ApiError::bad_request(
                "text/turtle input is not yet supported for Graph Store Protocol. \
                 Use application/n-triples instead.",
            ))
        }
        _ => Err(ApiError::bad_request(format!(
            "unsupported Content-Type: {base_ct}. Expected application/n-triples"
        ))),
    }
}

/// Builds an `INSERT DATA { ... }` SPARQL statement from N-Triples lines.
fn build_insert_data(target: &GraphTarget, nt_lines: &[String]) -> String {
    let triples_block = nt_lines.join("\n");
    match target {
        GraphTarget::Default => format!("INSERT DATA {{\n{triples_block}\n}}"),
        GraphTarget::Named(iri) => {
            format!("INSERT DATA {{ GRAPH <{iri}> {{\n{triples_block}\n}} }}")
        }
    }
}

/// Converts a Grafeo `Value` to an N-Triples term string.
fn value_to_nt_term(value: &grafeo_common::Value) -> String {
    use grafeo_common::Value;
    match value {
        Value::String(s) => {
            let s_str = s.to_string();
            if s_str.starts_with("http://")
                || s_str.starts_with("https://")
                || s_str.starts_with("urn:")
            {
                format!("<{s_str}>")
            } else if s_str.starts_with("_:") {
                s_str
            } else {
                // Escape the string for N-Triples.
                let escaped = s_str
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                format!("\"{escaped}\"")
            }
        }
        Value::Int64(i) => {
            format!("\"{i}\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        }
        Value::Float64(f) => {
            format!("\"{f}\"^^<http://www.w3.org/2001/XMLSchema#double>")
        }
        Value::Bool(b) => {
            format!("\"{b}\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
        }
        _ => {
            let s = format!("{value:?}");
            let escaped = s.replace('"', "\\\"");
            format!("\"{escaped}\"")
        }
    }
}
