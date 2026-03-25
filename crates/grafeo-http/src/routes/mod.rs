//! HTTP API route handlers.

pub mod admin;
pub mod batch;
pub mod database;
pub mod query;
#[cfg(feature = "replication")]
pub mod replication;
pub mod search;
#[cfg(feature = "sync")]
pub mod sync;
pub mod system;
pub mod transaction;
pub mod websocket;
