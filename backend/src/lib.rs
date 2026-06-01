//! Kotatsu — read-only, on-demand browser over Tansu's native S3 storage.
//!
//! Library crate exposing the configuration, storage reader and HTTP layers,
//! shared by the binary (`main.rs`) and integration tests.

pub mod api;
pub mod config;
pub mod http;
pub mod schema;
pub mod state;
pub mod storage;
