//! devin2api — Rust port of the Devin proxy (`min9lin9/devin2api`).
//!
//! Module roots are stubs established by task 1; each owning task fills in
//! behavior per the parity plan.

pub mod auxiliary;
pub mod config;
pub mod dashboard;
pub mod debuglog;
pub mod domain;
pub mod metrics;
pub mod protocol;
/// QA harness (oracle runner, comparator, contracts manifest). Dev tooling
/// for the feature-gated `qa` binary and integration tests; not part of
/// the released daemon surface.
pub mod qa;
pub mod randid;
pub mod server;
pub mod upstream;
