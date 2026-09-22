//! Upstream Devin connectivity: transport, catalog, request/response
//! projection, retry orchestration and the persisted rate gate.

pub mod catalog;
mod envproxy;
pub mod gate;
pub mod request;
pub mod response;
pub mod retry;
pub mod sanitize;
pub mod tool_definition;
pub mod transport;
