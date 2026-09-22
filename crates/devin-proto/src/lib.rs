//! Generated Devin protobuf/ConnectRPC bindings.
//!
//! The message and service modules under `generated` are produced by
//! `scripts/generate-proto.sh` (connectrpc-build + buffa, pinned versions)
//! from the flattened schema `proto/all-protos.proto` and are checked in so
//! normal builds need no upstream binaries or remote codegen. Do not edit
//! files under `src/generated/` by hand; regenerate instead.

pub use buffa;
pub use buffa_types;
pub use connectrpc;

/// Name of this workspace member, for provenance assertions.
pub const PROTO_CRATE: &str = "devin-proto";

/// Generated bindings for the flattened `exa.api_server_pb` package.
// The generator's own allow list covers clippy/style lints; the workspace
// `elided_lifetimes_in_paths` lint is silenced here because generated view
// types intentionally use hidden lifetimes. `clippy::pedantic` is allowed
// as a group: generated code is machine output whose style is fixed by
// connectrpc-build/buffa, so every pedantic lint is noise, not defects.
// `len_without_is_empty` is the one non-pedantic clippy lint the generated
// API shape trips. Keep this list in sync with `cargo clippy -p devin-proto`.
#[allow(
    elided_lifetimes_in_paths,
    clippy::pedantic,
    clippy::len_without_is_empty
)]
pub mod generated;
