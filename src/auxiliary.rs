//! Shared implementation for the auxiliary binaries (`probe`,
//! `protoextract`, `protocensus`, `loadtest`, `upstreamstub`) — the task-20
//! port of `G/cmd/{probe,protoextract,protocensus,loadtest,upstreamstub}`.
//! Descriptor extraction is deliberately byte-oriented: the source is an
//! opaque executable and is never executed.

pub mod census;
pub mod extract;
pub mod flatten;
pub mod goflag;
pub mod probe;
