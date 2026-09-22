//! Cross-request index (`logs/index.jsonl`) — port of
//! `G/internal/debuglog/index.go`. One summary line per completed request;
//! the file is capped at [`USAGE_REPLAY_TAIL_BYTES`] by rewriting its tail
//! half so startup replay always covers the whole file.

use serde::Deserialize;

use super::gojson::ObjWriter;

/// `indexFileCap` — default size cap for `index.jsonl`; past it the file is
/// rewritten keeping the tail half. Same value as the startup replay window
/// so aggregation always covers the full file.
pub const DEFAULT_INDEX_FILE_CAP: i64 = USAGE_REPLAY_TAIL_BYTES;

/// `usageReplayTailBytes` — startup replay reads at most this many tail
/// bytes of `index.jsonl` (256MB ≈ ~14 days of history).
pub const USAGE_REPLAY_TAIL_BYTES: i64 = 256 << 20;

/// `IndexEntry` — one `index.jsonl` line. Field order and `omitempty`
/// semantics match the Go struct exactly so Rust-appended lines are
/// byte-compatible.
// Four bools mirror the Go index-entry schema exactly; grouping them
// would change the serialized field set.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct IndexEntry {
    pub dir: String,
    pub started_at: String,
    pub duration_ms: i64,
    /// Latency breakdown; `None` = "did not happen" (Go `*int64` nil),
    /// `Some(0)` = "happened instantly".
    pub request_ready_ms: Option<i64>,
    pub upstream_sent_ms: Option<i64>,
    pub upstream_open_ms: Option<i64>,
    pub first_upstream_ms: Option<i64>,
    pub first_client_ms: Option<i64>,
    pub api: String,
    pub method: String,
    pub path: String,
    pub status_code: i64,
    pub result: String,
    pub requested_model: String,
    pub model: String,
    pub response_model: String,
    pub model_mismatch: bool,
    pub stream: bool,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub upstream_request_id: String,
    pub client_ip: String,
    pub key_hash: String,
    /// Client-supplied correlation ID (X-Request-Id etc).
    pub client_request_id: String,
    /// First failure stage (`http_decode`/`provider_stream`/...).
    pub error_stage: String,
    pub dropped_events: u64,
    /// Upstream rate-limit reset hint in seconds; 0 when not limited.
    pub retry_after_seconds: i64,
    /// Request was rate-limit terminated (upstream 429 / local gate /
    /// in-stream limit error) — `status_code` alone cannot see the 200 case.
    pub rate_limited: bool,
    /// Upstream resend count (attempt2+); details live in meta.json.
    pub retries: i64,
    /// "tool result then plain `end_turn`" suspicious finish marker.
    pub premature_end_turn: bool,
    /// Silent projection repairs total; details in meta.json `repairs`.
    pub repairs: i64,
}

impl IndexEntry {
    /// `json.Marshal(entry)` — compact, struct field order, `omitempty`.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("dir", &self.dir)
            .field_str("started_at", &self.started_at)
            .field_int("duration_ms", self.duration_ms)
            .field_opt_int_some("request_ready_ms", self.request_ready_ms)
            .field_opt_int_some("upstream_sent_ms", self.upstream_sent_ms)
            .field_opt_int_some("upstream_open_ms", self.upstream_open_ms)
            .field_opt_int_some("first_upstream_ms", self.first_upstream_ms)
            .field_opt_int_some("first_client_ms", self.first_client_ms)
            .field_str_nonempty("api", &self.api)
            .field_str("method", &self.method)
            .field_str("path", &self.path)
            .field_int("status_code", self.status_code)
            .field_str("result", &self.result)
            .field_str_nonempty("requested_model", &self.requested_model)
            .field_str_nonempty("model", &self.model)
            .field_str_nonempty("response_model", &self.response_model)
            .field_bool_true("model_mismatch", self.model_mismatch)
            .field_bool("stream", self.stream)
            .field_int_nonzero("input_tokens", self.input_tokens)
            .field_int_nonzero("output_tokens", self.output_tokens)
            .field_int_nonzero("cache_read_tokens", self.cache_read_tokens)
            .field_int_nonzero("cache_write_tokens", self.cache_write_tokens)
            .field_int_nonzero("reasoning_tokens", self.reasoning_tokens)
            .field_int_nonzero("total_tokens", self.total_tokens)
            .field_str_nonempty("upstream_request_id", &self.upstream_request_id)
            .field_str_nonempty("client_ip", &self.client_ip)
            .field_str_nonempty("key_hash", &self.key_hash)
            .field_str_nonempty("client_request_id", &self.client_request_id)
            .field_str_nonempty("error_stage", &self.error_stage)
            .field_uint_nonzero("dropped_events", self.dropped_events)
            .field_int_nonzero("retry_after_seconds", self.retry_after_seconds)
            .field_bool_true("rate_limited", self.rate_limited)
            .field_int_nonzero("retries", self.retries)
            .field_bool_true("premature_end_turn", self.premature_end_turn)
            .field_int_nonzero("repairs", self.repairs);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }

    /// Parse one index line; `None` on malformed input (Go callers skip
    /// unparseable lines the same way).
    pub fn parse(line: &[u8]) -> Option<Self> {
        serde_json::from_slice(line).ok()
    }
}

/// `optionalLatency` — the `-1` sentinel becomes `None`; every other value
/// (including a legitimate 0ms) passes through.
pub fn optional_latency(ms: i64) -> Option<i64> {
    if ms < 0 { None } else { Some(ms) }
}
