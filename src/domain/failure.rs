//! The single source of truth for error semantics: the classified record
//! [`Failure`] travels with the error chain so type information is not lost
//! before flattening to a string; consumers recover it once via [`classify`]
//! instead of re-deriving from message text.
//!
//! Port of `G/internal/llm/failure.go`. Fields split in two layers:
//! `code`/`message`/`cause`/`local_gate`/`upstream_fault`/
//! `retry_after_seconds`/`retry_after_minute` are structurally known facts
//! filled by producers (adapter, rate gate); the remaining derived fields are
//! filled uniformly by `derive` — reading a record without `classify` leaves
//! derived fields zeroed. `derive` never overrides producer-set fields.
//!
//! String parsing (code prefix, text markers, hint regexes) exists only in
//! `classify`'s fallback branch.

use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connectrpc::ConnectError;

use super::response::AssistantMessage;

/// Sentinel equivalent of Go's `context.Canceled` for error chains.
///
/// Rust producers wrap cancellation with this type so [`classify`] can
/// downcast it the way Go downcasts `context.Canceled`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context canceled")
    }
}

impl Error for Canceled {}

/// Sentinel equivalent of Go's `context.DeadlineExceeded` for error chains.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeadlineExceeded;

impl fmt::Display for DeadlineExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("context deadline exceeded")
    }
}

impl Error for DeadlineExceeded {}

/// A classified record of a request failure; implements [`Error`].
// The bool set mirrors Go's failure classification flags one-for-one;
// grouping them would obscure the port.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default)]
pub struct Failure {
    /// Connect protocol error code (`resource_exhausted` etc.); empty for
    /// non-Connect errors. Replaces the `"<code>: <msg>"` text-prefix dialect.
    pub code: String,
    /// Full human-readable error text (without the code prefix).
    pub message: String,
    /// Original error; the `source` chain (cancellation sentinels etc.) is
    /// not lost.
    pub cause: Option<Arc<dyn Error + Send + Sync>>,
    /// True when a local rate gate rejected the request — it never reached
    /// upstream; attribution differs from a real upstream refusal
    /// (`devin_connect`).
    pub local_gate: bool,
    /// True when responsibility lies upstream regardless of what `code`
    /// claims: transport breaks get wrapped by connect-go into
    /// `invalid_argument`/`internal` text, and upstream also packs real
    /// internal faults into fixable codes (the "an internal error occurred"
    /// template) — neither should be treated as a client request error.
    pub upstream_fault: bool,
    /// Rate-limit wait seconds structurally known by the producer (local
    /// gate); upstream only gives a text hint, parsed by `derive`.
    pub retry_after_seconds: i64,
    /// True when the hint is minute-granularity: the reset instant must align
    /// up to the upstream minute-bucket boundary (floor-rounded remainder),
    /// not be treated as exact seconds.
    pub retry_after_minute: bool,

    // The fields below are derived by `derive`.
    /// Request exceeded the upstream context window — client-fixable.
    pub context_length: bool,
    /// Rate-limit-class failure (`resource_exhausted` or local gate);
    /// always false when `upstream_fault` is set — a transport break
    /// masquerading as that code (e.g. http2 `ENHANCE_YOUR_CALM` mapped to
    /// `resource_exhausted` by connect-go) is not an upstream rate-limit
    /// signal.
    pub rate_limited: bool,
    /// Client-initiated disconnect/cancel.
    pub canceled: bool,
    /// Wait timeout (`DeadlineExceeded` or upstream equivalent).
    pub timeout: bool,
    /// Client-fixable request error (4xx-family code, context overflow) —
    /// error type and status map back to `invalid_request`/4xx. Always false
    /// when `upstream_fault` is set.
    pub client_fixable: bool,
    /// Diagnostic anchor extracted from an upstream error tail
    /// `"(trace ID: …)"`.
    pub trace_id: String,
    /// Upstream text carried a reset declaration (explicit 0 included) —
    /// `retry_after_seconds == 0` therefore splits into two semantics: no
    /// declaration, and "reset time is now". Observed "reset in 0 seconds"
    /// always arrives at a bucket boundary: the new bucket is already blown
    /// with no extra ban, and `rate_limit_reset` returns `now` for the
    /// latter.
    pub reset_hint: bool,
}

impl Failure {
    /// A record with only a message — the Rust stand-in for Go's plain
    /// `errors.New`/`fmt.Errorf` decode errors (no code, no cause).
    pub fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::default()
        }
    }

    /// An `invalid_argument` record — the code every decode-layer rejection
    /// carries (missing fields, unsupported shapes, validation failures).
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_argument".to_string(),
            message: message.into(),
            ..Self::default()
        }
    }

    /// Attaches an owned cause, preserving the `source` chain.
    #[must_use]
    pub fn with_cause(mut self, cause: impl Error + Send + Sync + 'static) -> Self {
        self.cause = Some(Arc::new(cause));
        self
    }

    /// Returns a copy with `prefix` prepended to the message — the Rust
    /// equivalent of Go's `fmt.Errorf("prefix: %w", err)` on a `*Failure`,
    /// which preserves the code and the wrapped cause.
    #[must_use]
    pub fn prefixed(mut self, prefix: impl fmt::Display) -> Self {
        self.message = format!("{prefix}: {}", self.message);
        self
    }

    /// Resolves the rate-limit reset instant to an absolute time: exact
    /// producer-known seconds map to `now + N`; a minute-granularity hint is
    /// the upstream's floor-rounded remainder of the current minute bucket
    /// ("reset in 1 minute" really means end of this bucket, at most ~119s
    /// away), so it aligns up to the next `:59` bucket boundary — upstream's
    /// clock runs ~1s fast and observed boundaries land at local
    /// `:58.5`–`:59.5`. An explicit 0-second declaration ("reset in 0
    /// seconds", `reset_hint` set) returns `now` — the cooldown latch expires
    /// at the declared instant instead of a fallback latch duration.
    pub fn rate_limit_reset(&self, now: SystemTime) -> Option<SystemTime> {
        if self.retry_after_seconds <= 0 && !self.reset_hint {
            return None;
        }
        if self.retry_after_minute {
            // The :59 boundary of the minute bucket containing now+Nmin; if
            // that instant is already past :59, take the next minute's :59.
            // N=0 ("reset in 0 minutes") aligns to this bucket's :59 —
            // minute-granularity 0 is floor-rounded, the real remainder is
            // at most ~59s.
            let Some(target) = now.checked_add(Duration::from_secs(
                self.retry_after_seconds.max(0).cast_unsigned(),
            )) else {
                // Go's time arithmetic wraps on absurd hints; a saturated
                // "now" is the bounded equivalent (never panics).
                return Some(now);
            };
            let secs = target
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut reset_secs = secs - secs % 60 + 59;
            if reset_secs <= secs {
                reset_secs += 60;
            }
            return Some(SystemTime::UNIX_EPOCH + Duration::from_secs(reset_secs));
        }
        if self.retry_after_seconds <= 0 {
            // Explicit 0 seconds: the declared reset instant is now.
            return Some(now);
        }
        // Go's time arithmetic wraps on absurd hints; "now" is the bounded
        // equivalent (never panics).
        Some(
            now.checked_add(Duration::from_secs(
                self.retry_after_seconds.cast_unsigned(),
            ))
            .unwrap_or(now),
        )
    }
}

impl PartialEq for Failure {
    /// Field equality ignoring `cause` — Go tests compare records by fields;
    /// the retained error chain is identity, not value.
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
            && self.message == other.message
            && self.local_gate == other.local_gate
            && self.upstream_fault == other.upstream_fault
            && self.retry_after_seconds == other.retry_after_seconds
            && self.retry_after_minute == other.retry_after_minute
            && self.context_length == other.context_length
            && self.rate_limited == other.rate_limited
            && self.canceled == other.canceled
            && self.timeout == other.timeout
            && self.client_fixable == other.client_fixable
            && self.trace_id == other.trace_id
            && self.reset_hint == other.reset_hint
    }
}

impl fmt::Display for Failure {
    /// Keeps the existing wire format `"<code>: <msg>"` — the old text
    /// contract (logs, client display, remaining text-fallback parsing) does
    /// not change with record-ification.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.code.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl Error for Failure {
    /// Exposes the original error chain; cancellation/deadline sentinels
    /// remain detectable.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // &(dyn Error + Send + Sync) coerces to &dyn Error at coercion
        // sites (auto-trait drop), which `as` casts do not allow.
        self.cause.as_deref().map(|cause| cause as _)
    }
}

/// Facts gathered by walking an error chain once — the Rust equivalent of
/// Go's `errors.Is`/`errors.As` probes on `failure.Cause`.
// Four independent fact probes; a bitfield would add nothing.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default, Clone, Copy)]
struct ChainFacts {
    canceled: bool,
    deadline: bool,
    eof: bool,
    net: bool,
}

fn chain_facts(err: &(dyn Error + 'static)) -> ChainFacts {
    let mut facts = ChainFacts::default();
    let mut current = Some(err);
    while let Some(err) = current {
        if err.is::<Canceled>() {
            facts.canceled = true;
        }
        if err.is::<DeadlineExceeded>() || err.is::<tokio::time::error::Elapsed>() {
            facts.deadline = true;
        }
        if let Some(io_err) = err.downcast_ref::<io::Error>() {
            if io_err.kind() == io::ErrorKind::UnexpectedEof {
                facts.eof = true;
            }
            // Rust's io::Error is the closest equivalent of Go's net.Error
            // probe — transport read/write/connect failures surface through
            // it (reqwest/hyper wrap it in their source chains).
            facts.net = true;
        }
        if let Some(req_err) = err.downcast_ref::<reqwest::Error>() {
            if req_err.is_timeout() {
                facts.deadline = true;
            }
            if req_err.is_connect() || req_err.is_request() || req_err.is_body() {
                facts.net = true;
            }
        }
        current = err.source();
    }
    facts
}

/// Normalizes any error into a classified record: a `Failure` in the chain
/// is taken directly and its derived fields filled in, a `ConnectError`
/// contributes its structural fields, everything else falls back to text
/// (code prefix + marker matching). Derivation is idempotent and pure;
/// repeated calls agree.
///
/// The retained `cause` is not stored on the returned record (the input is
/// borrowed); chain facts are evaluated eagerly during classification, which
/// is all `derive` needs.
pub fn classify(err: &(dyn Error + 'static)) -> Failure {
    let facts = chain_facts(err);
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(typed) = err.downcast_ref::<Failure>() {
            return derive(typed.clone(), facts);
        }
        current = err.source();
    }
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(connect_err) = err.downcast_ref::<ConnectError>() {
            // `message` carries no code prefix; `Display` reassembles the
            // original wire text. An absent message is not backfilled from
            // `ConnectError`'s Display — that would duplicate the prefix.
            let mut failure = Failure::plain(
                connect_err
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            );
            failure.code = connect_err.code.as_str().to_string();
            return derive(failure, facts);
        }
        current = err.source();
    }
    let mut failure = Failure::plain(err.to_string());
    if let Some((code, rest)) = split_code_prefix(&failure.message) {
        failure.code = code;
        failure.message = rest;
    }
    derive(failure, facts)
}

/// Returns the classified record of an error assistant message: prefers the
/// typed `Failure` carried by the producer, falls back to classifying the
/// `error_message` text when absent.
pub fn failure_of(message: Option<&AssistantMessage>) -> Failure {
    if let Some(message) = message
        && let Some(failure) = &message.failure
    {
        return derive((**failure).clone(), chain_facts_of_failure(failure));
    }
    classify_text(message.map_or("", |message| message.error_message.as_str()))
}

fn chain_facts_of_failure(failure: &Failure) -> ChainFacts {
    match &failure.cause {
        Some(cause) => chain_facts(cause.as_ref()),
        None => ChainFacts::default(),
    }
}

/// Classifies bare text with no error carrier (flattened `error_message` at
/// the event layer, test constructions).
pub fn classify_text(message: &str) -> Failure {
    let mut failure = Failure::plain(message);
    if let Some((code, rest)) = split_code_prefix(&failure.message) {
        failure.code = code;
        failure.message = rest;
    }
    derive(failure, ChainFacts::default())
}

/// Fills derived fields from the raw ones; producer-set fields are kept —
/// producers can know these facts structurally (local gate, upstream detail
/// fields) without text markers.
fn derive(mut failure: Failure, facts: ChainFacts) -> Failure {
    let message = failure.message.to_lowercase();
    for marker in CONTEXT_LENGTH_MARKERS {
        if message.contains(marker) {
            failure.context_length = true;
            break;
        }
    }
    // connect-go also maps a peer RST_STREAM CANCEL to the canceled code
    // (when the local ctx was not cancelled) — that is an upstream transport
    // break, not a client cancel: on http2 transport wording the code
    // derivation does not hold and the call is handed back to
    // `transport_break` for `upstream_fault`.
    let code_canceled = failure.code == "canceled" && !is_http2_transport_error(&failure.message);
    failure.canceled =
        failure.canceled || code_canceled || facts.canceled || message.contains("context canceled");
    failure.timeout = failure.timeout
        || failure.code == "deadline_exceeded"
        || facts.deadline
        || message.contains("context deadline exceeded");
    failure.upstream_fault = failure.upstream_fault
        || message.contains(INTERNAL_ERROR_MARKER)
        || transport_break(&failure, facts);
    // With `upstream_fault` set, the code's claimed semantics are untrusted:
    // `resource_exhausted` may be a transport break in disguise (http2
    // ENHANCE_YOUR_CALM) and must not arm a cooldown latch or be reported to
    // the client as `rate_limit_exceeded`.
    failure.rate_limited = failure.rate_limited
        || (!failure.upstream_fault
            && (failure.code == "resource_exhausted" || failure.local_gate));
    failure.client_fixable = failure.client_fixable
        || (!failure.upstream_fault
            && (failure.context_length || REQUEST_CODE_SET.contains(&failure.code.as_str())));
    if let Some(trace_id) = extract_trace_id(&failure.message) {
        failure.trace_id = trace_id;
    }
    if failure.retry_after_seconds == 0 {
        let (seconds, minute, hint) = parse_reset_hint(&failure.message);
        failure.retry_after_seconds = seconds;
        failure.retry_after_minute = minute;
        failure.reset_hint = hint;
    }
    failure
}

/// The fixed template text of an upstream internal fault — Devin packs real
/// internal errors into fixable codes like `invalid_argument`/
/// `permission_denied`; the text is its only reliable self-report.
const INTERNAL_ERROR_MARKER: &str = "an internal error occurred";

/// Detects a transport-layer break: connect-go wraps RoundTrip/read-write
/// breaks as `CodeUnavailable`, truncated envelope frames as
/// `CodeInvalidArgument` "protocol error: ...", bare mid-stream EOF as
/// `CodeUnknown`, and peer `RST_STREAM/GOAWAY` into semantic codes
/// (`REFUSED_STREAM`→unavailable, `ENHANCE_YOUR_CALM`→`resource_exhausted`
/// etc.) — the judgement looks at io/net errors in the unwrap chain and the
/// connect/http2 stack's fixed wording, not the code itself. Errors already
/// classified as cancel/timeout do not count as transport faults.
fn transport_break(failure: &Failure, facts: ChainFacts) -> bool {
    if failure.canceled || failure.timeout {
        return false;
    }
    if facts.eof || facts.net {
        return true;
    }
    is_http2_transport_error(&failure.message)
        || is_idle_conn_closed_error(&failure.message)
        || is_connect_stream_break(&failure.message)
        || ((failure.code == "invalid_argument" || failure.code == "internal")
            && failure.message.starts_with("protocol error:"))
}

/// The Rust connectrpc client's fixed wording for local frame-parse
/// failures — the equivalents of connect-go's "protocol error: ..." that
/// Go's `isTransientConnectError` recognizes: a truncated envelope
/// sequence (mid-stream EOF missing its `END_STREAM` terminus) and a body
/// read break are transport breaks, not upstream semantics. Upstream
/// semantic errors arrive via `END_STREAM` metadata and never carry this
/// wording.
pub fn is_connect_stream_break(message: &str) -> bool {
    message.starts_with("Connect streaming response ended without END_STREAM envelope")
        || message.starts_with("failed to read response body")
        // connectrpc/buffa reports an oversized or corrupt envelope using
        // resource_exhausted, but this wording is a local frame parser
        // failure, not an upstream quota refusal. Treating it as rate limit
        // would emit a retryable 429 instead of a transport failure.
        || (message.starts_with("message size ") && message.contains(" exceeds limit "))
}

/// Fixed wording the local http2 stack writes into error text.
/// connect-go maps peer `RST_STREAM` to semantic codes by trailing code
/// (`REFUSED_STREAM`→unavailable, `PROTOCOL_ERROR`/`INTERNAL_ERROR`
/// etc.→internal, `ENHANCE_YOUR_CALM`→`resource_exhausted`,
/// `INADEQUATE_SECURITY`→`permission_denied`, `CANCEL`→canceled/
/// `deadline_exceeded`); the vendored http2 types are not exported, so only
/// the wording is recognizable — shaped like
/// "stream error: stream ID N; CODE; received from peer", with
/// `ENHANCE_YOUR_CALM`/`INADEQUATE_SECURITY` additionally wrapped in
/// "bandwidth exhausted: "/"transport protocol insecure: " prefixes, hence
/// `contains` not `starts_with`. GOAWAY does not take that mapping and
/// surfaces at connect time as
/// "unavailable: http2: server sent GOAWAY and closed the connection; ...".
/// Real upstream semantic errors arrive via `EndStream` trailers (connect
/// prefers the trailer error over the transport error) and never carry this
/// local wording — a hit means a transport break and the mapped code is
/// semantics-free.
const HTTP2_TRANSPORT_MARKERS: &[&str] = &["stream error: stream ID ", "http2: server sent GOAWAY"];

/// Reports whether error text carries the local http2 stack's transport
/// wording (`RST_STREAM/GOAWAY`). Either `ConnectError::message` or the whole
/// `Display` text may be passed — the markers never appear in a code prefix.
pub fn is_http2_transport_error(message: &str) -> bool {
    HTTP2_TRANSPORT_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
}

/// Reports whether error text is the net/http connection pool's
/// `errServerClosedIdle` fixed wording ("http: server closed idle
/// connection"): raised when the pool reuses an idle connection the peer
/// already closed — h1-pool-specific (`force_http1`). The failure happens
/// before any byte is written, same class as RST/GOAWAY transport breaks,
/// safe to retry. h2 has no such shape: GOAWAY arrives before the reuse
/// race.
pub fn is_idle_conn_closed_error(message: &str) -> bool {
    message.contains("server closed idle connection")
}

/// All Connect protocol error codes — the text fallback only treats a prefix
/// in this set as a code, so local text like "read request: …" is not
/// mistaken for a regime marker.
const CONNECT_CODES: &[&str] = &[
    "canceled",
    "unknown",
    "invalid_argument",
    "deadline_exceeded",
    "not_found",
    "already_exists",
    "permission_denied",
    "resource_exhausted",
    "failed_precondition",
    "aborted",
    "out_of_range",
    "unimplemented",
    "internal",
    "unavailable",
    "data_loss",
    "unauthenticated",
];

/// The family of caller-fixable request-error codes — same source as
/// `HTTPStatus`'s 4xx branch. `failed_precondition` is observed to be a
/// request-shape/pre-state problem (e.g. a non-CASCADE `request_type`
/// lacking a real session), same class as `invalid_argument`; Devin upstream
/// folds content-policy blocks, invalid model UIDs and unauthorized models
/// into `permission_denied`, equally normalizable.
const REQUEST_CODE_SET: &[&str] = &[
    "invalid_argument",
    "failed_precondition",
    "permission_denied",
];

/// Splits the `"<code>: <msg>"` dialect into structural fields: only a
/// prefix hitting a known Connect code counts; returns the code and the
/// body without the prefix — `message` stores the stripped body so
/// `Display` reassembles byte-identical wire text.
fn split_code_prefix(message: &str) -> Option<(String, String)> {
    let index = message.find(':')?;
    let code = message[..index].trim();
    if !CONNECT_CODES.contains(&code) {
        return None;
    }
    Some((code.to_string(), message[index + 1..].trim().to_string()))
}

/// Text signatures upstream uses for "input exceeds the context window".
const CONTEXT_LENGTH_MARKERS: &[&str] = &[
    "prompt is too long",
    "context length",
    "context window",
    "maximum context",
    "too many tokens",
];

/// Extracts the `(trace ID: …)` tail marker of upstream in-stream errors —
/// the equivalent of Go's `\(trace ID: ([^)\s]+)\)` where `\s` is
/// `[\t\n\f\r ]`. Upstream error text is uniformly a vague "internal
/// error"; the trace ID is the only diagnostic anchor.
fn extract_trace_id(message: &str) -> Option<String> {
    const MARKER: &str = "(trace ID: ";
    let start = message.find(MARKER)? + MARKER.len();
    let rest = &message[start..];
    let end = rest.find([')', '\t', '\n', '\x0C', '\r', ' '])?;
    if rest.as_bytes()[end] != b')' || end == 0 {
        return None;
    }
    Some(rest[..end].to_string())
}

/// Parses the retry window out of upstream rate-limit text. Two units are
/// observed: under a minute remaining it reports "reset in N seconds",
/// longer waits report "reset in N minute(s)" (floor-rounded). Upstream
/// gives neither a `Retry-After` header nor a `RetryInfo` detail — this text
/// is the only actionable hint. Equivalent of Go's
/// `(?i)reset in (\d+)\s*(seconds?|minutes?)`.
///
/// Three-state result: no hint (`ok=false`), explicit 0 (`ok=true`,
/// `seconds=0` — upstream reports "reset in 0 seconds" at a bucket boundary,
/// meaning "new bucket already blown, no extra ban", reset time is now),
/// positive wait. Returns literal seconds (minutes ×60); for an actionable
/// wait/absolute instant use `rate_limit_reset` — a minute hint is the
/// floor-rounded bucket remainder and needs upward alignment.
fn parse_reset_hint(message: &str) -> (i64, bool, bool) {
    const MARKER: &str = "reset in ";
    let lower = message.to_lowercase();
    let mut search_from = 0usize;
    while let Some(found) = lower[search_from..].find(MARKER) {
        let digits_start = search_from + found + MARKER.len();
        let digits_len = lower[digits_start..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        if digits_len == 0 {
            search_from = digits_start;
            continue;
        }
        let digits_end = digits_start + digits_len;
        let unit_start = digits_end
            + lower[digits_end..]
                .bytes()
                .take_while(|b| matches!(b, b'\t' | b'\n' | b'\x0B' | b'\x0C' | b'\r' | b' '))
                .count();
        let unit = &lower[unit_start..];
        let minute = if unit.starts_with("second") {
            false
        } else if unit.starts_with("minute") {
            true
        } else {
            search_from = digits_end;
            continue;
        };
        let Ok(n) = lower[digits_start..digits_end].parse::<i64>() else {
            // Go's strconv.Atoi overflow path: the match exists but the
            // number is unrepresentable — no hint.
            return (0, false, false);
        };
        if minute {
            return (n.saturating_mul(60), true, true);
        }
        return (n, false, true);
    }
    (0, false, false)
}
