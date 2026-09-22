//! Stage file names and shared top-level file names — the cross-module
//! contract ported verbatim from `G/internal/debuglog/stages.go`.

/// `01-http-request.json` — the client's original request projection.
pub const STAGE_HTTP_REQUEST: &str = "01-http-request.json";
/// `02-request-messages.json` — the intermediate-model projection.
pub const STAGE_REQUEST_MESSAGES: &str = "02-request-messages.json";
/// `03-devin-request.json` — the first upstream wire request; retry shards
/// use [`stage_devin_request_attempt`].
pub const STAGE_DEVIN_REQUEST: &str = "03-devin-request.json";
/// `04-devin-response.jsonl` — raw upstream response frames.
pub const STAGE_DEVIN_RESPONSE: &str = "04-devin-response.jsonl";
/// `05-response-events.jsonl` — internal response event stream.
pub const STAGE_RESPONSE_EVENTS: &str = "05-response-events.jsonl";
/// `06-http-response.jsonl` — SSE frames sent to the client.
pub const STAGE_HTTP_RESPONSE: &str = "06-http-response.jsonl";

/// `attachments/` — per-request attachment subdirectory.
pub const ATTACHMENTS_DIR: &str = "attachments";
/// `meta.json` — request metadata (written at start and completion).
pub const META_FILE: &str = "meta.json";
/// `error.json` — the first failure point; capacity eviction keys on it.
pub const ERROR_FILE: &str = "error.json";
/// `index.jsonl` — cross-request index, one summary line per completion.
pub const INDEX_FILE: &str = "index.jsonl";
/// `stderr.log` — process stderr log (written by the deploy redirect).
pub const STDERR_FILE: &str = "stderr.log";
/// `bind-failure.json` — last listen bind failure record.
pub const BIND_FAILURE_FILE: &str = "bind-failure.json";

/// `http_read` — request body read failure (over limit / connection drop).
pub const ERR_STAGE_HTTP_READ: &str = "http_read";
/// `http_decode` — request body decode or intermediate validation failure.
pub const ERR_STAGE_HTTP_DECODE: &str = "http_decode";
/// `request_build` — local request projection failure; never hit upstream.
pub const ERR_STAGE_REQUEST_BUILD: &str = "request_build";
/// `provider_stream` — upstream stream failed mid-flight.
pub const ERR_STAGE_PROVIDER_STREAM: &str = "provider_stream";
/// `http_stream` — client SSE write failure (not a disconnect).
pub const ERR_STAGE_HTTP_STREAM: &str = "http_stream";
/// `response_event` — internal event → protocol frame projection failure.
pub const ERR_STAGE_RESPONSE_EVENT: &str = "response_event";
/// `http_encode` — response body serialization failure.
pub const ERR_STAGE_HTTP_ENCODE: &str = "http_encode";
/// `client_disconnected` — client disconnect/cancel ended the request.
pub const ERR_STAGE_CLIENT_DISCONNECTED: &str = "client_disconnected";
/// `devin_connect` — upstream semantic rejection (Connect-layer error).
pub const ERR_STAGE_DEVIN_CONNECT: &str = "devin_connect";
/// `devin_transport` — upstream transport break (EOF / truncated frame).
pub const ERR_STAGE_DEVIN_TRANSPORT: &str = "devin_transport";
/// `rate_gate` — local rate-gate fast-fail; never hit upstream.
pub const ERR_STAGE_RATE_GATE: &str = "rate_gate";

/// Shared stem of upstream request file names: the first request is
/// `03-devin-request.json`, retry N is `03-devin-request.attemptN.json`.
const DEVIN_REQUEST_STAGE_STEM: &str = "03-devin-request";

/// `StageDevinRequestAttempt(attempt)` — file name of upstream resend
/// `attempt` (attempt >= 2).
pub fn stage_devin_request_attempt(attempt: u32) -> String {
    format!("{DEVIN_REQUEST_STAGE_STEM}.attempt{attempt}.json")
}

/// `DevinRequestStages(dir)` — all upstream wire request files in a request
/// dir (main file plus attemptN shards), sorted by name.
pub fn devin_request_stages(dir: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_json = std::path::Path::new(&name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
        if name.starts_with(DEVIN_REQUEST_STAGE_STEM) && is_json {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

/// `isPayloadName` — whether a request-dir member belongs to the bulky
/// payload layer stripped by `payload_hours` (evidence files stay).
pub fn is_payload_name(name: &str) -> bool {
    if name.starts_with(&format!("{DEVIN_REQUEST_STAGE_STEM}.")) {
        return true;
    }
    matches!(
        name,
        STAGE_DEVIN_RESPONSE | STAGE_HTTP_RESPONSE | ATTACHMENTS_DIR
    )
}
