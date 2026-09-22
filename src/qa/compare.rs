//! Differential comparator: canonicalizes volatile fields (timestamps,
//! durations, generated ids, resource metrics, directory paths) and diffs
//! two captured transcripts.
//!
//! Contract (from the plan): canonicalization may normalize time, random
//! ids and directory names only. Status, error kinds, ordering, usage,
//! signatures, protobuf presence, retry counts and truncation flags are
//! never normalized. JSON object key order is insignificant (JSON
//! semantics); array order is always significant unless a path is
//! explicitly opted into `unordered_arrays`.

use std::collections::BTreeMap;
use std::fmt;

/// Canonicalization rules. The default set covers the volatile surface of
/// the Go daemon; later tasks may extend it but may not add rules that
/// mask semantic fields.
#[derive(Debug, Clone)]
pub struct CanonRules {
    /// Object keys whose values are timestamps (string or epoch number).
    pub timestamp_keys: Vec<String>,
    /// Object keys whose values are durations.
    pub duration_keys: Vec<String>,
    /// Object keys whose values are generated ids (canonicalized even when
    /// they do not match a known id pattern).
    pub id_keys: Vec<String>,
    /// Object keys whose values are resource metrics (rss/cpu/uptime/pid).
    pub metric_keys: Vec<String>,
    /// Literal path prefixes replaced with `<DIR:label>` wherever they
    /// appear in strings or text lines.
    pub path_substitutions: Vec<(String, String)>,
    /// JSON pointer prefixes whose arrays compare order-insensitively.
    /// Empty by default: ordering is semantic.
    pub unordered_arrays: Vec<String>,
    /// Header names whose values are volatile (date, request-id, ...).
    pub volatile_headers: Vec<String>,
}

impl Default for CanonRules {
    fn default() -> Self {
        Self {
            timestamp_keys: [
                "created",
                "created_at",
                "completed_at",
                "timestamp",
                "started_at",
                "finished_at",
                "loaded_at",
                "file_mtime",
                "at",
                "first_at",
                "last_at",
                "recovered_at",
            ]
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
            duration_keys: ["duration_ms", "retry_after", "uptime_seconds"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            id_keys: ["request_id", "response_id", "message_id", "session_id"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            metric_keys: [
                "pid",
                "active_requests",
                "rss_bytes",
                "cpu_seconds",
                "goroutines",
                "heap_alloc",
            ]
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
            path_substitutions: vec![],
            unordered_arrays: vec![],
            volatile_headers: ["date", "request-id", "retry-after", "keep-alive"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        }
    }
}

/// One captured HTTP response (or synthesized equivalent).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CapturedResponse {
    pub status: u16,
    /// Lowercased header names, in wire order.
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// One named case result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CaseResult {
    pub name: String,
    pub response: CapturedResponse,
}

/// A single difference found by a comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff {
    /// Where the difference is (JSON pointer, `case:name`, `header:name`...).
    pub path: String,
    pub expected: String,
    pub actual: String,
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: expected {}, got {}",
            self.path, self.expected, self.actual
        )
    }
}

/// Outcome of a comparison.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub diffs: Vec<Diff>,
    /// Number of cases compared (0 is never a pass).
    pub cases: usize,
}

impl Verdict {
    /// A comparison passes only when there are no diffs AND at least one
    /// case executed — zero executed cases is a failure per the QA contract.
    pub fn matched(&self) -> bool {
        self.diffs.is_empty() && self.cases > 0
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.matched() {
            return write!(f, "match ({} cases)", self.cases);
        }
        if self.cases == 0 {
            return write!(f, "FAIL: zero cases executed");
        }
        writeln!(
            f,
            "FAIL: {} diff(s) over {} cases",
            self.diffs.len(),
            self.cases
        )?;
        for d in &self.diffs {
            writeln!(f, "  {d}")?;
        }
        Ok(())
    }
}

/// Per-document canonicalization state: maps each distinct volatile value
/// to a stable placeholder so cross-event linkage is preserved.
struct Canon<'a> {
    rules: &'a CanonRules,
    ids: BTreeMap<String, usize>,
}

impl<'a> Canon<'a> {
    fn new(rules: &'a CanonRules) -> Self {
        Self {
            rules,
            ids: BTreeMap::new(),
        }
    }

    fn id_placeholder(&mut self, raw: &str) -> String {
        let next = self.ids.len() + 1;
        let n = *self.ids.entry(raw.to_string()).or_insert(next);
        format!("<id#{n}>")
    }

    fn canonicalize_value(
        &mut self,
        key: Option<&str>,
        value: &serde_json::Value,
        path: &str,
    ) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, v) in map {
                    let child = format!("{path}/{k}");
                    out.insert(k.clone(), self.canonicalize_value(Some(k), v, &child));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(items) => {
                let mut out: Vec<serde_json::Value> = items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| self.canonicalize_value(None, v, &format!("{path}/{i}")))
                    .collect();
                if self
                    .rules
                    .unordered_arrays
                    .iter()
                    .any(|p| path.starts_with(p.as_str()))
                {
                    out.sort_by_key(canonical_sort_key);
                }
                serde_json::Value::Array(out)
            }
            serde_json::Value::String(s) => {
                serde_json::Value::String(self.canonicalize_string(key, s))
            }
            serde_json::Value::Number(n) => {
                if let Some(k) = key
                    && (self.rules.timestamp_keys.iter().any(|t| t == k)
                        || self.rules.duration_keys.iter().any(|t| t == k)
                        || self.rules.metric_keys.iter().any(|t| t == k))
                {
                    return serde_json::Value::String(format!("<num:{k}>"));
                }
                serde_json::Value::Number(n.clone())
            }
            other => other.clone(),
        }
    }

    fn canonicalize_string(&mut self, key: Option<&str>, s: &str) -> String {
        let mut s = s.to_string();
        for (from, label) in &self.rules.path_substitutions {
            if s.contains(from.as_str()) {
                s = s.replace(from.as_str(), &format!("<DIR:{label}>"));
            }
        }
        if let Some(k) = key {
            if self.rules.timestamp_keys.iter().any(|t| t == k) {
                return "<timestamp>".into();
            }
            if self.rules.duration_keys.iter().any(|t| t == k) {
                return "<duration>".into();
            }
            if self.rules.metric_keys.iter().any(|t| t == k) {
                return "<metric>".into();
            }
            if self.rules.id_keys.iter().any(|t| t == k) {
                return self.id_placeholder(&s);
            }
        }
        if let Some(kind) = volatile_pattern(&s) {
            return match kind {
                Pattern::GeneratedId => self.id_placeholder(&s),
                other => format!("<{other}>"),
            };
        }
        // Path-like strings: canonicalize volatile *segments* (request-dir
        // names embed a timestamp + random suffix, e.g.
        // `2026-09-16T03-00-00_ab1cd2`) while keeping literal segments.
        if s.contains('/') {
            return s
                .split('/')
                .map(|seg| {
                    if is_request_dir_segment(seg) {
                        "<dir>".to_string()
                    } else {
                        match volatile_pattern(seg) {
                            Some(Pattern::GeneratedId) => self.id_placeholder(seg),
                            Some(other) => format!("<{other}>"),
                            None => seg.to_string(),
                        }
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
        }
        s
    }
}

#[derive(Debug, Clone, Copy)]
enum Pattern {
    Timestamp,
    Duration,
    Uuid,
    GeneratedId,
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Timestamp => "timestamp",
            Self::Duration => "duration",
            Self::Uuid => "uuid",
            Self::GeneratedId => "id",
        })
    }
}

/// Whole-string volatile patterns. Only full matches canonicalize — a
/// timestamp embedded in prose is content, not a volatile field.
fn volatile_pattern(s: &str) -> Option<Pattern> {
    if is_rfc3339(s) {
        return Some(Pattern::Timestamp);
    }
    if is_uuid(s) {
        return Some(Pattern::Uuid);
    }
    if is_go_duration(s) {
        return Some(Pattern::Duration);
    }
    if is_generated_id(s) {
        return Some(Pattern::GeneratedId);
    }
    None
}

/// RFC3339 / ISO-8601 timestamps: `2026-09-16T03:00:00Z`,
/// `...T03:00:00.123456789+02:00`, also date-only `2026-09-16`.
fn is_rfc3339(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 10 {
        return false;
    }
    let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    if !(digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && b[4] == b'-'
        && digit(5)
        && digit(6)
        && b[7] == b'-'
        && digit(8)
        && digit(9))
    {
        return false;
    }
    if b.len() == 10 {
        return true;
    }
    if b[10] != b'T' && b[10] != b't' && b[10] != b' ' {
        return false;
    }
    // hh:mm:ss then optional fraction and Z or ±hh:mm offset.
    if b.len() < 19
        || !(digit(11)
            && digit(12)
            && b[13] == b':'
            && digit(14)
            && digit(15)
            && b[16] == b':'
            && digit(17)
            && digit(18))
    {
        return false;
    }
    let mut i = 19;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    match b.get(i) {
        Some(&b'Z' | &b'z') => i + 1 == b.len(),
        Some(&b'+' | &b'-') => {
            b.len() == i + 6
                && b[i + 3] == b':'
                && digit(i + 1)
                && digit(i + 2)
                && digit(i + 4)
                && digit(i + 5)
        }
        None => i == b.len(),
        _ => false,
    }
}

/// UUID: 8-4-4-4-12 hex.
fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if c != b'-' {
                    return false;
                }
            }
            _ => {
                if !c.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

/// Go-style duration: `1.5s`, `250ms`, `2m30s`, `-5s`, `1h2m3.004s`, `10µs`.
fn is_go_duration(s: &str) -> bool {
    // Longest unit first so "ms" is not eaten as "m" + "s".
    const UNITS: &[&str] = &["ns", "us", "\u{b5}s", "\u{3bc}s", "ms", "s", "m", "h"];
    let mut rest = s.strip_prefix(['-', '+']).unwrap_or(s);
    let mut units = 0usize;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let mut i = digits;
        if rest[i..].starts_with('.') {
            let frac = rest[i + 1..].bytes().take_while(u8::is_ascii_digit).count();
            if frac == 0 {
                return false;
            }
            i += 1 + frac;
        }
        if i == 0 {
            return false;
        }
        match UNITS.iter().find(|u| rest[i..].starts_with(*u)) {
            Some(unit) => {
                units += 1;
                rest = &rest[i + unit.len()..];
            }
            None => return false,
        }
    }
    units > 0
}

/// Request-directory segment: `YYYY-MM-DDThh-mm-ss[_suffix]` — the Go
/// debuglog request dir name shape (timestamp + random suffix).
fn is_request_dir_segment(seg: &str) -> bool {
    let b = seg.as_bytes();
    if b.len() < 19 {
        return false;
    }
    let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && b[4] == b'-'
        && digit(5)
        && digit(6)
        && b[7] == b'-'
        && digit(8)
        && digit(9)
        && (b[10] == b'T' || b[10] == b't')
        && digit(11)
        && digit(12)
        && b[13] == b'-'
        && digit(14)
        && digit(15)
        && b[16] == b'-'
        && digit(17)
        && digit(18)
        && (b.len() == 19 || b[19] == b'_' || b[19] == b'.')
}

/// Generated ids: known prefixes (`req_`, `resp_`, `msg_`, `chatcmpl-`,
/// `item_`, `call_`, `toolu_`, `sess_`, `dir-`...) followed by enough
/// hex/alnum entropy, or a bare 24+ char hex string.
fn is_generated_id(s: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "req_",
        "resp_",
        "msg_",
        "chatcmpl-",
        "item_",
        "call_",
        "toolu_",
        "sess_",
        "fc_",
        "rs_",
        "msg-",
        "req-",
    ];
    for prefix in PREFIXES {
        if let Some(rest) = s.strip_prefix(prefix)
            && rest.len() >= 8
            && rest
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return true;
        }
    }
    false
}

fn canonical_sort_key(v: &serde_json::Value) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

/// Canonicalize a JSON value under `rules`.
pub fn canonicalize_json(value: &serde_json::Value, rules: &CanonRules) -> serde_json::Value {
    Canon::new(rules).canonicalize_value(None, value, "")
}

/// Compare two JSON values after canonicalization. Object key order is
/// insignificant; array order is significant unless opted out per path.
pub fn compare_json(
    expected: &serde_json::Value,
    actual: &serde_json::Value,
    rules: &CanonRules,
) -> Verdict {
    let left = canonicalize_json(expected, rules);
    let right = canonicalize_json(actual, rules);
    let mut diffs = Vec::new();
    diff_json(&left, &right, "", &mut diffs);
    Verdict { diffs, cases: 1 }
}

fn diff_json(
    expected: &serde_json::Value,
    actual: &serde_json::Value,
    path: &str,
    diffs: &mut Vec<Diff>,
) {
    match (expected, actual) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (k, v) in a {
                let child = format!("{path}/{k}");
                match b.get(k) {
                    Some(w) => diff_json(v, w, &child, diffs),
                    None => diffs.push(Diff {
                        path: child,
                        expected: short(v),
                        actual: "<missing>".into(),
                    }),
                }
            }
            for k in b.keys() {
                if !a.contains_key(k) {
                    diffs.push(Diff {
                        path: format!("{path}/{k}"),
                        expected: "<absent>".into(),
                        actual: short(&b[k]),
                    });
                }
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            if a.len() != b.len() {
                diffs.push(Diff {
                    path: format!("{path}/<len>"),
                    expected: a.len().to_string(),
                    actual: b.len().to_string(),
                });
            }
            for (i, (v, w)) in a.iter().zip(b.iter()).enumerate() {
                diff_json(v, w, &format!("{path}/{i}"), diffs);
            }
        }
        (a, b) if a != b => diffs.push(Diff {
            path: if path.is_empty() {
                "/".into()
            } else {
                path.to_string()
            },
            expected: short(a),
            actual: short(b),
        }),
        _ => {}
    }
}

fn short(v: &serde_json::Value) -> String {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.len() > 120 {
        format!("{}…", &s[..120])
    } else {
        s
    }
}

/// One parsed SSE event.
#[derive(Debug, PartialEq)]
struct SseEvent {
    event: Option<String>,
    id: Option<String>,
    data: String,
}

fn parse_sse(body: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut event: Option<String> = None;
    let mut id: Option<String> = None;
    let mut data = String::new();
    let mut flush = |event: &mut Option<String>, id: &mut Option<String>, data: &mut String| {
        if event.is_none() && id.is_none() && data.is_empty() {
            return;
        }
        events.push(SseEvent {
            event: event.take(),
            id: id.take(),
            data: std::mem::take(data),
        });
    };
    for line in body.lines() {
        if line.is_empty() {
            flush(&mut event, &mut id, &mut data);
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("id:") {
            id = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
        // comments (`:` lines) and unknown fields are ignored per SSE spec.
    }
    flush(&mut event, &mut id, &mut data);
    events
}

/// Compare two SSE wire bodies: event count, order, event names and
/// canonicalized JSON data payloads. `data: [DONE]` compares literally.
pub fn compare_sse(expected: &str, actual: &str, rules: &CanonRules) -> Verdict {
    let a = parse_sse(expected);
    let b = parse_sse(actual);
    let mut diffs = Vec::new();
    if a.len() != b.len() {
        diffs.push(Diff {
            path: "sse/<event-count>".into(),
            expected: a.len().to_string(),
            actual: b.len().to_string(),
        });
    }
    for (i, (ea, eb)) in a.iter().zip(b.iter()).enumerate() {
        let path = format!("sse/event[{i}]");
        if ea.event != eb.event {
            diffs.push(Diff {
                path: format!("{path}.event"),
                expected: ea.event.clone().unwrap_or_default(),
                actual: eb.event.clone().unwrap_or_default(),
            });
        }
        match (&ea.id, &eb.id) {
            (Some(ia), Some(ib)) => {
                let mut canon = Canon::new(rules);
                let (ca, cb) = (
                    canon.canonicalize_string(None, ia),
                    canon.canonicalize_string(None, ib),
                );
                if ca != cb {
                    diffs.push(Diff {
                        path: format!("{path}.id"),
                        expected: ca,
                        actual: cb,
                    });
                }
            }
            (x, y) if x != y => diffs.push(Diff {
                path: format!("{path}.id"),
                expected: x.clone().unwrap_or("<absent>".into()),
                actual: y.clone().unwrap_or("<absent>".into()),
            }),
            _ => {}
        }
        let da = ea.data.trim();
        let db = eb.data.trim();
        if da == "[DONE]" || db == "[DONE]" {
            if da != db {
                diffs.push(Diff {
                    path: format!("{path}.data"),
                    expected: da.into(),
                    actual: db.into(),
                });
            }
            continue;
        }
        if let (Ok(ja), Ok(jb)) = (
            serde_json::from_str::<serde_json::Value>(da),
            serde_json::from_str::<serde_json::Value>(db),
        ) {
            let sub = compare_json(&ja, &jb, rules);
            for d in sub.diffs {
                diffs.push(Diff {
                    path: format!("{path}.data{}", d.path),
                    expected: d.expected,
                    actual: d.actual,
                });
            }
        } else {
            let mut canon = Canon::new(rules);
            let (ta, tb) = (
                canon.canonicalize_string(None, da),
                canon.canonicalize_string(None, db),
            );
            if ta != tb {
                diffs.push(Diff {
                    path: format!("{path}.data"),
                    expected: ta,
                    actual: tb,
                });
            }
        }
    }
    Verdict {
        diffs,
        cases: a.len().max(b.len()).max(1),
    }
}

/// Compare two captured responses: status, canonicalized headers and body.
/// JSON bodies compare structurally; `text/event-stream` bodies compare as
/// SSE; other bodies compare as canonicalized text lines.
pub fn compare_response(
    expected: &CapturedResponse,
    actual: &CapturedResponse,
    rules: &CanonRules,
) -> Verdict {
    let mut diffs = Vec::new();
    if expected.status != actual.status {
        diffs.push(Diff {
            path: "status".into(),
            expected: expected.status.to_string(),
            actual: actual.status.to_string(),
        });
    }
    let ea = canonical_headers(&expected.headers, rules);
    let eb = canonical_headers(&actual.headers, rules);
    for (name, value) in &ea {
        match eb.iter().find(|(n, _)| n == name) {
            Some((_, w)) if w != value => diffs.push(Diff {
                path: format!("header:{name}"),
                expected: value.clone(),
                actual: w.clone(),
            }),
            Some(_) => {}
            None => diffs.push(Diff {
                path: format!("header:{name}"),
                expected: value.clone(),
                actual: "<missing>".into(),
            }),
        }
    }
    for (name, value) in &eb {
        if !ea.iter().any(|(n, _)| n == name) {
            diffs.push(Diff {
                path: format!("header:{name}"),
                expected: "<absent>".into(),
                actual: value.clone(),
            });
        }
    }
    let content_type = expected
        .headers
        .iter()
        .find(|(n, _)| n == "content-type")
        .map_or("", |(_, v)| v.as_str());
    let body_verdict = if content_type.contains("text/event-stream") {
        compare_sse(&expected.body, &actual.body, rules)
    } else if content_type.contains("json")
        || serde_json::from_str::<serde_json::Value>(&expected.body).is_ok()
    {
        match (
            serde_json::from_str::<serde_json::Value>(&expected.body),
            serde_json::from_str::<serde_json::Value>(&actual.body),
        ) {
            (Ok(a), Ok(b)) => compare_json(&a, &b, rules),
            _ => compare_text(&expected.body, &actual.body, rules),
        }
    } else {
        compare_text(&expected.body, &actual.body, rules)
    };
    for d in body_verdict.diffs {
        diffs.push(Diff {
            path: format!("body{}", d.path),
            expected: d.expected,
            actual: d.actual,
        });
    }
    Verdict { diffs, cases: 1 }
}

fn canonical_headers(headers: &[(String, String)], rules: &CanonRules) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(n, v)| {
            let name = n.to_ascii_lowercase();
            if rules.volatile_headers.iter().any(|h| h == &name) {
                (name, "<volatile>".to_string())
            } else {
                (name, v.clone())
            }
        })
        .collect()
}

/// Line-wise text comparison with path substitutions and volatile pattern
/// replacement inside each line (for logs and plain-text endpoints).
pub fn compare_text(expected: &str, actual: &str, rules: &CanonRules) -> Verdict {
    let canon = |line: &str| -> String {
        let mut out = line.to_string();
        for (from, label) in &rules.path_substitutions {
            out = out.replace(from.as_str(), &format!("<DIR:{label}>"));
        }
        // Token-wise volatile replacement: timestamps, uuids, durations,
        // generated ids appearing as whole whitespace-separated tokens.
        out.split_whitespace()
            .map(|tok| {
                let trimmed =
                    tok.trim_matches(|c: char| c == ',' || c == ';' || c == '(' || c == ')');
                if volatile_pattern(trimmed).is_some() {
                    tok.replacen(trimmed, "<volatile>", 1)
                } else {
                    tok.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let a: Vec<String> = expected.lines().map(canon).collect();
    let b: Vec<String> = actual.lines().map(canon).collect();
    let mut diffs = Vec::new();
    if a.len() != b.len() {
        diffs.push(Diff {
            path: "text/<line-count>".into(),
            expected: a.len().to_string(),
            actual: b.len().to_string(),
        });
    }
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        if x != y {
            diffs.push(Diff {
                path: format!("text/line[{i}]"),
                expected: x.clone(),
                actual: y.clone(),
            });
        }
    }
    Verdict {
        diffs,
        cases: a.len().max(b.len()).max(1),
    }
}

/// Compare two named case sets: every expected case must exist in `actual`
/// and match; extra cases are reported; zero cases is a failure.
pub fn compare_case_sets(
    expected: &[CaseResult],
    actual: &[CaseResult],
    rules: &CanonRules,
) -> Verdict {
    let mut diffs = Vec::new();
    for case in expected {
        match actual.iter().find(|c| c.name == case.name) {
            Some(found) => {
                let sub = compare_response(&case.response, &found.response, rules);
                for d in sub.diffs {
                    diffs.push(Diff {
                        path: format!("case:{}:{}", case.name, d.path),
                        expected: d.expected,
                        actual: d.actual,
                    });
                }
            }
            None => diffs.push(Diff {
                path: format!("case:{}", case.name),
                expected: "present".into(),
                actual: "<missing>".into(),
            }),
        }
    }
    for case in actual {
        if !expected.iter().any(|c| c.name == case.name) {
            diffs.push(Diff {
                path: format!("case:{}", case.name),
                expected: "<absent>".into(),
                actual: "extra case".into(),
            });
        }
    }
    Verdict {
        diffs,
        cases: expected.len().max(actual.len()),
    }
}
