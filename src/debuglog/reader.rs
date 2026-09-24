//! Read side of debug logs: index tail listing, per-request detail and file
//! reads, live snapshots of in-flight requests — port of
//! `G/internal/debuglog/reader.go`. Writers live in `recorder.rs`/`index.rs`.

use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use jiff::Zoned;

use super::gojson::{self, JVal, ObjWriter};
use super::gotime;
use super::index::IndexEntry;
use super::stages::{ATTACHMENTS_DIR, META_FILE, STDERR_FILE};

/// `indexTailBytes` — `list_requests` reads only this much of the index
/// tail; 4MB covers ~10k request records, older history is greppable in the
/// file itself.
const INDEX_TAIL_BYTES: i64 = 4 << 20;

/// `fileReadCap` — per-file read cap; beyond it the response is a truncated
/// prefix flagged `truncated`.
pub const FILE_READ_CAP: i64 = 4 << 20;

/// `processLogTailBytes` — per-call tail cap for the process log.
const PROCESS_LOG_TAIL_BYTES: i64 = 256 << 10;

/// `requestDirPattern` — `^\d{8}-\d{6}(-\d{2,})?$`; guards against path
/// traversal. Same-second suffixes are `%02d` without an upper digit bound
/// (the 100th+ same-second request gets `-100` and must still match).
pub fn is_request_dir_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 15 {
        return false;
    }
    if !bytes[..8].iter().all(u8::is_ascii_digit)
        || bytes[8] != b'-'
        || !bytes[9..15].iter().all(u8::is_ascii_digit)
    {
        return false;
    }
    if bytes.len() == 15 {
        return true;
    }
    bytes[15] == b'-' && bytes[16..].len() >= 2 && bytes[16..].iter().all(u8::is_ascii_digit)
}

/// `RequestFileInfo` — one file entry inside a request dir.
#[derive(Debug, Clone)]
pub struct RequestFileInfo {
    /// Path relative to the request dir (`meta.json`, `attachments/x`).
    pub name: String,
    pub size: i64,
}

impl RequestFileInfo {
    fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("name", &self.name).field_int("size", self.size);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `RequestDetail` — one request dir's aggregate view: raw meta.json plus
/// the full file listing.
#[derive(Debug, Clone)]
pub struct RequestDetail {
    pub dir: String,
    /// Raw `meta.json` bytes; `None` (JSON null) when missing/corrupt.
    pub meta: Option<Vec<u8>>,
    pub files: Vec<RequestFileInfo>,
}

impl RequestDetail {
    /// `json.Marshal(detail)` in Go field order.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("dir", &self.dir);
        match &self.meta {
            Some(raw) => {
                w.field_raw("meta", raw);
            }
            None => {
                w.field("meta", &JVal::Null);
            }
        }
        // Go marshals a nil slice as null.
        if self.files.is_empty() {
            w.field("files", &JVal::Null);
        } else {
            w.field(
                "files",
                &JVal::Arr(
                    self.files
                        .iter()
                        .map(|f| JVal::Raw(f.to_go_json().into()))
                        .collect(),
                ),
            );
        }
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `listRequestFiles` — all regular files in a request dir, descending one
/// level into `attachments/` only.
pub fn list_request_files(root: &Path) -> Vec<RequestFileInfo> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return files;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        if meta.is_dir() {
            // One level of subdirectory (attachments) only.
            if let Ok(sub) = std::fs::read_dir(entry.path()) {
                for sub_entry in sub.flatten() {
                    if let Ok(sub_meta) = sub_entry.metadata()
                        && sub_meta.is_file()
                    {
                        files.push(RequestFileInfo {
                            name: format!("{name}/{}", sub_entry.file_name().to_string_lossy()),
                            size: sub_meta.len().cast_signed(),
                        });
                    }
                }
            }
        } else if meta.is_file() {
            files.push(RequestFileInfo {
                name,
                size: meta.len().cast_signed(),
            });
        }
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    files
}

/// `validFileRelPath` — a top-level file or one level under `attachments/`;
/// rejects traversal, absolute paths and non-clean forms (Go
/// `filepath.Clean` parity: no `.`/`..`/empty segments).
pub fn valid_file_rel_path(name: &str) -> bool {
    if name.is_empty() || name.starts_with('/') || name.contains('\\') {
        return false;
    }
    let parts: Vec<&str> = name.split('/').collect();
    if parts
        .iter()
        .any(|p| p.is_empty() || *p == "." || *p == "..")
    {
        return false;
    }
    parts.len() == 1 || (parts.len() == 2 && parts[0] == ATTACHMENTS_DIR)
}

/// `ActiveRequest` — observable snapshot of an in-flight request.
#[derive(Debug, Clone)]
pub struct ActiveRequest {
    pub dir: String,
    pub meta: super::RequestMeta,
    pub model: String,
    pub resolved_model: String,
    pub retries: i64,
    pub last_retry_cause: String,
    pub started_at: Zoned,
    pub elapsed_ms: i64,
    /// `waiting_upstream` / `receiving_upstream` / `streaming_client`.
    pub state: String,
    /// `first_upstream_ms`; JSON null when it has not happened.
    pub first_upstream_ms: Option<i64>,
    pub client_bytes: i64,
    pub queued_events: i64,
    pub dropped_events: u64,
    pub abortable: bool,
    pub files: Vec<RequestFileInfo>,
}

impl ActiveRequest {
    /// `json.Marshal(active)` in Go field order.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("dir", &self.dir)
            .field_raw("meta", &self.meta.to_go_json())
            .field_str_nonempty("model", &self.model)
            .field_str_nonempty("resolved_model", &self.resolved_model)
            .field_int_nonzero("retries", self.retries)
            .field_str_nonempty("last_retry_cause", &self.last_retry_cause)
            .field_str("started_at", &gotime::rfc3339_nano(&self.started_at))
            .field_int("elapsed_ms", self.elapsed_ms)
            .field_str("state", &self.state)
            .field_opt_int("first_upstream_ms", self.first_upstream_ms)
            .field_int("client_bytes", self.client_bytes)
            .field_int("queued_events", self.queued_events)
            .field_uint("dropped_events", self.dropped_events)
            .field_bool("abortable", self.abortable);
        if self.files.is_empty() {
            w.field("files", &JVal::Null);
        } else {
            w.field(
                "files",
                &JVal::Arr(
                    self.files
                        .iter()
                        .map(|f| JVal::Raw(f.to_go_json().into()))
                        .collect(),
                ),
            );
        }
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `ListResult` — matched entries plus the history-truncation signal.
#[derive(Debug, Clone, Default)]
pub struct ListResult {
    /// Newest first.
    pub entries: Vec<IndexEntry>,
    /// `index.jsonl` has older history outside the read window (file larger
    /// than the tail cap, or unscanned lines remain inside it).
    pub has_more: bool,
    /// Earliest `started_at` covered by this tail read; combined with
    /// `filter.since` it tells whether the time window is fully covered.
    pub index_tail_start: String,
}

impl ListResult {
    /// `json.Marshal(result)` in Go field order.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field(
            "entries",
            &JVal::Arr(
                self.entries
                    .iter()
                    .map(|e| JVal::Raw(e.to_go_json().into()))
                    .collect(),
            ),
        )
        .field_bool("has_more", self.has_more)
        .field_str_nonempty("index_tail_start", &self.index_tail_start);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `RequestFilter` — structured list filter; the zero value matches all.
#[derive(Debug, Clone, Default)]
pub struct RequestFilter {
    /// Status class: "2xx"/"4xx"/"5xx".
    pub status_class: String,
    /// Status expression, comma-separated OR; each term is an exact code
    /// (499), class (4xx), comparison (>=400/<300) or negation (!200/!2xx).
    pub status: String,
    /// `completed`/`failed`/`disconnected`/`aborted`.
    pub result: String,
    /// Exact match against requested/actual/response model.
    pub model: String,
    /// Exact failure-stage match.
    pub error_stage: String,
    /// Keep requests started after this time; `None` = unbounded.
    pub since: Option<Zoned>,
    /// Keep requests started before this time; `None` = unbounded.
    pub until: Option<Zoned>,
    /// Substring match over `dir/model/key_hash/client_request_id/path`.
    pub query: String,
}

/// `statusCond` — one compiled status term; terms OR together.
#[derive(Debug, Clone, Copy)]
struct StatusCond {
    /// `!` prefix negates the whole term.
    neg: bool,
    /// '=' exact, 'x' class (hundreds), 'g' '>=', 'l' '<=', '>' '>', '<' '<'.
    op: u8,
    val: i64,
}

impl StatusCond {
    fn ok(&self, code: i64) -> bool {
        let m = match self.op {
            b'x' => code / 100 == self.val,
            b'g' => code >= self.val,
            b'l' => code <= self.val,
            b'>' => code > self.val,
            b'<' => code < self.val,
            _ => code == self.val,
        };
        m != self.neg
    }
}

/// `parseStatusExpr` — compile the expression; empty = no filter. Any
/// invalid term yields a never-matching sentinel: user input should produce
/// an explicit empty result, not silently degrade to unfiltered.
fn parse_status_expr(s: &str) -> Option<Vec<StatusCond>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for part in s.split(',') {
        match parse_status_term(part.trim()) {
            Some(cond) => out.push(cond),
            None => {
                return Some(vec![StatusCond {
                    neg: false,
                    op: b'=',
                    val: -1,
                }]);
            }
        }
    }
    Some(out)
}

/// `parseStatusTerm` — optional `!` + Nxx class / comparator + 3-digit code
/// / bare 3-digit code.
fn parse_status_term(t: &str) -> Option<StatusCond> {
    let mut cond = StatusCond {
        neg: false,
        op: b'=',
        val: 0,
    };
    let mut t = t;
    if let Some(rest) = t.strip_prefix('!') {
        cond.neg = true;
        t = rest;
    }
    for (pre, op) in [(">=", b'g'), ("<=", b'l'), (">", b'>'), ("<", b'<')] {
        if let Some(rest) = t.strip_prefix(pre) {
            cond.op = op;
            t = rest;
            break;
        }
    }
    // Class form "4xx" is only valid in exact semantics (">=4xx" is
    // meaningless).
    if t.len() == 3 && t[1..] == *"xx" && t.as_bytes()[0].is_ascii_digit() {
        if cond.op != b'=' {
            return None;
        }
        cond.op = b'x';
        cond.val = i64::from(t.as_bytes()[0] - b'0');
        return Some(cond);
    }
    let n: i64 = t.parse().ok()?;
    if !(100..=999).contains(&n) {
        return None;
    }
    cond.val = n;
    Some(cond)
}

impl RequestFilter {
    /// `match` — whether one index line satisfies the filter. `conds` is
    /// compiled once by the caller; `query` is pre-lowercased there.
    fn matches(&self, e: &IndexEntry, conds: Option<&[StatusCond]>) -> bool {
        if let Some(conds) = conds
            && !conds.iter().any(|c| c.ok(e.status_code))
        {
            return false;
        }
        if !self.status_class.is_empty() {
            let class = e.status_code / 100;
            let want = i64::from(self.status_class.as_bytes()[0].wrapping_sub(b'0'));
            if self.status_class.len() != 3 || self.status_class[1..] != *"xx" || class != want {
                return false;
            }
        }
        if !self.result.is_empty() && e.result != self.result {
            return false;
        }
        if !self.model.is_empty()
            && e.model != self.model
            && e.requested_model != self.model
            && e.response_model != self.model
        {
            return false;
        }
        if !self.error_stage.is_empty() && e.error_stage != self.error_stage {
            return false;
        }
        if self.since.is_some() || self.until.is_some() {
            let Some(started) = gotime::parse_rfc3339(&e.started_at) else {
                return false;
            };
            if let Some(since) = &self.since
                && started.timestamp() <= since.timestamp()
            {
                return false;
            }
            if let Some(until) = &self.until
                && started.timestamp() >= until.timestamp()
            {
                return false;
            }
        }
        if !self.query.is_empty() {
            let haystack = format!(
                "{} {} {} {} {} {} {} {} {} {}",
                e.dir,
                e.method,
                e.path,
                e.model,
                e.requested_model,
                e.response_model,
                e.key_hash,
                e.client_request_id,
                e.error_stage,
                e.result
            );
            if !haystack.to_lowercase().contains(&self.query) {
                return false;
            }
        }
        true
    }
}

/// `listIndexCache` — parsed tail window of `index.jsonl` keyed by the
/// file's (size, mtime): the index only changes on completion appends or
/// cap rewrites, so a hit leaves `list_requests` a pure in-memory filter.
/// Entries store in file order (old→new); callers iterate in reverse.
#[derive(Default)]
pub struct ListIndexCache {
    key: String,
    entries: Vec<IndexEntry>,
    /// Bytes covered by the cached window (tail-read length at the time),
    /// for the `has_more` check.
    window_bytes: i64,
}

impl ListIndexCache {
    /// `ListRequests` — newest `limit` index summaries. The index survives
    /// dir cleanup, so the list is complete history even when `detail` 404s.
    pub fn list(
        cache: &std::sync::Mutex<ListIndexCache>,
        root: &Path,
        limit: usize,
        filter: &RequestFilter,
    ) -> ListResult {
        if limit == 0 {
            return ListResult::default();
        }
        let path = root.join(super::stages::INDEX_FILE);
        let Ok(info) = std::fs::metadata(&path) else {
            return ListResult::default();
        };
        let mtime_nanos = info
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        let key = format!("{}:{}", info.len(), mtime_nanos);

        let (entries, window_bytes) = {
            let mut guard = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if guard.key != key {
                let Ok(data) = tail_read(&path, INDEX_TAIL_BYTES) else {
                    return ListResult::default();
                };
                let mut all = Vec::new();
                for line in data.split(|b| *b == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    if let Some(e) = IndexEntry::parse(line) {
                        all.push(e);
                    }
                }
                *guard = ListIndexCache {
                    key,
                    entries: all,
                    window_bytes: super::to_i64(data.len()),
                };
            }
            (guard.entries.clone(), guard.window_bytes)
        };

        let mut out = Vec::with_capacity(limit.min(entries.len()));
        let conds = parse_status_expr(&filter.status);
        let mut filter = filter.clone();
        filter.query = filter.query.to_lowercase();
        // scanned_all=false means the window still holds unscanned lines
        // (limit exhausted) — older history definitely exists.
        let mut scanned_all = true;
        for e in entries.iter().rev() {
            if out.len() >= limit {
                scanned_all = false;
                break;
            }
            if filter.matches(e, conds.as_deref()) {
                out.push(e.clone());
            }
        }
        // File larger than the read window → history beyond it; unscanned
        // lines inside the window → same.
        let has_more = info.len().cast_signed() > window_bytes;
        let mut result = ListResult {
            entries: out,
            has_more: has_more || !scanned_all,
            index_tail_start: String::new(),
        };
        if let Some(first) = entries.first() {
            result.index_tail_start.clone_from(&first.started_at);
        }
        result
    }
}

/// `ReadProcessLog` — tail of `stderr.log` plus the offset for the next
/// incremental pull. `offset > 0` continues from that offset (offsets are
/// naturally monotonic — this log never rotates).
pub fn read_process_log(root: &Path, offset: i64) -> std::io::Result<(Vec<u8>, i64)> {
    let path = root.join(STDERR_FILE);
    let info = std::fs::metadata(&path)?;
    let size = info.len().cast_signed();
    if offset > 0 && offset <= size {
        // Incremental mode: read from the last offset to now.
        let mut start = offset;
        if size - offset > PROCESS_LOG_TAIL_BYTES {
            start = size - PROCESS_LOG_TAIL_BYTES;
        }
        let mut data = vec![0u8; usize::try_from(size - start).unwrap_or_default()];
        let mut file = std::fs::File::open(&path)?;
        file.seek(SeekFrom::Start(u64::try_from(start).unwrap_or_default()))?;
        file.read_exact(&mut data)?;
        return Ok((data, size));
    }
    // Full-tail mode: the last PROCESS_LOG_TAIL_BYTES.
    let data = tail_read(&path, PROCESS_LOG_TAIL_BYTES)?;
    Ok((data, size))
}

/// `TailRead` — at most `max` tail bytes; the whole file when smaller. The
/// result starts at a byte boundary, so a torn first line is naturally
/// skipped by line-based parsers.
pub fn tail_read(path: &Path, max: i64) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len().cast_signed();
    let start = if size > max { size - max } else { 0 };
    let mut data = vec![0u8; usize::try_from(size - start).unwrap_or_default()];
    file.seek(SeekFrom::Start(u64::try_from(start).unwrap_or_default()))?;
    file.read_exact(&mut data)?;
    Ok(data)
}

/// `TruncateToTail` — shrink `path` to at most `keep` tail bytes: read the
/// tail, drop the torn first line, rewrite in place; returns the kept byte
/// count. A file already within `keep` is left untouched. Shared by the
/// JSONL append logs (index/quota) — a truncation point mid-line would
/// leave a half line that poisons line-based consumers.
pub fn truncate_to_tail(path: &Path, keep: i64) -> std::io::Result<i64> {
    let info = std::fs::metadata(path)?;
    if info.len().cast_signed() <= keep {
        return Ok(info.len().cast_signed());
    }
    let data = tail_read(path, keep)?;
    let data = match data.iter().position(|b| *b == b'\n') {
        Some(idx) => &data[idx + 1..],
        // The whole tail is one torn line — drop it all, leave an empty file.
        None => &[][..],
    };
    let mut file = super::create_file(path)?;
    file.write_all(data)?;
    Ok(super::to_i64(data.len()))
}

/// Read a request-dir file with the `FILE_READ_CAP` truncation contract.
/// Returns `(data, total_size, truncated)`.
pub fn read_request_file(
    root: &Path,
    dir: &str,
    name: &str,
) -> std::io::Result<(Vec<u8>, i64, bool)> {
    if !is_request_dir_name(dir) || !valid_file_rel_path(name) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invalid request dir or file name",
        ));
    }
    let path: PathBuf = root.join(dir).join(name);
    // Go maps every stat failure to os.ErrNotExist.
    let info = std::fs::metadata(&path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "."))?;
    if !info.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not a regular file",
        ));
    }
    let mut file = std::fs::File::open(&path)?;
    let mut read_size = info.len().cast_signed();
    let mut truncated = false;
    if read_size > FILE_READ_CAP {
        read_size = FILE_READ_CAP;
        truncated = true;
    }
    let mut data = vec![0u8; usize::try_from(read_size).unwrap_or_default()];
    file.read_exact(&mut data)?;
    Ok((data, info.len().cast_signed(), truncated))
}

/// `Detail` — meta.json plus the file listing for a completed or in-flight
/// request dir. `dir` must match the request-dir pattern so the panel
/// endpoint cannot traverse arbitrary paths.
pub fn detail(root: &Path, dir: &str) -> std::io::Result<RequestDetail> {
    if !is_request_dir_name(dir) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "invalid request dir name",
        ));
    }
    let root_dir = root.join(dir);
    // Go maps every stat failure to os.ErrNotExist.
    let info = std::fs::metadata(&root_dir)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "."))?;
    if !info.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not a directory",
        ));
    }
    let meta = std::fs::read(root_dir.join(META_FILE))
        .ok()
        .filter(|data| gojson::json_valid(data));
    Ok(RequestDetail {
        dir: dir.to_string(),
        meta,
        files: list_request_files(&root_dir),
    })
}

/// `json.Valid` check used by `detail` and `Stats` (bind-failure passthrough).
pub fn json_valid(data: &[u8]) -> bool {
    gojson::json_valid(data)
}

/// Serialize a slice of `ActiveRequest` like Go's `json.Marshal` on the slice.
pub fn active_requests_json(list: &[ActiveRequest]) -> Vec<u8> {
    gojson::marshal(&JVal::Arr(
        list.iter()
            .map(|r| JVal::Raw(r.to_go_json().into()))
            .collect(),
    ))
    .unwrap_or_else(|_| b"[]".to_vec())
}
