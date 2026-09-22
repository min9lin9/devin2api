//! Layered retention for request log dirs — port of
//! `G/internal/debuglog/cleaner.go`.
//!
//! - a background ticker runs retention passes; failures only warn;
//! - dirs still being written (`active_dirs`) are never deleted;
//! - bulky payloads (upstream response / client SSE / attachments / retry
//!   shards) strip first; evidence files (meta/error/01/02/05) keep the full
//!   retention period — payload is the bulk of disk use and the small files
//!   preserve the debugging entry point;
//! - capacity eviction protects the newest N error dirs (with `error.json`);
//! - `index.jsonl`, `quota.jsonl`, `stderr.log` and other top-level files
//!   are not request dirs.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use jiff::Timestamp;

use super::RetentionPolicy;
use super::gotime;
use super::stages::{ERROR_FILE, is_payload_name};

/// `cleanerInterval` — retention pass period. All three dimensions are
/// coarse (hour-level payload strip, day-level eviction, GB-level soft cap);
/// 5 minutes amortizes the per-dir `dir_size` walk to nothing.
pub const CLEANER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// `requestDir` — the dir summary a cleanup decision needs.
struct RequestDir {
    name: String,
    size: i64,
    /// Age basis: the dir name's embedded timestamp wins, mtime is the
    /// fallback.
    at: Timestamp,
    /// Contains `error.json` — a failure scene.
    has_error: bool,
}

/// `cleanOnce` — one retention pass; returns the number of removed dirs.
/// Order: skip active dirs → strip over-age payloads → delete over-age dirs
/// → evict oldest over the total cap (protected error dirs excepted).
pub fn clean_once(root: &Path, active_dirs: &BTreeSet<String>, policy: &RetentionPolicy) -> i64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };

    let max_bytes = policy.max_total_mb << 20;
    let now = Timestamp::now();
    let age_cutoff = now
        .checked_sub(jiff::SignedDuration::from_hours(policy.days))
        .unwrap_or(Timestamp::MIN);
    let payload_cutoff = now
        .checked_sub(jiff::SignedDuration::from_hours(policy.payload_hours))
        .unwrap_or(Timestamp::MIN);

    let mut dirs: Vec<RequestDir> = Vec::new();
    let mut total_bytes: i64 = 0;
    let mut removed: i64 = 0;
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue; // index.jsonl and friends are not request dirs
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if active_dirs.contains(&name) {
            continue; // still being written — protect evidence integrity
        }
        let full = entry.path();
        // Age comes from the dir name's embedded request-start time:
        // strip_payload refreshes dir mtime to strip time, so mtime-based
        // aging would defer day eviction and scramble error-dir ordering;
        // the embedded timestamp is immune to maintenance. Non-request dir
        // names fall back to mtime.
        let at = if let Some(at) = request_dir_time(&name) {
            at
        } else {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            meta.modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(Timestamp::MIN, |d| {
                    Timestamp::from_second(d.as_secs().cast_signed()).unwrap_or(Timestamp::MIN)
                })
        };
        if policy.payload_hours > 0 && at < payload_cutoff {
            strip_payload(&full);
        }
        if policy.days > 0 && at < age_cutoff && std::fs::remove_dir_all(&full).is_ok() {
            removed += 1;
            continue;
        }
        let mut dir = RequestDir {
            name,
            at,
            size: 0,
            has_error: false,
        };
        if max_bytes > 0 {
            dir.size = dir_size(&full);
            total_bytes += dir.size;
            if policy.keep_error_dirs > 0 && full.join(ERROR_FILE).is_file() {
                dir.has_error = true;
            }
        }
        dirs.push(dir);
    }

    // Over the cap: evict oldest first until back under the limit. The
    // newest `keep_error_dirs` failure dirs are exempt — failure scenes are
    // exactly what you want to revisit later.
    if max_bytes > 0 && total_bytes > max_bytes {
        dirs.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.name.cmp(&b.name)));
        let error_count = super::to_i64(dirs.iter().filter(|d| d.has_error).count());
        // Sorted old→new: the first `deletable_errors` failure dirs can still
        // be evicted; the trailing keep_error_dirs are exempt.
        let mut deletable_errors = error_count - policy.keep_error_dirs;
        for dir in &dirs {
            if total_bytes <= max_bytes {
                break;
            }
            if dir.has_error {
                if deletable_errors > 0 {
                    deletable_errors -= 1;
                } else {
                    continue;
                }
            }
            if std::fs::remove_dir_all(root.join(&dir.name)).is_ok() {
                total_bytes -= dir.size;
                removed += 1;
            }
        }
    }
    removed
}

/// `stripPayload` — delete the bulky payload files, keep the evidence
/// layer. Returns freed bytes; a missing file is not an error.
fn strip_payload(dir: &Path) -> i64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut freed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_payload_name(&name) {
            continue;
        }
        let path = entry.path();
        let mut size = 0i64;
        let mut is_dir = false;
        if let Ok(meta) = entry.metadata() {
            size = meta.len().cast_signed();
            if meta.is_dir() {
                is_dir = true;
                size = dir_size(&path);
            }
        }
        // Go os.RemoveAll handles files and dirs alike.
        let gone = if is_dir {
            std::fs::remove_dir_all(&path).is_ok()
        } else {
            std::fs::remove_file(&path).is_ok()
        };
        if gone {
            freed += size;
        }
    }
    freed
}

/// `dirSize` — recursive byte total; unreadable files count as 0.
fn dir_size(root: &Path) -> i64 {
    fn walk(path: &Path, size: &mut i64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    walk(&entry.path(), size);
                } else if meta.is_file() {
                    *size += meta.len().cast_signed();
                }
            }
        }
    }
    let mut size = 0;
    walk(root, &mut size);
    size
}

/// `requestDirTime` — the dir name's embedded start time
/// (`20060102-150405` in local time, optional `-NN` suffix); `None` for
/// non-request names, where the caller falls back to mtime.
pub fn request_dir_time(name: &str) -> Option<Timestamp> {
    if !super::reader::is_request_dir_name(name) {
        return None;
    }
    gotime::parse_dir_time(&name[..15])
}

/// Convenience for tests: a `PathBuf` join.
pub fn request_dir_path(root: &Path, name: &str) -> PathBuf {
    root.join(name)
}
