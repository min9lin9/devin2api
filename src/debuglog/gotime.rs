//! Go `time` package semantics on top of `jiff`.
//!
//! The Go recorder stamps `time.RFC3339Nano` strings, names request dirs
//! with local civil time (`20060102-150405`), buckets usage by local day and
//! by 600-second unix slots, and parses index `started_at` strings back.
//! `jiff` provides the same primitives; this module pins the exact Go
//! surface so the rest of the port reads like the reference.

use std::str::FromStr;

use jiff::civil::DateTime;
use jiff::tz::{Offset, TimeZone};
use jiff::{SignedDuration, Timestamp, Zoned};

/// `time.Now()` in the local zone.
pub fn now() -> Zoned {
    Zoned::now()
}

/// `time.Since(started)` in whole milliseconds (Go `.Milliseconds()`).
pub fn since_ms(started: &Zoned) -> i64 {
    millis_between(started.timestamp(), Timestamp::now())
}

/// `end.Sub(start).Milliseconds()` — truncated toward zero like Go.
pub fn millis_between(start: Timestamp, end: Timestamp) -> i64 {
    let d: SignedDuration = start.duration_until(end);
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// `t.Format("20060102-150405")` — local civil time, the request dir stem.
pub fn dir_stamp(t: &Zoned) -> String {
    let dt = t.datetime();
    format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// `t.Format(time.RFC3339Nano)`: seconds plus a trimmed fractional part
/// (omitted when zero), `Z` for a zero offset, `±hh:mm` otherwise.
pub fn rfc3339_nano(t: &Zoned) -> String {
    let dt = t.datetime();
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    let nanos = t.timestamp().subsec_nanosecond();
    if nanos != 0 {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push('.');
        out.push_str(&frac);
    }
    push_offset(&mut out, t.offset());
    out
}

/// `t.Format(time.RFC3339)` — no fractional seconds.
pub fn rfc3339(t: &Zoned) -> String {
    let dt = t.datetime();
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    push_offset(&mut out, t.offset());
    out
}

fn push_offset(out: &mut String, offset: Offset) {
    let secs = offset.seconds();
    if secs == 0 {
        out.push('Z');
        return;
    }
    let sign = if secs < 0 { '-' } else { '+' };
    let abs = secs.unsigned_abs();
    let _ = std::fmt::Write::write_fmt(
        out,
        format_args!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60),
    );
}

/// `t.Local().Format("2006-01-02")` — local calendar day key.
pub fn day_key(t: &Zoned) -> String {
    let dt = t.datetime();
    format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day())
}

/// `time.Parse(time.RFC3339Nano, s)` — accepts `Z` and `±hh:mm` offsets and
/// preserves the parsed offset (Go keeps the location, so a later
/// `Format(RFC3339)` round-trips the original offset).
pub fn parse_rfc3339(s: &str) -> Option<Zoned> {
    if let Ok(z) = Zoned::from_str(s) {
        return Some(z);
    }
    // Fallback: parse the instant and re-attach the trailing numeric offset
    // as a fixed zone (covers parsers that demand a bracketed zone name).
    let ts = Timestamp::from_str(s).ok()?;
    let offset = parse_trailing_offset(s)?;
    Some(ts.to_zoned(TimeZone::fixed(offset)))
}

fn parse_trailing_offset(s: &str) -> Option<Offset> {
    let tail = s.rsplit_once('T').map_or(s, |(_, t)| t);
    if tail.ends_with('Z') || tail.ends_with('z') {
        return Some(Offset::UTC);
    }
    let bytes = tail.as_bytes();
    let len = bytes.len();
    let (sign, body) = match len {
        n if n >= 6 && (bytes[n - 6] == b'+' || bytes[n - 6] == b'-') => {
            (bytes[n - 6], &tail[n - 5..])
        }
        n if n >= 3 && (bytes[n - 3] == b'+' || bytes[n - 3] == b'-') => {
            (bytes[n - 3], &tail[n - 2..])
        }
        _ => return None,
    };
    let mut parts = body.splitn(2, ':');
    let hh: i32 = parts.next()?.parse().ok()?;
    let mm: i32 = parts.next().unwrap_or("0").parse().ok()?;
    let secs = hh * 3600 + mm * 60;
    Offset::from_seconds(if sign == b'-' { -secs } else { secs }).ok()
}

/// `time.ParseInLocation("20060102-150405", name, time.Local)` — parse a
/// request dir name's embedded local timestamp. `None` when the name is not
/// a request dir or the civil time is invalid/ambiguous (Go callers fall
/// back to the dir mtime).
pub fn parse_dir_time(name: &str) -> Option<Timestamp> {
    let dt = DateTime::strptime("%Y%m%d-%H%M%S", name).ok()?;
    dt.to_zoned(TimeZone::system()).ok().map(|z| z.timestamp())
}

/// `t.Unix()` — seconds since the epoch.
pub fn unix(t: &Zoned) -> i64 {
    t.timestamp().as_second()
}

/// `t.Unix()` for a bare timestamp.
pub fn unix_ts(t: Timestamp) -> i64 {
    t.as_second()
}

/// `time.Now().Unix()`.
pub fn unix_now() -> i64 {
    Timestamp::now().as_second()
}

/// `time.Now().Local()` — current local zoned time.
pub fn local_now() -> Zoned {
    now()
}
