//! Go `encoding/json`-compatible serialization for debug log records.
//!
//! The Go recorder writes `json.Marshal`/`json.MarshalIndent` output. To keep
//! Rust-written history byte-compatible with Go-written history this module
//! reproduces the observable rules:
//!
//! - map keys sort lexicographically (`JVal::Obj` is a `BTreeMap`);
//! - `<`, `>`, `&`, U+2028 and U+2029 inside string literals are emitted as
//!   `\u00xx`/`\u2028`/`\u2029` (Go's HTML escaping);
//! - `json.RawMessage` payloads are compacted (insignificant whitespace
//!   dropped) and HTML-escaped, never re-parsed — numbers keep their source
//!   spelling;
//! - numbers decoded by `json.Unmarshal` into `any` are `float64` and
//!   re-encode with Go's `strconv.AppendFloat(-1)` shortest form;
//! - `MarshalIndent` uses two-space indents and renders `{}`/`[]` inline.
//!
//! `ObjWriter` builds compact objects in field order for the Go struct
//! marshals (`IndexEntry`, `ActiveRequest`, usage rows) whose key order is
//! declaration order, not sorted.

use std::io::Write as _;

/// A JSON tree node mirroring Go's `any` after `json.Unmarshal`, plus `Raw`
/// for `json.RawMessage` passthrough.
#[derive(Debug, Clone, PartialEq)]
pub enum JVal {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Integer emitted verbatim (Go `int`/`int64`/`uint64` marshal).
    Int(i64),
    /// Float emitted with Go's shortest-round-trip formatting.
    Float(f64),
    /// JSON string.
    Str(String),
    /// JSON array.
    Arr(Vec<JVal>),
    /// JSON object; keys sort on emit like Go map marshaling.
    Obj(std::collections::BTreeMap<String, JVal>),
    /// `json.RawMessage`: verbatim JSON bytes, compacted+escaped on emit.
    Raw(Vec<u8>),
}

impl JVal {
    /// Convenience object builder.
    pub fn obj() -> Obj {
        Obj::default()
    }

    /// Convenience array builder.
    pub fn arr(items: Vec<JVal>) -> Self {
        Self::Arr(items)
    }
}

/// Builder for `JVal::Obj` (sorted on emit regardless of insert order).
#[derive(Default)]
pub struct Obj {
    map: std::collections::BTreeMap<String, JVal>,
}

impl Obj {
    /// Insert a field unconditionally.
    #[must_use]
    pub fn set(mut self, key: &str, value: JVal) -> Self {
        self.map.insert(key.to_string(), value);
        self
    }

    /// Insert a field only when `cond` holds (Go `if x != ""` style guards).
    #[must_use]
    pub fn set_if(self, cond: bool, key: &str, value: JVal) -> Self {
        if cond { self.set(key, value) } else { self }
    }

    /// Insert a string field only when non-empty (Go `omitempty` on strings).
    #[must_use]
    pub fn set_str(self, key: &str, value: &str) -> Self {
        self.set_if(!value.is_empty(), key, JVal::Str(value.to_string()))
    }

    /// Insert an integer field only when non-zero (Go `omitempty` on ints).
    #[must_use]
    pub fn set_int(self, key: &str, value: i64) -> Self {
        self.set_if(value != 0, key, JVal::Int(value))
    }

    /// Finish into `JVal::Obj`.
    pub fn build(self) -> JVal {
        JVal::Obj(self.map)
    }
}

/// Compact object writer preserving insertion (struct field) order.
///
/// Go marshals structs in declaration order; use this for `IndexEntry`,
/// `ActiveRequest`, `RequestMeta` and the usage rows. `finish` validates any
/// embedded raw payloads so a bad `Raw` fails the whole record like Go's
/// `json.Marshal` does.
#[derive(Default)]
pub struct ObjWriter {
    buf: Vec<u8>,
    fields: usize,
    err: Option<String>,
}

impl ObjWriter {
    /// Start a new object.
    pub fn new() -> Self {
        let mut w = ObjWriter::default();
        w.buf.push(b'{');
        w
    }

    fn sep(&mut self) {
        if self.fields > 0 {
            self.buf.push(b',');
        }
        self.fields += 1;
    }

    fn key(&mut self, name: &str) {
        self.sep();
        escape_str(&mut self.buf, name);
        self.buf.push(b':');
    }

    /// Write `name: <jval>` compact.
    pub fn field(&mut self, name: &str, value: &JVal) -> &mut Self {
        self.key(name);
        if let Err(err) = marshal_into(&mut self.buf, value) {
            self.err.get_or_insert(err);
        }
        self
    }

    /// Write `name: <jval>` only when `cond` holds.
    pub fn field_if(&mut self, cond: bool, name: &str, value: &JVal) -> &mut Self {
        if cond {
            self.field(name, value);
        }
        self
    }

    /// Write `name: "value"` (Go string escaping).
    pub fn field_str(&mut self, name: &str, value: &str) -> &mut Self {
        self.key(name);
        escape_str(&mut self.buf, value);
        self
    }

    /// Write `name: "value"` only when non-empty (Go `omitempty`).
    pub fn field_str_nonempty(&mut self, name: &str, value: &str) -> &mut Self {
        if !value.is_empty() {
            self.field_str(name, value);
        }
        self
    }

    /// Write `name: <int>`.
    pub fn field_int(&mut self, name: &str, value: i64) -> &mut Self {
        self.key(name);
        let _ = self.buf.write_fmt(format_args!("{value}"));
        self
    }

    /// Write `name: <int>` only when non-zero.
    pub fn field_int_nonzero(&mut self, name: &str, value: i64) -> &mut Self {
        if value != 0 {
            self.field_int(name, value);
        }
        self
    }

    /// Write `name: <uint>`.
    pub fn field_uint(&mut self, name: &str, value: u64) -> &mut Self {
        self.key(name);
        let _ = self.buf.write_fmt(format_args!("{value}"));
        self
    }

    /// Write `name: <uint>` only when non-zero.
    pub fn field_uint_nonzero(&mut self, name: &str, value: u64) -> &mut Self {
        if value != 0 {
            self.field_uint(name, value);
        }
        self
    }

    /// Write `name: <float>` with Go float formatting; NaN/Inf fail the
    /// record like Go's `json.Marshal`.
    pub fn field_float(&mut self, name: &str, value: f64) -> &mut Self {
        self.key(name);
        match go_float(value) {
            Ok(text) => self.buf.extend_from_slice(text.as_bytes()),
            Err(err) => {
                self.err.get_or_insert(err);
            }
        }
        self
    }

    /// Write `name: true|false`.
    pub fn field_bool(&mut self, name: &str, value: bool) -> &mut Self {
        self.key(name);
        self.buf
            .extend_from_slice(if value { b"true" } else { b"false" });
        self
    }

    /// Write `name: true` only when set (Go `omitempty` on bools).
    pub fn field_bool_true(&mut self, name: &str, value: bool) -> &mut Self {
        if value {
            self.field_bool(name, value);
        }
        self
    }

    /// Write `name: <raw>` where `raw` is verbatim JSON (`json.RawMessage`
    /// field). Compaction+escaping happen here; invalid JSON fails the
    /// record.
    pub fn field_raw(&mut self, name: &str, raw: &[u8]) -> &mut Self {
        self.key(name);
        match compact_escape(raw) {
            Ok(bytes) => self.buf.extend_from_slice(&bytes),
            Err(err) => {
                self.err.get_or_insert(err);
            }
        }
        self
    }

    /// Write `name: <raw>` only when `raw` is non-empty.
    pub fn field_raw_nonempty(&mut self, name: &str, raw: &[u8]) -> &mut Self {
        if !raw.is_empty() {
            self.field_raw(name, raw);
        }
        self
    }

    /// Write `name: null` or `name: <int>` for Go `*int64` fields.
    pub fn field_opt_int(&mut self, name: &str, value: Option<i64>) -> &mut Self {
        if let Some(v) = value {
            self.field_int(name, v)
        } else {
            self.key(name);
            self.buf.extend_from_slice(b"null");
            self
        }
    }

    /// Write `name: <int>` only when `Some` (Go `*int64` + `omitempty`).
    pub fn field_opt_int_some(&mut self, name: &str, value: Option<i64>) -> &mut Self {
        if let Some(v) = value {
            self.field_int(name, v);
        }
        self
    }

    /// Finish the object; `Err` when any field failed (Go marshal parity).
    pub fn finish(mut self) -> Result<Vec<u8>, String> {
        self.buf.push(b'}');
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.buf),
        }
    }
}

/// `json.Marshal` of a `JVal`: compact, HTML-escaped.
pub fn marshal(value: &JVal) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    marshal_into(&mut out, value)?;
    Ok(out)
}

fn marshal_into(out: &mut Vec<u8>, value: &JVal) -> Result<(), String> {
    match value {
        JVal::Null => out.extend_from_slice(b"null"),
        JVal::Bool(true) => out.extend_from_slice(b"true"),
        JVal::Bool(false) => out.extend_from_slice(b"false"),
        JVal::Int(v) => {
            let _ = out.write_fmt(format_args!("{v}"));
        }
        JVal::Float(v) => out.extend_from_slice(go_float(*v)?.as_bytes()),
        JVal::Str(s) => escape_str(out, s),
        JVal::Arr(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                marshal_into(out, item)?;
            }
            out.push(b']');
        }
        JVal::Obj(map) => {
            out.push(b'{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                escape_str(out, key);
                out.push(b':');
                marshal_into(out, item)?;
            }
            out.push(b'}');
        }
        JVal::Raw(raw) => out.extend_from_slice(&compact_escape(raw)?),
    }
    Ok(())
}

/// `json.MarshalIndent(v, "", "  ")` of a `JVal`.
pub fn marshal_indent(value: &JVal) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    marshal_indent_into(&mut out, value, 0)?;
    Ok(out)
}

fn marshal_indent_into(out: &mut Vec<u8>, value: &JVal, depth: usize) -> Result<(), String> {
    match value {
        JVal::Arr(items) if !items.is_empty() => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                newline_indent(out, depth + 1);
                marshal_indent_into(out, item, depth + 1)?;
            }
            newline_indent(out, depth);
            out.push(b']');
        }
        JVal::Obj(map) if !map.is_empty() => {
            out.push(b'{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                newline_indent(out, depth + 1);
                escape_str(out, key);
                out.extend_from_slice(b": ");
                marshal_indent_into(out, item, depth + 1)?;
            }
            newline_indent(out, depth);
            out.push(b'}');
        }
        JVal::Raw(raw) => {
            // Go marshals RawMessage by compacting (with HTML escaping) then
            // re-indenting the bytes — the raw subtree keeps its own number
            // spellings and key order.
            let compact = compact_escape(raw)?;
            indent_bytes(out, &compact, depth);
        }
        _ => marshal_into(out, value)?,
    }
    Ok(())
}

fn newline_indent(out: &mut Vec<u8>, depth: usize) {
    out.push(b'\n');
    for _ in 0..depth {
        out.extend_from_slice(b"  ");
    }
}

/// Port of Go `appendCompact(src, escape=true)` plus the scanner's validity
/// check: insignificant whitespace is dropped and `<>&`/U+2028/U+2029 are
/// escaped anywhere (they can only legally appear inside strings). Invalid
/// JSON is an error like Go's `json.Marshal`.
pub fn compact_escape(src: &[u8]) -> Result<Vec<u8>, String> {
    if !json_valid(src) {
        return Err("invalid JSON payload".to_string());
    }
    let mut out = Vec::with_capacity(src.len());
    let mut in_str = false;
    let mut esc = false;
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        if in_str {
            if esc {
                esc = false;
                out.push(c);
                i += 1;
                continue;
            }
            match c {
                b'\\' => {
                    esc = true;
                    out.push(c);
                }
                b'"' => {
                    in_str = false;
                    out.push(c);
                }
                b'<' => out.extend_from_slice(b"\\u003c"),
                b'>' => out.extend_from_slice(b"\\u003e"),
                b'&' => out.extend_from_slice(b"\\u0026"),
                0xE2 if i + 2 < src.len() && src[i + 1] == 0x80 && (src[i + 2] & !1) == 0xA8 => {
                    out.extend_from_slice(b"\\u202");
                    out.push(hex_digit(src[i + 2] & 0xF));
                    i += 2;
                }
                _ => out.push(c),
            }
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_str = true;
                out.push(c);
            }
            b' ' | b'\t' | b'\r' | b'\n' => {}
            b'<' => out.extend_from_slice(b"\\u003c"),
            b'>' => out.extend_from_slice(b"\\u003e"),
            b'&' => out.extend_from_slice(b"\\u0026"),
            0xE2 if i + 2 < src.len() && src[i + 1] == 0x80 && (src[i + 2] & !1) == 0xA8 => {
                out.extend_from_slice(b"\\u202");
                out.push(hex_digit(src[i + 2] & 0xF));
                i += 2;
            }
            _ => out.push(c),
        }
        i += 1;
    }
    Ok(out)
}

/// Port of Go `appendIndent(src, "", "  ")` applied at base `depth`: each
/// element starts on a new line indented `depth` levels deeper than its
/// enclosing bracket; empty `{}`/`[]` stay inline.
pub fn indent_bytes(out: &mut Vec<u8>, src: &[u8], depth: usize) {
    let mut in_str = false;
    let mut esc = false;
    let mut need_indent = false;
    let mut level = depth;
    for &c in src {
        if in_str {
            out.push(c);
            if esc {
                esc = false;
            } else if c == b'\\' {
                esc = true;
            } else if c == b'"' {
                in_str = false;
            }
            continue;
        }
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => {}
            b'"' => {
                if need_indent {
                    need_indent = false;
                    level += 1;
                    newline_indent(out, level);
                }
                in_str = true;
                out.push(c);
            }
            b'{' | b'[' => {
                if need_indent {
                    level += 1;
                    newline_indent(out, level);
                }
                need_indent = true;
                out.push(c);
            }
            b',' => {
                out.push(c);
                newline_indent(out, level);
            }
            b':' => out.extend_from_slice(b": "),
            b'}' | b']' => {
                if need_indent {
                    need_indent = false;
                } else {
                    level = level.saturating_sub(1);
                    newline_indent(out, level);
                }
                out.push(c);
            }
            _ => {
                if need_indent {
                    need_indent = false;
                    level += 1;
                    newline_indent(out, level);
                }
                out.push(c);
            }
        }
    }
}

/// Iterative JSON validity check — Go's scanner has no depth limit, so a
/// recursive parser (`serde_json` caps at 128) would wrongly reject deep
/// payloads the reference accepts. Returns false on any malformed input.
// One iterative state machine mirroring Go's scanner; splitting the
// frame table would obscure the port.
#[allow(clippy::too_many_lines)]
fn json_valid_inner(src: &[u8]) -> Option<()> {
    /// What the container top expects next.
    #[derive(Clone, Copy, PartialEq)]
    enum Frame {
        /// `{` seen: a `"key"` or `}`.
        ObjKeyOrEnd,
        /// key seen: a `:`.
        ObjColon,
        /// `:` seen: a value.
        ObjValue,
        /// value seen: a `,` or `}`.
        ObjCommaOrEnd,
        /// `,` seen in an object: a `"key"` (no trailing comma).
        ObjKey,
        /// `[` seen: a value or `]`.
        ArrValueOrEnd,
        /// `,` seen in an array: a value (no trailing comma).
        ArrValue,
        /// value seen: a `,` or `]`.
        ArrCommaOrEnd,
    }
    fn scan_string(src: &[u8], mut i: usize) -> Option<usize> {
        i += 1; // opening quote
        while i < src.len() {
            match src[i] {
                b'"' => return Some(i + 1),
                b'\\' => {
                    i += 1;
                    if i >= src.len() {
                        return None;
                    }
                    match src[i] {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => i += 1,
                        b'u' => {
                            if i + 4 >= src.len()
                                || !src[i + 1..=i + 4].iter().all(u8::is_ascii_hexdigit)
                            {
                                return None;
                            }
                            i += 5;
                        }
                        _ => return None,
                    }
                }
                c if c < 0x20 => return None,
                _ => i += 1,
            }
        }
        None
    }
    fn scan_number(src: &[u8], mut i: usize) -> Option<usize> {
        if i < src.len() && src[i] == b'-' {
            i += 1;
        }
        let digits_start = i;
        while i < src.len() && src[i].is_ascii_digit() {
            i += 1;
        }
        if i == digits_start {
            return None;
        }
        if i < src.len() && src[i] == b'.' {
            i += 1;
            let frac_start = i;
            while i < src.len() && src[i].is_ascii_digit() {
                i += 1;
            }
            if i == frac_start {
                return None;
            }
        }
        if i < src.len() && (src[i] == b'e' || src[i] == b'E') {
            i += 1;
            if i < src.len() && (src[i] == b'+' || src[i] == b'-') {
                i += 1;
            }
            let exp_start = i;
            while i < src.len() && src[i].is_ascii_digit() {
                i += 1;
            }
            if i == exp_start {
                return None;
            }
        }
        Some(i)
    }
    fn scan_literal(src: &[u8], i: usize, lit: &[u8]) -> Option<usize> {
        if src.len() - i >= lit.len() && &src[i..i + lit.len()] == lit {
            Some(i + lit.len())
        } else {
            None
        }
    }
    /// Parse one scalar or container-open at `i`; returns the new position.
    /// Container opens push their "expect key-or-end / value-or-end" frame.
    fn scan_value(src: &[u8], i: usize, stack: &mut Vec<Frame>) -> Option<usize> {
        match *src.get(i)? {
            b'"' => scan_string(src, i),
            b'{' => {
                stack.push(Frame::ObjKeyOrEnd);
                Some(i + 1)
            }
            b'[' => {
                stack.push(Frame::ArrValueOrEnd);
                Some(i + 1)
            }
            b't' => scan_literal(src, i, b"true"),
            b'f' => scan_literal(src, i, b"false"),
            b'n' => scan_literal(src, i, b"null"),
            b'-' | b'0'..=b'9' => scan_number(src, i),
            _ => None,
        }
    }

    let mut stack: Vec<Frame> = Vec::new();
    let mut i = 0usize;
    macro_rules! skip_ws {
        () => {
            while i < src.len() && matches!(src[i], b' ' | b'\t' | b'\r' | b'\n') {
                i += 1;
            }
        };
    }
    // Top-level: exactly one value then EOF.
    skip_ws!();
    i = scan_value(src, i, &mut stack)?;
    loop {
        skip_ws!();
        let Some(&top) = stack.last() else {
            return (i == src.len()).then_some(());
        };
        if i >= src.len() {
            return None;
        }
        match top {
            Frame::ObjKeyOrEnd | Frame::ObjKey => match src[i] {
                b'"' => {
                    i = scan_string(src, i)?;
                    *stack.last_mut().expect("top checked") = Frame::ObjColon;
                }
                b'}' if top == Frame::ObjKeyOrEnd => {
                    stack.pop();
                    i += 1;
                }
                _ => return None,
            },
            Frame::ObjColon => {
                if src[i] != b':' {
                    return None;
                }
                i += 1;
                *stack.last_mut().expect("top checked") = Frame::ObjValue;
            }
            Frame::ObjValue => {
                // Mark the value consumed before scanning: a container open
                // pushes its own frame on top of this one.
                *stack.last_mut().expect("top checked") = Frame::ObjCommaOrEnd;
                i = scan_value(src, i, &mut stack)?;
            }
            Frame::ObjCommaOrEnd => match src[i] {
                b',' => {
                    *stack.last_mut().expect("top checked") = Frame::ObjKey;
                    i += 1;
                }
                b'}' => {
                    stack.pop();
                    i += 1;
                }
                _ => return None,
            },
            Frame::ArrValueOrEnd | Frame::ArrValue => {
                if src[i] == b']' && top == Frame::ArrValueOrEnd {
                    stack.pop();
                    i += 1;
                    continue;
                }
                *stack.last_mut().expect("top checked") = Frame::ArrCommaOrEnd;
                i = scan_value(src, i, &mut stack)?;
            }
            Frame::ArrCommaOrEnd => match src[i] {
                b',' => {
                    *stack.last_mut().expect("top checked") = Frame::ArrValue;
                    i += 1;
                }
                b']' => {
                    stack.pop();
                    i += 1;
                }
                _ => return None,
            },
        }
    }
}

/// Iterative JSON validity check — Go's scanner has no depth limit, so a
/// recursive parser (`serde_json` caps at 128) would wrongly reject deep
/// payloads the reference accepts.
pub fn json_valid(src: &[u8]) -> bool {
    json_valid_inner(src).is_some()
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_digit(nibble: u8) -> u8 {
    HEX[nibble as usize]
}

/// Go `encoding/json` string escaping (appendString with HTML escaping on):
/// `"` and `\` are backslash-escaped, `\n`/`\r`/`\t` use short forms, other
/// control bytes use `\u00xx`, `<>&` become `\u003c/\u003e/\u0026`, U+2028/9
/// become `\u2028/\u2029`, invalid UTF-8 becomes `\ufffd`. Output includes
/// the surrounding quotes.
pub fn escape_str(out: &mut Vec<u8>, s: &str) {
    escape_bytes(out, s.as_bytes());
}

/// Byte-slice variant for content that may not be valid UTF-8.
pub fn escape_bytes(out: &mut Vec<u8>, src: &[u8]) {
    out.push(b'"');
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        match c {
            b'"' | b'\\' => {
                out.push(b'\\');
                out.push(c);
                i += 1;
            }
            b'\n' => {
                out.extend_from_slice(b"\\n");
                i += 1;
            }
            b'\r' => {
                out.extend_from_slice(b"\\r");
                i += 1;
            }
            b'\t' => {
                out.extend_from_slice(b"\\t");
                i += 1;
            }
            b'<' => {
                out.extend_from_slice(b"\\u003c");
                i += 1;
            }
            b'>' => {
                out.extend_from_slice(b"\\u003e");
                i += 1;
            }
            b'&' => {
                out.extend_from_slice(b"\\u0026");
                i += 1;
            }
            _ if c < 0x20 => {
                out.extend_from_slice(b"\\u00");
                out.push(hex_digit(c >> 4));
                out.push(hex_digit(c & 0xF));
                i += 1;
            }
            _ if c < 0x80 => {
                out.push(c);
                i += 1;
            }
            _ => {
                // Multi-byte sequence: validate UTF-8; U+2028/U+2029 escape,
                // invalid bytes become U+FFFD like Go.
                let width = utf8_width(src, i);
                if width == 0 {
                    out.extend_from_slice(b"\\ufffd");
                    i += 1;
                    continue;
                }
                let seq = &src[i..i + width];
                if seq == b"\xe2\x80\xa8" {
                    out.extend_from_slice(b"\\u2028");
                } else if seq == b"\xe2\x80\xa9" {
                    out.extend_from_slice(b"\\u2029");
                } else {
                    out.extend_from_slice(seq);
                }
                i += width;
            }
        }
    }
    out.push(b'"');
}

/// Length of the UTF-8 sequence starting at `src[i]`, or 0 when invalid.
fn utf8_width(src: &[u8], i: usize) -> usize {
    let c = src[i];
    let want = if c >= 0xF0 {
        4
    } else if c >= 0xE0 {
        3
    } else if c >= 0xC0 {
        2
    } else {
        return 0;
    };
    if i + want > src.len() {
        return 0;
    }
    if std::str::from_utf8(&src[i..i + want]).is_ok() {
        want
    } else {
        0
    }
}

/// Go `encoding/json` float formatting: `strconv.AppendFloat('g'→'e'/'f',
/// -1, 64)` — shortest round-trip; 'e' form when the decimal exponent is
/// < -6 or > 20, 'f' otherwise; exponent keeps its sign and drops a leading
/// zero (`e-09` → `e-9`, `e+21` stays). NaN/Inf are marshal errors.
pub fn go_float(v: f64) -> Result<String, String> {
    if !v.is_finite() {
        return Err("json: unsupported value: NaN or Inf".to_string());
    }
    if v == 0.0 {
        return Ok(if v.is_sign_negative() { "-0" } else { "0" }.to_string());
    }
    let abs = v.abs();
    if !(1e-6..1e21).contains(&abs) {
        // 'e' form: Rust {:e} prints "1e21"/"1.5e-7"; Go wants "1e+21".
        let s = format!("{v:e}");
        if let Some(pos) = s.find('e') {
            let (mantissa, exp) = s.split_at(pos);
            let exp = &exp[1..];
            if exp.starts_with('-') {
                return Ok(format!("{mantissa}e{exp}"));
            }
            return Ok(format!("{mantissa}e+{exp}"));
        }
        return Ok(s);
    }
    Ok(format!("{v}"))
}

/// Parse JSON bytes into a `JVal` tree with Go `json.Unmarshal` semantics:
/// objects become sorted maps, every number becomes `float64`.
pub fn parse(src: &[u8]) -> Result<JVal, String> {
    let value: serde_json::Value =
        serde_json::from_slice(src).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok(from_serde(value))
}

/// Convert a `serde_json::Value` into `JVal`, folding every number to f64
/// (Go `any` decode semantics).
pub fn from_serde(value: serde_json::Value) -> JVal {
    match value {
        serde_json::Value::Null => JVal::Null,
        serde_json::Value::Bool(b) => JVal::Bool(b),
        serde_json::Value::Number(n) => JVal::Float(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => JVal::Str(s),
        serde_json::Value::Array(items) => JVal::Arr(items.into_iter().map(from_serde).collect()),
        serde_json::Value::Object(map) => JVal::Obj(
            map.into_iter()
                .map(|(k, v)| (k, from_serde(v)))
                .collect::<std::collections::BTreeMap<_, _>>(),
        ),
    }
}
