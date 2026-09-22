//! Random ID generation shared by protocol encoders and the upstream
//! adapter — port of `G/internal/randid/randid.go`.
//!
//! Go's `crypto/rand` read can theoretically fail; Rust's thread RNG is
//! infallible, so the documented zero-UUID/timestamp fallbacks are
//! unreachable here and intentionally not reproduced.

/// Lowercase hex encoding of `bytes` (`encoding/hex.EncodeToString`).
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// `randid.Hex(size)`: `size` random bytes hex-encoded.
pub fn hex(size: usize) -> String {
    let mut bytes = vec![0u8; size];
    rand::fill(&mut bytes);
    hex_encode(&bytes)
}

/// `randid.UUID()`: a random v4 UUID.
pub fn uuid() -> String {
    let mut value = [0u8; 16];
    rand::fill(&mut value);
    format_uuid(value)
}

/// `randid.FormatUUID`: formats 16 bytes as a v4 UUID string, forcing the
/// version/variant bits. Shared by random IDs and content-derived session
/// IDs.
pub fn format_uuid(mut value: [u8; 16]) -> String {
    value[6] = (value[6] & 0x0f) | 0x40;
    value[8] = (value[8] & 0x3f) | 0x80;
    let mut out = String::with_capacity(36);
    out.push_str(&hex_encode(&value[0..4]));
    out.push('-');
    out.push_str(&hex_encode(&value[4..6]));
    out.push('-');
    out.push_str(&hex_encode(&value[6..8]));
    out.push('-');
    out.push_str(&hex_encode(&value[8..10]));
    out.push('-');
    out.push_str(&hex_encode(&value[10..16]));
    out
}

/// `randid.Prefixed(prefix)`: `prefix` + 16 random bytes hex-encoded, e.g.
/// `"msg_…"`.
pub fn prefixed(prefix: &str) -> String {
    let mut value = [0u8; 16];
    rand::fill(&mut value);
    let mut out = String::with_capacity(prefix.len() + 32);
    out.push_str(prefix);
    out.push_str(&hex_encode(&value));
    out
}
