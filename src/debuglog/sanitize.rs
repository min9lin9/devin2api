//! Value sanitization and attachment extraction — port of
//! `G/internal/debuglog/sanitize.go`.
//!
//! `sanitize` normalizes a `LogValue` into a `JVal` tree and recursively
//! redacts sensitive keys; `extract_image`/`write_data_url` pull inline
//! base64 images out of the tree into `attachments/` and leave a reference.
//! The key-name rules (`SECRET_KEY_NAMES`, `METADATA_SECRET_KEY_NAMES`) are
//! the last gate before bytes hit disk and are centralized here.

use std::collections::BTreeMap;

use base64::Engine as _;
use sha2::Digest as _;

use super::LogValue;
use super::gojson::{self, JVal};
use super::stages::ATTACHMENTS_DIR;

/// JSON key names that are always redacted (after `_`/`-` removal and
/// lowercasing). Shared by `secret_key` and `raw_needs_sanitize` so the two
/// paths cannot drift.
const SECRET_KEY_NAMES: &[&str] = &[
    "authorization",
    "cookie",
    "setcookie",
    "apikey",
    "accesskey",
    "token",
    "sessiontoken",
    "accesstoken",
    "refreshtoken",
    "bearertoken",
    "password",
    "clientsecret",
    "devicefingerprint",
    // modelAssignmentJwt is the per-request router JWT issued by
    // AssignModel — a credential-grade field inside 03 request bodies.
    "modelassignmentjwt",
];

/// Key names sensitive only inside a `metadata` object: upstream
/// `Metadata.f` is a device fingerprint, but `f` is a legitimate short key
/// in client payloads — a global rule would over-redact.
const METADATA_SECRET_KEY_NAMES: &[&str] = &["f"];

/// Whether `key` is a globally sensitive name. Go's `secretKey` normalizes
/// with `strings.ToLower` — Unicode simple case folding — so non-ASCII
/// spellings still redact: `APİKEY` (U+0130 folds to `i`) and
/// `API\u{212A}EY` (Kelvin sign folds to `k`) both hit `apikey`.
fn secret_key(key: &str) -> bool {
    SECRET_KEY_NAMES
        .iter()
        .any(|name| normalized_key_eq(key, name))
}

/// Whether `key` is sensitive only inside a `metadata` scope.
fn metadata_secret_key(key: &str) -> bool {
    METADATA_SECRET_KEY_NAMES
        .iter()
        .any(|name| normalized_key_eq(key, name))
}

/// Whether `key` opens a `metadata` scope.
fn is_metadata_key(key: &str) -> bool {
    normalized_key_eq(key, "metadata")
}

/// `strings.ToLower(keyNormalizer.Replace(key)) == name`: strip `_`/`-`,
/// then compare under Unicode *simple* case folding. `char::to_lowercase`
/// yields the full mapping (`İ` → `i` + combining dot), but its first
/// char is exactly the simple mapping — the single rune Go's
/// `unicode.ToLower` emits — so `next()` reproduces Go byte-for-byte
/// (verified against go1.27: `İ`→`i`, `\u{212A}`→`k`, `Σ`→`σ` with no
/// final-sigma context rule).
fn normalized_key_eq(key: &str, name: &str) -> bool {
    key.chars()
        .filter(|c| *c != '_' && *c != '-')
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .eq(name.chars())
}

/// Prescreen a raw JSON record: only inline images or sensitive key names
/// need the full unmarshal+tree-walk. `"image/"` covers both `data:image/`
/// values and `{"mime_type":"image/*","data":...}` objects; key names only
/// match in `"key":` position (normalized like `secret_key`), so same-named
/// string values stay on the fast path. Keys containing escapes cannot be
/// byte-normalized — they take the slow path too. Zero-allocation scan.
pub fn raw_needs_sanitize(data: &[u8]) -> bool {
    if memchr_subslice(data, b"image/") {
        return true;
    }
    let mut i = 0;
    while i < data.len() {
        if data[i] != b'"' {
            i += 1;
            continue;
        }
        let mut end = i + 1;
        let mut escaped = false;
        while end < data.len() && data[end] != b'"' {
            if data[end] == b'\\' {
                escaped = true;
                end += 1;
            }
            end += 1;
        }
        if end >= data.len() {
            break;
        }
        let mut colon = end + 1;
        while colon < data.len() && matches!(data[colon], b' ' | b'\t' | b'\r' | b'\n') {
            colon += 1;
        }
        if colon < data.len()
            && data[colon] == b':'
            && (escaped || secret_key_span(&data[i + 1..end]))
        {
            return true;
        }
        i = end + 1;
    }
    false
}

fn memchr_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Whether a quoted key span hits the redaction list (same normalization as
/// `secret_key`). The prescreen cannot tell nesting depth, so metadata-only
/// names also match — the slow path decides by scope.
fn secret_key_span(span: &[u8]) -> bool {
    SECRET_KEY_NAMES
        .iter()
        .chain(METADATA_SECRET_KEY_NAMES)
        .any(|name| equal_fold_key(span, name))
}

/// Compare a raw key span with a normalized list entry: skip `_`/`-`,
/// fold ASCII case. Go's `equalFoldKey` is deliberately byte-wise ASCII —
/// the prescreen cannot decode Unicode folds, so a raw payload key spelled
/// `APİKEY` slips through unredacted in both implementations (the
/// tree-path `secretKey` still catches that spelling; the asymmetry is
/// Go's own).
fn equal_fold_key(span: &[u8], name: &str) -> bool {
    let mut i = 0;
    for &want in name.as_bytes() {
        while i < span.len() && (span[i] == b'_' || span[i] == b'-') {
            i += 1;
        }
        if i >= span.len() {
            return false;
        }
        let mut c = span[i];
        i += 1;
        if c.is_ascii_uppercase() {
            c += b'a' - b'A';
        }
        if c != want {
            return false;
        }
    }
    while i < span.len() && (span[i] == b'_' || span[i] == b'-') {
        i += 1;
    }
    i == span.len()
}

/// Attachment state owned by the write worker (`attachmentByHash` /
/// `attachmentCount` in Go).
#[derive(Default)]
pub struct AttachmentStore {
    by_hash: BTreeMap<String, Vec<u8>>,
    count: u32,
}

/// Sanitizer bound to one request's shared state (worker-side).
pub(crate) struct Sanitizer<'a> {
    pub(crate) shared: &'a super::Shared,
    pub(crate) attachments: &'a mut AttachmentStore,
}

impl Sanitizer<'_> {
    /// `sanitize`: normalize a `LogValue` into a `JVal` tree and recursively
    /// redact. `Raw` payloads take the prescreen fast path — clean bytes
    /// pass through untouched; dirty bytes are parsed and walked.
    pub fn sanitize(&mut self, value: LogValue) -> JVal {
        match value {
            LogValue::Raw(data) => {
                if !raw_needs_sanitize(&data) {
                    return JVal::Raw(data);
                }
                match gojson::parse(&data) {
                    Ok(mut tree) => {
                        self.sanitize_value(&mut tree, false);
                        tree
                    }
                    Err(err) => serialization_error(&err),
                }
            }
            LogValue::Serde(thunk) => match thunk() {
                // Go's `json.Marshaler` arm prescreens before unmarshaling:
                // clean bytes pass through as `json.RawMessage` verbatim —
                // no tree build, no number re-spelling.
                Ok(data) => {
                    if !raw_needs_sanitize(&data) {
                        return JVal::Raw(data.into());
                    }
                    match gojson::parse(&data) {
                        Ok(tree) => {
                            let mut tree = tree;
                            self.sanitize_value(&mut tree, false);
                            tree
                        }
                        Err(err) => serialization_error(&err),
                    }
                }
                Err(err) => serialization_error(&err),
            },
            LogValue::Tree(mut tree) => {
                self.sanitize_value(&mut tree, false);
                tree
            }
            LogValue::Text(text) => {
                let mut value = JVal::Str(text);
                self.sanitize_value(&mut value, false);
                value
            }
            // `eval_deferred` normally unwraps thunks before sanitize; a
            // thunk that yields another thunk evaluates one more level,
            // matching Go's single `evalDeferred` unwrap.
            LogValue::Deferred(f) => self.sanitize(f()),
        }
    }

    /// `sanitizeValue`: recursive in-place redaction — Go mutates the
    /// projected `map[string]any`/`[]any` in place too ("map/slice 输入会被
    /// 原地改写"), so the tree is walked, not rebuilt. `metadata_scope`
    /// marks subtrees under a `metadata` key where `f` is also sensitive.
    fn sanitize_value(&mut self, value: &mut JVal, metadata_scope: bool) {
        match value {
            JVal::Arr(items) => {
                for item in items {
                    self.sanitize_value(item, metadata_scope);
                }
            }
            JVal::Obj(map) => {
                for (key, item) in map.iter_mut() {
                    if secret_key(key) || (metadata_scope && metadata_secret_key(key)) {
                        *item = JVal::Str("<redacted>".to_string());
                    }
                }
                if let Some(reference) = self.extract_image(map) {
                    *value = JVal::Raw(reference.into());
                    return;
                }
                for (key, item) in map.iter_mut() {
                    let scope = metadata_scope || is_metadata_key(key);
                    self.sanitize_value(item, scope);
                }
            }
            JVal::Str(text) => {
                if text.starts_with("data:image/")
                    && let Some(reference) = self.write_data_url(text)
                {
                    *value = JVal::Raw(reference.into());
                }
            }
            JVal::Raw(raw) => {
                if !raw_needs_sanitize(raw) {
                    return;
                }
                if let Ok(mut tree) = gojson::parse(raw) {
                    self.sanitize_value(&mut tree, false);
                    *value = tree;
                }
            }
            _ => {}
        }
    }

    /// `extractImage`: `{mime_type: "image/*", data: "<base64>"}` objects
    /// become attachment references.
    fn extract_image(&mut self, value: &BTreeMap<String, JVal>) -> Option<Vec<u8>> {
        let mime_type = string_field(value, &["mime_type", "mimeType", "MIMEType"])?;
        let encoded = string_field(value, &["data", "base64_data", "base64Data", "Data"])?;
        if !mime_type.starts_with("image/") || encoded.is_empty() {
            return None;
        }
        let data = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .ok()?;
        Some(self.write_attachment(&data, &mime_type))
    }

    /// `writeDataURL`: `data:image/...;base64,...` strings become
    /// attachment references.
    fn write_data_url(&mut self, value: &str) -> Option<Vec<u8>> {
        let (header, encoded) = value.split_once(',')?;
        if !header.ends_with(";base64") {
            return None;
        }
        let mime_type = header
            .strip_prefix("data:")?
            .strip_suffix(";base64")?
            .to_string();
        let data = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        Some(self.write_attachment(&data, &mime_type))
    }

    /// `writeAttachment`: dedupe by SHA-256, write `attachments/image-NNN.ext`,
    /// return the compact JSON of the reference struct (field order:
    /// file, `mime_type`, size, sha256).
    fn write_attachment(&mut self, data: &[u8], mime_type: &str) -> Vec<u8> {
        let hash = hex_lower(&sha2::Sha256::digest(data));
        if let Some(reference) = self.attachments.by_hash.get(&hash) {
            return reference.clone();
        }
        self.attachments.count += 1;
        let file_name = format!(
            "image-{:03}{}",
            self.attachments.count,
            image_extension(mime_type)
        );
        let dir = self.shared.directory.join(ATTACHMENTS_DIR);
        if let Err(err) = std::fs::create_dir_all(&dir) {
            super::self_note_io_err(self.shared, "file", &err);
        }
        if let Err(err) = std::fs::write(dir.join(&file_name), data) {
            super::self_note_io_err(self.shared, "file", &err);
        }
        let mut writer = gojson::ObjWriter::new();
        writer
            .field_str("file", &format!("{ATTACHMENTS_DIR}/{file_name}"))
            .field_str("mime_type", mime_type)
            .field_int("size", i64::try_from(data.len()).unwrap_or(i64::MAX))
            .field_str("sha256", &hash);
        let reference = writer.finish().unwrap_or_else(|_| b"{}".to_vec());
        self.attachments.by_hash.insert(hash, reference.clone());
        reference
    }
}

fn serialization_error(message: &str) -> JVal {
    JVal::obj()
        .set("serialization_error", JVal::Str(message.to_string()))
        .build()
}

fn string_field(value: &BTreeMap<String, JVal>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(JVal::Str(text)) = value.get(*key) {
            return Some(text.clone());
        }
    }
    None
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(b & 0xF), 16).unwrap_or('0'));
    }
    out
}

/// `imageExtension`: fixed table for the common types, `.bin` fallback.
/// Go consults the system mime DB (`mime.ExtensionsByType`), whose answer
/// varies by host — the deterministic table keeps fixture and production
/// naming identical.
fn image_extension(mime_type: &str) -> &'static str {
    match mime_type {
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        _ => ".bin",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `strings.ToLower` case table (verified against go1.27.1):
    /// simple per-rune folding — `İ`→`i` (no combining dot), Kelvin
    /// `\u{212A}`→`k`, `Σ`→`σ` (no final-sigma context rule), `ß`
    /// unchanged. The tree-path key check must match it exactly.
    #[test]
    fn normalized_key_eq_matches_go_to_lower() {
        // ASCII + separator stripping (the pre-existing coverage).
        assert!(normalized_key_eq("api_key", "apikey"));
        assert!(normalized_key_eq("API-KEY", "apikey"));
        assert!(normalized_key_eq("Set-Cookie", "setcookie"));
        // Unicode simple folds that Go redacts: dotted İ (U+0130) folds
        // to plain `i`, Kelvin sign (U+212A) folds to `k`.
        assert!(normalized_key_eq("AP\u{0130}KEY", "apikey"));
        assert!(normalized_key_eq("api\u{212A}ey", "apikey"));
        assert!(normalized_key_eq("\u{212A}ey", "key"));
        // Simple mapping only: `Σ` folds to `σ`, never the final `ς` —
        // `str::to_lowercase` would apply the context rule and diverge.
        assert!(normalized_key_eq("\u{03A3}\u{03A3}", "\u{03C3}\u{03C3}"));
        assert!(!normalized_key_eq("\u{03A3}\u{03A3}", "\u{03C3}\u{03C2}"));
        // `ß` has no simple fold to `ss` — Go leaves it, so no match.
        assert!(!normalized_key_eq("\u{00DF}", "ss"));
        // Non-matches stay non-matches.
        assert!(!normalized_key_eq("apikeys", "apikey"));
        assert!(!normalized_key_eq("apikey\u{0130}", "apikey"));
        assert!(!normalized_key_eq("", "apikey"));
    }

    /// The tree-path gates Go drives through `strings.ToLower`.
    #[test]
    fn secret_key_folds_unicode() {
        assert!(secret_key("AP\u{0130}KEY"));
        assert!(secret_key("ACCESS\u{212A}EY")); // Kelvin sign folds to `k`
        assert!(is_metadata_key("METADATA"));
        assert!(metadata_secret_key("F"));
        assert!(!secret_key("plain"));
    }

    /// The raw prescreen stays byte-wise ASCII like Go's `equalFoldKey`:
    /// a non-ASCII spelling that the tree path redacts does NOT trip the
    /// prescreen — Go leaks it in raw payloads too (parity quirk).
    #[test]
    fn prescreen_stays_ascii_only() {
        assert!(equal_fold_key(b"api-key", "apikey"));
        assert!(!equal_fold_key("AP\u{0130}KEY".as_bytes(), "apikey"));
        assert!(!equal_fold_key("api\u{212A}ey".as_bytes(), "apikey"));
        // End-to-end: a raw payload with the İ spelling passes through
        // untouched, exactly like Go's rawNeedsSanitize fast path.
        let raw = "{\"AP\u{0130}KEY\":\"secret\"}".as_bytes().to_vec();
        assert!(!raw_needs_sanitize(&raw));
        assert!(raw_needs_sanitize(br#"{"apikey":"secret"}"#));
    }
}
