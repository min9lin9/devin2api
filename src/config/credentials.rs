//! Devin credential discovery — the port of `ResolveDevinToken` and
//! `devinCredentialsPaths`.
//!
//! The Go original scans `credentials.toml` with the regex
//! `(?m)^\s*windsurf_api_key\s*=\s*"([^"]+)"` rather than a TOML parser, so
//! malformed files simply yield no token instead of an error. That contract
//! is preserved: [`credentials_token`] is a line-oriented port of the same
//! pattern.

use super::platform::{Platform, join_for};

/// Go regexp `\s` (no `\v`) used before the key and around `=`.
fn is_regexp_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

/// `strings.TrimSpace`-style trim for the captured value (Unicode
/// whitespace, including `\v`).
fn trim_space(value: &str) -> &str {
    value.trim()
}

/// Extracts `windsurf_api_key` from `credentials.toml` bytes — the port of
/// `devinCredentialsTokenPattern.FindSubmatch`. Returns the first match's
/// captured value after `TrimSpace`, or `None`.
///
/// The pattern is `(?m)^\s*windsurf_api_key\s*=\s*"([^"]+)"`. Because Go's
/// `\s` includes `\n`, the whitespace runs may span line boundaries (the
/// key, `=` and quoted value can sit on separate lines) and the captured
/// `[^"]+` may contain newlines. `^` anchors only at position 0 and right
/// after a `\n`, so the scan tries exactly those offsets, in order — the
/// same leftmost-first match Go returns.
#[must_use]
pub fn credentials_token(data: &[u8]) -> Option<String> {
    let mut i = 0usize;
    while i <= data.len() {
        // `^\s*`: whitespace run starting at a line start.
        let mut j = i;
        while j < data.len() && is_regexp_space(data[j]) {
            j += 1;
        }
        if data[j..].starts_with(b"windsurf_api_key") {
            let mut k = j + b"windsurf_api_key".len();
            while k < data.len() && is_regexp_space(data[k]) {
                k += 1;
            }
            if k < data.len() && data[k] == b'=' {
                k += 1;
                while k < data.len() && is_regexp_space(data[k]) {
                    k += 1;
                }
                if k < data.len() && data[k] == b'"' {
                    let value_start = k + 1;
                    // `[^"]+"`: first closing quote ends the capture; the
                    // `+` requires at least one byte.
                    if let Some(rel) = data[value_start..].iter().position(|&b| b == b'"')
                        && rel > 0
                    {
                        let value = &data[value_start..value_start + rel];
                        return Some(trim_space(&String::from_utf8_lossy(value)).to_string());
                    }
                }
            }
        }
        // Advance to the next line start (the only other `^` position).
        match data[i..].iter().position(|&b| b == b'\n') {
            Some(rel) => i += rel + 1,
            None => break,
        }
    }
    None
}

/// Port of `devinCredentialsPaths`: candidate `credentials.toml` locations.
/// Windows probes `%AppData%\devin\credentials.toml` then
/// `%LocalAppData%\devin\credentials.toml` (the CLI ships inside the
/// Windsurf desktop app there); other platforms use
/// `$XDG_DATA_HOME/devin/credentials.toml` with `~/.local/share` fallback.
#[must_use]
pub fn devin_credentials_paths(
    env: &dyn Fn(&str) -> Option<String>,
    platform: Platform,
) -> Vec<String> {
    let mut dirs: Vec<String> = Vec::new();
    if platform == Platform::Windows {
        for name in ["APPDATA", "LOCALAPPDATA"] {
            if let Some(dir) = env(name)
                && !dir.is_empty()
            {
                dirs.push(dir);
            }
        }
    } else {
        let mut dir = env("XDG_DATA_HOME").unwrap_or_default();
        if dir.is_empty()
            && let Some(home) = env("HOME")
            && !home.is_empty()
        {
            dir = join_for(platform, &[&home, ".local", "share"]);
        }
        if !dir.is_empty() {
            dirs.push(dir);
        }
    }
    dirs.iter()
        .map(|dir| join_for(platform, &[dir, "devin", "credentials.toml"]))
        .collect()
}

/// Port of `ResolveDevinToken` with injected environment, platform and file
/// reader: `DEVIN_TOKEN` then `WINDSURF_API_KEY` (both `TrimSpace`d), then
/// each candidate `credentials.toml` in order. Returns `""` when nothing is
/// found.
#[must_use]
pub fn resolve_devin_token_with(
    env: &dyn Fn(&str) -> Option<String>,
    platform: Platform,
) -> String {
    resolve_devin_token_impl(env, platform, &|path| {
        std::fs::read(path).map_err(|err| err.to_string())
    })
}

/// The injectable-reader form used by tests to simulate credential files on
/// any platform.
#[must_use]
pub fn resolve_devin_token_impl(
    env: &dyn Fn(&str) -> Option<String>,
    platform: Platform,
    read: &dyn Fn(&str) -> Result<Vec<u8>, String>,
) -> String {
    for name in ["DEVIN_TOKEN", "WINDSURF_API_KEY"] {
        if let Some(value) = env(name) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    for path in devin_credentials_paths(env, platform) {
        let Ok(data) = read(&path) else {
            continue;
        };
        if let Some(token) = credentials_token(&data) {
            return token;
        }
    }
    String::new()
}
