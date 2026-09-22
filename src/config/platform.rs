//! Platform path resolution — the port of `DefaultConfigPath`,
//! `DefaultStateDir`, `ResolveConfigPath`, `ResolveStateDir`,
//! `reusePortEnabled` and the `filepath.Abs`/`filepath.Join` semantics they
//! rely on.
//!
//! Every function takes an explicit [`Platform`] and an environment lookup
//! so Windows/macOS cases are testable on Linux without mutating the real
//! process environment.

use std::path::Path;

use super::ConfigError;

/// The platform whose path conventions apply — mirrors `runtime.GOOS` for
/// the three supported families (`windows`, `darwin`, everything else).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// `GOOS=windows`: `%AppData%`/`%LocalAppData%`, `\` separators.
    Windows,
    /// `GOOS=darwin`: `~/Library/Application Support`, `/` separators.
    Darwin,
    /// All other Unix-likes: XDG base directories, `/` separators.
    Unix,
}

impl Platform {
    /// The platform this binary was compiled for.
    #[must_use]
    pub fn current() -> Self {
        match std::env::consts::OS {
            "windows" => Self::Windows,
            "macos" => Self::Darwin,
            _ => Self::Unix,
        }
    }

    fn separator(self) -> char {
        match self {
            Self::Windows => '\\',
            _ => '/',
        }
    }
}

/// `os.Getenv` on Windows is case-insensitive; Go reads `AppData` /
/// `LocalAppData` / `USERPROFILE` with these exact spellings. Simulated
/// environments should populate the canonical casing.
fn env_get(env: &dyn Fn(&str) -> Option<String>, name: &str) -> String {
    env(name).unwrap_or_default()
}

/// Port of `os.UserConfigDir` for `platform`.
fn user_config_dir(
    platform: Platform,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    match platform {
        Platform::Windows => {
            let dir = env_get(env, "AppData");
            if dir.is_empty() {
                return Err(ConfigError::Resolve("%AppData% is not defined".to_string()));
            }
            Ok(dir)
        }
        Platform::Darwin => {
            let home = env_get(env, "HOME");
            if home.is_empty() {
                return Err(ConfigError::Resolve("$HOME is not defined".to_string()));
            }
            Ok(format!("{home}/Library/Application Support"))
        }
        Platform::Unix => {
            let dir = env_get(env, "XDG_CONFIG_HOME");
            if dir.is_empty() {
                let home = env_get(env, "HOME");
                if home.is_empty() {
                    return Err(ConfigError::Resolve(
                        "neither $XDG_CONFIG_HOME nor $HOME are defined".to_string(),
                    ));
                }
                Ok(format!("{home}/.config"))
            } else if !is_abs_for(platform, &dir) {
                Err(ConfigError::Resolve(
                    "path in $XDG_CONFIG_HOME is relative".to_string(),
                ))
            } else {
                Ok(dir)
            }
        }
    }
}

/// Port of `os.UserCacheDir` for `platform`.
fn user_cache_dir(
    platform: Platform,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    match platform {
        Platform::Windows => {
            let dir = env_get(env, "LocalAppData");
            if dir.is_empty() {
                return Err(ConfigError::Resolve(
                    "%LocalAppData% is not defined".to_string(),
                ));
            }
            Ok(dir)
        }
        Platform::Darwin => {
            let home = env_get(env, "HOME");
            if home.is_empty() {
                return Err(ConfigError::Resolve("$HOME is not defined".to_string()));
            }
            Ok(format!("{home}/Library/Caches"))
        }
        Platform::Unix => {
            let dir = env_get(env, "XDG_CACHE_HOME");
            if dir.is_empty() {
                let home = env_get(env, "HOME");
                if home.is_empty() {
                    return Err(ConfigError::Resolve(
                        "neither $XDG_CACHE_HOME nor $HOME are defined".to_string(),
                    ));
                }
                Ok(format!("{home}/.cache"))
            } else if !is_abs_for(platform, &dir) {
                Err(ConfigError::Resolve(
                    "path in $XDG_CACHE_HOME is relative".to_string(),
                ))
            } else {
                Ok(dir)
            }
        }
    }
}

/// Port of `os.UserHomeDir` for `platform` (Unix `$HOME`, Windows
/// `%USERPROFILE%`; the android/ios fallbacks do not apply to the three
/// supported families).
fn user_home_dir(
    platform: Platform,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let (name, display) = match platform {
        Platform::Windows => ("USERPROFILE", "%userprofile%"),
        _ => ("HOME", "$HOME"),
    };
    let value = env_get(env, name);
    if value.is_empty() {
        return Err(ConfigError::Resolve(format!("{display} is not defined")));
    }
    Ok(value)
}

/// Port of `DefaultConfigPath`: `<user config dir>/devin-2api/config.yaml`.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the user config dir is unknown.
pub fn default_config_path_for(
    platform: Platform,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let dir = user_config_dir(platform, env)
        .map_err(|err| ConfigError::Resolve(format!("resolve user config dir: {err}")))?;
    Ok(join_for(platform, &[&dir, "devin-2api", "config.yaml"]))
}

/// Port of `DefaultStateDir`: Windows → `%LocalAppData%\devin-2api`,
/// macOS → `~/Library/Application Support/devin-2api`, other Unix →
/// `$XDG_STATE_HOME/devin-2api` (default `~/.local/state/devin-2api`).
///
/// Note the Go original does NOT require `XDG_STATE_HOME` to be absolute
/// (unlike `XDG_CONFIG_HOME` via `os.UserConfigDir`); that asymmetry is
/// preserved.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the base dir is unknown.
pub fn default_state_dir_for(
    platform: Platform,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    match platform {
        Platform::Windows => {
            let dir = user_cache_dir(platform, env)
                .map_err(|err| ConfigError::Resolve(format!("resolve user cache dir: {err}")))?;
            Ok(join_for(platform, &[&dir, "devin-2api"]))
        }
        Platform::Darwin => {
            let dir = user_config_dir(platform, env)
                .map_err(|err| ConfigError::Resolve(format!("resolve user config dir: {err}")))?;
            Ok(join_for(platform, &[&dir, "devin-2api"]))
        }
        Platform::Unix => {
            let dir = env_get(env, "XDG_STATE_HOME");
            if !dir.is_empty() {
                return Ok(join_for(platform, &[&dir, "devin-2api"]));
            }
            let home = user_home_dir(platform, env)
                .map_err(|err| ConfigError::Resolve(format!("resolve home dir: {err}")))?;
            Ok(join_for(
                platform,
                &[&home, ".local", "state", "devin-2api"],
            ))
        }
    }
}

/// Port of `ResolveConfigPath` with injected environment, working directory
/// and platform: flag → `DEVIN2API_CONFIG` → `./config.yaml` (only when it
/// exists under `cwd`) → platform default.
///
/// `cwd` is a real filesystem path used for the `./config.yaml` existence
/// probe; pass `Path::new("")` for the process working directory.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the platform default is unknown.
pub fn resolve_config_path_with(
    flag_path: &str,
    env: &dyn Fn(&str) -> Option<String>,
    cwd: &Path,
    platform: Platform,
) -> Result<String, ConfigError> {
    if !flag_path.is_empty() {
        return Ok(flag_path.to_string());
    }
    let env_value = env_get(env, "DEVIN2API_CONFIG");
    let env_value = env_value.trim();
    if !env_value.is_empty() {
        return Ok(env_value.to_string());
    }
    if cwd.join("config.yaml").exists() {
        return Ok("config.yaml".to_string());
    }
    default_config_path_for(platform, env)
}

/// Port of `ResolveStateDir` with injected environment and platform:
/// flag → `DEVIN2API_STATE_DIR` → platform default.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the platform default is unknown.
pub fn resolve_state_dir_with(
    flag_dir: &str,
    env: &dyn Fn(&str) -> Option<String>,
    platform: Platform,
) -> Result<String, ConfigError> {
    if !flag_dir.is_empty() {
        return Ok(flag_dir.to_string());
    }
    let env_value = env_get(env, "DEVIN2API_STATE_DIR");
    let env_value = env_value.trim();
    if !env_value.is_empty() {
        return Ok(env_value.to_string());
    }
    default_state_dir_for(platform, env)
}

/// Port of `reusePortEnabled`: `DEVIN2API_REUSEPORT` is `1` or `true`
/// (case-insensitive) and the platform supports `SO_REUSEPORT` handoff —
/// never on Windows.
#[must_use]
pub fn reuse_port_enabled_for(env: &dyn Fn(&str) -> Option<String>, platform: Platform) -> bool {
    let supported = !matches!(platform, Platform::Windows);
    let value = env_get(env, "DEVIN2API_REUSEPORT");
    supported && (value == "1" || value.eq_ignore_ascii_case("true"))
}

/// Port of `filepath.Abs` for `platform`: `Clean(path)` when already
/// absolute, otherwise `join(cwd, path)`.
///
/// Windows note: the Go original calls `syscall.FullPath`, which resolves
/// drive-relative paths (`c:foo`) against the per-drive working directory.
/// This port joins them against `cwd` instead — the daemon only ever
/// absolutizes flag/env/config paths, where drive-relative input is not a
/// supported form.
#[must_use]
pub fn absolutize_for(platform: Platform, path: &str, cwd: &str) -> String {
    if is_abs_for(platform, path) {
        return clean_for(platform, path);
    }
    join_for(platform, &[cwd, path])
}

/// Port of `filepath.Join` for `platform`. Unix joins from the first
/// non-empty element with `/` then `Clean`s; Windows additionally strips
/// leading separators after a separator-terminated element (UNC guard),
/// inserts `.\` between a bare `\` and a `??` element, and adds no
/// separator after a `:`-terminated (drive-relative) element.
#[must_use]
pub fn join_for(platform: Platform, elems: &[&str]) -> String {
    if platform == Platform::Windows {
        join_windows(elems)
    } else {
        for (i, elem) in elems.iter().enumerate() {
            if !elem.is_empty() {
                let joined = elems[i..].join("/");
                return clean_for(platform, &joined);
            }
        }
        String::new()
    }
}

fn join_windows(elems: &[&str]) -> String {
    let mut b = String::new();
    let mut last_char = 0u8;
    for &elem in elems {
        let mut e = elem;
        if b.is_empty() {
            // Add the first non-empty path element unchanged.
        } else if is_sep_windows(last_char) {
            // Strip leading slashes from the next element to avoid creating
            // a UNC path from non-UNC elements.
            while !e.is_empty() && is_sep_windows(e.as_bytes()[0]) {
                e = &e[1..];
            }
            // `\` + `??` needs an extra `.\` to avoid a Root Local Device
            // path.
            if b.len() == 1
                && e.starts_with("??")
                && (e.len() == 2 || is_sep_windows(e.as_bytes()[2]))
            {
                b.push_str(".\\");
            }
        } else if last_char == b':' {
            // Drive-relative: no separator; leading slashes preserved.
        } else {
            b.push('\\');
            last_char = b'\\';
        }
        if !e.is_empty() {
            b.push_str(e);
            last_char = e.as_bytes()[e.len() - 1];
        }
    }
    if b.is_empty() {
        return String::new();
    }
    clean_for(Platform::Windows, &b)
}

/// Port of `filepathlite.IsAbs` for `platform`.
#[must_use]
pub fn is_abs_for(platform: Platform, path: &str) -> bool {
    match platform {
        Platform::Windows => {
            let bytes = path.as_bytes();
            let vol_len = volume_name_len(bytes);
            if vol_len == 0 {
                return false;
            }
            // A double-separator volume name (UNC) is absolute by itself.
            if is_sep_windows(bytes[0]) && is_sep_windows(bytes[1]) {
                return true;
            }
            let rest = &bytes[vol_len..];
            !rest.is_empty() && is_sep_windows(rest[0])
        }
        _ => path.starts_with('/'),
    }
}

fn is_sep_windows(byte: u8) -> bool {
    byte == b'/' || byte == b'\\'
}

fn is_sep(platform: Platform, byte: u8) -> bool {
    match platform {
        Platform::Windows => is_sep_windows(byte),
        _ => byte == b'/',
    }
}

fn to_upper(byte: u8) -> u8 {
    byte.to_ascii_uppercase()
}

/// Port of `pathHasPrefixFold`: `s` starts with `prefix` ignoring case and
/// treating `/` and `\` as equivalent; a longer `s` must continue with a
/// separator.
fn path_has_prefix_fold(s: &[u8], prefix: &[u8]) -> bool {
    if s.len() < prefix.len() {
        return false;
    }
    for (i, &p) in prefix.iter().enumerate() {
        if is_sep_windows(p) {
            if !is_sep_windows(s[i]) {
                return false;
            }
        } else if to_upper(p) != to_upper(s[i]) {
            return false;
        }
    }
    if s.len() > prefix.len() && !is_sep_windows(s[prefix.len()]) {
        return false;
    }
    true
}

/// Port of `cutPath`: split around the first path separator.
fn cut_path(path: &[u8]) -> (&[u8], &[u8], bool) {
    for (i, &byte) in path.iter().enumerate() {
        if is_sep_windows(byte) {
            return (&path[..i], &path[i + 1..], true);
        }
    }
    (path, &[], false)
}

/// Port of `uncLen`: length of the UNC volume prefix after `prefix_len`.
fn unc_len(path: &[u8], prefix_len: usize) -> usize {
    let mut count = 0;
    for (i, &byte) in path.iter().enumerate().skip(prefix_len) {
        if is_sep_windows(byte) {
            count += 1;
            if count == 2 {
                return i;
            }
        }
    }
    path.len()
}

/// Port of `validVolumeNameLen`: `n` unless `path[..n]` contains a `..`
/// component, in which case 0.
fn valid_volume_name_len(path: &[u8], n: usize) -> usize {
    let mut rest = &path[..n];
    while !rest.is_empty() {
        let (part, after, _) = cut_path(rest);
        if part == b".." {
            return 0;
        }
        rest = after;
    }
    n
}

/// Port of `volumeNameLen` (Windows): length of the leading volume name.
fn volume_name_len(path: &[u8]) -> usize {
    if path.len() >= 2 && path[1] == b':' {
        // Drive letter (Go does not restrict it to A-Z).
        return 2;
    }
    if path.is_empty() || !is_sep_windows(path[0]) {
        return 0;
    }
    if path_has_prefix_fold(path, b"\\\\.")
        || path_has_prefix_fold(path, b"\\\\?")
        || path_has_prefix_fold(path, b"\\??")
    {
        // Device prefixes: \\.\ , \\?\ , \??\
        if path.len() == 3 {
            return 3;
        }
        if path_has_prefix_fold(&path[4..], b"UNC") {
            return valid_volume_name_len(path, unc_len(path, "\\\\.\\UNC\\".len()));
        }
        let (_, rest, ok) = cut_path(&path[4..]);
        if !ok {
            return valid_volume_name_len(path, path.len());
        }
        return valid_volume_name_len(path, path.len() - rest.len() - 1);
    }
    if path.len() >= 2 && is_sep_windows(path[1]) {
        // UNC path.
        return valid_volume_name_len(path, unc_len(path, 2));
    }
    0
}

/// Port of `filepathlite.Clean` (== `filepath.Clean`) for `platform`,
/// including the Windows `postClean` guard that keeps a relative path
/// relative when a `:` lands in the first element.
#[must_use]
pub fn clean_for(platform: Platform, path: &str) -> String {
    let sep = platform.separator() as u8;
    let original = path.as_bytes();
    let vol_len = match platform {
        Platform::Windows => volume_name_len(original),
        _ => 0,
    };
    let path_bytes = &original[vol_len..];
    if path_bytes.is_empty() {
        if vol_len > 1 && is_sep(platform, original[0]) && is_sep(platform, original[1]) {
            // UNC volume name alone: separators normalized.
            return from_slash(platform, path);
        }
        return format!("{path}.");
    }
    let rooted = is_sep(platform, path_bytes[0]);

    // lazybuf equivalent: write into `out`; `w` is the write cursor and the
    // output may share a prefix with the input (we materialize eagerly —
    // only the observable result matters).
    let n = path_bytes.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        out.push(sep);
        r = 1;
        dotdot = 1;
    }

    while r < n {
        if is_sep(platform, path_bytes[r]) {
            // empty path element
            r += 1;
        } else if path_bytes[r] == b'.' && (r + 1 == n || is_sep(platform, path_bytes[r + 1])) {
            // . element
            r += 1;
        } else if path_bytes[r] == b'.'
            && path_bytes[r + 1] == b'.'
            && (r + 2 == n || is_sep(platform, path_bytes[r + 2]))
        {
            // .. element: remove to last separator
            r += 2;
            if out.len() > dotdot {
                // can backtrack: Go decrements w to the separator INDEX,
                // which drops both the last element and its separator.
                out.pop();
                while out.len() > dotdot && !is_sep(platform, out[out.len() - 1]) {
                    out.pop();
                }
                if out.len() > dotdot && is_sep(platform, out[out.len() - 1]) {
                    out.pop();
                }
            } else if !rooted {
                // cannot backtrack, but not rooted: append .. element.
                if !out.is_empty() {
                    out.push(sep);
                }
                out.extend_from_slice(b"..");
                dotdot = out.len();
            }
        } else {
            // real path element: add separator if needed, then copy.
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push(sep);
            }
            while r < n && !is_sep(platform, path_bytes[r]) {
                out.push(path_bytes[r]);
                r += 1;
            }
        }
    }

    if out.is_empty() {
        out.push(b'.');
    }

    // postClean (Windows only): if a ':' appears in the first path element
    // of a volume-less, changed relative path, prepend `.\` so `a/../c:`
    // does not become the drive-relative `c:`. The Go lazybuf only marks
    // the path "changed" when the output diverges from the input prefix.
    if platform == Platform::Windows && vol_len == 0 {
        let changed = out[..] != path_bytes[..out.len().min(n)];
        if changed {
            for &byte in &out {
                if is_sep_windows(byte) {
                    break;
                }
                if byte == b':' {
                    let mut prefixed = Vec::with_capacity(out.len() + 2);
                    prefixed.extend_from_slice(b".\\");
                    prefixed.extend_from_slice(&out);
                    out = prefixed;
                    break;
                }
            }
        }
    }

    let mut result = String::with_capacity(vol_len + out.len());
    result.push_str(&String::from_utf8_lossy(&original[..vol_len]));
    result.push_str(&String::from_utf8_lossy(&out));
    from_slash(platform, &result)
}

/// Port of `filepathlite.FromSlash`: on Windows every `/` becomes `\`;
/// elsewhere the path is unchanged.
fn from_slash(platform: Platform, path: &str) -> String {
    match platform {
        Platform::Windows => path.replace('/', "\\"),
        _ => path.to_string(),
    }
}
