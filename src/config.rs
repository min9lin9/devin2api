//! Configuration loading, credential discovery and platform paths.
//!
//! Port of `G/internal/config/config.go` plus the flag/env surface of
//! `G/cmd/devin-2api/main.go`. A loaded [`Config`] is an immutable snapshot:
//! hot-applied runtime changes update the holders directly, never mutate a
//! snapshot in place (see the Go comment on `config.Config`).

pub mod credentials;
pub mod flags;
mod gourl;
pub mod platform;

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::path::Path;

pub use credentials::{credentials_token, devin_credentials_paths, resolve_devin_token_with};
pub use flags::{FlagError, Flags, parse_flags, usage_text};
pub use platform::{
    Platform, absolutize_for, default_config_path_for, default_state_dir_for,
    resolve_config_path_with, resolve_state_dir_with, reuse_port_enabled_for,
};

/// Errors produced while loading configuration.
///
/// Message shapes mirror the Go `fmt.Errorf` wrappers in `config.Load` and the
/// path-resolution helpers so log output stays greppable across ports.
#[derive(Debug)]
pub enum ConfigError {
    /// `open config "<path>": <io error>`
    Open {
        path: String,
        source: std::io::Error,
    },
    /// `decode config "<path>": <yaml error>`
    Decode { path: String, message: String },
    /// `validate config "<path>": <validation error>`
    Validate {
        path: String,
        source: ValidationError,
    },
    /// Platform path resolution failed (e.g. `$HOME` undefined).
    Resolve(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => write!(f, "open config {path:?}: {source}"),
            Self::Decode { path, message } => write!(f, "decode config {path:?}: {message}"),
            Self::Validate { path, source } => write!(f, "validate config {path:?}: {source}"),
            Self::Resolve(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open { source, .. } => Some(source),
            Self::Validate { source, .. } => Some(source),
            Self::Decode { .. } | Self::Resolve(_) => None,
        }
    }
}

/// A config validation failure (the Go `Validate` error return).
#[derive(Debug)]
pub struct ValidationError(pub String);

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ValidationError {}

impl From<String> for ValidationError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// One configuration load snapshot, mirroring `config.Config`.
///
/// `Option` fields mirror the Go pointer fields: `None` means "key absent"
/// and is filled with the documented default by [`Config::validate`]; an
/// explicit `0`/negative stays `Some(..)` and disables the feature, exactly
/// like a non-nil Go pointer to a non-positive value.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// HTTP server configuration (`server`).
    #[serde(deserialize_with = "go_section")]
    pub server: ServerConfig,
    /// Devin Connect upstream configuration (`devin`).
    #[serde(deserialize_with = "go_section")]
    pub devin: DevinConfig,
    /// Local-diagnostics logging configuration (`debug`).
    #[serde(deserialize_with = "go_section")]
    pub debug: DebugConfig,
    /// Admin panel configuration (`dashboard`).
    #[serde(deserialize_with = "go_section")]
    pub dashboard: DashboardConfig,
    /// Access control for the OpenAI-compatible surface (`auth`).
    #[serde(deserialize_with = "go_section")]
    pub auth: AuthConfig,
}

/// `server` section — HTTP listen configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// HTTP listen address (`server.listen`); required.
    #[serde(deserialize_with = "go_string")]
    pub listen: String,
    /// Max concurrent `/v1/*` requests; `<=0` means default 1024.
    #[serde(deserialize_with = "go_i64")]
    pub max_concurrency: i64,
}

/// `devin` section — upstream call configuration.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DevinConfig {
    /// Devin Connect base URL (`devin.base_url`).
    #[serde(deserialize_with = "go_string")]
    pub base_url: String,
    /// Devin session token (`devin.token`); never written to logs.
    #[serde(deserialize_with = "go_string")]
    pub token: String,
    /// Devin chat model UID (`devin.model`).
    #[serde(deserialize_with = "go_string")]
    pub model: String,
    /// Optional HTTP/HTTPS/SOCKS5 proxy (`devin.proxy`); empty means direct
    /// or system `HTTP_PROXY`/`HTTPS_PROXY` environment.
    #[serde(deserialize_with = "go_string")]
    pub proxy: String,
    /// Force HTTP/1.1 with a fresh TCP connection per request
    /// (`devin.force_http1`); absent means the default `true`.
    #[serde(deserialize_with = "go_opt_bool")]
    pub force_http1: Option<bool>,
    /// Client model name → upstream UID map (`devin.aliases`), normalized by
    /// [`Config::validate`]. Duplicate YAML keys are rejected like
    /// `yaml.v3`'s strict decoder.
    #[serde(deserialize_with = "deserialize_aliases")]
    pub aliases: BTreeMap<String, String>,
    /// `metadata.extension_name`/`ide_name` override (`devin.client_name`).
    #[serde(deserialize_with = "go_string")]
    pub client_name: String,
    /// `metadata.extension_version`/`ide_version` override
    /// (`devin.client_version`).
    #[serde(deserialize_with = "go_string")]
    pub client_version: String,
    /// `metadata.os` override (`devin.client_os`).
    #[serde(deserialize_with = "go_string")]
    pub client_os: String,
    /// Per-minute `GetChatMessage` quota (`devin.max_rpm`); `<=0` disables
    /// window pacing (the cooldown latch always applies).
    #[serde(deserialize_with = "go_i64")]
    pub max_rpm: i64,
    /// Longest in-gate queue wait in seconds (`devin.gate_max_hold_seconds`);
    /// `<=0` defaults to 15 downstream.
    #[serde(deserialize_with = "go_i64")]
    pub gate_max_hold_seconds: i64,
    /// Latched-probe drip interval in seconds
    /// (`devin.gate_drip_interval_seconds`); `<=0` defaults to 8 downstream.
    #[serde(deserialize_with = "go_i64")]
    pub gate_drip_interval_seconds: i64,
    /// Fallback latch seconds when upstream omits a reset hint
    /// (`devin.gate_default_latch_seconds`); `<=0` defaults to 60 downstream.
    #[serde(deserialize_with = "go_i64")]
    pub gate_default_latch_seconds: i64,
    /// Estimated upstream minute-bucket boundary within the local minute
    /// (`devin.gate_window_offset_seconds`); default 0.
    #[serde(deserialize_with = "go_i64")]
    pub gate_window_offset_seconds: i64,
    /// Dead-zone seconds on both sides of the estimated bucket boundary
    /// (`devin.gate_window_guard_seconds`); `<=0` defaults to 2 downstream.
    #[serde(deserialize_with = "go_i64")]
    pub gate_window_guard_seconds: i64,
}

/// `debug` section — request-level debug log configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DebugConfig {
    /// Whether request debug logs are written (`debug.enabled`).
    #[serde(deserialize_with = "go_bool")]
    pub enabled: bool,
    /// Request-log retention days (`debug.retention_days`); absent → 14,
    /// `<=0` disables time-based cleanup.
    #[serde(deserialize_with = "go_opt_i64")]
    pub retention_days: Option<i64>,
    /// Total `logs/` size cap in MB (`debug.max_total_mb`); absent → 1024,
    /// `<=0` disables size-based cleanup.
    #[serde(deserialize_with = "go_opt_i64")]
    pub max_total_mb: Option<i64>,
    /// Hours before bulky stage files are stripped (`debug.payload_hours`);
    /// absent → 24, `<=0` disables stripping.
    #[serde(deserialize_with = "go_opt_i64")]
    pub payload_hours: Option<i64>,
    /// Newest failure directories protected from capacity eviction
    /// (`debug.keep_error_dirs`); absent → 32, `<=0` disables protection.
    #[serde(deserialize_with = "go_opt_i64")]
    pub keep_error_dirs: Option<i64>,
    /// Quota snapshot interval in minutes (`debug.quota_interval_minutes`);
    /// absent → 5, `<=0` disables sampling.
    #[serde(deserialize_with = "go_opt_i64")]
    pub quota_interval_minutes: Option<i64>,
    /// Dedicated diagnostics listener address (`debug.pprof_listen`); empty
    /// disables it. Unauthenticated — bind loopback only.
    #[serde(deserialize_with = "go_string")]
    pub pprof_listen: String,
}

/// `dashboard` section — admin panel configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DashboardConfig {
    /// Panel password (`dashboard.password`); empty means no login required.
    #[serde(deserialize_with = "go_string")]
    pub password: String,
}

/// `auth` section — client-facing API access control.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Key required on `/v1/*` (`auth.api_key`); empty disables auth.
    #[serde(deserialize_with = "go_string")]
    pub api_key: String,
}

/// Documented defaults applied by `Validate` when the key is absent.
pub const DEFAULT_MAX_CONCURRENCY: i64 = 1024;
/// Default `devin.force_http1` when the key is absent.
pub const DEFAULT_FORCE_HTTP1: bool = true;
/// Default `debug.retention_days`.
pub const DEFAULT_RETENTION_DAYS: i64 = 14;
/// Default `debug.max_total_mb`.
pub const DEFAULT_MAX_TOTAL_MB: i64 = 1024;
/// Default `debug.payload_hours`.
pub const DEFAULT_PAYLOAD_HOURS: i64 = 24;
/// Default `debug.keep_error_dirs`.
pub const DEFAULT_KEEP_ERROR_DIRS: i64 = 32;
/// Default `debug.quota_interval_minutes`.
pub const DEFAULT_QUOTA_INTERVAL_MINUTES: i64 = 5;

/// Loads and validates a YAML config file — the port of `config.Load`.
///
/// Decoding mirrors `yaml.NewDecoder(file)` + `KnownFields(true)` +
/// `Decode(&config)`: unknown fields are rejected, only the first YAML
/// document is read, and an empty stream fails to decode.
///
/// # Errors
///
/// Returns [`ConfigError::Open`], [`ConfigError::Decode`] or
/// [`ConfigError::Validate`] mirroring the Go error wrappers.
pub fn load(path: &str) -> Result<Config, ConfigError> {
    let data = std::fs::read(path).map_err(|source| ConfigError::Open {
        path: path.to_string(),
        source,
    })?;
    let mut config = decode_config(&data).map_err(|message| ConfigError::Decode {
        path: path.to_string(),
        message,
    })?;
    config.validate().map_err(|source| ConfigError::Validate {
        path: path.to_string(),
        source,
    })?;
    Ok(config)
}

/// Decodes the first YAML document of `data` into a [`Config`].
///
/// # Errors
///
/// Returns the `serde_yaml_ng` error text for malformed YAML, unknown fields,
/// type mismatches or an empty stream.
pub fn decode_config(data: &[u8]) -> Result<Config, String> {
    use serde::Deserialize;
    // Go's `Decode` hits EOF when the stream holds no document at all —
    // empty, whitespace-only, comment-only and directive-only inputs are
    // decode errors. serde_yaml_ng yields a Void document for those, so
    // the check happens on the raw text first.
    if !has_yaml_document(data) {
        return Err("empty YAML stream".to_string());
    }
    let mut documents = serde_yaml_ng::Deserializer::from_slice(data);
    let Some(first) = documents.next() else {
        return Err("empty YAML stream".to_string());
    };
    // A null document (`---`) decodes to the zero Config in Go — the
    // `d.null` failure is silent, so `Load` reports it later as the
    // `server.listen is required` validate error, not a decode error.
    <Option<Config> as Deserialize>::deserialize(first)
        .map(std::option::Option::unwrap_or_default)
        .map_err(|err| err.to_string())
}

/// Whether `data` contains any YAML content line: a line whose first
/// non-whitespace character is not `#` (comment) or `%` (directive).
/// `---`/`...` markers count as content — `---` alone is a null document
/// in Go, not EOF.
fn has_yaml_document(data: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(data) else {
        // Non-UTF-8 input is content; the parser will report it.
        return true;
    };
    text.lines().any(|line| {
        let trimmed = line.trim_start();
        !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with('%')
    })
}

impl Config {
    /// Validates required fields and applies defaults — the port of
    /// `Config.Validate` using the real process environment and platform.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for a missing `server.listen` or invalid
    /// `devin.aliases`.
    pub fn validate(&mut self) -> Result<(), ValidationError> {
        self.validate_with(&real_env, Platform::current())
    }

    /// `validate` with injected environment and platform so tests can pin
    /// token discovery and credential paths without mutating `std::env`
    /// (edition-2024 `set_var` is `unsafe`, which this workspace forbids).
    ///
    /// # Errors
    ///
    /// Same contract as [`Config::validate`].
    pub fn validate_with(
        &mut self,
        env: &dyn Fn(&str) -> Option<String>,
        platform: Platform,
    ) -> Result<(), ValidationError> {
        if self.server.listen.is_empty() {
            return Err("server.listen is required".to_string().into());
        }
        if self.server.max_concurrency <= 0 {
            self.server.max_concurrency = DEFAULT_MAX_CONCURRENCY;
        }
        // force_http1 defaults on: HTTP/2 single-connection stream reuse is
        // the root cause of the concurrency first-byte latency spike.
        if self.devin.force_http1.is_none() {
            self.devin.force_http1 = Some(DEFAULT_FORCE_HTTP1);
        }
        // Debug logs default to 14 days / 1 GiB so the disk cannot fill
        // silently.
        if self.debug.retention_days.is_none() {
            self.debug.retention_days = Some(DEFAULT_RETENTION_DAYS);
        }
        if self.debug.max_total_mb.is_none() {
            self.debug.max_total_mb = Some(DEFAULT_MAX_TOTAL_MB);
        }
        if self.debug.payload_hours.is_none() {
            self.debug.payload_hours = Some(DEFAULT_PAYLOAD_HOURS);
        }
        if self.debug.keep_error_dirs.is_none() {
            self.debug.keep_error_dirs = Some(DEFAULT_KEEP_ERROR_DIRS);
        }
        if self.debug.quota_interval_minutes.is_none() {
            self.debug.quota_interval_minutes = Some(DEFAULT_QUOTA_INTERVAL_MINUTES);
        }
        self.devin.aliases = normalize_aliases(std::mem::take(&mut self.devin.aliases))?;
        // Empty devin.token falls back to discovery: env vars, then the
        // Devin CLI credentials.toml.
        if self.devin.token.trim().is_empty() {
            self.devin.token = resolve_devin_token_with(env, platform);
        }
        Ok(())
    }

    /// Redacted introspection view of this snapshot — the port of
    /// `runtimeConfigView`'s `config` field: the snapshot serialized with
    /// YAML key names, then [`redact_config_secrets`] applied.
    #[must_use]
    pub fn redacted_view(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut fields = match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
        redact_config_secrets(&mut fields);
        fields
    }
}

/// Reads an environment variable the way `os.Getenv` sees it: absent and
/// empty both yield `None`-equivalent semantics for the callers here.
fn real_env(name: &str) -> Option<String> {
    std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
}

// ---------------------------------------------------------------------------
// yaml.v3 scalar coercion
//
// `yaml.v3` resolves a scalar first, then coerces the RESOLVED value into the
// target Go type: any non-null scalar assigned to a string field stores the
// raw scalar text (`listen: 8080` → "8080"); bool fields additionally accept
// the YAML 1.1 words y/yes/on/n/no/off (any listed casing); int fields accept
// int/uint and truncate in-range floats; and a null scalar into ANY
// non-nullable kind silently leaves the zero value (`listen: ~` → "",
// `n: ~` → 0, `b: ~` → false, `{a: ~}` → {"a": ""}, a null map key drops
// the pair). serde_yaml_ng resolves the same YAML 1.2 core schema but is
// strict about the target type, so these visitors reproduce the Go coercion
// on top of its resolution. Known residual gaps (documented in task-4
// evidence): Go parses `010` as octal and `1_000`/`08` as ints while
// serde_yaml_ng yields 10 / unresolvable strings, and Go stores the raw
// scalar text for non-string scalars where the resolved value is used here
// (e.g. `1.50` → "1.5", `0x10` → "16").
// ---------------------------------------------------------------------------

/// A string field decoded with yaml.v3 semantics: the raw scalar text for
/// any non-null scalar; null leaves the zero value `""` (Go `d.null` fails
/// silently for non-nullable kinds, it does not error).
fn go_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = String;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a scalar")
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<String, E> {
            // `!!binary` decodes to the raw bytes in Go.
            Ok(String::from_utf8_lossy(v).into_owned())
        }
        fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<String, E> {
            Ok(String::from_utf8_lossy(&v).into_owned())
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<String, E> {
            Ok(String::new())
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<String, E> {
            Ok(String::new())
        }
    }
    deserializer.deserialize_any(V)
}

/// `int64(f)` with Go/amd64 semantics: truncates in range, `i64::MIN` for
/// out-of-range or NaN inputs that reach the conversion.
// The `as i64` cast is the point: Rust saturates, Go/amd64 wraps to
// i64::MIN — the explicit TWO63 gate reproduces the wrap, and the in-range
// truncation is exactly what the cast must do.
#[allow(clippy::cast_possible_truncation)]
fn go_f64_as_i64(v: f64) -> i64 {
    // 2^63 as f64; `v as i64` saturates below -2^63 which is also amd64's
    // result, so only the upper boundary needs the explicit wrap.
    const TWO63: f64 = 9_223_372_036_854_776_000.0;
    if v >= TWO63 { i64::MIN } else { v as i64 }
}

/// An `int` field decoded with yaml.v3 semantics: signed/unsigned ints
/// within range, floats truncated after Go's `resolved <= math.MaxInt64`
/// gate (float64 `MaxInt64` is 2^63, so `f <= 2^63` converts and `f > 2^63`
/// errors); null leaves 0; strings and bools are decode errors.
fn go_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = i64;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an integer")
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<i64, E> {
            i64::try_from(v).map_err(|_| E::custom("cannot unmarshal uint64 into int"))
        }
        fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<i64, E> {
            i64::try_from(v).map_err(|_| E::custom("cannot unmarshal int into int"))
        }
        fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<i64, E> {
            i64::try_from(v).map_err(|_| E::custom("cannot unmarshal uint into int"))
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<i64, E> {
            const TWO63: f64 = 9_223_372_036_854_776_000.0;
            if v <= TWO63 {
                Ok(go_f64_as_i64(v))
            } else {
                Err(E::custom("cannot unmarshal float into int"))
            }
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<i64, E> {
            Ok(0)
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<i64, E> {
            Ok(0)
        }
    }
    deserializer.deserialize_any(V)
}

/// `*int` field: null/absent → `None`, otherwise [`go_i64`].
fn go_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Option<i64>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an optional integer")
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Option<i64>, E> {
            Ok(None)
        }
        fn visit_some<D2: serde::Deserializer<'de>>(self, d: D2) -> Result<Option<i64>, D2::Error> {
            go_i64(d).map(Some)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Option<i64>, E> {
            Ok(Some(v))
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Option<i64>, E> {
            i64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("cannot unmarshal uint64 into int"))
        }
        fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<Option<i64>, E> {
            i64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("cannot unmarshal int into int"))
        }
        fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<Option<i64>, E> {
            i64::try_from(v)
                .map(Some)
                .map_err(|_| E::custom("cannot unmarshal uint into int"))
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Option<i64>, E> {
            const TWO63: f64 = 9_223_372_036_854_776_000.0;
            if v <= TWO63 {
                Ok(Some(go_f64_as_i64(v)))
            } else {
                Err(E::custom("cannot unmarshal float into int"))
            }
        }
    }
    deserializer.deserialize_option(V)
}

/// A `bool` field decoded with yaml.v3 semantics: resolved bools plus the
/// YAML 1.1 compatibility words accepted when the target is a typed bool
/// (note: quoted "true"/"false" are NOT in that list — Go rejects them);
/// null leaves false.
fn go_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = bool;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a boolean")
        }
        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<bool, E> {
            match v {
                "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" => Ok(true),
                "n" | "N" | "no" | "No" | "NO" | "off" | "Off" | "OFF" => Ok(false),
                _ => Err(E::custom(format!("cannot unmarshal !!str {v:?} into bool"))),
            }
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<bool, E> {
            Ok(false)
        }
    }
    deserializer.deserialize_any(V)
}

/// `*bool` field: null/absent → `None`, otherwise [`go_bool`].
fn go_opt_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Option<bool>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an optional boolean")
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Option<bool>, E> {
            Ok(None)
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Option<bool>, E> {
            Ok(None)
        }
        fn visit_some<D2: serde::Deserializer<'de>>(
            self,
            d: D2,
        ) -> Result<Option<bool>, D2::Error> {
            go_bool(d).map(Some)
        }
        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Option<bool>, E> {
            Ok(Some(v))
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Option<bool>, E> {
            match v {
                "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" => Ok(Some(true)),
                "n" | "N" | "no" | "No" | "NO" | "off" | "Off" | "OFF" => Ok(Some(false)),
                _ => Err(E::custom(format!("cannot unmarshal !!str {v:?} into bool"))),
            }
        }
    }
    deserializer.deserialize_option(V)
}

/// A section field (`server`, `devin`, ...) decoded with yaml.v3
/// semantics: a null section leaves the zero struct (Go `d.null` fails
/// silently for structs too — `server: ~` decodes to the zero
/// `ServerConfig`, which then fails `server.listen is required` at
/// validate time, exactly like Go).
fn go_section<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
        .map(std::option::Option::unwrap_or_default)
}

/// Newtype used for `devin.aliases` keys/values so both sides get the
/// yaml.v3 string coercion (`{a: 1}` → `"a": "1"`, `{a: ~}` → `"a": ""`).
struct GoMapString(String);

impl<'de> serde::Deserialize<'de> for GoMapString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        go_string(deserializer).map(Self)
    }
}

/// `devin.aliases` deserializer: yaml.v3 `map[string]string` semantics —
/// null yields an empty map, keys/values are scalar-coerced, a null key
/// drops the pair (Go's failed key unmarshal `continue`s), and duplicate
/// keys are rejected like `yaml.v3`'s `uniqueKeys` ("mapping key already
/// defined"), which serde's map consumer would otherwise silently keep.
fn deserialize_aliases<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct AliasesVisitor;

    impl<'de> serde::de::Visitor<'de> for AliasesVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a string-to-string mapping")
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(BTreeMap::new())
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut map = BTreeMap::new();
            while let Some(key) = access.next_key::<Option<GoMapString>>()? {
                let Some(GoMapString(key)) = key else {
                    // Null key: Go's failed key unmarshal skips the pair.
                    access.next_value::<serde::de::IgnoredAny>()?;
                    continue;
                };
                let GoMapString(value) = access.next_value::<GoMapString>()?;
                if map.insert(key.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "mapping key {key:?} already defined"
                    )));
                }
            }
            Ok(map)
        }
    }

    deserializer.deserialize_any(AliasesVisitor)
}

/// Normalizes `devin.aliases` — the port of `normalizeAliases`.
///
/// Keys and targets are trimmed; empty keys, empty targets, `"*"` as a
/// target, post-trim duplicate keys and case-only-differing keys are
/// rejected (folded matching must stay unambiguous). Chained mappings are
/// then expanded to their final target and cycles are reported, so runtime
/// lookup is a single hop in the order exact → case-folded → `"*"`.
///
/// # Errors
///
/// Returns [`ValidationError`] with the same messages as the Go original.
pub fn normalize_aliases(
    aliases: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ValidationError> {
    if aliases.is_empty() {
        return Ok(aliases);
    }
    let mut normalized = BTreeMap::new();
    let mut folded: BTreeMap<String, String> = BTreeMap::new();
    for (key, target) in &aliases {
        let key = key.trim();
        let target = target.trim();
        if key.is_empty() {
            return Err("devin.aliases contains an empty key".to_string().into());
        }
        if target.is_empty() {
            return Err(format!("devin.aliases[{key:?}] has an empty target").into());
        }
        if target == "*" {
            return Err(format!(
                "devin.aliases[{key:?}]: \"*\" is only valid as a catch-all key, not a target"
            )
            .into());
        }
        if let Some(prev) = normalized.get(key)
            && prev != target
        {
            return Err(
                format!("devin.aliases: key {key:?} maps to both {prev:?} and {target:?}").into(),
            );
        }
        if let Some(prev) = folded.get(&key.to_lowercase())
            && prev != key
        {
            return Err(
                format!("devin.aliases: keys {prev:?} and {key:?} differ only by case").into(),
            );
        }
        normalized.insert(key.to_string(), target.to_string());
        folded.insert(key.to_lowercase(), key.to_string());
    }
    for key in normalized.keys().cloned().collect::<Vec<_>>() {
        let mut seen: HashSet<String> = HashSet::from([key.clone()]);
        let mut target = normalized[&key].clone();
        while let Some(next) = normalized.get(&target) {
            if seen.contains(&target) {
                return Err(format!("devin.aliases: cycle detected through {key:?}").into());
            }
            seen.insert(target.clone());
            target.clone_from(next);
        }
        normalized.insert(key, target);
    }
    Ok(normalized)
}

/// Resolves the config file path — the port of `ResolveConfigPath`:
/// explicit `-config` flag → `DEVIN2API_CONFIG` → `./config.yaml` (only when
/// it exists) → the platform default. The result may not exist; [`load`]
/// reports it with the path attached.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the platform default cannot be
/// resolved.
pub fn resolve_config_path(flag_path: &str) -> Result<String, ConfigError> {
    resolve_config_path_with(flag_path, &real_env, Path::new(""), Platform::current())
}

/// Resolves the state root directory — the port of `ResolveStateDir`:
/// `-state-dir` flag → `DEVIN2API_STATE_DIR` → the platform default.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the platform default cannot be
/// resolved.
pub fn resolve_state_dir(flag_dir: &str) -> Result<String, ConfigError> {
    resolve_state_dir_with(flag_dir, &real_env, Platform::current())
}

/// The platform-default config path — the port of `DefaultConfigPath` for
/// the current platform.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the user config dir is unknown.
pub fn default_config_path() -> Result<String, ConfigError> {
    default_config_path_for(Platform::current(), &real_env)
}

/// The platform-default state/log root — the port of `DefaultStateDir` for
/// the current platform.
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the state dir is unknown.
pub fn default_state_dir() -> Result<String, ConfigError> {
    default_state_dir_for(Platform::current(), &real_env)
}

/// Discovers a Devin session token — the port of `ResolveDevinToken`:
/// `DEVIN_TOKEN`/`WINDSURF_API_KEY` environment variables, then the Devin
/// CLI `credentials.toml` candidates for the current platform. Returns an
/// empty string when nothing is found; the caller decides whether that is
/// an error.
#[must_use]
pub fn resolve_devin_token() -> String {
    resolve_devin_token_with(&real_env, Platform::current())
}

/// Absolutizes `path` against the process working directory — the port of
/// the `filepath.Abs` calls in `main` (the approved relative-path fix: the
/// resolved `./config.yaml` and relative flag/env values are anchored to
/// the startup cwd before use).
///
/// # Errors
///
/// Returns [`ConfigError::Resolve`] when the working directory is unknown.
pub fn absolutize(path: &str) -> Result<String, ConfigError> {
    let cwd =
        std::env::current_dir().map_err(|err| ConfigError::Resolve(format!("getwd: {err}")))?;
    let cwd = cwd.to_string_lossy();
    Ok(absolutize_for(Platform::current(), path, &cwd))
}

/// Whether `SO_REUSEPORT` handoff is enabled — the port of
/// `reusePortEnabled`: `DEVIN2API_REUSEPORT` is `1` or `true`
/// (case-insensitive) and the platform supports it (never on Windows).
#[must_use]
pub fn reuse_port_enabled() -> bool {
    reuse_port_enabled_for(&real_env, Platform::current())
}

/// Replaces credential values in a serialized config view with
/// `sha256:<first-6-bytes-hex>` — the port of `redactConfigSecrets`.
/// `devin.proxy` may carry `user:pass@host` userinfo, which is also a
/// credential: the whole userinfo is stripped while the host stays
/// identifiable.
pub fn redact_config_secrets(fields: &mut serde_json::Map<String, serde_json::Value>) {
    use sha2::Digest;
    for (section_name, key) in [
        ("devin", "token"),
        ("auth", "api_key"),
        ("dashboard", "password"),
    ] {
        let Some(serde_json::Value::Object(section)) = fields.get_mut(section_name) else {
            continue;
        };
        let Some(serde_json::Value::String(raw)) = section.get(key) else {
            continue;
        };
        if raw.is_empty() {
            continue;
        }
        let sum = sha2::Sha256::digest(raw.as_bytes());
        let hex: String = sum[..6].iter().fold(String::new(), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        });
        section.insert(
            key.to_string(),
            serde_json::Value::String(format!("sha256:{hex}")),
        );
    }
    if let Some(serde_json::Value::Object(devin)) = fields.get_mut("devin")
        && let Some(serde_json::Value::String(raw)) = devin.get("proxy")
        && !raw.is_empty()
        && let Some(stripped) = strip_url_userinfo(raw)
    {
        devin.insert("proxy".to_string(), serde_json::Value::String(stripped));
    }
}

/// Strips `userinfo` from a URL exactly the way Go's `url.Parse` plus
/// `User = nil` plus `String()` does — see [`gourl`] for the ported
/// parser. Returns `None` when Go's parse fails or the URL carries no
/// userinfo, in which case the caller keeps the original string.
fn strip_url_userinfo(raw: &str) -> Option<String> {
    gourl::strip_url_userinfo(raw)
}
