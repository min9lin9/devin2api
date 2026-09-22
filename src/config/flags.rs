//! Command-line flags for `devin-2api` — a port of the Go `flag` package
//! behavior used by `G/cmd/devin-2api/main.go`: `-config`, `-state-dir`
//! (string flags) and `-version` (bool flag).
//!
//! Go `flag` semantics preserved here: single and double dashes are
//! equivalent, `-flag=value` and `-flag value` both work for strings, bool
//! flags take no separate argument (`-version=false` only), parsing stops
//! at the first non-flag argument or `--`, `-h`/`-help` request usage, and
//! failures carry the `failf` message text. Callers map
//! [`FlagError::Help`] to exit 0 and other errors to exit 2, matching
//! `flag.ExitOnError`.

use std::fmt;

/// Parsed flag values (all defaults match the Go `flag.String/Bool` calls).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Flags {
    /// `-config` value: YAML config path override.
    pub config: String,
    /// `-state-dir` value: state/log root override.
    pub state_dir: String,
    /// `-version`: print the build version and exit.
    pub version: bool,
    /// Positional arguments after flag parsing stopped (`flag.Args()`).
    pub rest: Vec<String>,
}

/// A flag-parse failure. `message` is the Go `failf` text (already printed
/// to stderr by `flag.ExitOnError` before usage); [`FlagError::Help`] is
/// the `ErrHelp` case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagError {
    /// `-h`/`-help`/`--help` requested usage (Go `ErrHelp`, exit 0).
    Help,
    /// A parse failure with the Go `failf` message (exit 2 after usage).
    Failed(String),
}

impl fmt::Display for FlagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Help => f.write_str("flag: help requested"),
            Self::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for FlagError {}

/// The Go `strconv.ParseBool` accepted set.
fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// Parses `args` (argv without argv[0]) like Go's `flag.Parse` over the
/// three daemon flags.
///
/// # Errors
///
/// Returns [`FlagError::Help`] for `-h`/`-help`/`--help` and
/// [`FlagError::Failed`] with the `failf` message for everything else.
pub fn parse_flags(args: &[String]) -> Result<Flags, FlagError> {
    let mut flags = Flags::default();
    let mut i = 0;
    while i < args.len() {
        let s = &args[i];
        let bytes = s.as_bytes();
        if bytes.len() < 2 || bytes[0] != b'-' {
            break;
        }
        let mut num_minuses = 1;
        if bytes[1] == b'-' {
            num_minuses = 2;
            if bytes.len() == 2 {
                // "--" terminates the flags.
                i += 1;
                break;
            }
        }
        let name_part = &s[num_minuses..];
        let nb = name_part.as_bytes();
        if nb.is_empty() || nb[0] == b'-' || nb[0] == b'=' {
            return Err(FlagError::Failed(format!("bad flag syntax: {s}")));
        }
        i += 1; // the flag arg is consumed either way

        // Split name=value at the first '=' (Go: strings.Cut on name[1:] —
        // '=' cannot be the first byte, checked above).
        let (name, mut value, mut has_value) = match name_part.find('=') {
            Some(eq) => (&name_part[..eq], &name_part[eq + 1..], true),
            None => (name_part, "", false),
        };

        match name {
            "config" | "state-dir" => {
                if !has_value && i < args.len() {
                    value = &args[i];
                    has_value = true;
                    i += 1;
                }
                if !has_value {
                    return Err(FlagError::Failed(format!(
                        "flag needs an argument: -{name}"
                    )));
                }
                if name == "config" {
                    flags.config = value.to_string();
                } else {
                    flags.state_dir = value.to_string();
                }
            }
            "version" => {
                if has_value {
                    match parse_go_bool(value) {
                        Some(parsed) => flags.version = parsed,
                        None => {
                            return Err(FlagError::Failed(format!(
                                "invalid boolean value {value:?} for -version: parse error"
                            )));
                        }
                    }
                } else {
                    flags.version = true;
                }
            }
            "h" | "help" => return Err(FlagError::Help),
            _ => {
                return Err(FlagError::Failed(format!(
                    "flag provided but not defined: -{name}"
                )));
            }
        }
    }
    flags.rest = args[i..].to_vec();
    Ok(flags)
}

/// The Go default-usage text for the daemon flag set, sorted
/// lexicographically like `PrintDefaults`. `program` is `argv[0]`.
#[must_use]
pub fn usage_text(program: &str) -> String {
    format!(
        "Usage of {program}:\n  -config string\n    \tYAML 配置文件路径；缺省按 $DEVIN2API_CONFIG → ./config.yaml → 平台默认目录解析\n  -state-dir string\n    \t日志与状态文件根目录；缺省按 $DEVIN2API_STATE_DIR → 平台默认目录解析\n  -version\n    \t打印构建版本后退出\n"
    )
}
