//! A Go `flag` package port for the auxiliary binaries: single and double
//! dashes are equivalent, `-flag=value` and `-flag value` both work for
//! non-bool flags, bool flags take no separate argument (`-flag=false`
//! only), parsing stops at the first non-flag argument or `--`, and
//! `-h`/`-help` produce the `ErrHelp` path.
//!
//! Error text mirrors Go's `failf` messages (`flag provided but not
//! defined`, `flag needs an argument`, `invalid value ... for flag ...`,
//! `invalid boolean value ...`) so stderr stays greppable across ports.
//! `ErrorHandling::ContinueOnError` returns the error after printing
//! message+usage (probe subcommand `FlagSets`); `ExitOnError` exits with
//! code 2 (0 for help) like `flag.Parse`.

use std::cell::RefCell;
use std::fmt;
use std::time::Duration;

/// How a parse failure is surfaced (Go `flag.ErrorHandling`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ErrorHandling {
    /// Print message + usage, return the error (probe `FlagSets`).
    ContinueOnError,
    /// Print message + usage, exit 2 (0 on help) — `flag.Parse`.
    ExitOnError,
}

/// A flag-parse failure. `message` is the Go `failf` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagError {
    /// `-h`/`-help`/`--help` requested usage (Go `ErrHelp`).
    Help,
    /// A parse failure with the Go `failf` message.
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

/// Go `strconv.ParseInt`/`ParseUint`/`ParseFloat` error text.
fn parse_num_error(value: &str, name: &str, kind: &str) -> FlagError {
    FlagError::Failed(format!(
        "invalid value {value:?} for flag -{name}: strconv.{kind}: parsing {value:?}: invalid syntax"
    ))
}

/// Go `time.ParseDuration`: `[+-]?(\d+(\.\d*)?|\.\d+)(ns|us|µs|ms|s|m|h)`
/// sequences, optionally signed. Empty and unit-less numbers are errors.
// The f64→u64 cast intentionally saturates like Go's overflow clamp, and
// `u64::MAX as f64` is the documented comparison bound — the precision
// loss is inherent to the Go algorithm being mirrored.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn parse_duration(value: &str) -> Result<Duration, String> {
    let err = || format!("time: invalid duration {value:?}");
    let mut rest = value;
    let mut negative = false;
    if let Some(stripped) = rest.strip_prefix('-') {
        negative = true;
        rest = stripped;
    } else if let Some(stripped) = rest.strip_prefix('+') {
        rest = stripped;
    }
    if rest.is_empty() {
        return Err(err());
    }
    if rest == "0" {
        return Ok(Duration::ZERO);
    }
    let mut total = Duration::ZERO;
    while !rest.is_empty() {
        let digit_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if digit_end == 0 {
            return Err(err());
        }
        let number: f64 = rest[..digit_end].parse().map_err(|_| err())?;
        rest = &rest[digit_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let nanos = match &rest[..unit_end] {
            "ns" => 1f64,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => return Err(err()),
        };
        // Go saturates on overflow; clamp the same way.
        total += Duration::from_nanos((number * nanos).min(u64::MAX as f64) as u64);
        rest = &rest[unit_end..];
    }
    if negative {
        // Durations are signed in Go; the aux tools never use negative
        // values, so clamp to zero rather than inventing a sign.
        return Ok(Duration::ZERO);
    }
    Ok(total)
}

/// One flag definition's storage.
enum Slot {
    Str(RefCell<String>),
    Int(RefCell<i64>),
    Uint(RefCell<u64>),
    Float(RefCell<f64>),
    Bool(RefCell<bool>),
    Dur(RefCell<Duration>),
    /// Repeatable string flag (Go `flag.Var` accumulating values).
    List(RefCell<Vec<String>>),
}

struct FlagDef {
    name: &'static str,
    usage: &'static str,
    slot: Slot,
    /// Placeholder printed in usage (`string`, `int`, `duration`, ...).
    placeholder: &'static str,
    default_text: String,
}

/// A flag's handle returned by the `define_*` methods.
#[derive(Clone, Copy)]
pub struct Flag(usize);

/// A Go `flag.FlagSet` equivalent.
pub struct FlagSet {
    name: String,
    handling: ErrorHandling,
    defs: Vec<FlagDef>,
    /// Positional arguments after parsing stopped (`flag.Args()`).
    args: Vec<String>,
}

impl FlagSet {
    /// `flag.NewFlagSet(name, handling)`.
    #[must_use]
    pub fn new(name: &str, handling: ErrorHandling) -> Self {
        Self {
            name: name.to_string(),
            handling,
            defs: Vec::new(),
            args: Vec::new(),
        }
    }

    fn define(
        &mut self,
        name: &'static str,
        usage: &'static str,
        placeholder: &'static str,
        default_text: String,
        slot: Slot,
    ) -> Flag {
        self.defs.push(FlagDef {
            name,
            usage,
            slot,
            placeholder,
            default_text,
        });
        Flag(self.defs.len() - 1)
    }

    /// `fs.String(name, default, usage)`.
    pub fn string(&mut self, name: &'static str, default: &str, usage: &'static str) -> Flag {
        self.define(
            name,
            usage,
            "string",
            format!(" (default {default:?})"),
            Slot::Str(RefCell::new(default.to_string())),
        )
    }

    /// `fs.Int(name, default, usage)`.
    pub fn int(&mut self, name: &'static str, default: i64, usage: &'static str) -> Flag {
        let text = if default == 0 {
            String::new()
        } else {
            format!(" (default {default})")
        };
        self.define(name, usage, "int", text, Slot::Int(RefCell::new(default)))
    }

    /// `fs.Uint64`-style non-negative integer flag.
    pub fn uint(&mut self, name: &'static str, default: u64, usage: &'static str) -> Flag {
        let text = if default == 0 {
            String::new()
        } else {
            format!(" (default {default})")
        };
        self.define(name, usage, "uint", text, Slot::Uint(RefCell::new(default)))
    }

    /// `fs.Float64(name, default, usage)`.
    pub fn float(&mut self, name: &'static str, default: f64, usage: &'static str) -> Flag {
        self.define(
            name,
            usage,
            "float",
            format!(" (default {default})"),
            Slot::Float(RefCell::new(default)),
        )
    }

    /// `fs.Bool(name, default, usage)`.
    pub fn bool(&mut self, name: &'static str, default: bool, usage: &'static str) -> Flag {
        let text = if default {
            " (default true)".to_string()
        } else {
            String::new()
        };
        self.define(name, usage, "", text, Slot::Bool(RefCell::new(default)))
    }

    /// `fs.Duration(name, default, usage)` — Go `time.ParseDuration`.
    pub fn duration(&mut self, name: &'static str, default: Duration, usage: &'static str) -> Flag {
        let text = if default.is_zero() {
            String::new()
        } else {
            format!(" (default {}s)", default.as_secs_f64())
        };
        self.define(
            name,
            usage,
            "duration",
            text,
            Slot::Dur(RefCell::new(default)),
        )
    }

    /// `fs.Var` accumulating repeated `-name v` occurrences.
    pub fn list(&mut self, name: &'static str, usage: &'static str) -> Flag {
        self.define(
            name,
            usage,
            "value",
            String::new(),
            Slot::List(RefCell::new(Vec::new())),
        )
    }

    /// Current string value.
    #[must_use]
    pub fn str(&self, flag: Flag) -> String {
        match &self.defs[flag.0].slot {
            Slot::Str(v) => v.borrow().clone(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Current int value.
    #[must_use]
    pub fn get_int(&self, flag: Flag) -> i64 {
        match &self.defs[flag.0].slot {
            Slot::Int(v) => *v.borrow(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Current uint value.
    #[must_use]
    pub fn get_uint(&self, flag: Flag) -> u64 {
        match &self.defs[flag.0].slot {
            Slot::Uint(v) => *v.borrow(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Current float value.
    #[must_use]
    pub fn get_float(&self, flag: Flag) -> f64 {
        match &self.defs[flag.0].slot {
            Slot::Float(v) => *v.borrow(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Current bool value.
    #[must_use]
    pub fn get_bool(&self, flag: Flag) -> bool {
        match &self.defs[flag.0].slot {
            Slot::Bool(v) => *v.borrow(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Current duration value.
    #[must_use]
    pub fn get_duration(&self, flag: Flag) -> Duration {
        match &self.defs[flag.0].slot {
            Slot::Dur(v) => *v.borrow(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Accumulated values of a repeatable flag.
    #[must_use]
    pub fn get_list(&self, flag: Flag) -> Vec<String> {
        match &self.defs[flag.0].slot {
            Slot::List(v) => v.borrow().clone(),
            _ => unreachable!("flag kind mismatch"),
        }
    }

    /// Positional arguments after parsing stopped (`fs.Args()`).
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// `fs.PrintDefaults()` — `  -name placeholder\n    \tusage (default v)`
    /// sorted lexicographically by flag name like Go.
    #[must_use]
    pub fn defaults_text(&self) -> String {
        let mut defs: Vec<&FlagDef> = self.defs.iter().collect();
        defs.sort_by(|a, b| a.name.cmp(b.name));
        let mut out = String::new();
        for def in defs {
            out.push_str("  -");
            out.push_str(def.name);
            if !def.placeholder.is_empty() {
                out.push(' ');
                out.push_str(def.placeholder);
            }
            out.push_str("\n    \t");
            out.push_str(def.usage);
            out.push_str(&def.default_text);
            out.push('\n');
        }
        out
    }

    /// `fs.Parse(args)` — Go's parse loop with `failf` message text.
    ///
    /// # Errors
    ///
    /// [`FlagError::Help`] for `-h`/`-help`/`--help`, [`FlagError::Failed`]
    /// with the `failf` message otherwise. Under `ExitOnError` the process
    /// exits (2, or 0 for help) after printing message + usage.
    pub fn parse(&mut self, args: &[String]) -> Result<(), FlagError> {
        match self.parse_inner(args) {
            Ok(()) => Ok(()),
            Err(err) => {
                eprintln!("{err}");
                eprint!("Usage of {}:\n{}", self.name, self.defaults_text());
                if self.handling == ErrorHandling::ExitOnError {
                    std::process::exit(if err == FlagError::Help { 0 } else { 2 });
                }
                Err(err)
            }
        }
    }

    // Mirrors Go flag.Parse's per-argument loop for parity review.
    #[allow(clippy::too_many_lines)]
    fn parse_inner(&mut self, args: &[String]) -> Result<(), FlagError> {
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
                    i += 1;
                    break;
                }
            }
            let name_part = &s[num_minuses..];
            let nb = name_part.as_bytes();
            if nb.is_empty() || nb[0] == b'-' || nb[0] == b'=' {
                return Err(FlagError::Failed(format!("bad flag syntax: {s}")));
            }
            i += 1;
            let (name, mut value, mut has_value) = match name_part.find('=') {
                Some(eq) => (&name_part[..eq], &name_part[eq + 1..], true),
                None => (name_part, "", false),
            };
            let Some(index) = self.defs.iter().position(|d| d.name == name) else {
                if name == "h" || name == "help" {
                    return Err(FlagError::Help);
                }
                return Err(FlagError::Failed(format!(
                    "flag provided but not defined: -{name}"
                )));
            };
            let def = &self.defs[index];
            match &def.slot {
                Slot::Bool(cell) => {
                    if has_value {
                        match parse_go_bool(value) {
                            Some(parsed) => *cell.borrow_mut() = parsed,
                            None => {
                                return Err(FlagError::Failed(format!(
                                    "invalid boolean value {value:?} for -{name}: parse error"
                                )));
                            }
                        }
                    } else {
                        *cell.borrow_mut() = true;
                    }
                }
                Slot::Str(cell) => {
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
                    *cell.borrow_mut() = value.to_string();
                }
                Slot::List(cell) => {
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
                    cell.borrow_mut().push(value.to_string());
                }
                Slot::Int(cell) => {
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
                    match value.parse::<i64>() {
                        Ok(parsed) => *cell.borrow_mut() = parsed,
                        Err(_) => return Err(parse_num_error(value, name, "ParseInt")),
                    }
                }
                Slot::Uint(cell) => {
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
                    match value.parse::<u64>() {
                        Ok(parsed) => *cell.borrow_mut() = parsed,
                        Err(_) => return Err(parse_num_error(value, name, "ParseUint")),
                    }
                }
                Slot::Float(cell) => {
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
                    match value.parse::<f64>() {
                        Ok(parsed) => *cell.borrow_mut() = parsed,
                        Err(_) => return Err(parse_num_error(value, name, "ParseFloat")),
                    }
                }
                Slot::Dur(cell) => {
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
                    match parse_duration(value) {
                        Ok(parsed) => *cell.borrow_mut() = parsed,
                        Err(message) => {
                            return Err(FlagError::Failed(format!(
                                "invalid value {value:?} for flag -{name}: {message}"
                            )));
                        }
                    }
                }
            }
        }
        self.args = args[i..].to_vec();
        Ok(())
    }
}
