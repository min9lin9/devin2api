//! Conversion of intermediate tool definitions to Devin native function
//! tools, plus injection of tool descriptions into the system prompt.
//!
//! Port of `G/internal/adapter/devin/tool_definition.go`.
//!
//! JSON parity notes: Go's `encoding/json` unmarshal produces `any` trees
//! (float64 numbers, map[string]any objects) and `Marshal` emits sorted
//! keys, HTML-escaped strings (`<`→`<`, `&`→`&`, `>`→`>`,
//! U+2028/29 escaped), and ES6-style float formatting (`f` mode inside
//! [1e-6, 1e21), `e` mode outside with `e+NN`/`e-N` exponents). The
//! `normalize_schema` pass mutates shared subtrees — `$ref` merges insert
//! *references*, so cyclic refs produce real cycles that `json.Marshal`
//! rejects with "encountered a cycle". `GJson` (Rc<RefCell> graph) plus the
//! hand-rolled `go_json_marshal` reproduce both behaviors exactly; a plain
//! `serde_json::Value` tree cannot.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::sync::{LazyLock, Mutex};

use devin_proto::generated::exa::api_server_pb as pb;
use regex_automata::meta::Regex;

use crate::domain::failure::Failure;
use crate::domain::request::ToolDefinition;

static DESCRIPTION_LIST_ITEM_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("^(?:[-*+][ \\t\\n\\x0C\\r]+|[0-9]+[.):][ \\t\\n\\x0C\\r]+|\\[[0-9]+\\][ \\t\\n\\x0C\\r]+)(.+)$")
        .expect("list item pattern must compile")
});

/// Appends non-empty tool descriptions to the Devin system prompt so the
/// model understands native tool purposes.
pub fn with_tool_descriptions(system_prompt: &str, tools: &[ToolDefinition]) -> String {
    let mut section = String::new();
    for tool in tools {
        let description = tool.description.trim();
        if description.is_empty() {
            continue;
        }
        if section.is_empty() {
            section.push_str("# tools descriptions");
        }
        section.push_str("\n<tool name=\"");
        section.push_str(&escape_xml_attribute(&tool.name));
        section.push_str("\">\n");
        section.push_str(&escape_xml_text(&format_tool_description(description)));
        section.push_str("\n</tool>");
    }
    if section.is_empty() {
        return system_prompt.to_string();
    }
    let trimmed_prompt = system_prompt.trim_end_matches(['\r', '\n']);
    if trimmed_prompt.trim().is_empty() {
        return section;
    }
    format!("{trimmed_prompt}\n\n{section}")
}

/// Reformats natural-language sentences into numbered items while keeping
/// code fences and JSON examples structurally intact.
fn format_tool_description(description: &str) -> String {
    fn append_blank_line(output: &mut Vec<String>) {
        if output.last().is_some_and(|last| !last.is_empty()) {
            output.push(String::new());
        }
    }
    let description = description.replace("\r\n", "\n").replace('\r', "\n");
    let mut output: Vec<String> = Vec::new();
    let mut prose: Vec<&str> = Vec::new();
    let mut item_number = 1usize;
    let mut in_code_fence = false;

    macro_rules! append_sentences {
        ($value:expr) => {{
            for sentence in split_description_sentences($value) {
                output.push(format!("{item_number}. {sentence}"));
                item_number += 1;
            }
        }};
    }
    macro_rules! flush_prose {
        () => {{
            if !prose.is_empty() {
                let paragraph = prose.join("\n").trim().to_string();
                prose.clear();
                if !paragraph.is_empty() {
                    if serde_json::from_str::<serde::de::IgnoredAny>(&paragraph).is_ok() {
                        output.push(paragraph);
                    } else {
                        append_sentences!(&paragraph);
                    }
                }
            }
        }};
    }

    for line in description.split('\n') {
        let trimmed_line = line.trim();
        if trimmed_line.starts_with("```") || trimmed_line.starts_with("~~~") {
            flush_prose!();
            output.push(line.to_string());
            in_code_fence = !in_code_fence;
            continue;
        }
        if in_code_fence {
            output.push(line.to_string());
            continue;
        }
        if trimmed_line.is_empty() {
            flush_prose!();
            append_blank_line(&mut output);
            continue;
        }
        if let Some(caps) = DESCRIPTION_LIST_ITEM_PATTERN
            .captures_iter(trimmed_line)
            .next()
            && let Some(item) = caps.get_group(1)
        {
            flush_prose!();
            append_sentences!(trimmed_line[item.start..item.end].trim());
            continue;
        }
        prose.push(line);
    }
    flush_prose!();
    output.join("\n").trim().to_string()
}

/// Splits a paragraph at sentence boundaries: a terminator counts only
/// when followed by whitespace (abbreviation false-positives are caught by
/// `ends_with_abbreviation`); CJK full-width punctuation without trailing
/// space also ends a sentence.
fn split_description_sentences(paragraph: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut start = 0usize;
    let mut offset = 0usize;
    while offset < paragraph.len() {
        let character = paragraph[offset..].chars().next().unwrap_or('\0');
        let end = offset + character.len_utf8();
        if is_sentence_terminator(character) {
            let mut next = end;
            while next < paragraph.len() {
                let next_character = paragraph[next..].chars().next().unwrap_or('\0');
                if !next_character.is_whitespace() {
                    break;
                }
                next += next_character.len_utf8();
            }
            let has_sentence_boundary =
                next > end || character == '。' || character == '！' || character == '？';
            if has_sentence_boundary
                && next < paragraph.len()
                && !ends_with_abbreviation(&paragraph[start..end])
            {
                sentences.push(paragraph[start..end].trim().to_string());
                start = next;
                offset = next;
                continue;
            }
        }
        offset = end;
    }
    if let Some(remaining) = paragraph
        .get(start..)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sentences.push(remaining.to_string());
    }
    sentences
}

/// Sentence terminators, both ASCII and CJK sets.
fn is_sentence_terminator(character: char) -> bool {
    matches!(character, '.' | '!' | '?' | '。' | '！' | '？')
}

/// Whether the fragment's last word is a common non-terminal abbreviation
/// (e.g./i.e./etc./vs./titles), so the terminator rule does not split at
/// the abbreviation's period.
fn ends_with_abbreviation(fragment: &str) -> bool {
    let lower = fragment.to_lowercase();
    let Some(word) = lower.split_whitespace().last() else {
        return false;
    };
    let word = word.trim_matches(|ch| matches!(ch, '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}'));
    matches!(
        word,
        "e.g." | "i.e." | "etc." | "vs." | "mr." | "mrs." | "dr." | "prof." | "no."
    )
}

/// Escapes tool/parameter names for XML attribute values — port of Go's
/// `xml.EscapeText` (escapeNewline=false): `"`→`&#34;`, `'`→`&#39;`,
/// `&`→`&amp;`, `<`→`&lt;`, `>`→`&gt;`, `\t`→`&#x9;`, `\r`→`&#xD;`,
/// newline kept literal, out-of-range runes → U+FFFD.
fn escape_xml_attribute(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push('\n'),
            '\r' => out.push_str("&#xD;"),
            _ if !is_in_character_range(ch) => out.push('\u{FFFD}'),
            _ => out.push(ch),
        }
    }
    out
}

/// Go `encoding/xml.isInCharacterRange` (XML 1.0 Char production).
fn is_in_character_range(r: char) -> bool {
    matches!(r,
        '\t' | '\n' | '\r'
        | '\u{20}'..='\u{D7FF}'
        | '\u{E000}'..='\u{FFFD}'
        | '\u{10000}'..='\u{10FFFF}')
}

/// Escapes only the characters that would break XML text boundaries;
/// ordinary quotes inside code examples are preserved.
fn escape_xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Cache of `convert_tool_definition` products keyed by (name, schema):
/// the same client tool set is resent verbatim per request, so the
/// strip+normalize double pass is pure repeated work. Cleared wholesale
/// past the cap to bound growth.
static TOOL_DEFINITION_CACHE: LazyLock<Mutex<BTreeMap<String, pb::ExaChatPb_ChatToolDefinition>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// Keeps tool identity and JSON Schema constraints, removing only
/// natural-language annotations.
///
/// The name charset is validated at `ToolDefinition::validate` (upstream
/// accepts only `[A-Za-z0-9_-]`); names arriving here are legal — illegal
/// names in replayed history are accepted upstream, the charset gate lives
/// on declarations only. No silent renaming: rewriting would break
/// client-replayed `tool_call` names and destroy the name's semantic signal.
///
/// Name/Description both send the bare name — the real description does
/// not travel in `description` but is merged into the system prompt via
/// `with_tool_descriptions` (natural-language annotations inside schemas
/// provably trigger upstream tool classification; the description field
/// feeds the same upstream tool-cognition channel — a fingerprint-
/// avoidance decision, never verified as a rejection).
pub fn convert_tool_definition(
    tool: &ToolDefinition,
) -> Result<pb::ExaChatPb_ChatToolDefinition, Failure> {
    let cache_key = format!("{}\0{}", tool.name, tool.input_schema);
    if let Some(cached) = TOOL_DEFINITION_CACHE
        .lock()
        .expect("tool definition cache poisoned")
        .get(&cache_key)
    {
        return Ok(cached.clone());
    }
    let schema = strip_schema_annotations(&tool.input_schema).map_err(|err| {
        Failure::plain(format!("sanitize Devin tool {:?} schema: {err}", tool.name))
    })?;
    let schema = normalize_schema(&schema).map_err(|err| {
        Failure::plain(format!(
            "normalize Devin tool {:?} schema: {err}",
            tool.name
        ))
    })?;
    let converted = pb::ExaChatPb_ChatToolDefinition {
        name: Some(tool.name.clone()),
        description: Some(tool.name.clone()),
        json_schema_string: Some(schema),
        ..Default::default()
    };
    let mut cache = TOOL_DEFINITION_CACHE
        .lock()
        .expect("tool definition cache poisoned");
    if cache.len() >= 512 {
        cache.clear();
    }
    cache.insert(cache_key, converted.clone());
    Ok(converted)
}

/// Strips annotation keys the upstream rejects from an input schema (see
/// `strip_schema_value_annotations`); invalid JSON errors as-is.
fn strip_schema_annotations(schema: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(schema).map_err(|err| go_unmarshal_error(&err))?;
    let cleaned = strip_schema_value_annotations(&value, false);
    go_json_marshal(&GJson::from_value(&cleaned))
}

/// Maps serde parse errors to Go `encoding/json` message text where the
/// shapes line up; otherwise the serde text is kept (both are opaque
/// "invalid JSON" diagnostics — the corpus only asserts error presence).
fn go_unmarshal_error(err: &serde_json::Error) -> String {
    if err.is_eof() {
        return "unexpected end of JSON input".to_string();
    }
    err.to_string()
}

/// Recursively strips annotations: `property_names` marks that the current
/// level is a `properties` key-name level — property names themselves are
/// keys to keep, not annotations.
fn strip_schema_value_annotations(
    value: &serde_json::Value,
    property_names: bool,
) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| strip_schema_value_annotations(item, false))
                .collect(),
        ),
        serde_json::Value::Object(map) => {
            let mut cleaned = serde_json::Map::new();
            for (key, child) in map {
                if is_natural_language_annotation(key) && !property_names {
                    continue;
                }
                if is_schema_literal(key) && !property_names {
                    cleaned.insert(key.clone(), child.clone());
                    continue;
                }
                cleaned.insert(
                    key.clone(),
                    strip_schema_value_annotations(child, key == "properties"),
                );
            }
            serde_json::Value::Object(cleaned)
        }
        _ => value.clone(),
    }
}

/// Keys whose content is a business value, not a recursively cleanable
/// schema definition.
fn is_schema_literal(key: &str) -> bool {
    matches!(key, "const" | "default" | "enum" | "example" | "examples")
}

/// Schema metadata confirmed to trigger Devin's upstream tool
/// classification.
fn is_natural_language_annotation(key: &str) -> bool {
    matches!(key, "description" | "title" | "$comment") || key.to_lowercase().starts_with("x-")
}

/// Eliminates the two schema shapes upstream deterministically rejects
/// (verified):
/// 1. Local `$ref`s (`#/$defs/x` JSON-pointers) inlined; top-level
///    `$defs`/`definitions`/`$schema` stripped.
/// 2. A top-level "bare property map" — no schema keywords, all values
///    objects — wrapped as `{"type":"object","properties":…}`.
///
/// Cyclic or unresolvable refs drop the `$ref` key and keep sibling
/// constraints (equivalent to `any`) — more debuggable than bouncing the
/// whole request off upstream's vague `invalid_argument`. Every other shape
/// upstream tolerates passes through unchanged.
fn normalize_schema(schema: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(schema).map_err(|err| go_unmarshal_error(&err))?;
    let root = GJson::from_value(&value);
    let mut resolving = HashSet::new();
    normalize_schema_value(&root, &root, &mut resolving, 0);
    if let GJson::Obj(map) = &root {
        map.borrow_mut().remove("$defs");
        map.borrow_mut().remove("definitions");
        map.borrow_mut().remove("$schema");
        if is_bare_property_map(&map.borrow()) {
            let properties = GJson::Obj(Rc::new(RefCell::new(
                map.borrow()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )));
            let mut wrapped: BTreeMap<String, GJson> = BTreeMap::new();
            wrapped.insert("type".to_string(), GJson::Str("object".to_string()));
            wrapped.insert("properties".to_string(), properties);
            let wrapped = GJson::Obj(Rc::new(RefCell::new(wrapped)));
            return go_json_marshal(&wrapped);
        }
    }
    go_json_marshal(&root)
}

/// Bounds `$ref` expansion depth so pathological nesting cannot inflate
/// without limit.
const MAX_SCHEMA_REF_DEPTH: usize = 32;

/// Recursively expands `$ref`s and normalizes schema structure: `root`
/// resolves references, `resolving` detects cycles, `depth` caps
/// pathological nesting. Mirrors Go's in-place map mutation: merged `$ref`
/// targets are inserted as *shared* nodes, so a self-referencing schema
/// produces a real cycle that `go_json_marshal` rejects.
fn normalize_schema_value(
    value: &GJson,
    root: &GJson,
    resolving: &mut HashSet<String>,
    depth: usize,
) {
    if depth > MAX_SCHEMA_REF_DEPTH {
        return;
    }
    match value {
        GJson::Arr(items) => {
            for index in 0..items.borrow().len() {
                let child = items.borrow()[index].clone();
                normalize_schema_value(&child, root, resolving, depth + 1);
            }
        }
        GJson::Obj(map) => {
            let ref_target = {
                let borrowed = map.borrow();
                match borrowed.get("$ref") {
                    Some(GJson::Str(reference)) if reference.starts_with('#') => {
                        resolve_local_ref(root, reference)
                            .filter(|_| !resolving.contains(reference))
                            .map(|target| (reference.clone(), target))
                    }
                    _ => None,
                }
            };
            if let Some((reference, target)) = ref_target {
                resolving.insert(reference.clone());
                normalize_schema_value(&target, root, resolving, depth + 1);
                resolving.remove(&reference);
                let mut borrowed = map.borrow_mut();
                borrowed.remove("$ref");
                // A $ref with sibling keys merges: siblings win over
                // same-named fields of the referenced side.
                if let GJson::Obj(resolved) = &target {
                    for (key, child) in resolved.borrow().iter() {
                        if !borrowed.contains_key(key) {
                            borrowed.insert(key.clone(), child.clone());
                        }
                    }
                }
            } else {
                // External URL, unresolvable path or cyclic ref: drop
                // $ref, keep the other keys. (Non-string or non-"#"
                // $refs are left untouched entirely.)
                let is_hash_string = matches!(
                    map.borrow().get("$ref"),
                    Some(GJson::Str(reference)) if reference.starts_with('#')
                );
                if is_hash_string {
                    map.borrow_mut().remove("$ref");
                }
            }
            let keys: Vec<String> = map.borrow().keys().cloned().collect();
            for key in keys {
                // Maps inside business-value literals are not schemas —
                // skip so $ref keys inside them are not expanded.
                if is_schema_literal(&key) {
                    continue;
                }
                let child = map.borrow().get(&key).cloned();
                if let Some(child) = child {
                    normalize_schema_value(&child, root, resolving, depth + 1);
                }
            }
        }
        _ => {}
    }
}

/// Resolves a `#/a/b` local JSON-pointer, handling `~0`/`~1` escapes.
fn resolve_local_ref(root: &GJson, reference: &str) -> Option<GJson> {
    if reference == "#" {
        return Some(root.clone());
    }
    let path = reference.strip_prefix("#/")?;
    let mut current = root.clone();
    for segment in path.split('/') {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        let next = {
            let GJson::Obj(map) = &current else {
                return None;
            };
            map.borrow().get(&segment).cloned()
        };
        current = next?;
    }
    Some(current)
}

/// Keywords used to decide "is this map a schema"; presence is enough —
/// the set need not exhaust JSON Schema.
fn is_schema_keyword(key: &str) -> bool {
    matches!(
        key,
        "type"
            | "properties"
            | "items"
            | "required"
            | "additionalProperties"
            | "allOf"
            | "anyOf"
            | "oneOf"
            | "not"
            | "enum"
            | "const"
            | "format"
            | "pattern"
            | "minLength"
            | "maxLength"
            | "minimum"
            | "maximum"
            | "exclusiveMinimum"
            | "exclusiveMaximum"
            | "multipleOf"
            | "minItems"
            | "maxItems"
            | "uniqueItems"
            | "contains"
            | "minProperties"
            | "maxProperties"
            | "patternProperties"
            | "propertyNames"
            | "dependentRequired"
            | "dependentSchemas"
            | "prefixItems"
            | "if"
            | "then"
            | "else"
            | "readOnly"
            | "writeOnly"
            | "deprecated"
            | "description"
            | "title"
            | "default"
            | "examples"
    )
}

/// Whether the object is the upstream-rejected "bare property map": no
/// schema keywords, no `$`/`x-` prefixed keys, and every value an object
/// (i.e. property subschemas).
fn is_bare_property_map(map: &BTreeMap<String, GJson>) -> bool {
    if map.is_empty() {
        return false;
    }
    map.iter().all(|(key, child)| {
        !is_schema_keyword(key)
            && !key.starts_with('$')
            && !key.to_lowercase().starts_with("x-")
            && matches!(child, GJson::Obj(_))
    })
}

/// JSON value as a shared mutable graph — the Rust equivalent of Go's
/// `any` tree where maps/slices are reference types. `$ref` merges insert
/// node references, so cyclic schemas become real cycles.
enum GJson {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Rc<RefCell<Vec<GJson>>>),
    Obj(Rc<RefCell<BTreeMap<String, GJson>>>),
}

impl GJson {
    fn from_value(value: &serde_json::Value) -> GJson {
        match value {
            serde_json::Value::Null => GJson::Null,
            serde_json::Value::Bool(b) => GJson::Bool(*b),
            serde_json::Value::Number(n) => GJson::Num(n.as_f64().unwrap_or(f64::NAN)),
            serde_json::Value::String(s) => GJson::Str(s.clone()),
            serde_json::Value::Array(items) => GJson::Arr(Rc::new(RefCell::new(
                items.iter().map(GJson::from_value).collect(),
            ))),
            serde_json::Value::Object(map) => GJson::Obj(Rc::new(RefCell::new(
                map.iter()
                    .map(|(k, v)| (k.clone(), GJson::from_value(v)))
                    .collect(),
            ))),
        }
    }
}

impl Clone for GJson {
    fn clone(&self) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Bool(b) => Self::Bool(*b),
            Self::Num(n) => Self::Num(*n),
            Self::Str(s) => Self::Str(s.clone()),
            Self::Arr(items) => Self::Arr(Rc::clone(items)),
            Self::Obj(map) => Self::Obj(Rc::clone(map)),
        }
    }
}

/// Go `encoding/json.Marshal` on an `any` tree: sorted object keys,
/// HTML-escaped strings, ES6 float formatting, and cycle detection with
/// Go's exact error text. `BTreeMap` already iterates sorted by UTF-8
/// bytes, matching Go's key ordering.
fn go_json_marshal(value: &GJson) -> Result<String, String> {
    let mut out = String::new();
    let mut stack: HashSet<usize> = HashSet::new();
    marshal_value(value, &mut out, &mut stack)?;
    Ok(out)
}

fn marshal_value(
    value: &GJson,
    out: &mut String,
    stack: &mut HashSet<usize>,
) -> Result<(), String> {
    match value {
        GJson::Null => out.push_str("null"),
        GJson::Bool(true) => out.push_str("true"),
        GJson::Bool(false) => out.push_str("false"),
        GJson::Num(number) => out.push_str(&go_json_number(*number)),
        GJson::Str(text) => go_json_string(text, out),
        GJson::Arr(items) => {
            let id = Rc::as_ptr(items) as usize;
            if !stack.insert(id) {
                return Err(
                    "json: unsupported value: encountered a cycle via []interface {}".to_string(),
                );
            }
            out.push('[');
            for (index, item) in items.borrow().iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                marshal_value(item, out, stack)?;
            }
            out.push(']');
            stack.remove(&id);
        }
        GJson::Obj(map) => {
            let id = Rc::as_ptr(map) as usize;
            if !stack.insert(id) {
                return Err(
                    "json: unsupported value: encountered a cycle via map[string]interface {}"
                        .to_string(),
                );
            }
            out.push('{');
            // BTreeMap already iterates sorted; collect to drop the borrow
            // before recursing into children that may share this map.
            let keys: Vec<String> = map.borrow().keys().cloned().collect();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                go_json_string(key, out);
                out.push(':');
                if let Some(child) = map.borrow().get(key) {
                    marshal_value(child, out, stack)?;
                }
            }
            out.push('}');
            stack.remove(&id);
        }
    }
    Ok(())
}

/// Go `encoding/json` float64 formatting (ES6 number-to-string): `f` mode
/// for |x| in [1e-6, 1e21), `e` mode outside with `e+NN`/`e-N` exponents
/// (the `e-0N` zero pad is stripped). Rust's `Display` never emits
/// exponents and `{:e}` matches Go's shortest `e`-form digits.
fn go_json_number(value: f64) -> String {
    if !value.is_finite() {
        // Unreachable through serde_json parse (which rejects non-finite);
        // Go errors at marshal time — keep a stable placeholder.
        return "null".to_string();
    }
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        let raw = format!("{value:e}");
        let Some((mantissa, exponent)) = raw.split_once('e') else {
            return raw;
        };
        let (sign, digits) = match exponent.strip_prefix('-') {
            Some(digits) => ("-", digits),
            None => ("+", exponent),
        };
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        format!("{mantissa}e{sign}{digits}")
    } else {
        format!("{value}")
    }
}

/// Go `encoding/json` string encoding with `escapeHTML` on: `"` and `\`
/// backslash-escaped, `\b\f\n\r\t` shorthand, other C0 controls and
/// `<`/`>`/`&` as `\u00XX`, U+2028/U+2029 as `\u2028`/`\u2029`, everything
/// else verbatim (Rust `&str` is always valid UTF-8, so Go's
/// invalid-byte→`\ufffd` path is unreachable).
fn go_json_string(text: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            ch if (ch as u32) < 0x20 => {
                let byte = ch as u32;
                out.push_str("\\u00");
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0xf) as usize] as char);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}
