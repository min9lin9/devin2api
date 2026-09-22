//! `protocensus` core: protocol census over debuglog traffic and
//! descriptor-set drift diff. Port of `G/cmd/protocensus/main.go`.
//!
//! The census resolves wire names against the flattened descriptor set
//! (`proto/all-protos.fds`, the same flattened symbol space the generated
//! code registers in Go's global registry) — the file is embedded so the
//! binary needs no runtime schema path.

use std::collections::BTreeMap;
use std::path::Path;

use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_descriptor::{DescriptorPool, FieldKind, SingularKind};
use serde::Serialize;

use crate::auxiliary::extract::{decode_set, field_label_name, field_type_name};

/// `requestTypeName` / `responseTypeName` — the wire types the proxy calls.
pub const REQUEST_TYPE_NAME: &str = "exa.api_server_pb.GetChatMessageRequest";
pub const RESPONSE_TYPE_NAME: &str = "exa.api_server_pb.GetChatMessageResponse";

/// The embedded flattened descriptor set (generated-code symbol space).
const FLATTENED_FDS: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/proto/all-protos.fds"));

/// Build the descriptor pool over the embedded flattened set — the Rust
/// equivalent of Go's `protoregistry.GlobalFiles` populated by the
/// generated-code import.
///
/// # Errors
///
/// `PoolError` when the embedded set fails to link (a build-time defect).
pub fn schema_pool() -> Result<DescriptorPool, buffa_descriptor::PoolError> {
    DescriptorPool::decode_with_options(
        FLATTENED_FDS,
        &buffa::DecodeOptions::new().with_element_memory_limit(usize::MAX),
    )
}

// ---- census -------------------------------------------------------------------

/// `msgCensus` — per-message-type occurrence and field-hit counts.
#[derive(Debug, Default, Serialize)]
pub struct MsgCensus {
    pub occurrences: usize,
    pub fields: BTreeMap<String, usize>,
}

/// `unknownKey` — a JSON key the descriptor cannot resolve.
#[derive(Debug, Serialize)]
pub struct UnknownKey {
    pub message: String,
    pub key: String,
    pub count: usize,
    pub examples: Vec<String>,
}

/// `enumAnomaly` — an enum value outside the descriptor's member table.
#[derive(Debug, Serialize)]
pub struct EnumAnomaly {
    pub message: String,
    pub field: String,
    pub value: String,
    pub count: usize,
    pub examples: Vec<String>,
}

/// `census` — one traffic direction's accumulated statistics.
pub struct Census<'a> {
    pool: &'a DescriptorPool,
    pub messages: BTreeMap<String, MsgCensus>,
    unknown: BTreeMap<String, UnknownKey>,
    enum_anomalies: BTreeMap<String, EnumAnomaly>,
    current_dir: String,
}

impl<'a> Census<'a> {
    /// A census bound to the shared schema pool.
    #[must_use]
    pub fn new(pool: &'a DescriptorPool) -> Self {
        Self {
            pool,
            messages: BTreeMap::new(),
            unknown: BTreeMap::new(),
            enum_anomalies: BTreeMap::new(),
            current_dir: String::new(),
        }
    }

    /// The request-dir name currently being scanned (example provenance).
    pub fn set_current_dir(&mut self, dir: &str) {
        self.current_dir = dir.to_string();
    }

    /// `walk` — resolve each JSON key against the message descriptor:
    /// resolved fields count and recurse into submessages, unresolved keys
    /// are recorded, enum values are checked against the member table.
    pub fn walk(
        &mut self,
        md: &buffa_descriptor::MessageDescriptor,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) {
        let type_name = md.full_name().to_string();
        self.messages
            .entry(type_name.clone())
            .or_default()
            .occurrences += 1;
        for (key, val) in obj {
            let Some(fd) = md.field_by_name(key) else {
                self.record_unknown(&type_name, key);
                continue;
            };
            *self
                .messages
                .get_mut(&type_name)
                .expect("message census was initialized")
                .fields
                .entry(fd.name().to_string())
                .or_insert(0) += 1;
            self.walk_value(fd, val);
        }
    }

    fn record_unknown(&mut self, type_name: &str, key: &str) {
        let id = format!("{type_name}|{key}");
        let entry = self.unknown.entry(id).or_insert_with(|| UnknownKey {
            message: type_name.to_string(),
            key: key.to_string(),
            count: 0,
            examples: Vec::new(),
        });
        entry.count += 1;
        append_example(&mut entry.examples, &self.current_dir);
    }

    /// `walkValue` — repeated fields walk each element, maps walk each
    /// value, everything else walks as a single value.
    fn walk_value(&mut self, fd: &buffa_descriptor::FieldDescriptor, val: &serde_json::Value) {
        match fd.kind() {
            FieldKind::Map { value, .. } => {
                if let serde_json::Value::Object(map) = val {
                    for v in map.values() {
                        self.walk_singular(fd, value, v);
                    }
                }
            }
            FieldKind::List(kind) => {
                if let serde_json::Value::Array(items) = val {
                    for item in items {
                        self.walk_singular(fd, kind, item);
                    }
                }
            }
            FieldKind::Singular(kind) => self.walk_singular(fd, kind, val),
        }
    }

    /// `walkSingle` — recurse into submessages, check enum membership.
    fn walk_singular(
        &mut self,
        fd: &buffa_descriptor::FieldDescriptor,
        kind: SingularKind,
        val: &serde_json::Value,
    ) {
        match kind {
            SingularKind::Message(index) => {
                if let serde_json::Value::Object(obj) = val {
                    let md = self.pool.message(index).clone();
                    self.walk(&md, obj);
                }
            }
            SingularKind::Enum(index) => {
                let enumeration = self.pool.enumeration(index).clone();
                self.check_enum(fd, &enumeration, val);
            }
            SingularKind::Scalar(_) => {}
        }
    }

    /// `checkEnum` — protojson prints unknown members as numbers and known
    /// members as names; anything else is drift.
    fn check_enum(
        &mut self,
        fd: &buffa_descriptor::FieldDescriptor,
        enumeration: &buffa_descriptor::EnumDescriptor,
        val: &serde_json::Value,
    ) {
        let bad = match val {
            serde_json::Value::String(name) => {
                if enumeration.value_by_name(name).is_none() {
                    Some(name.clone())
                } else {
                    None
                }
            }
            serde_json::Value::Number(number) => {
                let as_i32 = number
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or_default();
                if enumeration.value(as_i32).is_none() {
                    Some(format!("number:{number}"))
                } else {
                    None
                }
            }
            _ => None,
        };
        let Some(bad) = bad else { return };
        let id = format!("{}|{}|{bad}", enumeration.full_name(), fd.name());
        let entry = self
            .enum_anomalies
            .entry(id)
            .or_insert_with(|| EnumAnomaly {
                message: enumeration.full_name().to_string(),
                field: fd.name().to_string(),
                value: bad.clone(),
                count: 0,
                examples: Vec::new(),
            });
        entry.count += 1;
        append_example(&mut entry.examples, &self.current_dir);
    }

    /// `neverSeen` — fields of observed message types with zero hits.
    #[must_use]
    pub fn never_seen(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (type_name, ms) in &self.messages {
            if ms.occurrences == 0 {
                continue;
            }
            let Some(md) = self.pool.message_by_name(type_name) else {
                continue;
            };
            for field in md.fields() {
                if !ms.fields.contains_key(field.name()) {
                    out.push(format!("{type_name}.{}", field.name()));
                }
            }
        }
        out.sort();
        out
    }
}

/// `appendExample` — up to 3 distinct request-dir names.
fn append_example(examples: &mut Vec<String>, dir: &str) {
    if dir.is_empty() || examples.len() >= 3 || examples.iter().any(|e| e == dir) {
        return;
    }
    examples.push(dir.to_string());
}

/// `requestDirs` — request directory names sorted newest-first (names are
/// time-ordered), truncated to `max_dirs` when non-zero.
///
/// # Errors
///
/// The `read_dir` error for a missing/unreadable logs dir.
pub fn request_dirs(logs_dir: &Path, max_dirs: usize) -> std::io::Result<Vec<String>> {
    let mut dirs: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(logs_dir)? {
        let entry = entry?;
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            dirs.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    dirs.sort_by(|a, b| b.cmp(a));
    if max_dirs > 0 && dirs.len() > max_dirs {
        dirs.truncate(max_dirs);
    }
    Ok(dirs)
}

/// `censusSection` — the per-direction report object.
#[must_use]
pub fn census_section(census: &Census<'_>) -> serde_json::Value {
    let mut unknown: Vec<&UnknownKey> = census.unknown.values().collect();
    unknown.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.key.cmp(&b.key)));
    let mut anomalies: Vec<&EnumAnomaly> = census.enum_anomalies.values().collect();
    anomalies.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
    serde_json::json!({
        "messages": census.messages,
        "fields_never_seen": census.never_seen(),
        "unknown_keys": unknown,
        "enum_anomalies": anomalies,
    })
}

// ---- diff ---------------------------------------------------------------------

/// `symbolTable` — a `FileDescriptorSet` flattened into comparable symbols.
#[derive(Default)]
pub struct SymbolTable {
    /// `"Msg.field"` → `"number=3 type=TYPE_STRING type_name= label=REPEATED"`.
    pub fields: BTreeMap<String, String>,
    /// `"Enum.VALUE"` → number.
    pub enums: BTreeMap<String, i32>,
    /// `"Svc.Method"` → `"Req -> Resp cs=false ss=true"`.
    pub methods: BTreeMap<String, String>,
    /// Fully-qualified message/enum/service names.
    pub types: std::collections::BTreeSet<String>,
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

fn index_message(
    table: &mut SymbolTable,
    prefix: &str,
    message: &buffa_descriptor::generated::descriptor::DescriptorProto,
) {
    let fqn = join(prefix, message.name.as_deref().unwrap_or(""));
    table.types.insert(fqn.clone());
    for field in &message.field {
        table.fields.insert(
            format!("{fqn}.{}", field.name.as_deref().unwrap_or("")),
            format!(
                "number={} type={} type_name={} label={}",
                field.number.unwrap_or_default(),
                field_type_name(field),
                field.type_name.as_deref().unwrap_or(""),
                field_label_name(field),
            ),
        );
    }
    for nested in &message.nested_type {
        index_message(table, &fqn, nested);
    }
    for enumeration in &message.enum_type {
        index_enum(table, &fqn, enumeration);
    }
}

fn index_enum(
    table: &mut SymbolTable,
    fqn: &str,
    enumeration: &buffa_descriptor::generated::descriptor::EnumDescriptorProto,
) {
    table.types.insert(fqn.to_string());
    for value in &enumeration.value {
        table.enums.insert(
            format!("{fqn}.{}", value.name.as_deref().unwrap_or("")),
            value.number.unwrap_or_default(),
        );
    }
}

/// `loadSymbols` — parse a descriptor set file into a symbol table.
///
/// # Errors
///
/// Read or unmarshal failure (`unmarshal <path>: <err>`).
pub fn load_symbols(path: &Path) -> anyhow::Result<SymbolTable> {
    let set = decode_set(path)?;
    Ok(symbols_of(&set))
}

/// Flatten a decoded `FileDescriptorSet` into a symbol table.
#[must_use]
pub fn symbols_of(set: &FileDescriptorSet) -> SymbolTable {
    let mut table = SymbolTable::default();
    for file in &set.file {
        let pkg = file.package.as_deref().unwrap_or("");
        for message in &file.message_type {
            index_message(&mut table, pkg, message);
        }
        for enumeration in &file.enum_type {
            index_enum(
                &mut table,
                &join(pkg, enumeration.name.as_deref().unwrap_or("")),
                enumeration,
            );
        }
        for service in &file.service {
            let svc = join(pkg, service.name.as_deref().unwrap_or(""));
            table.types.insert(svc.clone());
            for method in &service.method {
                table.methods.insert(
                    format!("{svc}.{}", method.name.as_deref().unwrap_or("")),
                    format!(
                        "{} -> {} cs={} ss={}",
                        method
                            .input_type
                            .as_deref()
                            .unwrap_or("")
                            .trim_start_matches('.'),
                        method
                            .output_type
                            .as_deref()
                            .unwrap_or("")
                            .trim_start_matches('.'),
                        method.client_streaming.unwrap_or(false),
                        method.server_streaming.unwrap_or(false),
                    ),
                );
            }
        }
    }
    table
}

/// `diffAdded` — symbols in `new` but not `old` (type/field/enum/rpc).
#[must_use]
pub fn diff_added(old: &SymbolTable, new: &SymbolTable) -> Vec<String> {
    let mut out = Vec::new();
    for name in new.types.difference(&old.types) {
        out.push(format!("type {name}"));
    }
    for name in new.fields.keys() {
        if !old.fields.contains_key(name) {
            out.push(format!("field {name}"));
        }
    }
    for name in new.enums.keys() {
        if !old.enums.contains_key(name) {
            out.push(format!("enum {name}"));
        }
    }
    for name in new.methods.keys() {
        if !old.methods.contains_key(name) {
            out.push(format!("rpc {name}"));
        }
    }
    out.sort();
    out
}

/// `diffChanged` — symbols present in both with different signatures.
#[must_use]
pub fn diff_changed(old: &SymbolTable, new: &SymbolTable) -> Vec<String> {
    let mut out = Vec::new();
    for (name, new_sig) in &new.fields {
        if let Some(old_sig) = old.fields.get(name)
            && old_sig != new_sig
        {
            out.push(format!("field {name}: {old_sig} -> {new_sig}"));
        }
    }
    for (name, new_num) in &new.enums {
        if let Some(old_num) = old.enums.get(name)
            && old_num != new_num
        {
            out.push(format!("enum {name}: {old_num} -> {new_num}"));
        }
    }
    for (name, new_sig) in &new.methods {
        if let Some(old_sig) = old.methods.get(name)
            && old_sig != new_sig
        {
            out.push(format!("rpc {name}: {old_sig} -> {new_sig}"));
        }
    }
    out.sort();
    out
}

/// Scan one request dir's upstream request stages + response frames into
/// the censuses. Returns the number of response frames parsed.
pub fn scan_request_dir(
    logs_dir: &Path,
    dir: &str,
    req: &mut Census<'_>,
    resp: &mut Census<'_>,
    req_md: &buffa_descriptor::MessageDescriptor,
    resp_md: &buffa_descriptor::MessageDescriptor,
) -> usize {
    let dir_path = logs_dir.join(dir);
    req.set_current_dir(dir);
    resp.set_current_dir(dir);
    // The first request and attemptN retry shards both count — retries
    // send a different wire shape (model swap, appended continue).
    if let Ok(stages) = crate::debuglog::stages::devin_request_stages(&dir_path) {
        for stage in stages {
            if let Ok(raw) = std::fs::read(dir_path.join(&stage))
                && let Ok(serde_json::Value::Object(obj)) = serde_json::from_slice(&raw)
            {
                req.walk(req_md, &obj);
            }
        }
    }
    let mut frames = 0;
    let response_path = dir_path.join(crate::debuglog::stages::STAGE_DEVIN_RESPONSE);
    if let Ok(raw) = std::fs::read(&response_path) {
        // Go uses a 4MiB scanner buffer; a longer line aborts the scan
        // with a warning rather than silently truncating the census.
        const MAX_LINE: usize = 4 << 20;
        for line in raw.split(|b| *b == b'\n') {
            if line.len() > MAX_LINE {
                eprintln!(
                    "warn: scan {dir}/{}: bufio.Scanner: token too long",
                    crate::debuglog::stages::STAGE_DEVIN_RESPONSE
                );
                break;
            }
            if let Ok(serde_json::Value::Object(obj)) =
                serde_json::from_slice::<serde_json::Value>(line)
            {
                frames += 1;
                resp.walk(resp_md, &obj);
            }
        }
    }
    frames
}

/// `printReport` — the census JSON document.
#[must_use]
pub fn census_report(
    dirs: usize,
    frames: usize,
    req: &Census<'_>,
    resp: &Census<'_>,
) -> serde_json::Value {
    serde_json::json!({
        "dirs_scanned": dirs,
        "response_frames": frames,
        "request": census_section(req),
        "response": census_section(resp),
    })
}
