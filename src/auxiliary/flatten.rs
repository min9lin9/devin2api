//! `protoextract` flattening: merge every recovered
//! `FileDescriptorProto` into one compilable proto2 file, renaming
//! non-root-package symbols with a package-derived prefix. Port of
//! `G/cmd/protoextract/flatten.go` plus a minimal proto2 printer standing
//! in for `protoprint` (the Go tool's printer dependency is not ported;
//! the rendered source is semantically equivalent, not byte-identical).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use buffa::MessageField;
use buffa_descriptor::generated::descriptor::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FieldOptions, FileDescriptorProto, MessageOptions, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto, SourceCodeInfo,
    field_descriptor_proto::{Label, Type},
    source_code_info::Location as SourceCodeInfo_Location,
};

use crate::auxiliary::extract::DEFAULT_BUNDLE_NAME;

/// `preferredRootPackage` — the package the Devin descriptors live in.
pub const PREFERRED_ROOT_PACKAGE: &str = "exa.api_server_pb";

/// `symbolMapping` — one renamed (or identity-mapped) symbol.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SymbolMapping {
    pub kind: String,
    pub original: String,
    pub flattened: String,
}

/// `flattenMetadata` — the manifest's `flattened` section.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct FlattenMetadata {
    pub package: String,
    pub syntax: String,
    pub symbol_mappings: Vec<SymbolMapping>,
    pub warnings: Vec<String>,
}

/// `usize` → `i32` for descriptor indexes and `SourceCodeInfo` paths. Go's
/// `int32(n)` wraps; a descriptor can never hold 2^31 entries, so a failed
/// conversion is unreachable and panics rather than silently corrupting
/// the emitted path.
fn idx(n: usize) -> i32 {
    i32::try_from(n).expect("descriptor index fits i32")
}

#[derive(Default)]
struct FlattenState {
    root_package: String,
    used_top_level: HashSet<String>,
    type_names: HashMap<String, String>,
    service_names: HashMap<String, String>,
    extension_names: HashMap<String, String>,
    enum_values: HashMap<String, HashMap<String, String>>,
    mappings: Vec<SymbolMapping>,
}

fn qualify(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else if name.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}.{name}")
    }
}

fn short_name(full: &str) -> &str {
    full.rsplit('.').next().unwrap_or(full)
}

/// `packagePrefix` — `exa.chat_pb` → `ExaChatPb`; empty → `NoPackage`.
fn package_prefix(pkg: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for ch in pkg.chars() {
        if !ch.is_alphanumeric() {
            upper = true;
            continue;
        }
        let ch = if upper { ch.to_ascii_uppercase() } else { ch };
        upper = false;
        out.push(ch);
    }
    if out.is_empty() {
        "NoPackage".to_string()
    } else {
        out
    }
}

impl FlattenState {
    fn reserve(&mut self, candidate: &str) -> String {
        let mut name = candidate.to_string();
        let mut suffix = 2;
        while self.used_top_level.contains(&name) {
            name = format!("{candidate}_{suffix}");
            suffix += 1;
        }
        self.used_top_level.insert(name.clone());
        name
    }

    fn add_mapping(&mut self, kind: &str, original: String, flattened: String) {
        self.mappings.push(SymbolMapping {
            kind: kind.to_string(),
            original,
            flattened,
        });
    }

    /// `registerFileSymbols` — root-package declarations reserve their
    /// original names; other packages get `Prefix_` names.
    fn register_file_symbols(&mut self, file: &FileDescriptorProto) {
        let prefix = package_prefix(file.package.as_deref().unwrap_or(""));
        let root = file.package.as_deref().unwrap_or("") == self.root_package;
        let pkg = file.package.as_deref().unwrap_or("");
        for message in &file.message_type {
            let original = qualify(pkg, message.name.as_deref().unwrap_or(""));
            let mut name = message.name.clone().unwrap_or_default();
            if !root {
                name = format!("{prefix}_{name}");
            }
            let name = self.reserve(&name);
            let flattened = qualify(&self.root_package, &name);
            self.type_names.insert(original.clone(), flattened.clone());
            self.add_mapping("message", original.clone(), flattened.clone());
            self.register_nested_message_symbols(&original, &flattened, message);
        }
        for enumeration in &file.enum_type {
            let original = qualify(pkg, enumeration.name.as_deref().unwrap_or(""));
            let mut name = enumeration.name.clone().unwrap_or_default();
            if !root {
                name = format!("{prefix}_{name}");
            }
            let name = self.reserve(&name);
            let flattened = qualify(&self.root_package, &name);
            self.type_names.insert(original.clone(), flattened.clone());
            self.add_mapping("enum", original.clone(), flattened.clone());
            let mut values = HashMap::with_capacity(enumeration.value.len());
            for value in &enumeration.value {
                let value_name = value.name.clone().unwrap_or_default();
                let new_name = if root {
                    self.used_top_level.insert(value_name.clone());
                    value_name.clone()
                } else {
                    self.reserve(&format!("{name}_{value_name}"))
                };
                values.insert(value_name.clone(), new_name.clone());
                if new_name != value_name {
                    self.add_mapping(
                        "enum_value",
                        format!("{original}.{value_name}"),
                        format!("{flattened}.{new_name}"),
                    );
                }
            }
            self.enum_values.insert(original, values);
        }
        for service in &file.service {
            let original = qualify(pkg, service.name.as_deref().unwrap_or(""));
            let mut name = service.name.clone().unwrap_or_default();
            if !root {
                name = format!("{prefix}_{name}");
            }
            let name = self.reserve(&name);
            let flattened = qualify(&self.root_package, &name);
            self.service_names
                .insert(original.clone(), flattened.clone());
            self.add_mapping("service", original, flattened);
        }
        for extension in &file.extension {
            let original = qualify(pkg, extension.name.as_deref().unwrap_or(""));
            let mut name = extension.name.clone().unwrap_or_default();
            if !root {
                name = format!("{prefix}_{name}");
            }
            let name = self.reserve(&name);
            let flattened = qualify(&self.root_package, &name);
            self.extension_names
                .insert(original.clone(), flattened.clone());
            self.add_mapping("extension", original, flattened);
        }
    }

    /// `registerNestedMessageSymbols` — nested types keep their leaf name
    /// under the (possibly renamed) parent; no top-level reservation.
    fn register_nested_message_symbols(
        &mut self,
        original_parent: &str,
        flattened_parent: &str,
        message: &DescriptorProto,
    ) {
        for nested in &message.nested_type {
            let original = qualify(original_parent, nested.name.as_deref().unwrap_or(""));
            let flattened = qualify(flattened_parent, nested.name.as_deref().unwrap_or(""));
            self.type_names.insert(original.clone(), flattened.clone());
            self.register_nested_message_symbols(&original, &flattened, nested);
        }
        for enumeration in &message.enum_type {
            let original = qualify(original_parent, enumeration.name.as_deref().unwrap_or(""));
            let flattened = qualify(flattened_parent, enumeration.name.as_deref().unwrap_or(""));
            self.type_names.insert(original.clone(), flattened);
            let values = enumeration
                .value
                .iter()
                .map(|v| {
                    (
                        v.name.clone().unwrap_or_default(),
                        v.name.clone().unwrap_or_default(),
                    )
                })
                .collect();
            self.enum_values.insert(original, values);
        }
    }

    /// `rewriteTypeName` — map a `.qualified` reference through the
    /// flattened symbol table; unmapped names pass through unchanged.
    fn rewrite_type_name(&self, name: &str) -> String {
        if name.is_empty() {
            return String::new();
        }
        let original = name.trim_start_matches('.');
        if let Some(mapped) = self.type_names.get(original) {
            return format!(".{mapped}");
        }
        name.to_string()
    }

    /// `rewriteField` — retarget type names, drop `json_name` on extensions,
    /// rewrite enum defaults, keep only the effective `packed` option.
    fn rewrite_field(&self, file: &FileDescriptorProto, field: &mut FieldDescriptorProto) {
        let is_extension = field.extendee.as_deref().is_some_and(|e| !e.is_empty());
        let original_type = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.')
            .to_string();
        let rewritten = self.rewrite_type_name(field.type_name.as_deref().unwrap_or(""));
        field.type_name = if rewritten.is_empty() {
            None
        } else {
            Some(rewritten)
        };
        let rewritten = self.rewrite_type_name(field.extendee.as_deref().unwrap_or(""));
        field.extendee = if rewritten.is_empty() {
            None
        } else {
            Some(rewritten)
        };
        if is_extension {
            field.json_name = None;
        }
        if field
            .default_value
            .as_deref()
            .is_some_and(|v| !v.is_empty())
            && field.r#type == Some(Type::TYPE_ENUM)
            && let Some(mapped) = self
                .enum_values
                .get(&original_type)
                .and_then(|values| values.get(field.default_value.as_deref().unwrap_or("")))
            && !mapped.is_empty()
        {
            field.default_value = Some(mapped.clone());
        }
        if effective_packed(file, field) {
            field.options = MessageField::some(FieldOptions {
                packed: Some(true),
                ..Default::default()
            });
        } else {
            field.options = MessageField::default();
        }
        field.proto3_optional = None;
    }

    /// `rewriteMessage` — clean options, remove synthetic oneofs, recurse.
    fn rewrite_message(
        &self,
        file: &FileDescriptorProto,
        original: &str,
        message: &mut DescriptorProto,
    ) -> Result<(), String> {
        message.options = clean_message_options(message.options.take());
        remove_synthetic_oneofs(message).map_err(|e| format!("{original}: {e}"))?;
        for field in &mut message.field {
            self.rewrite_field(file, field);
        }
        for extension in &mut message.extension {
            self.rewrite_field(file, extension);
        }
        for nested in &mut message.nested_type {
            let nested_original = qualify(original, nested.name.as_deref().unwrap_or(""));
            self.rewrite_message(file, &nested_original, nested)?;
        }
        for enumeration in &mut message.enum_type {
            let enum_original = qualify(original, enumeration.name.as_deref().unwrap_or(""));
            self.rewrite_enum(&enum_original, enumeration);
        }
        for oneof in &mut message.oneof_decl {
            oneof.options = MessageField::default();
        }
        Ok(())
    }

    /// `rewriteEnum` — apply renamed values, drop all but `allow_alias`.
    fn rewrite_enum(&self, original: &str, enumeration: &mut EnumDescriptorProto) {
        enumeration.options = clean_enum_options(enumeration.options.take());
        for value in &mut enumeration.value {
            if let Some(mapped) = self
                .enum_values
                .get(original)
                .and_then(|values| values.get(value.name.as_deref().unwrap_or("")))
                && !mapped.is_empty()
            {
                value.name = Some(mapped.clone());
            }
            value.options = MessageField::default();
        }
    }

    /// `rewriteService` — retarget method types, drop all options.
    fn rewrite_service(&self, service: &mut ServiceDescriptorProto) {
        service.options = MessageField::default();
        for method in &mut service.method {
            let input = self.rewrite_type_name(method.input_type.as_deref().unwrap_or(""));
            method.input_type = Some(input);
            let output = self.rewrite_type_name(method.output_type.as_deref().unwrap_or(""));
            method.output_type = Some(output);
            method.options = MessageField::default();
        }
    }
}

/// `effectivePacked` — repeated packable fields are packed when the
/// explicit option says so, else when the source file is proto3.
fn effective_packed(file: &FileDescriptorProto, field: &FieldDescriptorProto) -> bool {
    if field.label != Some(Label::LABEL_REPEATED) || !field.r#type.is_some_and(is_packable) {
        return false;
    }
    if let Some(options) = field.options.as_option()
        && let Some(packed) = options.packed
    {
        return packed;
    }
    file.syntax.as_deref() == Some("proto3")
}

fn is_packable(kind: Type) -> bool {
    !matches!(
        kind,
        Type::TYPE_STRING | Type::TYPE_BYTES | Type::TYPE_MESSAGE | Type::TYPE_GROUP
    )
}

/// `removeSyntheticOneofs` — proto3 `optional` fields lose their synthetic
/// oneof wrapper; real oneof indexes are remapped.
fn remove_synthetic_oneofs(message: &mut DescriptorProto) -> Result<(), String> {
    let mut removed = HashSet::new();
    for field in &message.field {
        if field.proto3_optional == Some(true) {
            match field.oneof_index {
                Some(index) => {
                    removed.insert(index);
                }
                None => {
                    return Err(format!(
                        "proto3_optional field {} has no oneof index",
                        field.name.as_deref().unwrap_or("")
                    ));
                }
            }
        }
    }
    if removed.is_empty() {
        return Ok(());
    }
    let mut index_map = HashMap::with_capacity(message.oneof_decl.len());
    let mut kept: Vec<OneofDescriptorProto> = Vec::with_capacity(message.oneof_decl.len());
    for (old_index, oneof) in message.oneof_decl.iter().enumerate() {
        if removed.contains(&idx(old_index)) {
            continue;
        }
        index_map.insert(idx(old_index), idx(kept.len()));
        kept.push(oneof.clone());
    }
    message.oneof_decl = kept;
    for field in &mut message.field {
        if field.proto3_optional == Some(true) {
            field.oneof_index = None;
            field.proto3_optional = None;
            continue;
        }
        if let Some(index) = field.oneof_index {
            match index_map.get(&index) {
                Some(mapped) => field.oneof_index = Some(*mapped),
                None => {
                    return Err(format!(
                        "field {} references removed synthetic oneof",
                        field.name.as_deref().unwrap_or("")
                    ));
                }
            }
        }
    }
    Ok(())
}

/// `cleanMessageOptions` — keep only `message_set_wire_format`/`map_entry`.
fn clean_message_options(
    options: Option<MessageOptions>,
) -> MessageField<MessageOptions, buffa::Inline<MessageOptions>> {
    let Some(options) = options else {
        return MessageField::default();
    };
    let clean = MessageOptions {
        message_set_wire_format: options.message_set_wire_format,
        map_entry: options.map_entry,
        ..Default::default()
    };
    if clean.message_set_wire_format.is_none() && clean.map_entry.is_none() {
        return MessageField::default();
    }
    MessageField::some(clean)
}

/// `cleanEnumOptions` — keep only `allow_alias`.
fn clean_enum_options(
    options: Option<buffa_descriptor::generated::descriptor::EnumOptions>,
) -> MessageField<
    buffa_descriptor::generated::descriptor::EnumOptions,
    buffa::Inline<buffa_descriptor::generated::descriptor::EnumOptions>,
> {
    match options.and_then(|o| o.allow_alias) {
        Some(allow) => MessageField::some(buffa_descriptor::generated::descriptor::EnumOptions {
            allow_alias: Some(allow),
            ..Default::default()
        }),
        None => MessageField::default(),
    }
}

/// `chooseRootPackage` — the preferred package when present, else the
/// lexicographically smallest non-empty package, else a fallback.
fn choose_root_package(files: &[FileDescriptorProto], preferred: &str) -> String {
    for file in files {
        if file.package.as_deref() == Some(preferred) {
            return preferred.to_string();
        }
    }
    let mut packages: Vec<&str> = Vec::new();
    let mut seen = HashSet::new();
    for file in files {
        let Some(pkg) = file.package.as_deref() else {
            continue;
        };
        if pkg.is_empty() || !seen.insert(pkg) {
            continue;
        }
        packages.push(pkg);
    }
    packages.sort_unstable();
    packages.first().map_or_else(
        || "protoextract_flattened".to_string(),
        |p| (*p).to_string(),
    )
}

/// `flattenDescriptors` — merge `files` into one proto2
/// `FileDescriptorProto` under the root package, with the symbol mapping
/// metadata the manifest reports.
///
/// # Errors
///
/// Propagates `removeSyntheticOneofs` structural errors.
// Kept as one function to mirror `flattenDescriptors` in
// G/cmd/protoextract/flatten.go for side-by-side parity review.
#[allow(clippy::too_many_lines)]
pub fn flatten_descriptors(
    files: &[FileDescriptorProto],
    preferred_package: &str,
) -> Result<(FileDescriptorProto, FlattenMetadata), String> {
    let root_package = choose_root_package(files, preferred_package);
    let mut state = FlattenState {
        root_package: root_package.clone(),
        ..Default::default()
    };
    let mut ordered: Vec<&FileDescriptorProto> = files.iter().collect();
    ordered.sort_by(|a, b| a.name.cmp(&b.name));
    for root_pass in [true, false] {
        for file in &ordered {
            if (file.package.as_deref().unwrap_or("") == root_package) != root_pass {
                continue;
            }
            state.register_file_symbols(file);
        }
    }

    let mut flat = FileDescriptorProto {
        name: Some(DEFAULT_BUNDLE_NAME.to_string()),
        package: Some(root_package.clone()),
        syntax: Some("proto2".to_string()),
        ..Default::default()
    };
    let mut header_comments: Vec<String> = Vec::new();
    for file in &ordered {
        let message_offset = idx(flat.message_type.len());
        let enum_offset = idx(flat.enum_type.len());
        let service_offset = idx(flat.service.len());
        let extension_offset = idx(flat.extension.len());

        for message in &file.message_type {
            let original = qualify(
                file.package.as_deref().unwrap_or(""),
                message.name.as_deref().unwrap_or(""),
            );
            let mut cloned = message.clone();
            state.rewrite_message(file, &original, &mut cloned)?;
            cloned.name = Some(
                short_name(state.type_names.get(&original).map_or("", String::as_str)).to_string(),
            );
            flat.message_type.push(cloned);
        }
        for enumeration in &file.enum_type {
            let original = qualify(
                file.package.as_deref().unwrap_or(""),
                enumeration.name.as_deref().unwrap_or(""),
            );
            let mut cloned = enumeration.clone();
            state.rewrite_enum(&original, &mut cloned);
            cloned.name = Some(
                short_name(state.type_names.get(&original).map_or("", String::as_str)).to_string(),
            );
            flat.enum_type.push(cloned);
        }
        for service in &file.service {
            let original = qualify(
                file.package.as_deref().unwrap_or(""),
                service.name.as_deref().unwrap_or(""),
            );
            let mut cloned = service.clone();
            state.rewrite_service(&mut cloned);
            cloned.name = Some(
                short_name(
                    state
                        .service_names
                        .get(&original)
                        .map_or("", String::as_str),
                )
                .to_string(),
            );
            flat.service.push(cloned);
        }
        for extension in &file.extension {
            let original = qualify(
                file.package.as_deref().unwrap_or(""),
                extension.name.as_deref().unwrap_or(""),
            );
            let mut cloned = extension.clone();
            state.rewrite_field(file, &mut cloned);
            cloned.name = Some(
                short_name(
                    state
                        .extension_names
                        .get(&original)
                        .map_or("", String::as_str),
                )
                .to_string(),
            );
            flat.extension.push(cloned);
        }

        let (mapped, unmapped) = remap_source_info(
            file,
            message_offset,
            enum_offset,
            service_offset,
            extension_offset,
        );
        let (mapped, removed) =
            filter_source_info(&flat, file.name.as_deref().unwrap_or(""), mapped);
        merge_source_info(&mut flat, mapped);
        header_comments.extend(unmapped);
        header_comments.extend(removed);
    }
    if !header_comments.is_empty() {
        flat.source_code_info
            .get_or_insert_default()
            .location
            .push(SourceCodeInfo_Location {
                path: vec![],
                span: vec![0, 0, 0],
                leading_comments: Some(header_comments.join("\n")),
                ..Default::default()
            });
    }

    let metadata = FlattenMetadata {
        package: root_package,
        syntax: "proto2".to_string(),
        symbol_mappings: state.mappings,
        warnings: vec![
            "descriptors.pb is the lossless authority for original file names, packages, options, and syntax".to_string(),
            "non-root package symbols are renamed and non-root service RPC paths therefore change".to_string(),
            "proto3 declarations use proto2 optional syntax in the flattened view; wire encoding is preserved but generated presence and open-enum APIs may differ".to_string(),
            "custom declaration options are removed from the flattened source so all definitions can compile without imports".to_string(),
        ],
    };
    Ok((flat, metadata))
}

/// `remapSourceInfo` — shift top-level declaration indexes in each
/// location path by the per-kind offsets; comments on unmappable paths
/// (file-level, options) are collected for the bundle header.
fn remap_source_info(
    file: &FileDescriptorProto,
    message_offset: i32,
    enum_offset: i32,
    service_offset: i32,
    extension_offset: i32,
) -> (Option<SourceCodeInfo>, Vec<String>) {
    let Some(info) = file.source_code_info.as_option() else {
        return (None, Vec::new());
    };
    let mut mapped = SourceCodeInfo::default();
    let mut unmapped = Vec::new();
    for location in &info.location {
        let mut cloned = location.clone();
        if cloned.path.len() >= 2 {
            match cloned.path[0] {
                4 => cloned.path[1] += message_offset,
                5 => cloned.path[1] += enum_offset,
                6 => cloned.path[1] += service_offset,
                7 => cloned.path[1] += extension_offset,
                _ => {
                    if let Some(text) =
                        location_comment_text(file.name.as_deref().unwrap_or(""), &cloned)
                    {
                        unmapped.push(text);
                    }
                    continue;
                }
            }
            mapped.location.push(cloned);
            continue;
        }
        if let Some(text) = location_comment_text(file.name.as_deref().unwrap_or(""), &cloned) {
            unmapped.push(text);
        }
    }
    (Some(mapped), unmapped)
}

/// `locationCommentText` — `Comments from <file>:` block or `None`.
fn location_comment_text(file_name: &str, location: &SourceCodeInfo_Location) -> Option<String> {
    let mut parts: Vec<String> = location.leading_detached_comments.clone();
    if let Some(leading) = &location.leading_comments
        && !leading.is_empty()
    {
        parts.push(leading.clone());
    }
    if let Some(trailing) = &location.trailing_comments
        && !trailing.is_empty()
    {
        parts.push(trailing.clone());
    }
    if parts.is_empty() {
        return None;
    }
    Some(format!("Comments from {file_name}:\n{}", parts.join("\n")))
}

/// `mergeSourceInfo` — append the mapped locations into the flat file.
fn merge_source_info(flat: &mut FileDescriptorProto, addition: Option<SourceCodeInfo>) {
    let Some(addition) = addition else { return };
    if addition.location.is_empty() {
        return;
    }
    flat.source_code_info
        .get_or_insert_default()
        .location
        .extend(addition.location);
}

// ---- source-path existence check (Go `sourcePathExists`) ---------------------

/// A read-only view over a descriptor node for path walking.
#[derive(Clone, Copy)]
enum Node<'a> {
    File(&'a FileDescriptorProto),
    Msg(&'a DescriptorProto),
    Field(&'a FieldDescriptorProto),
    Enum(&'a EnumDescriptorProto),
    EnumValue(&'a EnumValueDescriptorProto),
    Service(&'a ServiceDescriptorProto),
    Method(&'a MethodDescriptorProto),
    Oneof(&'a OneofDescriptorProto),
    /// A scalar/leaf or absent child — Go treats scalar fields as
    /// terminal-present; options messages are opaque leaves here.
    Leaf,
    Missing,
}

impl<'a> Node<'a> {
    /// `message.Descriptor().Fields().ByNumber(n)` + `message.Get(field)`
    /// for the descriptor types. Returns `(node, is_list, is_message)`.
    // One arm table per descriptor node kind; splitting it would scatter
    // the field-number map the Go protoreflect walk keeps in one switch.
    #[allow(clippy::too_many_lines)]
    fn child(&self, number: i32, index: usize) -> (Node<'a>, bool, bool) {
        macro_rules! list_item {
            ($list:expr, $node:expr) => {
                match $list.get(index) {
                    Some(item) => ($node(item), true, true),
                    None => (Node::Missing, true, true),
                }
            };
        }
        match self {
            Node::File(f) => match number {
                1 | 2 | 12 | 14 => (Node::Leaf, false, false),
                3 | 15 => list_item!(f.dependency, |_| Node::Leaf),
                4 => list_item!(f.message_type, Node::Msg),
                5 => list_item!(f.enum_type, Node::Enum),
                6 => list_item!(f.service, Node::Service),
                7 => list_item!(f.extension, Node::Field),
                8 => (
                    f.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                9 => (
                    f.source_code_info
                        .as_option()
                        .map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                10 => list_item!(f.public_dependency, |_| Node::Leaf),
                11 => list_item!(f.weak_dependency, |_| Node::Leaf),
                _ => (Node::Missing, false, false),
            },
            Node::Msg(m) => match number {
                1 => (Node::Leaf, false, false),
                2 => list_item!(m.field, Node::Field),
                3 => list_item!(m.nested_type, Node::Msg),
                4 => list_item!(m.enum_type, Node::Enum),
                5 => list_item!(m.extension_range, |_| Node::Leaf),
                6 => list_item!(m.extension, Node::Field),
                7 => (
                    m.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                8 => list_item!(m.oneof_decl, Node::Oneof),
                9 => list_item!(m.reserved_range, |_| Node::Leaf),
                10 => list_item!(m.reserved_name, |_| Node::Leaf),
                _ => (Node::Missing, false, false),
            },
            Node::Field(f) => match number {
                // 2 (extendee) is a scalar reference like the other leaves.
                1..=7 | 9..=11 | 17 => (Node::Leaf, false, false),
                8 => (
                    f.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::Enum(e) => match number {
                1 | 4 | 5 => (Node::Leaf, false, false),
                2 => list_item!(e.value, Node::EnumValue),
                3 => (
                    e.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::EnumValue(v) => match number {
                1 | 2 => (Node::Leaf, false, false),
                3 => (
                    v.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::Service(s) => match number {
                1 => (Node::Leaf, false, false),
                2 => list_item!(s.method, Node::Method),
                3 => (
                    s.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::Method(m) => match number {
                1 | 2 | 3 | 5 | 6 => (Node::Leaf, false, false),
                4 => (
                    m.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::Oneof(o) => match number {
                1 => (Node::Leaf, false, false),
                2 => (
                    o.options.as_option().map_or(Node::Missing, |_| Node::Leaf),
                    false,
                    true,
                ),
                _ => (Node::Missing, false, false),
            },
            Node::Leaf | Node::Missing => (Node::Missing, false, false),
        }
    }
}

/// `sourcePathExists` — walk `path` through the flat descriptor; mirrors
/// the Go protoreflect traversal semantics (scalar fields are
/// terminal-present, message fields must be populated, list indexes must
/// be in range).
fn source_path_exists(file: &FileDescriptorProto, path: &[i32]) -> bool {
    if path.is_empty() {
        return true;
    }
    let mut node = Node::File(file);
    let mut index = 0usize;
    while index < path.len() {
        let field_number = path[index];
        index += 1;
        // Peek at the field kind: we need list-ness before consuming the
        // element index, so resolve the child lazily via a two-step walk.
        let (child, is_list, is_message) = node.child(field_number, 0);
        if matches!(child, Node::Missing) && !is_list {
            return false;
        }
        if is_list {
            if index >= path.len() {
                return true;
            }
            // Negative path elements index nothing; usize::MAX misses
            // every list the same way Go's wrapped index did.
            let item_index = usize::try_from(path[index]).unwrap_or(usize::MAX);
            index += 1;
            let (item, _, _) = node.child(field_number, item_index);
            if matches!(item, Node::Missing) {
                return false;
            }
            if index == path.len() {
                return true;
            }
            if !is_message {
                return false;
            }
            node = item;
            continue;
        }
        if !is_message {
            return index == path.len();
        }
        if matches!(child, Node::Missing) {
            return false;
        }
        if index == path.len() {
            return true;
        }
        node = child;
    }
    true
}

/// `filterSourceInfo` — drop locations whose remapped path no longer
/// resolves in the flat file; their comments move to the header block.
fn filter_source_info(
    file: &FileDescriptorProto,
    original_name: &str,
    source: Option<SourceCodeInfo>,
) -> (Option<SourceCodeInfo>, Vec<String>) {
    let Some(source) = source else {
        return (None, Vec::new());
    };
    let mut valid = SourceCodeInfo::default();
    let mut removed = Vec::new();
    for location in &source.location {
        if source_path_exists(file, &location.path) {
            valid.location.push(location.clone());
            continue;
        }
        if let Some(text) = location_comment_text(original_name, location) {
            removed.push(text);
        }
    }
    (Some(valid), removed)
}

// ---- proto2 rendering (protoprint stand-in) -----------------------------------

/// Escape a string for a proto2 quoted literal.
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => write!(out, "\\x{:02x}", u32::from(c)).unwrap(),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// proto's derived JSON name for a field (lowerCamelCase).
fn derived_json_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for ch in name.chars() {
        if ch == '_' {
            upper = true;
            continue;
        }
        if upper {
            out.push(ch.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}

fn scalar_type_name(kind: Type) -> &'static str {
    match kind {
        Type::TYPE_DOUBLE => "double",
        Type::TYPE_FLOAT => "float",
        Type::TYPE_INT64 => "int64",
        Type::TYPE_UINT64 => "uint64",
        Type::TYPE_INT32 => "int32",
        Type::TYPE_FIXED64 => "fixed64",
        Type::TYPE_FIXED32 => "fixed32",
        Type::TYPE_BOOL => "bool",
        Type::TYPE_STRING => "string",
        Type::TYPE_BYTES => "bytes",
        Type::TYPE_UINT32 => "uint32",
        Type::TYPE_SFIXED32 => "sfixed32",
        Type::TYPE_SFIXED64 => "sfixed64",
        Type::TYPE_SINT32 => "sint32",
        Type::TYPE_SINT64 => "sint64",
        Type::TYPE_ENUM | Type::TYPE_MESSAGE | Type::TYPE_GROUP => "",
    }
}

/// Collect the comments attached to a source-info path.
struct CommentIndex<'a> {
    map: BTreeMap<Vec<i32>, &'a SourceCodeInfo_Location>,
}

impl<'a> CommentIndex<'a> {
    fn new(file: &'a FileDescriptorProto) -> Self {
        let mut map = BTreeMap::new();
        if let Some(info) = file.source_code_info.as_option() {
            for location in &info.location {
                if !location.path.is_empty() {
                    map.insert(location.path.clone(), location);
                }
            }
        }
        Self { map }
    }

    fn get(&self, path: &[i32]) -> Option<&'a SourceCodeInfo_Location> {
        self.map.get(path).copied()
    }
}

/// Emit detached + leading comments before an element, trailing after.
fn emit_comments(out: &mut String, location: Option<&SourceCodeInfo_Location>, indent: &str) {
    let Some(location) = location else { return };
    for detached in &location.leading_detached_comments {
        for line in detached.lines() {
            out.push_str(indent);
            out.push_str("//");
            out.push_str(line);
            out.push('\n');
        }
        out.push('\n');
    }
    if let Some(leading) = &location.leading_comments {
        for line in leading.trim_end_matches('\n').lines() {
            out.push_str(indent);
            out.push_str("//");
            out.push_str(line);
            out.push('\n');
        }
    }
}

fn emit_trailing(out: &mut String, location: Option<&SourceCodeInfo_Location>) {
    if let Some(trailing) = location.and_then(|l| l.trailing_comments.as_deref()) {
        let trimmed = trailing.trim_end_matches('\n');
        if !trimmed.is_empty() {
            out.push_str(" //");
            out.push_str(trimmed);
        }
    }
}

fn relative_type_name(name: &str) -> String {
    let parts: Vec<&str> = name.trim_start_matches('.').split('.').collect();
    let first_symbol = parts
        .iter()
        .position(|part| part.chars().next().is_some_and(char::is_uppercase))
        .unwrap_or(0);
    parts[first_symbol..].join(".")
}

fn field_type_text(field: &FieldDescriptorProto) -> String {
    if let Some(name) = &field.type_name {
        return relative_type_name(name);
    }
    scalar_type_name(field.r#type.unwrap_or(Type::TYPE_DOUBLE)).to_string()
}

/// The map-entry nested type a `map<K,V>` field references, if any.
fn map_entry_for<'a>(
    message: &'a DescriptorProto,
    field: &FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    if field.label != Some(Label::LABEL_REPEATED) {
        return None;
    }
    let type_name = field.type_name.as_deref()?.trim_start_matches('.');
    let leaf = type_name.rsplit('.').next()?;
    message.nested_type.iter().find(|nested| {
        nested.name.as_deref() == Some(leaf)
            && nested
                .options
                .as_option()
                .and_then(|o| o.map_entry)
                .unwrap_or(false)
    })
}

/// Render one field declaration line (no leading indent, no newline).
fn render_field_line(
    out: &mut String,
    field: &FieldDescriptorProto,
    in_oneof: bool,
    is_extension: bool,
    map_entry: Option<&DescriptorProto>,
) {
    let label = match field.label.unwrap_or(Label::LABEL_OPTIONAL) {
        Label::LABEL_REQUIRED => "required ",
        Label::LABEL_REPEATED if map_entry.is_none() => "repeated ",
        _ if in_oneof || map_entry.is_some() => "",
        _ => "optional ",
    };
    out.push_str(label);
    if let Some(entry) = map_entry {
        let key = entry
            .field
            .first()
            .map_or_else(|| "string".to_string(), field_type_text);
        let value = entry
            .field
            .get(1)
            .map_or_else(|| "bytes".to_string(), field_type_text);
        write!(out, "map<{key}, {value}>").unwrap();
    } else {
        out.push_str(&field_type_text(field));
    }
    out.push(' ');
    out.push_str(field.name.as_deref().unwrap_or("field"));
    out.push_str(" = ");
    out.push_str(&field.number.unwrap_or_default().to_string());
    let mut options: Vec<String> = Vec::new();
    if let Some(default) = &field.default_value {
        let rendered = match field.r#type {
            Some(Type::TYPE_STRING | Type::TYPE_BYTES) => quote(default),
            _ => default.clone(),
        };
        options.push(format!("default = {rendered}"));
    }
    if let Some(opts) = field.options.as_option()
        && opts.packed == Some(true)
    {
        options.push("packed = true".to_string());
    }
    if !is_extension && let Some(json_name) = &field.json_name {
        let proto_name = field.name.as_deref().unwrap_or("");
        if *json_name != derived_json_name(proto_name) {
            options.push(format!("json_name = {}", quote(json_name)));
        }
    }
    if options.is_empty() {
        out.push(';');
    } else {
        write!(out, " [{}];", options.join(", ")).unwrap();
    }
}

/// Render a message (recursively), enum, service or extension block.
// One recursive renderer mirroring Go's flatten walk for parity review.
#[allow(clippy::too_many_lines)]
fn render_message(
    out: &mut String,
    message: &DescriptorProto,
    indent: &str,
    comments: &CommentIndex<'_>,
    path: &[i32],
) {
    out.push_str(indent);
    out.push_str("message ");
    out.push_str(message.name.as_deref().unwrap_or("Unnamed"));
    out.push_str(" {");
    emit_trailing(out, comments.get(path));
    out.push('\n');
    let inner = format!("{indent}  ");
    if message
        .options
        .as_option()
        .and_then(|o| o.message_set_wire_format)
        .unwrap_or(false)
    {
        writeln!(out, "{inner}option message_set_wire_format = true;").unwrap();
    }
    // Fields not belonging to a oneof first, then oneof blocks, matching
    // declaration order within each group.
    let mut oneof_fields: BTreeMap<i32, Vec<(usize, &FieldDescriptorProto)>> = BTreeMap::new();
    for (i, field) in message.field.iter().enumerate() {
        let field_path = [path.to_vec(), vec![2, idx(i)]].concat();
        if let Some(oneof) = field.oneof_index {
            oneof_fields.entry(oneof).or_default().push((i, field));
            continue;
        }
        emit_comments(out, comments.get(&field_path), &inner);
        out.push_str(&inner);
        render_field_line(out, field, false, false, map_entry_for(message, field));
        emit_trailing(out, comments.get(&field_path));
        out.push('\n');
    }
    for (oneof_index, fields) in &oneof_fields {
        let Some(decl) = usize::try_from(*oneof_index)
            .ok()
            .and_then(|i| message.oneof_decl.get(i))
        else {
            continue;
        };
        let oneof_path = [path.to_vec(), vec![8, *oneof_index]].concat();
        emit_comments(out, comments.get(&oneof_path), &inner);
        writeln!(
            out,
            "{inner}oneof {} {{",
            decl.name.as_deref().unwrap_or("oneof")
        )
        .unwrap();
        for (field_index, field) in fields {
            let field_path = [path.to_vec(), vec![2, idx(*field_index)]].concat();
            emit_comments(out, comments.get(&field_path), &format!("{inner}  "));
            write!(out, "{inner}  ").unwrap();
            render_field_line(out, field, true, false, map_entry_for(message, field));
            emit_trailing(out, comments.get(&field_path));
            out.push('\n');
        }
        writeln!(out, "{inner}}}").unwrap();
    }
    for (i, extension) in message.extension.iter().enumerate() {
        let ext_path = [path.to_vec(), vec![6, idx(i)]].concat();
        emit_comments(out, comments.get(&ext_path), &inner);
        writeln!(
            out,
            "{inner}extend {} {{",
            extension.extendee.as_deref().unwrap_or("")
        )
        .unwrap();
        write!(out, "{inner}  ").unwrap();
        render_field_line(out, extension, false, true, None);
        emit_trailing(out, comments.get(&ext_path));
        writeln!(out, "\n{inner}}}").unwrap();
    }
    for (i, nested) in message.nested_type.iter().enumerate() {
        if nested
            .options
            .as_option()
            .and_then(|o| o.map_entry)
            .unwrap_or(false)
        {
            continue; // map entries render as map<K,V> fields
        }
        let nested_path = [path.to_vec(), vec![3, idx(i)]].concat();
        emit_comments(out, comments.get(&nested_path), &inner);
        render_message(out, nested, &inner, comments, &nested_path);
    }
    for (i, enumeration) in message.enum_type.iter().enumerate() {
        let enum_path = [path.to_vec(), vec![4, idx(i)]].concat();
        emit_comments(out, comments.get(&enum_path), &inner);
        render_enum(out, enumeration, &inner, comments, &enum_path);
    }
    for range in &message.reserved_range {
        writeln!(
            out,
            "{inner}reserved {} to {};",
            range.start.unwrap_or_default(),
            range.end.unwrap_or_default() - 1
        )
        .unwrap();
    }
    for name in &message.reserved_name {
        writeln!(out, "{inner}reserved {};", quote(name)).unwrap();
    }
    for range in &message.extension_range {
        writeln!(
            out,
            "{inner}extensions {} to {};",
            range.start.unwrap_or_default(),
            range.end.unwrap_or_default() - 1
        )
        .unwrap();
    }
    out.push_str(indent);
    out.push_str("}\n");
}

fn render_enum(
    out: &mut String,
    enumeration: &EnumDescriptorProto,
    indent: &str,
    comments: &CommentIndex<'_>,
    path: &[i32],
) {
    out.push_str(indent);
    out.push_str("enum ");
    out.push_str(enumeration.name.as_deref().unwrap_or("Unnamed"));
    out.push_str(" {");
    emit_trailing(out, comments.get(path));
    out.push('\n');
    let inner = format!("{indent}  ");
    if enumeration
        .options
        .as_option()
        .and_then(|o| o.allow_alias)
        .unwrap_or(false)
    {
        writeln!(out, "{inner}option allow_alias = true;").unwrap();
    }
    for (i, value) in enumeration.value.iter().enumerate() {
        let value_path = [path.to_vec(), vec![2, idx(i)]].concat();
        emit_comments(out, comments.get(&value_path), &inner);
        write!(
            out,
            "{inner}{} = {};",
            value.name.as_deref().unwrap_or("UNKNOWN"),
            value.number.unwrap_or_default()
        )
        .unwrap();
        emit_trailing(out, comments.get(&value_path));
        out.push('\n');
    }
    for range in &enumeration.reserved_range {
        writeln!(
            out,
            "{inner}reserved {} to {};",
            range.start.unwrap_or_default(),
            range.end.unwrap_or_default() - 1
        )
        .unwrap();
    }
    for name in &enumeration.reserved_name {
        writeln!(out, "{inner}reserved {};", quote(name)).unwrap();
    }
    out.push_str(indent);
    out.push_str("}\n");
}

fn render_service(
    out: &mut String,
    service: &ServiceDescriptorProto,
    indent: &str,
    comments: &CommentIndex<'_>,
    path: &[i32],
) {
    out.push_str(indent);
    out.push_str("service ");
    out.push_str(service.name.as_deref().unwrap_or("Unnamed"));
    out.push_str(" {");
    emit_trailing(out, comments.get(path));
    out.push('\n');
    let inner = format!("{indent}  ");
    for (i, method) in service.method.iter().enumerate() {
        let method_path = [path.to_vec(), vec![2, idx(i)]].concat();
        emit_comments(out, comments.get(&method_path), &inner);
        write!(
            out,
            "{inner}rpc {}({}{}) returns ({}{})",
            method.name.as_deref().unwrap_or("Call"),
            if method.client_streaming.unwrap_or(false) {
                "stream "
            } else {
                ""
            },
            relative_type_name(method.input_type.as_deref().unwrap_or("")),
            if method.server_streaming.unwrap_or(false) {
                "stream "
            } else {
                ""
            },
            relative_type_name(method.output_type.as_deref().unwrap_or("")),
        )
        .unwrap();
        out.push(';');
        emit_trailing(out, comments.get(&method_path));
        out.push('\n');
    }
    out.push_str(indent);
    out.push_str("}\n");
}

/// `renderFlattened` — print the flattened descriptor as one proto2 file
/// with the extraction header comments.
#[must_use]
pub fn render_flattened(file: &FileDescriptorProto, original_file_count: usize) -> String {
    let mut out = String::new();
    out.push_str("// Generated by protoextract from embedded FileDescriptorProto values.\n");
    writeln!(out, "// Flattened original file count: {original_file_count}. See manifest.json for renamed symbols."
    ).unwrap();
    out.push_str(
        "// descriptors.pb preserves the original packages, syntax, options, and file boundaries.\n",
    );
    out.push_str("syntax = \"proto2\";\n\n");
    writeln!(out, "package {};\n", file.package.as_deref().unwrap_or("")).unwrap();
    let comments = CommentIndex::new(file);
    // Header comments collected from unmappable locations (path == []).
    if let Some(info) = file.source_code_info.as_option() {
        for location in &info.location {
            if location.path.is_empty()
                && let Some(leading) = &location.leading_comments
            {
                for line in leading.trim_end_matches('\n').lines() {
                    out.push_str("//");
                    out.push_str(line);
                    out.push('\n');
                }
                out.push('\n');
            }
        }
    }
    for (i, message) in file.message_type.iter().enumerate() {
        let path = vec![4, idx(i)];
        emit_comments(&mut out, comments.get(&path), "");
        render_message(&mut out, message, "", &comments, &path);
        out.push('\n');
    }
    for (i, enumeration) in file.enum_type.iter().enumerate() {
        let path = vec![5, idx(i)];
        emit_comments(&mut out, comments.get(&path), "");
        render_enum(&mut out, enumeration, "", &comments, &path);
        out.push('\n');
    }
    for (i, service) in file.service.iter().enumerate() {
        let path = vec![6, idx(i)];
        emit_comments(&mut out, comments.get(&path), "");
        render_service(&mut out, service, "", &comments, &path);
        out.push('\n');
    }
    for (i, extension) in file.extension.iter().enumerate() {
        let path = vec![7, idx(i)];
        emit_comments(&mut out, comments.get(&path), "");
        write!(
            out,
            "extend {} {{\n  ",
            extension.extendee.as_deref().unwrap_or("")
        )
        .unwrap();
        render_field_line(&mut out, extension, false, true, None);
        emit_trailing(&mut out, comments.get(&path));
        out.push_str("\n}\n\n");
    }
    out
}
