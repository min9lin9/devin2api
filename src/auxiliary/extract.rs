//! `protoextract` core: scans an opaque binary for embedded
//! `FileDescriptorProto` values, deduplicates them by quality, writes the
//! raw `FileDescriptorSet` (`descriptors.pb`) and the extraction manifest.
//! Port of `G/cmd/protoextract/main.go`.
//!
//! The scan is deliberately byte-oriented: the source is an opaque
//! executable and is never executed. Candidate detection, boundary walking,
//! validity checks and quality ranking mirror the Go implementation field
//! for field so the same binaries yield the same descriptor set.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use buffa::{DecodeOptions, Message};
use buffa_descriptor::generated::descriptor::{
    FileDescriptorProto, FileDescriptorSet, field_descriptor_proto,
};
use serde::Serialize;

/// `all-protos.proto` — the flattened bundle file name.
pub const DEFAULT_BUNDLE_NAME: &str = "all-protos.proto";

/// Scan counters reported in the manifest (`scanStats`).
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct ScanStats {
    /// Candidate descriptors found (valid or not).
    pub candidates: usize,
    /// Candidates dropped because a same-named file was already kept.
    pub duplicates: usize,
}

// ---- protowire equivalents -------------------------------------------------

/// `protowire.ConsumeVarint` — returns `(value, bytes consumed)`.
fn consume_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, byte) in data.iter().copied().take(10).enumerate() {
        if i == 9 && byte > 1 {
            return None; // overflow
        }
        value |= u64::from(byte & 0x7f) << (i * 7);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// `protowire.ConsumeBytes` — returns `(contents, bytes consumed)`.
fn consume_bytes(data: &[u8]) -> Option<(&[u8], usize)> {
    let (len, n) = consume_varint(data)?;
    let len = usize::try_from(len).ok()?;
    let end = n.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    Some((&data[n..end], end))
}

/// `protowire.ConsumeTag` — returns `(field number, wire type, bytes)`.
fn consume_tag(data: &[u8]) -> Option<(u32, u8, usize)> {
    let (tag, n) = consume_varint(data)?;
    let number = u32::try_from(tag >> 3).ok()?;
    if number == 0 {
        return None;
    }
    Some((number, (tag & 7) as u8, n))
}

/// `protowire.ConsumeFieldValue` for the wire types a `FileDescriptorProto`
/// can carry (varint/fixed32/fixed64/bytes). Groups are never valid here —
/// `valid_file_descriptor_field` rejects them before this is called.
fn consume_field_value(wire_type: u8, data: &[u8]) -> Option<usize> {
    match wire_type {
        0 => consume_varint(data).map(|(_, n)| n),
        1 => (data.len() >= 8).then_some(8),
        2 => consume_bytes(data).map(|(_, n)| n),
        5 => (data.len() >= 4).then_some(4),
        _ => None,
    }
}

// ---- scanning ---------------------------------------------------------------

/// `isProtoPath` — printable-ASCII relative path with no empty/dot segments.
fn is_proto_path(value: &[u8]) -> bool {
    if value.is_empty() || value[0] == b'/' || value.contains(&b'\\') {
        return false;
    }
    if !value.iter().all(|b| (0x20..=0x7e).contains(b)) {
        return false;
    }
    let Ok(text) = std::str::from_utf8(value) else {
        return false;
    };
    text.split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// `validFileDescriptorField` — field numbers a `FileDescriptorProto` uses,
/// with their legal wire types (10/11 accept packed and unpacked).
fn valid_file_descriptor_field(number: u32, wire_type: u8) -> bool {
    match number {
        1..=9 | 12 | 15 => wire_type == 2,
        10 | 11 => wire_type == 0 || wire_type == 2,
        14 => wire_type == 0,
        _ => false,
    }
}

/// `validDescriptor` — the decoded candidate must carry the expected name,
/// no unknown fields, a known syntax, well-formed dependency paths and
/// in-range public/weak dependency indexes.
fn valid_descriptor(descriptor: &FileDescriptorProto, expected_name: &str) -> bool {
    if descriptor.name.as_deref() != Some(expected_name) || !is_proto_path(expected_name.as_bytes())
    {
        return false;
    }
    if !descriptor.__buffa_unknown_fields.is_empty() {
        return false;
    }
    let syntax = descriptor.syntax.as_deref().unwrap_or("");
    if !syntax.is_empty() && syntax != "proto2" && syntax != "proto3" && syntax != "editions" {
        return false;
    }
    for dependency in descriptor
        .dependency
        .iter()
        .chain(descriptor.option_dependency.iter())
    {
        let is_proto = Path::new(dependency)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("proto"));
        if !is_proto || !is_proto_path(dependency.as_bytes()) {
            return false;
        }
    }
    let dependency_count =
        i32::try_from(descriptor.dependency.len()).expect("dependency count fits i32");
    for index in descriptor
        .public_dependency
        .iter()
        .chain(descriptor.weak_dependency.iter())
    {
        if *index < 0 || *index >= dependency_count {
            return false;
        }
    }
    true
}

/// `descriptorQuality` — prefer complete declarations, then source
/// information/comments, then size.
fn descriptor_quality(descriptor: &FileDescriptorProto) -> usize {
    let declarations = descriptor.message_type.len()
        + descriptor.enum_type.len()
        + descriptor.service.len()
        + descriptor.extension.len();
    declarations * 1_000_000
        + count_comment_locations(descriptor) * 100_000
        + descriptor
            .source_code_info
            .as_option()
            .map_or(0, |info| info.location.len())
            * 1_000
        + descriptor.encoded_len() as usize
}

/// `extractDescriptorAt` — walk field boundaries from `start`, then try
/// decoding at each boundary from the last backwards so incidental
/// descriptor-looking bytes after the real end cannot poison extraction.
fn extract_descriptor_at(
    binary: &[u8],
    start: usize,
    expected_name: &str,
) -> Option<FileDescriptorProto> {
    let mut data = &binary[start..];
    let mut consumed = 0usize;
    let mut boundaries: Vec<usize> = Vec::with_capacity(32);
    while !data.is_empty() {
        let Some((number, wire_type, tag_bytes)) = consume_tag(data) else {
            break;
        };
        if !valid_file_descriptor_field(number, wire_type) {
            break;
        }
        data = &data[tag_bytes..];
        consumed += tag_bytes;
        let Some(value_bytes) = consume_field_value(wire_type, data) else {
            break;
        };
        data = &data[value_bytes..];
        consumed += value_bytes;
        boundaries.push(consumed);
    }
    for end in boundaries.iter().rev() {
        let Ok(descriptor) = DecodeOptions::new()
            .with_element_memory_limit(usize::MAX)
            .decode_from_slice::<FileDescriptorProto>(&binary[start..start + end])
        else {
            continue;
        };
        if valid_descriptor(&descriptor, expected_name) {
            return Some(descriptor);
        }
    }
    None
}

/// `scanFileDescriptors` — find every embedded `FileDescriptorProto` in
/// `binary`, deduplicating by name keeping the highest-quality candidate.
/// Returns files sorted by name (Go `sortedDescriptors`).
#[must_use]
pub fn scan_file_descriptors(binary: &[u8]) -> (Vec<FileDescriptorProto>, ScanStats) {
    let mut files: BTreeMap<String, FileDescriptorProto> = BTreeMap::new();
    let mut stats = ScanStats::default();
    for offset in 0..binary.len().saturating_sub(8) {
        if binary[offset] != 0x0a {
            continue;
        }
        let Some((name, name_bytes)) = consume_bytes(&binary[offset + 1..]) else {
            continue;
        };
        if name.len() < 7 || !name.ends_with(b".proto") || !is_proto_path(name) || name_bytes == 0 {
            continue;
        }
        let Ok(expected_name) = std::str::from_utf8(name) else {
            continue;
        };
        let Some(descriptor) = extract_descriptor_at(binary, offset, expected_name) else {
            continue;
        };
        stats.candidates += 1;
        if let Some(current) = files.get(expected_name) {
            stats.duplicates += 1;
            if descriptor_quality(&descriptor) <= descriptor_quality(current) {
                continue;
            }
        }
        files.insert(expected_name.to_string(), descriptor);
    }
    (files.into_values().collect(), stats)
}

/// Encode the descriptor set exactly like Go `proto.Marshal` of
/// `FileDescriptorSet{File: all}`.
#[must_use]
pub fn encode_set(files: Vec<FileDescriptorProto>) -> Vec<u8> {
    FileDescriptorSet {
        file: files,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Decode a `FileDescriptorSet` from `path` (protocensus `loadSymbols`).
///
/// # Errors
///
/// Returns the read error or a `unmarshal <path>: <decode error>` message.
pub fn decode_set(path: &Path) -> anyhow::Result<FileDescriptorSet> {
    let raw = std::fs::read(path)?;
    DecodeOptions::new()
        .with_element_memory_limit(usize::MAX)
        .decode_from_slice(&raw)
        .map_err(|e| anyhow::anyhow!("unmarshal {}: {e}", path.display()))
}

// ---- output path safety ------------------------------------------------------

/// `filepath.Abs` + `filepath.Clean`: absolute, lexically normalized, no
/// symlink resolution.
fn abs_clean(path: &Path) -> std::io::Result<PathBuf> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    Ok(out)
}

/// `pathContains(directory, candidate)` — Go `filepath.Rel` based check.
fn path_contains(directory: &Path, candidate: &Path) -> bool {
    let rel = candidate.strip_prefix(directory);
    match rel {
        Ok(rest) => rest.as_os_str().is_empty() || !rest.starts_with(".."),
        Err(_) => false,
    }
}

/// `validateDestructiveOutputPath` — refuse root, home, cwd-containing and
/// source-containing output directories before `remove_all`.
fn validate_destructive_output_path(source: &Path, output: &Path) -> anyhow::Result<()> {
    if output == Path::new("/") {
        anyhow::bail!("refusing to delete filesystem root as output directory");
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from)
        && output == abs_clean(&home).unwrap_or(home)
    {
        anyhow::bail!("refusing to delete the user home directory as output directory");
    }
    if let Ok(cwd) = std::env::current_dir() {
        let cwd = abs_clean(&cwd).unwrap_or(cwd);
        if output == cwd || path_contains(output, &cwd) {
            anyhow::bail!(
                "refusing to delete an output directory that contains the current working directory: {}",
                output.display()
            );
        }
    }
    if output == source || path_contains(output, source) {
        anyhow::bail!(
            "refusing to delete an output directory that contains the source binary: {}",
            output.display()
        );
    }
    Ok(())
}

/// `prepareFreshOutput` — resolve source/output paths, enforce the
/// destructive-path guards, then reset the output directory.
///
/// # Errors
///
/// Source must exist and be a regular file; output must pass the guards
/// and be (re)creatable.
pub fn prepare_fresh_output(
    source_path: &Path,
    output_path: &Path,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let source = abs_clean(source_path)?;
    let info = std::fs::metadata(&source).map_err(|e| anyhow::anyhow!("source binary: {e}"))?;
    if !info.is_file() {
        anyhow::bail!("source binary must be a regular file: {}", source.display());
    }
    let output = abs_clean(output_path)?;
    validate_destructive_output_path(&source, &output)?;
    if output.exists() {
        if output.is_dir() {
            std::fs::remove_dir_all(&output).map_err(|e| {
                anyhow::anyhow!("remove output directory {}: {e}", output.display())
            })?;
        } else {
            std::fs::remove_file(&output).map_err(|e| {
                anyhow::anyhow!("remove output directory {}: {e}", output.display())
            })?;
        }
    }
    std::fs::create_dir_all(&output)
        .map_err(|e| anyhow::anyhow!("create output directory {}: {e}", output.display()))?;
    Ok((source, output))
}

// ---- resolution check ---------------------------------------------------------

/// `resolveDescriptors` — whole-set resolvability check. Returns `Ok` when
/// the set links cleanly; on failure, each file is tried in isolation and
/// only a structurally broken file is fatal — dangling references degrade
/// to a warning at the call site (Go `protodesc.FileOptions{
/// AllowUnresolvable: true}` parity: `UnresolvedTypeName` is tolerated,
/// everything else is fatal).
///
/// # Errors
///
/// `<file>: <error>` when a single file cannot be resolved even leniently.
pub fn resolve_descriptors(set: &FileDescriptorSet) -> Result<(), String> {
    match buffa_descriptor::DescriptorPool::new(set.clone()) {
        Ok(_) => Ok(()),
        Err(registry_err) => {
            for file in &set.file {
                let single = FileDescriptorSet {
                    file: vec![file.clone()],
                    ..Default::default()
                };
                match buffa_descriptor::DescriptorPool::new(single) {
                    Ok(_) | Err(buffa_descriptor::PoolError::UnresolvedTypeName { .. }) => {}
                    Err(err) => {
                        return Err(format!("{}: {err}", file.name.as_deref().unwrap_or("")));
                    }
                }
            }
            Err(registry_err.to_string())
        }
    }
}

// ---- manifest -----------------------------------------------------------------

/// `fileManifest` — per-file manifest entry.
#[derive(Debug, Serialize)]
pub struct FileManifest {
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub package: String,
    pub syntax: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub option_dependencies: Vec<String>,
    pub messages: usize,
    pub enums: usize,
    pub services: usize,
    pub extensions: usize,
    pub source_info_locations: usize,
    pub locations_with_comments: usize,
}

/// `extractionManifest` — the manifest.json shape.
#[derive(Debug, Serialize)]
pub struct ExtractionManifest {
    pub binary: String,
    pub bundle: String,
    pub descriptor_count: usize,
    pub candidate_count: usize,
    pub duplicate_count: usize,
    pub files_with_source_info: usize,
    pub files_with_comments: usize,
    pub comment_location_count: usize,
    pub missing_dependencies: Vec<String>,
    pub bundle_is_compilable_as_one_proto: bool,
    pub bundle_compilation_guidance: String,
    pub flattened: crate::auxiliary::flatten::FlattenMetadata,
    pub files: Vec<FileManifest>,
}

/// `descriptorSyntax` — explicit syntax, `editions` when an edition is set,
/// else `proto2`.
fn descriptor_syntax(file: &FileDescriptorProto) -> String {
    if let Some(syntax) = &file.syntax
        && !syntax.is_empty()
    {
        return syntax.clone();
    }
    if file.edition.is_some() {
        return "editions".to_string();
    }
    "proto2".to_string()
}

/// `countCommentLocations` — locations carrying any comment text.
#[must_use]
pub fn count_comment_locations(file: &FileDescriptorProto) -> usize {
    file.source_code_info.as_option().map_or(0, |info| {
        info.location
            .iter()
            .filter(|loc| {
                loc.leading_comments
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
                    || loc
                        .trailing_comments
                        .as_deref()
                        .is_some_and(|s| !s.is_empty())
                    || !loc.leading_detached_comments.is_empty()
            })
            .count()
    })
}

/// `missingDependencies` — referenced imports not embedded in the set.
#[must_use]
pub fn missing_dependencies(files: &[FileDescriptorProto]) -> Vec<String> {
    let present: BTreeSet<&str> = files.iter().filter_map(|f| f.name.as_deref()).collect();
    let mut missing = BTreeSet::new();
    for file in files {
        for dependency in file.dependency.iter().chain(file.option_dependency.iter()) {
            if !present.contains(dependency.as_str()) {
                missing.insert(dependency.clone());
            }
        }
    }
    missing.into_iter().collect()
}

/// `buildManifest` — assemble manifest.json from the extracted files.
#[must_use]
pub fn build_manifest(
    binary_path: &Path,
    bundle_name: &str,
    files: &[FileDescriptorProto],
    stats: ScanStats,
    flattening: crate::auxiliary::flatten::FlattenMetadata,
) -> ExtractionManifest {
    let mut manifest = ExtractionManifest {
        binary: binary_path.display().to_string(),
        bundle: bundle_name.to_string(),
        descriptor_count: files.len(),
        candidate_count: stats.candidates,
        duplicate_count: stats.duplicates,
        files_with_source_info: 0,
        files_with_comments: 0,
        comment_location_count: 0,
        missing_dependencies: missing_dependencies(files),
        bundle_is_compilable_as_one_proto: true,
        bundle_compilation_guidance: "Compile all-protos.proto directly. Use descriptors.pb when original package names, service paths, syntax, or declaration options are required.".to_string(),
        flattened: flattening,
        files: Vec::with_capacity(files.len()),
    };
    for file in files {
        let locations = file
            .source_code_info
            .as_option()
            .map_or(0, |info| info.location.len());
        let comment_locations = count_comment_locations(file);
        if locations > 0 {
            manifest.files_with_source_info += 1;
        }
        if comment_locations > 0 {
            manifest.files_with_comments += 1;
            manifest.comment_location_count += comment_locations;
        }
        manifest.files.push(FileManifest {
            name: file.name.clone().unwrap_or_default(),
            package: file.package.clone().unwrap_or_default(),
            syntax: descriptor_syntax(file),
            dependencies: file.dependency.clone(),
            option_dependencies: file.option_dependency.clone(),
            messages: file.message_type.len(),
            enums: file.enum_type.len(),
            services: file.service.len(),
            extensions: file.extension.len(),
            source_info_locations: locations,
            locations_with_comments: comment_locations,
        });
    }
    manifest
}

/// Field type/label formatting for the census diff symbol table — Go
/// `FieldDescriptorProto.GetType()`/`GetLabel()` `String()` values, with
/// the proto defaults (`TYPE_DOUBLE` / `LABEL_OPTIONAL`) when unset.
#[must_use]
pub fn field_type_name(
    field: &buffa_descriptor::generated::descriptor::FieldDescriptorProto,
) -> String {
    use buffa::Enumeration;
    field.r#type.map_or_else(
        || {
            field_descriptor_proto::Type::TYPE_DOUBLE
                .proto_name()
                .to_string()
        },
        |t| t.proto_name().to_string(),
    )
}

/// `GetLabel()` string — `LABEL_OPTIONAL` when unset.
#[must_use]
pub fn field_label_name(
    field: &buffa_descriptor::generated::descriptor::FieldDescriptorProto,
) -> String {
    use buffa::Enumeration;
    field.label.map_or_else(
        || {
            field_descriptor_proto::Label::LABEL_OPTIONAL
                .proto_name()
                .to_string()
        },
        |l| l.proto_name().to_string(),
    )
}
