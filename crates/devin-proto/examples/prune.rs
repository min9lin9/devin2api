//! Prune all-protos.fds to the reachable subset for codegen.
//!
//! devin2api consumes `ApiServerService` plus the explicit type list in
//! `proto/prune-roots.txt`; the other ~2,400 types (~85% of generated code)
//! are dead weight that pushes rustc to ~16GB RSS and OOMs 16GB CI runners.
//!
//! Usage: `cargo run -p devin-proto --example prune -- <in.fds> <out.fds>`
//! Roots file format: `service <Name>` or bare `<Name>` per line, `#` comments.
//! Extensions and option bytes are preserved verbatim; codegen tolerates
//! references outside the pruned set (the flattened FDS already carries them).

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::path::PathBuf;

use buffa::Message;
use buffa_descriptor::generated::descriptor::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet,
};

struct Roots {
    services: HashSet<String>,
    types: HashSet<String>,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(input), Some(output)) = (args.next(), args.next()) else {
        eprintln!("usage: prune <in.fds> <out.fds>");
        std::process::exit(2);
    };
    let (input, output) = (PathBuf::from(input), PathBuf::from(output));

    let bytes = std::fs::read(&input).expect("read input fds");
    let mut set = FileDescriptorSet::default();
    set.merge_from_slice(&bytes).expect("decode fds");

    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let roots = parse_roots(&workspace.join("proto/prune-roots.txt"));

    let mut pruned = FileDescriptorSet::default();
    for file in set.file {
        pruned.file.push(prune_file(file, &roots));
    }

    let mut buf = Vec::new();
    pruned.encode(&mut buf);
    std::fs::write(&output, &buf).expect("write output fds");

    let kept_msgs: usize = pruned.file.iter().map(|f| f.message_type.len()).sum();
    let kept_enums: usize = pruned.file.iter().map(|f| f.enum_type.len()).sum();
    let kept_svcs: usize = pruned.file.iter().map(|f| f.service.len()).sum();
    eprintln!(
        "pruned: {}B -> {}B, {} messages, {} enums, {} services",
        bytes.len(),
        buf.len(),
        kept_msgs,
        kept_enums,
        kept_svcs
    );
}

fn parse_roots(path: &PathBuf) -> Roots {
    let mut roots = Roots {
        services: HashSet::new(),
        types: HashSet::new(),
    };
    for raw in std::fs::read_to_string(path)
        .expect("read prune-roots.txt")
        .lines()
    {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("service ") {
            roots.services.insert(name.trim().to_string());
        } else {
            roots.types.insert(line.to_string());
        }
    }
    roots
}

fn index_message(decls: &mut HashSet<String>, prefix: &str, msg: &DescriptorProto) {
    let fqn = format!("{prefix}.{}", msg.name.clone().unwrap_or_default());
    decls.insert(fqn.clone());
    for nested in &msg.nested_type {
        index_message(decls, &fqn, nested);
    }
    for e in &msg.enum_type {
        decls.insert(format!("{fqn}.{}", e.name.clone().unwrap_or_default()));
    }
}

fn index_file(file: &FileDescriptorProto, prefix: &str) -> HashSet<String> {
    let mut decls = HashSet::new();
    for m in &file.message_type {
        index_message(&mut decls, prefix, m);
    }
    for e in &file.enum_type {
        decls.insert(format!("{prefix}.{}", e.name.clone().unwrap_or_default()));
    }
    decls
}

fn seed_roots(
    file: &FileDescriptorProto,
    prefix: &str,
    decls: &HashSet<String>,
    roots: &Roots,
    keep: &mut BTreeSet<String>,
    queue: &mut VecDeque<String>,
) {
    for svc in &file.service {
        let sname = svc.name.clone().unwrap_or_default();
        if roots.services.contains(&sname) {
            keep.insert(format!("{prefix}.{sname}"));
            for m in &svc.method {
                for tn in [&m.input_type, &m.output_type] {
                    if let Some(t) = tn.clone()
                        && keep.insert(t.clone())
                    {
                        queue.push_back(t);
                    }
                }
            }
        }
    }
    for name in &roots.types {
        let fqn = format!("{prefix}.{name}");
        if decls.contains(&fqn) {
            if keep.insert(fqn.clone()) {
                queue.push_back(fqn);
            }
        } else {
            eprintln!("warning: root type not in fds: {fqn}");
        }
    }
    // Extension declarations extend option types; codegen resolves both the
    // extendee and the extension's own type, so both must survive pruning.
    for f in file
        .extension
        .iter()
        .chain(file.message_type.iter().flat_map(|m| m.extension.iter()))
    {
        for tn in [&f.extendee, &f.type_name] {
            if let Some(t) = tn.clone()
                && decls.contains(&t)
                && keep.insert(t.clone())
            {
                queue.push_back(t);
            }
        }
    }
}

fn closure(
    file: &FileDescriptorProto,
    prefix: &str,
    decls: &HashSet<String>,
    keep: &mut BTreeSet<String>,
    queue: &mut VecDeque<String>,
) {
    while let Some(fqn) = queue.pop_front() {
        // A nested type is encoded inside its parent; keep the whole chain.
        let mut anc = String::new();
        for part in fqn.trim_start_matches(&format!("{prefix}.")).split('.') {
            anc = if anc.is_empty() {
                format!("{prefix}.{part}")
            } else {
                format!("{anc}.{part}")
            };
            if decls.contains(&anc) && keep.insert(anc.clone()) {
                queue.push_back(anc.clone());
            }
        }
        let Some(msg) = find_message(&file.message_type, prefix, &fqn) else {
            continue;
        };
        for f in msg.field.iter().chain(msg.extension.iter()) {
            if let Some(t) = f.type_name.clone()
                && decls.contains(&t)
                && keep.insert(t.clone())
            {
                queue.push_back(t);
            }
        }
        for nested in &msg.nested_type {
            let nfqn = format!("{fqn}.{}", nested.name.clone().unwrap_or_default());
            if keep.insert(nfqn.clone()) {
                queue.push_back(nfqn);
            }
        }
        for e in &msg.enum_type {
            keep.insert(format!("{fqn}.{}", e.name.clone().unwrap_or_default()));
        }
    }
}

fn prune_file(mut file: FileDescriptorProto, roots: &Roots) -> FileDescriptorProto {
    let pkg = file.package.clone().unwrap_or_default();
    let prefix = if pkg.is_empty() {
        String::new()
    } else {
        format!(".{pkg}")
    };

    let decls = index_file(&file, &prefix);
    let mut keep: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    seed_roots(&file, &prefix, &decls, roots, &mut keep, &mut queue);
    closure(&file, &prefix, &decls, &mut keep, &mut queue);

    FileDescriptorProto {
        name: file.name.take(),
        package: file.package.take(),
        dependency: std::mem::take(&mut file.dependency),
        public_dependency: std::mem::take(&mut file.public_dependency),
        weak_dependency: std::mem::take(&mut file.weak_dependency),
        option_dependency: std::mem::take(&mut file.option_dependency),
        message_type: file
            .message_type
            .into_iter()
            .filter_map(|m| prune_message(&keep, &prefix, m))
            .collect(),
        enum_type: file
            .enum_type
            .into_iter()
            .filter(|e| keep.contains(&format!("{prefix}.{}", e.name.clone().unwrap_or_default())))
            .collect(),
        service: file
            .service
            .into_iter()
            .filter(|s| keep.contains(&format!("{prefix}.{}", s.name.clone().unwrap_or_default())))
            .collect(),
        extension: std::mem::take(&mut file.extension),
        options: std::mem::take(&mut file.options),
        source_code_info: buffa::MessageField::default(),
        syntax: file.syntax.take(),
        edition: file.edition.take(),
        ..Default::default()
    }
}

fn find_message<'a>(
    msgs: &'a [DescriptorProto],
    prefix: &str,
    fqn: &str,
) -> Option<&'a DescriptorProto> {
    let rel = fqn.strip_prefix(&format!("{prefix}."))?;
    let mut parts = rel.split('.');
    let first = parts.next()?;
    let mut cur = msgs.iter().find(|m| m.name.as_deref() == Some(first))?;
    for part in parts {
        cur = cur
            .nested_type
            .iter()
            .find(|m| m.name.as_deref() == Some(part))?;
    }
    Some(cur)
}

fn prune_message(
    keep: &BTreeSet<String>,
    prefix: &str,
    mut msg: DescriptorProto,
) -> Option<DescriptorProto> {
    let fqn = format!("{prefix}.{}", msg.name.clone().unwrap_or_default());
    if !keep.contains(&fqn) {
        return None;
    }
    msg.nested_type = msg
        .nested_type
        .into_iter()
        .filter_map(|n| prune_message(keep, &fqn, n))
        .collect();
    msg.enum_type
        .retain(|e| keep.contains(&format!("{fqn}.{}", e.name.clone().unwrap_or_default())));
    Some(msg)
}
