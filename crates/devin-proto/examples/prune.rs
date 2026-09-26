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

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use buffa::Message;
use buffa_descriptor::generated::descriptor::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let (input, output) = match (args.next(), args.next()) {
        (Some(i), Some(o)) => (PathBuf::from(i), PathBuf::from(o)),
        _ => {
            eprintln!("usage: prune <in.fds> <out.fds>");
            std::process::exit(2);
        }
    };

    let bytes = std::fs::read(&input).expect("read input fds");
    let mut set = FileDescriptorSet::default();
    set.merge_from_slice(&bytes).expect("decode fds");

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let roots_file = crate_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("proto/prune-roots.txt");
    let (root_services, root_types) = parse_roots(&roots_file);

    let mut pruned = FileDescriptorSet::default();
    for mut file in set.file {
        let pkg = file.package.clone().unwrap_or_default();
        let prefix = if pkg.is_empty() {
            String::new()
        } else {
            format!(".{pkg}")
        };

        let mut decls: HashMap<String, ()> = HashMap::new();
        for m in &file.message_type {
            index_message(&mut decls, &prefix, m);
        }
        for e in &file.enum_type {
            decls.insert(format!("{prefix}.{}", e.name.clone().unwrap_or_default()), ());
        }

        let mut keep: BTreeSet<String> = BTreeSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        for svc in &file.service {
            let sname = svc.name.clone().unwrap_or_default();
            if root_services.contains(&sname) {
                keep.insert(format!("{prefix}.{sname}"));
                for m in &svc.method {
                    for tn in [&m.input_type, &m.output_type] {
                        if let Some(t) = tn.clone() {
                            if keep.insert(t.clone()) {
                                queue.push_back(t);
                            }
                        }
                    }
                }
            }
        }
        for name in &root_types {
            let fqn = format!("{prefix}.{name}");
            if decls.contains_key(&fqn) && keep.insert(fqn.clone()) {
                queue.push_back(fqn);
            } else if !decls.contains_key(&fqn) {
                eprintln!("warning: root type not in fds: {fqn}");
            }
        }

        // Extension declarations extend option types; their extendees must
        // exist in the set or codegen fails resolving them.
        for f in file.extension.iter().chain(
            file.message_type
                .iter()
                .flat_map(|m| m.extension.iter()),
        ) {
            if let Some(t) = f.extendee.clone() {
                if decls.contains_key(&t) && keep.insert(t.clone()) {
                    queue.push_back(t);
                }
            }
            if let Some(t) = f.type_name.clone() {
                if decls.contains_key(&t) && keep.insert(t.clone()) {
                    queue.push_back(t);
                }
            }
        }

        while let Some(fqn) = queue.pop_front() {
            // A nested type is encoded inside its parent; keep the chain.
            let mut anc = String::new();
            for part in fqn.trim_start_matches(&format!("{prefix}.")).split('.') {
                anc = if anc.is_empty() {
                    format!("{prefix}.{part}")
                } else {
                    format!("{anc}.{part}")
                };
                if decls.contains_key(&anc) && keep.insert(anc.clone()) {
                    queue.push_back(anc.clone());
                }
            }
            if let Some(msg) = find_message(&file.message_type, &prefix, &fqn) {
                for f in msg.field.iter().chain(msg.extension.iter()) {
                    if let Some(t) = f.type_name.clone() {
                        if decls.contains_key(&t) && keep.insert(t.clone()) {
                            queue.push_back(t);
                        }
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

        let mut out = FileDescriptorProto {
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
            source_code_info: Default::default(),
            syntax: file.syntax.take(),
            edition: file.edition.take(),
            ..Default::default()
        };
        let _ = &mut out;
        pruned.file.push(out);
    }

    let mut buf = Vec::new();
    pruned.encode(&mut buf);
    std::fs::write(&output, &buf).expect("write output fds");

    let kept_msgs: usize = pruned
        .file
        .iter()
        .map(|f| f.message_type.len())
        .sum();
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

fn parse_roots(path: &PathBuf) -> (HashSet<String>, HashSet<String>) {
    let mut services = HashSet::new();
    let mut types = HashSet::new();
    for raw in std::fs::read_to_string(path).expect("read prune-roots.txt").lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("service ") {
            services.insert(name.trim().to_string());
        } else {
            types.insert(line.to_string());
        }
    }
    (services, types)
}

fn index_message(decls: &mut HashMap<String, ()>, prefix: &str, msg: &DescriptorProto) {
    let fqn = format!("{prefix}.{}", msg.name.clone().unwrap_or_default());
    decls.insert(fqn.clone(), ());
    for nested in &msg.nested_type {
        index_message(decls, &fqn, nested);
    }
    for e in &msg.enum_type {
        decls.insert(format!("{fqn}.{}", e.name.clone().unwrap_or_default()), ());
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
    msg.enum_type = msg
        .enum_type
        .into_iter()
        .filter(|e| keep.contains(&format!("{fqn}.{}", e.name.clone().unwrap_or_default())))
        .collect();
    Some(msg)
}
