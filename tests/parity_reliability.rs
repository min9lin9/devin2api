use std::collections::{BTreeMap, BTreeSet};

use devin2api::domain::failure::{Failure, classify};
use devin2api::qa::compare::{self, CanonRules};
use devin2api::qa::contracts;

#[test]
fn every_manifest_case_has_one_rust_disposition() {
    let manifest = contracts::load_committed().expect("committed contracts manifest");
    assert_eq!(
        manifest.go_test_cases.len(),
        308,
        "reference case count drift"
    );
    let mut source_ids = BTreeSet::new();
    let mut by_disposition = BTreeMap::new();
    for case in &manifest.go_test_cases {
        assert!(
            source_ids.insert((&case.file, &case.function)),
            "duplicate Go source disposition {}::{}",
            case.file,
            case.function
        );
        match case.disposition.as_str() {
            "rust_test" => {
                let (target, test) = case
                    .rust_case
                    .split_once("::")
                    .expect("exact executable test check id");
                assert!(!test.is_empty());
                assert!(
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("tests")
                        .join(format!("{target}.rs"))
                        .is_file(),
                    "missing target for {}",
                    case.rust_case
                );
                assert!(case.reason.is_empty());
            }
            "intentional_deviation" => {
                assert_eq!(case.kind, "benchmark");
                assert_eq!(case.owner_task, 24);
                assert!(case.rust_case.is_empty());
                assert!(!case.reason.is_empty());
            }
            other => panic!("unknown disposition {other}"),
        }
        *by_disposition
            .entry(case.disposition.as_str())
            .or_insert(0usize) += 1;
    }
    assert_eq!(source_ids.len(), manifest.go_test_cases.len());
    assert_eq!(by_disposition["rust_test"], 294);
    assert_eq!(by_disposition["intentional_deviation"], 14);
}

#[test]
fn required_surface_inventory_is_nonempty_and_unique() {
    let manifest = contracts::load_committed().expect("committed contracts manifest");
    assert!(!manifest.routes.is_empty());
    assert!(!manifest.config_keys.is_empty());
    assert!(!manifest.aux_commands.is_empty());
    assert!(!manifest.platform_assets.is_empty());

    let routes: BTreeSet<_> = manifest
        .routes
        .iter()
        .map(|route| (&route.method, &route.path))
        .collect();
    assert_eq!(
        routes.len(),
        manifest.routes.len(),
        "duplicate route inventory entry"
    );
    let keys: BTreeSet<_> = manifest
        .config_keys
        .iter()
        .map(|entry| &entry.key)
        .collect();
    assert_eq!(
        keys.len(),
        manifest.config_keys.len(),
        "duplicate config inventory entry"
    );
}

#[test]
fn negative_control_rejects_status_order_and_presence() {
    let rules = CanonRules::default();
    let expected = serde_json::json!({
        "status": "completed",
        "events": ["start", "delta", "done"],
        "optional": null
    });
    for (pointer, replacement) in [
        ("/status", serde_json::json!("failed")),
        ("/events", serde_json::json!(["delta", "start", "done"])),
        ("/optional", serde_json::json!({"present": true})),
    ] {
        let mut actual = expected.clone();
        *actual.pointer_mut(pointer).expect("fixed pointer") = replacement;
        assert!(
            !compare::compare_json(&expected, &actual, &rules).matched(),
            "semantic mutation at {pointer} escaped the verifier"
        );
    }
}

#[test]
fn oversized_connect_envelope_is_transport_not_rate_limit() {
    let wire_error = Failure {
        code: "resource_exhausted".into(),
        message: "message size 4294967295 exceeds limit 4194304".into(),
        ..Failure::default()
    };
    let classified = classify(&wire_error);
    assert!(classified.upstream_fault);
    assert!(!classified.rate_limited);
    assert!(!classified.client_fixable);
}

#[test]
fn task_23_qa_commands_are_real_handlers() {
    for name in ["parity", "faults"] {
        let command = devin2api::qa::find_subcommand(name).expect("registered QA command");
        assert!(command.implemented, "{name} remained a placeholder");
        assert_eq!(command.owner_task, 23);
    }
}
