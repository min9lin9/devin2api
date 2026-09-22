//! Task 13 acceptance: debuglog port reads copied Go history, appends
//! compatible Rust entries, preserves the five stage timestamps and
//! retention guarantees, and finalizes one request exactly once.
//!
//! `go_history_replay_and_rust_append` — the happy path: replay the
//! Go-generated fixture tree, append a Rust request, verify aggregates.
//! `disk_errors_queue_pressure_and_retention` — the failure path: injected
//! IO errors, bounded-queue pressure, layered retention and protected error
//! dirs.
//! `go_reads_rust_appended_history` — cross-check: the Go package itself
//! replays the Rust-appended tree (skipped without a Go toolchain).
//!
//! The remaining `#[test]`s mirror `tests/contracts.json` `rust_case` ids
//! (`debuglog_compat::<GoTestName>`), hence the file-level
//! `non_snake_case` allow.
#![allow(non_snake_case)]

mod support;

use std::path::{Path, PathBuf};

use devin2api::debuglog::{
    self, Completion, IndexEntry, JVal, LogValue, Manager, RequestFilter, RequestMeta,
    RetentionPolicy,
};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/task13")
}

/// Copy the Go history fixture into a per-test work dir (the test appends).
fn copy_history(tag: &str) -> PathBuf {
    let dst = support::work_dir(tag).join("logs");
    copy_tree(&fixture_dir().join("go-history"), &dst);
    dst
}

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().flatten() {
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn manifest() -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(fixture_dir().join("manifest.json")).unwrap())
        .unwrap()
}

fn read_index(root: &Path) -> Vec<IndexEntry> {
    std::fs::read_to_string(root.join("index.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| IndexEntry::parse(l.as_bytes()).expect("index line must parse"))
        .collect()
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

// One sequential replay scenario; splitting would scatter the timeline.
#[allow(clippy::too_many_lines)]
#[test]
fn go_history_replay_and_rust_append() {
    let root = copy_history("replay");
    let manifest = manifest();
    let manifest_usage = &manifest["usage"];

    // --- Phase 1: replay Go history -------------------------------------
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let snap = manager.usage_stats();

    assert_eq!(snap.entries, 4, "replayed entries");
    assert_eq!(
        snap.window_start,
        manifest_usage["window_start"].as_str().unwrap()
    );
    let mw = &manifest_usage["window"];
    assert_eq!(snap.window.requests, mw["requests"].as_i64().unwrap());
    assert_eq!(snap.window.errors, mw["errors"].as_i64().unwrap());
    assert_eq!(
        snap.window.disconnected,
        mw["disconnected"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.rate_limited,
        mw["rate_limited"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.client_faults,
        mw["client_faults"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.upstream_faults,
        mw["upstream_faults"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.input_tokens,
        mw["input_tokens"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.output_tokens,
        mw["output_tokens"].as_i64().unwrap()
    );
    assert_eq!(
        snap.window.total_tokens,
        mw["total_tokens"].as_i64().unwrap()
    );
    assert_eq!(snap.error_stages["devin_transport"], 1);
    assert_eq!(snap.error_stages["rate_gate"], 1);
    assert_eq!(snap.duration.samples, 4);
    assert_eq!(
        snap.duration.max,
        manifest_usage["duration"]["max"].as_i64().unwrap()
    );
    assert_eq!(snap.ttfb.samples, 1);
    // Per-model rows: swe-2 (2 reqs) then swe-2-max (2 reqs, name tiebreak).
    assert_eq!(snap.models.len(), 2);
    assert_eq!(snap.models[0].name, "swe-2");
    assert_eq!(snap.models[0].totals.requests, 2);
    assert_eq!(snap.models[0].totals.rate_limited, 1);
    assert_eq!(snap.models[1].name, "swe-2-max");
    assert_eq!(snap.models[1].totals.upstream_faults, 1);
    let swe2max_avg = manifest_usage["models"][1]["avg_duration_ms"]
        .as_f64()
        .unwrap();
    assert!((snap.models[1].avg_duration_ms - swe2max_avg).abs() < 1e-9);
    // Per-key rows.
    assert_eq!(snap.keys.len(), 2);
    assert_eq!(snap.keys[0].name, "khash001");
    assert_eq!(snap.keys[0].totals.requests, 2);
    // Rate-limit sampling: rpm=0 because the fixture's inflated duration_ms
    // pushes `end` past the 60s lookback — the port must reproduce that.
    assert_eq!(snap.rate_limit_events.len(), 1);
    assert_eq!(snap.rate_limit_events[0].rpm, 0);
    assert_eq!(snap.rate_limit_events[0].stage, "rate_gate");
    // Day row matches the fixture's generation date.
    assert_eq!(snap.days.len(), 1);
    assert_eq!(
        snap.days[0].date,
        manifest["base_local_date"].as_str().unwrap()
    );
    assert_eq!(snap.days[0].totals.requests, 4);
    // 10-minute points: 1152 buckets; the fixture's entries land in recent
    // buckets while the fixture is younger than the 8-day window.
    assert_eq!(snap.points.len(), debuglog::USAGE_MIN_BUCKETS);
    let point_reqs: i64 = snap.points.iter().map(|p| p.totals.requests).sum();
    let age_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .cast_signed()
        - manifest["base_unix"].as_i64().unwrap();
    if age_secs < 8 * 24 * 3600 {
        assert_eq!(point_reqs, 4, "all entries land in recent buckets");
    } else {
        assert!(point_reqs <= 4);
    }
    // model_days matrix.
    assert_eq!(
        snap.model_days["swe-2-max"][manifest["base_local_date"].as_str().unwrap()].requests,
        2
    );

    // Index listing over the Go-written file.
    let list = manager.list_requests(10, &RequestFilter::default());
    assert_eq!(list.entries.len(), 4);
    assert!(!list.has_more);
    assert_eq!(list.entries[0].dir, manifest["dirs"][3].as_str().unwrap());
    assert_eq!(list.entries[0].status_code, 429);
    assert!(list.entries[0].rate_limited);
    assert_eq!(list.entries[0].retry_after_seconds, 30);
    assert_eq!(list.entries[1].result, "disconnected");
    assert_eq!(list.entries[2].error_stage, "devin_transport");
    assert_eq!(list.entries[2].retries, 1);
    assert_eq!(list.entries[3].client_request_id, "cli-req-1");
    // Filters over Go rows.
    let filtered = manager.list_requests(
        10,
        &RequestFilter {
            status: ">=400".into(),
            ..RequestFilter::default()
        },
    );
    assert_eq!(filtered.entries.len(), 3);
    let filtered = manager.list_requests(
        10,
        &RequestFilter {
            error_stage: "rate_gate".into(),
            ..RequestFilter::default()
        },
    );
    assert_eq!(filtered.entries.len(), 1);

    // Detail + file reads over Go-written dirs.
    let first_dir = manifest["dirs"][0].as_str().unwrap();
    let detail = manager.detail(first_dir).unwrap();
    let meta = serde_json::from_slice::<serde_json::Value>(detail.meta.as_ref().unwrap()).unwrap();
    assert_eq!(meta["result"], "completed");
    assert_eq!(meta["upstream_request_id"], "up-1");
    // All five stage timestamps present in the Go meta.
    for key in [
        "request_ready_ms",
        "upstream_sent_ms",
        "upstream_open_ms",
        "first_upstream_ms",
        "first_client_ms",
    ] {
        assert!(meta.get(key).is_some(), "meta missing {key}");
    }
    let names: Vec<&str> = detail.files.iter().map(|f| f.name.as_str()).collect();
    for want in [
        "01-http-request.json",
        "02-request-messages.json",
        "03-devin-request.json",
        "04-devin-response.jsonl",
        "05-response-events.jsonl",
        "06-http-response.jsonl",
        "attachments/image-001.png",
        "meta.json",
    ] {
        assert!(names.contains(&want), "missing {want} in {names:?}");
    }
    let (data, total, truncated) = manager
        .read_file(first_dir, "attachments/image-001.png")
        .unwrap();
    assert_eq!(data, b"fake-png-bytes");
    assert_eq!(total, 14);
    assert!(!truncated);
    // Go's redaction and HTML escaping must be visible in the file bytes.
    let stage01 =
        std::fs::read_to_string(root.join(first_dir).join("01-http-request.json")).unwrap();
    assert!(stage01.contains("\\u003credacted\\u003e"), "{stage01}");
    assert!(stage01.contains("attachments/image-001.png"));
    let stage03 =
        std::fs::read_to_string(root.join(first_dir).join("03-devin-request.json")).unwrap();
    assert!(
        stage03.contains("\"f\": \"\\u003credacted\\u003e\""),
        "{stage03}"
    );
    // Process log + bind failure passthrough.
    let (log, next) = manager.read_process_log(0).unwrap();
    assert_eq!(log, b"line1\nline2\n");
    let (log2, _) = manager.read_process_log(next).unwrap();
    assert_eq!(log2, b"");
    let stats = debuglog::gojson::marshal(&manager.stats()).unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    assert_eq!(stats["last_bind_failure"]["count"], 2);
    assert_eq!(stats["io_errors"], 0);
    assert_eq!(stats["dropped_log_events"], 0);

    // --- Phase 2: append a Rust-written request --------------------------
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        api: "openai-chat".into(),
        client_ip: "192.0.2.9".into(),
        user_agent: "rust-test/1.0".into(),
        key_hash: "khash009".into(),
        client_request_id: "rust-req-1".into(),
    });
    assert!(recorder.is_active());
    let dir = recorder.dir_name();
    assert!(debuglog::is_request_dir_name(&dir));

    // Stage files: secrets redacted, image extracted, HTML escaping applied.
    let png = base64_encode(b"rust-png");
    recorder.write_json(
        "01-http-request.json",
        LogValue::Tree(
            JVal::obj()
                .set("model", JVal::Str("swe-2".into()))
                .set("authorization", JVal::Str("Bearer rust-secret".into()))
                .set("f", JVal::Str("client-field".into()))
                .set(
                    "image",
                    JVal::obj()
                        .set("mime_type", JVal::Str("image/png".into()))
                        .set("data", JVal::Str(png.clone()))
                        .build(),
                )
                .build(),
        ),
    );
    recorder.write_json(
        "03-devin-request.json",
        LogValue::Tree(
            JVal::obj()
                .set(
                    "metadata",
                    JVal::obj()
                        .set("api_key", JVal::Str("sekret".into()))
                        .set("f", JVal::Str("fingerprint".into()))
                        .build(),
                )
                .build(),
        ),
    );
    // Deferred serialization: the thunk runs inside the worker.
    recorder.write_json(
        "02-request-messages.json",
        LogValue::deferred(|| {
            LogValue::Tree(JVal::obj().set("deferred", JVal::Bool(true)).build())
        }),
    );
    // Raw passthrough (json.RawMessage parity).
    recorder.append_jsonl(
        "04-devin-response.jsonl",
        "frame",
        LogValue::raw(br#"{"text":"a < b & c"}"#.to_vec()),
    );
    recorder.append_jsonl(
        "04-devin-response.jsonl",
        "frame",
        LogValue::Tree(JVal::obj().set("text", JVal::Str("2nd".into())).build()),
    );
    recorder.append_value_jsonl(
        "05-response-events.jsonl",
        LogValue::Tree(JVal::obj().set("v", JVal::Int(1)).build()),
    );
    // Retry attempt shard + index retry count.
    recorder.note_retry_attempt(2, "transport: EOF");
    recorder.write_json(
        &debuglog::stage_devin_request_attempt(2),
        LogValue::Tree(JVal::obj().set("attempt", JVal::Int(2)).build()),
    );
    // First error wins.
    recorder.write_error(
        "provider_stream",
        &std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upstream broke"),
    );
    recorder.write_error("http_stream", &std::io::Error::other("must not overwrite"));
    // All five stage markers.
    recorder.note_request_ready();
    recorder.note_upstream_send();
    recorder.note_upstream_open();
    recorder.note_upstream_latency();
    recorder.note_client_latency();
    recorder.set_repairs(devin2api::domain::RequestRepairs {
        reordered_prompts: 2,
        ..Default::default()
    });
    recorder.complete(Completion {
        status_code: 201,
        result: "completed".into(),
        model: "rust-model".into(),
        requested_model: "swe-2".into(),
        response_model: "rust-model".into(),
        provider: "devin".into(),
        stream: true,
        upstream_request_id: "up-rust-1".into(),
        usage: devin2api::domain::Usage {
            input: 7,
            output: 3,
            total_tokens: 10,
            ..Default::default()
        },
        ..Default::default()
    });
    // Finalize once: a second complete must not rewrite meta or index.
    let meta_before = std::fs::read(root.join(&dir).join("meta.json")).unwrap();
    let index_before = std::fs::read(root.join("index.jsonl")).unwrap();
    recorder.complete(Completion {
        status_code: 500,
        result: "failed".into(),
        ..Default::default()
    });
    assert_eq!(
        std::fs::read(root.join(&dir).join("meta.json")).unwrap(),
        meta_before,
        "second complete rewrote meta.json"
    );
    assert_eq!(
        std::fs::read(root.join("index.jsonl")).unwrap(),
        index_before,
        "second complete appended another index line"
    );

    // --- Phase 3: verify the Rust-written artifacts ----------------------
    let meta = read_json(&root.join(&dir).join("meta.json"));
    for key in [
        "request_ready_ms",
        "upstream_sent_ms",
        "upstream_open_ms",
        "first_upstream_ms",
        "first_client_ms",
        "started_at",
        "finished_at",
        "duration_ms",
        "status_code",
        "result",
        "model",
        "provider",
        "stream",
        "requested_model",
        "response_model",
        "upstream_request_id",
        "usage",
        "repairs",
        "retry_attempts",
    ] {
        assert!(meta.get(key).is_some(), "rust meta missing {key}: {meta}");
    }
    assert_eq!(meta["status_code"], 201);
    assert_eq!(meta["result"], "completed");
    assert_eq!(meta["usage"]["input"], 7);
    assert_eq!(meta["repairs"]["reordered_prompts"], 2);
    assert_eq!(meta["retry_attempts"][0]["attempt"], 2);
    assert_eq!(meta["client"]["key_hash"], "khash009");

    let stage01 = std::fs::read_to_string(root.join(&dir).join("01-http-request.json")).unwrap();
    assert!(!stage01.contains("rust-secret"), "{stage01}");
    assert!(!stage01.contains(&png), "{stage01}");
    assert!(stage01.contains("\"f\": \"client-field\""), "{stage01}");
    assert!(stage01.contains("attachments/image-001.png"), "{stage01}");
    let stage03 = std::fs::read_to_string(root.join(&dir).join("03-devin-request.json")).unwrap();
    assert!(
        !stage03.contains("sekret") && !stage03.contains("fingerprint"),
        "{stage03}"
    );
    let error = read_json(&root.join(&dir).join("error.json"));
    assert_eq!(error["stage"], "provider_stream");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("upstream broke")
    );
    let lines: Vec<serde_json::Value> =
        std::fs::read_to_string(root.join(&dir).join("04-devin-response.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["seq"], 1);
    assert_eq!(lines[1]["seq"], 2);
    assert_eq!(lines[0]["event"], "frame");
    // Go HTML-escapes inside strings: "a < b & c" must land escaped.
    assert_eq!(lines[0]["data"]["text"], "a < b & c");
    let raw_line =
        std::fs::read_to_string(root.join(&dir).join("04-devin-response.jsonl")).unwrap();
    assert!(raw_line.contains("\\u003c"), "{raw_line}");
    let attachment = std::fs::read(root.join(&dir).join("attachments/image-001.png")).unwrap();
    assert_eq!(attachment, b"rust-png");

    // Index: the Rust line parses with the same schema and field order.
    let entries = read_index(&root);
    assert_eq!(entries.len(), 5);
    let tail_entry = &entries[4];
    assert_eq!(tail_entry.dir, dir);
    assert_eq!(tail_entry.status_code, 201);
    assert_eq!(tail_entry.result, "completed");
    assert_eq!(tail_entry.model, "rust-model");
    assert_eq!(tail_entry.requested_model, "swe-2");
    assert_eq!(tail_entry.api, "openai-chat");
    assert_eq!(tail_entry.key_hash, "khash009");
    assert_eq!(tail_entry.client_request_id, "rust-req-1");
    assert_eq!(tail_entry.error_stage, "provider_stream");
    assert_eq!(tail_entry.retries, 1);
    assert_eq!(tail_entry.repairs, 2);
    assert_eq!(tail_entry.input_tokens, 7);
    assert_eq!(tail_entry.output_tokens, 3);
    assert_eq!(tail_entry.total_tokens, 10);
    assert_eq!(tail_entry.upstream_request_id, "up-rust-1");
    for f in [
        tail_entry.request_ready_ms,
        tail_entry.upstream_sent_ms,
        tail_entry.upstream_open_ms,
        tail_entry.first_upstream_ms,
        tail_entry.first_client_ms,
    ] {
        assert!(f.is_some(), "index entry missing a stage timestamp");
    }
    // Field order matches the Go struct: dir, started_at, duration_ms first.
    let last_line = std::fs::read_to_string(root.join("index.jsonl"))
        .unwrap()
        .lines()
        .last()
        .unwrap()
        .to_string();
    assert!(
        last_line.starts_with("{\"dir\":"),
        "index line field order drifted: {last_line}"
    );
    assert!(last_line.contains("\"stream\":true"));

    // Aggregates now include the Rust entry (live path counts it once).
    let snap2 = manager.usage_stats();
    assert_eq!(snap2.entries, 5);
    assert_eq!(snap2.window.requests, 5);
    assert_eq!(snap2.window.input_tokens, 132);
    assert_eq!(snap2.error_stages["provider_stream"], 1);

    // --- Phase 4: restart replays Rust+Go history together ---------------
    manager.close();
    let manager2 = Manager::new(&root, &RetentionPolicy::default());
    let snap3 = manager2.usage_stats();
    assert_eq!(snap3.entries, 5, "restart must replay Rust-appended line");
    assert_eq!(snap3.window.requests, 5);
    assert_eq!(snap3.window.input_tokens, 132);
    assert_eq!(snap3.error_stages["provider_stream"], 1);
    let list2 = manager2.list_requests(10, &RequestFilter::default());
    assert_eq!(list2.entries.len(), 5);
    assert_eq!(list2.entries[0].dir, dir);
    manager2.close();
}

// One sequential fault-injection scenario; splitting would scatter it.
#[allow(clippy::too_many_lines)]
#[test]
fn disk_errors_queue_pressure_and_retention() {
    let root = support::work_dir("errors").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());

    // --- Injected IO errors: remove the request dir so every write fails.
    let rec = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/x".into(),
        ..Default::default()
    });
    assert!(rec.is_active());
    std::fs::remove_dir_all(rec.directory_path()).unwrap();
    rec.write_json(
        "01-http-request.json",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(1)).build()),
    );
    rec.write_json(
        "03-devin-request.json",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(2)).build()),
    );
    rec.append_jsonl(
        "04-devin-response.jsonl",
        "e",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(1)).build()),
    );
    rec.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    // Same dedup as Go: one "file" failure + one "jsonl" failure per dir.
    let stats = debuglog::gojson::marshal(&manager.stats()).unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    assert_eq!(stats["io_errors"], 2, "stats: {stats}");

    // --- Queue pressure: pause the worker, overflow the 4096 queue.
    let rec2 = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/y".into(),
        ..Default::default()
    });
    rec2.set_writer_paused(true);
    let total = debuglog::QUEUE_DEPTH + 150;
    for i in 0..total {
        rec2.append_jsonl(
            "04-devin-response.jsonl",
            "frame",
            LogValue::Tree(
                JVal::obj()
                    .set("i", JVal::Int(i64::try_from(i).unwrap_or(i64::MAX)))
                    .build(),
            ),
        );
    }
    let dropped = rec2.dropped();
    // The meta task may or may not have been dequeued before the pause —
    // dropped is deterministic within that one-task window.
    assert!(
        (148..=150).contains(&dropped),
        "dropped = {dropped}, want ~150"
    );
    // Bounded memory: the channel never holds more than QUEUE_DEPTH tasks.
    let stats = debuglog::gojson::marshal(&manager.stats()).unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    assert!(
        stats["queued_log_events"].as_i64().unwrap()
            <= i64::try_from(debuglog::QUEUE_DEPTH).unwrap_or(i64::MAX)
    );
    assert_eq!(
        stats["queue_capacity"].as_i64().unwrap(),
        i64::try_from(debuglog::QUEUE_DEPTH).unwrap_or(i64::MAX)
    );
    rec2.set_writer_paused(false);
    rec2.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    // Drain guarantee: exactly the non-dropped appends landed, in order.
    let lines: Vec<String> =
        std::fs::read_to_string(rec2.directory_path().join("04-devin-response.jsonl"))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
    assert_eq!(lines.len() as u64, total as u64 - dropped);
    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            v["seq"].as_i64().unwrap(),
            i64::try_from(i).unwrap_or(i64::MAX) + 1
        );
    }
    // Late writes after complete drop and count.
    rec2.write_json("late.json", LogValue::Tree(JVal::obj().build()));
    assert_eq!(rec2.dropped(), dropped + 1);
    let stats = debuglog::gojson::marshal(&manager.stats()).unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    assert_eq!(
        stats["dropped_log_events"].as_u64().unwrap(),
        dropped + 1,
        "dropped_total must fold queue drops + the late write"
    );

    // --- Index cap: shrink, write enough entries, verify tail rewrite.
    manager.set_index_file_cap(4 << 10);
    for _ in 0..60 {
        let r = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/v1/responses".into(),
            ..Default::default()
        });
        r.complete(Completion {
            status_code: 200,
            result: "completed".into(),
            ..Default::default()
        });
    }
    let index_meta = std::fs::metadata(root.join("index.jsonl")).unwrap();
    assert!(
        index_meta.len() <= 4 << 10,
        "index.jsonl {} exceeds cap",
        index_meta.len()
    );
    let lines = read_index(&root);
    assert!(!lines.is_empty() && lines.len() < 62);
    // Every surviving line is a complete entry (no torn head line).
    for e in &lines {
        assert!(!e.dir.is_empty());
    }

    // --- Layered retention ------------------------------------------------
    // Old dir: payload stripped, evidence kept.
    let old = root.join("20200101-000000");
    std::fs::create_dir_all(old.join("attachments")).unwrap();
    for name in [
        "03-devin-request.json",
        "03-devin-request.attempt2.json",
        "04-devin-response.jsonl",
        "06-http-response.jsonl",
        "meta.json",
        "error.json",
    ] {
        std::fs::write(old.join(name), b"x").unwrap();
    }
    std::fs::write(old.join("attachments/a.bin"), b"x").unwrap();
    // Fresh dirs for capacity eviction: two error dirs + one ok dir, all
    // over the cap when max_total_mb is tiny. Names embed the age.
    for (name, has_err) in [
        ("20200102-000000", true),
        ("20200103-000000", true),
        ("20200104-000000", false),
    ] {
        let d = root.join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("04-devin-response.jsonl"), vec![b'x'; 2048]).unwrap();
        if has_err {
            std::fs::write(d.join("error.json"), b"{}").unwrap();
        }
    }
    manager.set_policy(RetentionPolicy {
        days: 0,
        max_total_mb: 0,
        payload_hours: 1,
        keep_error_dirs: 1,
    });
    // Payload strip only (no size cap yet).
    manager.clean_once();
    for gone in [
        "03-devin-request.json",
        "03-devin-request.attempt2.json",
        "04-devin-response.jsonl",
        "06-http-response.jsonl",
        "attachments",
    ] {
        assert!(
            !old.join(gone).exists(),
            "payload {gone} should be stripped"
        );
    }
    for keep in ["meta.json", "error.json"] {
        assert!(old.join(keep).exists(), "evidence {keep} must remain");
    }
    // Capacity eviction with keep_error_dirs=1: oldest error dir and the
    // ok dir go; the newest error dir survives.
    manager.set_policy(RetentionPolicy {
        days: 0,
        max_total_mb: 0, // set below via a tiny cap
        payload_hours: 0,
        keep_error_dirs: 1,
    });
    // max_total_mb is in MB; use a 1-byte cap via direct policy? The policy
    // is MB-granular — instead fill dirs so any positive cap is exceeded:
    // write ~2MB into the ok dir so max_total_mb=1 triggers eviction.
    std::fs::write(
        root.join("20200104-000000").join("04-devin-response.jsonl"),
        vec![b'x'; 2 << 20],
    )
    .unwrap();
    manager.set_policy(RetentionPolicy {
        days: 0,
        max_total_mb: 1,
        payload_hours: 0,
        keep_error_dirs: 1,
    });
    let removed = manager.clean_once();
    assert!(removed >= 2, "removed={removed}");
    assert!(
        !root.join("20200102-000000").exists(),
        "oldest error dir evicted"
    );
    assert!(!root.join("20200104-000000").exists(), "ok dir evicted");
    assert!(
        root.join("20200103-000000").exists(),
        "newest error dir protected"
    );
    // The ancient stripped dir (20200101) is also evicted by the cap — it
    // is older than the protected error dir.
    assert!(!old.exists());

    // Day-based eviction ages by the dir name, not mtime. keep_error_dirs
    // only protects capacity eviction — time cleanup removes the 2020
    // error dir too (Go: "时间清理不受影响").
    let ancient = root.join("20190101-000000");
    std::fs::create_dir_all(&ancient).unwrap();
    std::fs::write(ancient.join("meta.json"), b"{}").unwrap();
    manager.set_policy(RetentionPolicy {
        days: 7,
        ..Default::default()
    });
    let removed = manager.clean_once();
    assert!(removed >= 2, "removed={removed}");
    assert!(!ancient.exists());
    assert!(!root.join("20200103-000000").exists());

    manager.close();
}

/// Cross-check: the Go package replays the Rust-appended tree. Skipped
/// without a Go toolchain (the offline suite still covers everything else).
#[test]
fn go_reads_rust_appended_history() {
    let Some(oracle) = support::require_go_oracle() else {
        return;
    };
    let root = copy_history("readback");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    manager.wait_replay();
    let rec = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        api: "openai-chat".into(),
        key_hash: "khash009".into(),
        ..Default::default()
    });
    rec.note_request_ready();
    rec.note_upstream_send();
    rec.note_upstream_open();
    rec.note_upstream_latency();
    rec.note_client_latency();
    rec.complete(Completion {
        status_code: 201,
        result: "completed".into(),
        model: "rust-appended".into(),
        requested_model: "swe-2".into(),
        stream: true,
        usage: devin2api::domain::Usage {
            input: 7,
            output: 3,
            total_tokens: 10,
            ..Default::default()
        },
        ..Default::default()
    });
    manager.close();

    // Inject the readback test into the Go package via -overlay (no writes
    // into G) and run it against the Rust-produced tree.
    let work = support::work_dir("readback-go");
    let overlay = work.join("overlay.json");
    let gen_src = fixture_dir().join("readback_test.go");
    let overlay_json = format!(
        "{{\"Replace\":{{\"{}\":\"{}\"}}}}",
        oracle
            .go_root()
            .join("internal/debuglog/zz_t13readback_test.go")
            .display(),
        gen_src.display()
    );
    std::fs::write(&overlay, overlay_json).unwrap();
    let out = std::process::Command::new(go_bin())
        .args([
            "test",
            "-overlay",
            overlay.to_str().unwrap(),
            "-run",
            "TestT13Readback",
            "-count=1",
            "-v",
            "./internal/debuglog/",
        ])
        .current_dir(oracle.go_root())
        .env("GOFLAGS", "-mod=readonly")
        .env("GOTOOLCHAIN", "local")
        .env("T13_READBACK", &root)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "go readback failed:\n{stdout}");
    // `t.Logf` prefixes the line with `file.go:NN:` — find the marker
    // anywhere in the line.
    let marker = stdout
        .lines()
        .find_map(|l| {
            l.find("T13READBACK:")
                .map(|i| &l[i + "T13READBACK:".len()..])
        })
        .expect("readback marker missing");
    let rb: serde_json::Value = serde_json::from_str(marker).unwrap();
    assert_eq!(rb["entries"], 5, "go replay: {rb}");
    assert_eq!(rb["window"]["requests"], 5);
    assert_eq!(rb["window"]["input_tokens"], 132);
    assert_eq!(rb["list_len"], 5);
    // `newest` is the Go-marshaled index entry as a string — comparing it
    // byte-for-byte against the raw line Rust appended proves field order
    // and omitempty parity.
    let newest_raw = rb["newest"].as_str().unwrap();
    let raw_last = rb["raw_last_line"].as_str().unwrap();
    assert_eq!(
        newest_raw, raw_last,
        "Rust index line is not byte-identical to Go's marshal of it"
    );
    let newest: serde_json::Value = serde_json::from_str(newest_raw).unwrap();
    assert_eq!(newest["dir"].as_str().unwrap(), rec.dir_name());
    assert_eq!(newest["status_code"], 201);
    assert_eq!(newest["model"], "rust-appended");
    assert_eq!(newest["requested_model"], "swe-2");
    assert_eq!(newest["key_hash"], "khash009");
    assert_eq!(newest["input_tokens"], 7);
    assert_eq!(newest["stream"], true);
    assert!(
        newest.get("request_ready_ms").is_some() && newest.get("first_client_ms").is_some(),
        "go could not see Rust stage timestamps: {newest}"
    );
}

fn go_bin() -> PathBuf {
    if let Ok(bin) = std::env::var("QA_GO_BIN") {
        return PathBuf::from(bin);
    }
    let sdk = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("sdk/go/bin/go");
    if sdk.is_file() {
        return sdk;
    }
    PathBuf::from("go")
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}

// ===========================================================================
// Go test-case mirrors — names match `tests/contracts.json` rust_case ids
// (`debuglog_compat::<GoTestName>`). Bodies port the corresponding Go tests
// in G/internal/debuglog/{recorder,index,usage}_test.go.
// ===========================================================================

use devin2api::debuglog::gotime;
use devin2api::debuglog::usage::UsageAggregator;
use devin2api::domain::Usage;
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp, Zoned};
use std::sync::Arc;
use std::time::Duration;

fn fixed_zoned() -> Zoned {
    jiff::civil::date(2027, 1, 1)
        .at(23, 54, 54, 0)
        .to_zoned(TimeZone::system())
        .unwrap()
}

fn fixed_clock() -> Box<dyn Fn() -> Zoned + Send + Sync> {
    Box::new(fixed_zoned)
}

fn zoned_before(dur: SignedDuration) -> Zoned {
    Timestamp::now()
        .checked_sub(dur)
        .unwrap()
        .to_zoned(TimeZone::system())
}

fn zoned_after(dur: SignedDuration) -> Zoned {
    Timestamp::now()
        .checked_add(dur)
        .unwrap()
        .to_zoned(TimeZone::system())
}

fn start_completed(
    manager: &Manager,
    meta: &RequestMeta,
    completion: Completion,
) -> debuglog::Recorder {
    let r = manager.start(meta);
    r.complete(completion);
    r
}

/// `TestRecorderWritesRedactedStagesAndAttachments` — diagnostic logs must
/// not leak credentials or duplicate large inline images.
#[test]
fn RecorderWritesRedactedStagesAndAttachments() {
    let root = support::work_dir("redact").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        ..Default::default()
    });
    assert!(recorder.is_active());
    let image = base64_encode(b"png-data");
    recorder.write_json(
        "01-http-request.json",
        LogValue::Tree(
            JVal::obj()
                .set("authorization", JVal::Str("secret".into()))
                // "f" is a plain short key in client payloads — only
                // upstream metadata.f fingerprints get redacted.
                .set("f", JVal::Str("client-field".into()))
                .set(
                    "body",
                    JVal::obj()
                        .set(
                            "image",
                            JVal::obj()
                                .set("mime_type", JVal::Str("image/png".into()))
                                .set("data", JVal::Str(image.clone()))
                                .build(),
                        )
                        .build(),
                )
                .build(),
        ),
    );
    recorder.write_json(
        "03-devin-request.json",
        LogValue::Tree(
            JVal::obj()
                .set(
                    "metadata",
                    JVal::obj()
                        .set("api_key", JVal::Str("secret".into()))
                        .set("f", JVal::Str("fingerprint".into()))
                        .build(),
                )
                .build(),
        ),
    );
    recorder.append_jsonl(
        "04-devin-response.jsonl",
        "message",
        LogValue::Tree(JVal::obj().set("delta_text", JVal::Str("a".into())).build()),
    );
    recorder.append_jsonl(
        "04-devin-response.jsonl",
        "message",
        LogValue::Tree(JVal::obj().set("delta_text", JVal::Str("b".into())).build()),
    );
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "model".into(),
        provider: "devin".into(),
        stream: true,
        ..Default::default()
    });

    let dir = recorder.directory_path();
    let http_log = std::fs::read_to_string(dir.join("01-http-request.json")).unwrap();
    assert!(
        !http_log.contains("secret") && !http_log.contains(&image),
        "{http_log}"
    );
    assert!(http_log.contains("\"f\": \"client-field\""), "{http_log}");
    assert!(
        http_log.contains("\"file\": \"attachments/image-001.png\""),
        "{http_log}"
    );
    let devin_log = std::fs::read_to_string(dir.join("03-devin-request.json")).unwrap();
    assert!(
        !devin_log.contains("secret") && !devin_log.contains("fingerprint"),
        "{devin_log}"
    );
    let jsonl = std::fs::read_to_string(dir.join("04-devin-response.jsonl")).unwrap();
    let lines: Vec<&str> = jsonl.trim().lines().collect();
    assert!(lines.len() == 2 && lines[0].contains("\"seq\":1") && lines[1].contains("\"seq\":2"));
    assert_eq!(
        std::fs::read(dir.join("attachments/image-001.png")).unwrap(),
        b"png-data"
    );
    let meta = std::fs::read_to_string(dir.join("meta.json")).unwrap();
    assert!(meta.contains("\"provider\": \"devin\"") && meta.contains("\"result\": \"completed\""));
    manager.close();
}

/// `TestManagerAllocatesCollisionSuffix` — same-second concurrent requests
/// must not share a directory.
#[test]
fn ManagerAllocatesCollisionSuffix() {
    let root = support::work_dir("collision").join("logs");
    let manager = Manager::with_clock(&root, &RetentionPolicy::default(), fixed_clock());
    let first = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        ..Default::default()
    });
    let second = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        ..Default::default()
    });
    assert!(first.is_active() && second.is_active());
    assert_ne!(first.directory_path(), second.directory_path());
    assert!(second.dir_name().ends_with("-02"), "{}", second.dir_name());
    manager.close();
}

/// `TestWriteErrorKeepsFirstCause` — the stage closest to the fault must
/// not be overwritten by a generic outer error.
#[test]
fn WriteErrorKeepsFirstCause() {
    let root = support::work_dir("firsterr").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta::default());
    recorder.write_error(
        "devin_connect",
        &std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
    );
    recorder.write_error(
        "provider_stream",
        &std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
    );
    recorder.complete(Completion {
        status_code: 500,
        result: "failed".into(),
        ..Default::default()
    });
    let log = std::fs::read_to_string(recorder.directory_path().join("error.json")).unwrap();
    assert!(
        log.contains("devin_connect") && !log.contains("provider_stream"),
        "{log}"
    );
    manager.close();
}

/// `TestSameSecondSuffixBeyondPattern` — the 100th+ same-second request
/// gets a three-digit suffix (`-105`) which the read side must still accept.
#[test]
fn SameSecondSuffixBeyondPattern() {
    let root = support::work_dir("suffix").join("logs");
    let manager = Manager::with_clock(&root, &RetentionPolicy::default(), fixed_clock());
    let mut recorders = Vec::new();
    for i in 0..105 {
        let r = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/x".into(),
            ..Default::default()
        });
        assert!(r.is_active(), "request {i}: start returned disabled");
        recorders.push(r);
    }
    let dir = recorders[104].dir_name();
    assert!(dir.ends_with("-105"), "{dir}");
    for r in &recorders {
        r.complete(Completion {
            status_code: 200,
            result: "completed".into(),
            ..Default::default()
        });
    }
    manager.detail(&dir).unwrap();
    manager.read_file(&dir, "meta.json").unwrap();
    manager.close();
}

/// `TestSanitizeEscapedAndHyphenatedKeys` — key names the prescreen cannot
/// byte-match (`JSON`-escaped spellings, hyphenated variants) still get
/// redacted via the slow path.
#[test]
fn SanitizeEscapedAndHyphenatedKeys() {
    let root = support::work_dir("escaped").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta::default());
    recorder.write_json(
        "01-http-request.json",
        LogValue::raw(br#"{"apikey":"secret","plain":1}"#.to_vec()),
    );
    // Escaped key spelling: the byte prescreen cannot decode `api\u006bey`,
    // so the "key contains escapes → slow path" fallback must catch it.
    recorder.write_json(
        "02-request-messages.json",
        LogValue::raw(br#"{"apik ey":"x"}"#.to_vec()),
    );
    recorder.write_json(
        "03-devin-request.json",
        LogValue::raw(br#"{"api-key":"secret","set-cookie":"secret","keep":"ok"}"#.to_vec()),
    );
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    for name in ["01-http-request.json", "03-devin-request.json"] {
        let log = std::fs::read_to_string(recorder.directory_path().join(name)).unwrap();
        assert!(
            !log.contains("secret"),
            "{name} contains unredacted credentials: {log}"
        );
    }
    manager.close();
}

/// `TestIOErrorsCountedOncePerKind` — worker write failures count into
/// `io_errors`, deduplicated per kind per dir.
#[test]
fn IOErrorsCountedOncePerKind() {
    let root = support::work_dir("ioerr").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/x".into(),
        ..Default::default()
    });
    // With the dir gone, every file write and JSONL open fails:
    // one "file" failure (01/03/meta) + one "jsonl" failure (04 open).
    std::fs::remove_dir_all(recorder.directory_path()).unwrap();
    recorder.write_json(
        "01-http-request.json",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(1)).build()),
    );
    recorder.write_json(
        "03-devin-request.json",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(2)).build()),
    );
    recorder.append_jsonl(
        "04-devin-response.jsonl",
        "e",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(1)).build()),
    );
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    let stats = debuglog::gojson::marshal(&manager.stats()).unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    assert_eq!(
        stats["io_errors"], 2,
        "want one file + one jsonl failure: {stats}"
    );
    manager.close();
}

/// `TestIndexWrittenOnComplete` — every completed request appends a
/// locatable summary line to the global index.
#[test]
fn IndexWrittenOnComplete() {
    let root = support::work_dir("index").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        client_ip: "127.0.0.1".into(),
        key_hash: "abcd1234".into(),
        ..Default::default()
    });
    recorder.note_upstream_latency();
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "swe-2-max".into(),
        requested_model: "swe-2".into(),
        response_model: "swe-2-max".into(),
        stream: true,
        upstream_request_id: "req-1".into(),
        ..Default::default()
    });
    let index = std::fs::read_to_string(root.join("index.jsonl")).unwrap();
    for want in [
        "\"dir\":",
        "\"api\":\"anthropic\"",
        "\"requested_model\":\"swe-2\"",
        "\"response_model\":\"swe-2-max\"",
        "\"upstream_request_id\":\"req-1\"",
        "\"key_hash\":\"abcd1234\"",
        "\"first_upstream_ms\"",
    ] {
        assert!(index.contains(want), "index.jsonl missing {want}: {index}");
    }
    manager.close();
}

/// `TestCleanerRemovesExpiredDirs` — the cleaner deletes over-age dirs,
/// skips active ones and leaves the index alone.
#[test]
fn CleanerRemovesExpiredDirs() {
    let root = support::work_dir("cleaner").join("logs");
    let manager = Manager::new(
        &root,
        &RetentionPolicy {
            days: 7,
            ..Default::default()
        },
    );
    let old = root.join("20200101-000000");
    std::fs::create_dir_all(&old).unwrap();
    std::fs::write(old.join("meta.json"), b"{}").unwrap();
    let active = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/x".into(),
        ..Default::default()
    });
    assert_eq!(manager.clean_once(), 1);
    assert!(!old.exists(), "expired dir should be deleted");
    assert!(
        active.directory_path().exists(),
        "active dir must be protected"
    );
    active.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    manager.close();
}

/// `TestDroppedCounterOnClosedQueue` — writes after `complete` drop and
/// count.
#[test]
fn DroppedCounterOnClosedQueue() {
    let root = support::work_dir("closedq").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta::default());
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    recorder.write_json(
        "late.json",
        LogValue::Tree(JVal::obj().set("x", JVal::Int(1)).build()),
    );
    assert_eq!(recorder.dropped(), 1);
    manager.close();
}

/// `TestDroppedCounterOnFullQueue` — a full queue drops via the nonblocking
/// send. `start_paused` holds the worker so the queue fills
/// deterministically (the Go test builds the channel without a worker).
#[test]
fn DroppedCounterOnFullQueue() {
    let root = support::work_dir("fullq").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start_paused(&RequestMeta::default());
    // The worker receives before it checks the pause gate (Go: `range` then
    // `pauseGate.Wait()`), so the meta task leaves the channel and the
    // worker blocks holding it — QUEUE_DEPTH slots are free. `queued == 0`
    // is the dequeue signal; waiting for it makes the fill deterministic.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while recorder.queued() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "worker never dequeued meta"
        );
        std::thread::yield_now();
    }
    for _ in 0..debuglog::QUEUE_DEPTH {
        recorder.append_jsonl("04-devin-response.jsonl", "e", LogValue::Tree(JVal::Null));
    }
    assert_eq!(recorder.dropped(), 0, "queue should hold QUEUE_DEPTH tasks");
    recorder.append_jsonl("04-devin-response.jsonl", "e", LogValue::Tree(JVal::Null));
    assert_eq!(recorder.dropped(), 1);
    recorder.set_writer_paused(false);
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    manager.close();
}

/// `TestReaderListDetailAndFiles` — index tail listing, per-request detail
/// and file reads.
#[test]
fn ReaderListDetailAndFiles() {
    let root = support::work_dir("reader").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    for model in ["m-a", "m-b"] {
        let recorder = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/v1/messages".into(),
            api: "anthropic".into(),
            ..Default::default()
        });
        recorder.write_json(
            "03-devin-request.json",
            LogValue::Tree(JVal::obj().set("model", JVal::Str(model.into())).build()),
        );
        recorder.complete(Completion {
            status_code: 200,
            result: "completed".into(),
            model: model.into(),
            ..Default::default()
        });
    }
    let result = manager.list_requests(10, &RequestFilter::default());
    assert!(
        result.entries.len() == 2
            && result.entries[0].model == "m-b"
            && result.entries[1].model == "m-a",
        "order: {:?}",
        result.entries.iter().map(|e| &e.model).collect::<Vec<_>>()
    );
    let detail = manager.detail(&result.entries[0].dir).unwrap();
    let meta = String::from_utf8(detail.meta.unwrap()).unwrap();
    assert!(meta.contains("m-b"), "meta: {meta}");
    let names: Vec<&str> = detail.files.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"meta.json") && names.contains(&"03-devin-request.json"));
    let (data, total, truncated) = manager
        .read_file(&result.entries[0].dir, "03-devin-request.json")
        .unwrap();
    assert!(!truncated && total > 0 && String::from_utf8_lossy(&data).contains("m-b"));
    manager.close();
}

/// `TestReaderRejectsTraversal` — dir and file name traversal defenses.
#[test]
fn ReaderRejectsTraversal() {
    let root = support::work_dir("traversal").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta::default());
    let dir = recorder.dir_name();
    recorder.complete(Completion {
        status_code: 200,
        ..Default::default()
    });
    assert!(manager.detail("../etc").is_err());
    for bad in ["../meta.json", "meta.json/../x", "/abs", "sub/dir/x.json"] {
        assert!(
            manager.read_file(&dir, bad).is_err(),
            "read_file should reject {bad}"
        );
    }
    manager.read_file(&dir, "meta.json").unwrap();
    manager.close();
}

/// `TestActiveRequestsSnapshot` — in-flight requests are visible and vanish
/// after `complete`.
#[test]
fn ActiveRequestsSnapshot() {
    let root = support::work_dir("active").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        ..Default::default()
    });
    let active = manager.active_requests();
    assert!(
        active.len() == 1 && active[0].meta.api == "anthropic",
        "{active:?}"
    );
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    assert!(manager.active_requests().is_empty());
    manager.close();
}

/// `TestRequestFilters` — structured filters and the `has_more` signal.
#[test]
fn RequestFilters() {
    let root = support::work_dir("filters").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    for (model, status, result, stage) in [
        ("m-a", 200, "completed", ""),
        ("m-b", 400, "failed", "http_decode"),
        ("m-a", 500, "failed", "provider_stream"),
    ] {
        let recorder = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/v1/messages".into(),
            ..Default::default()
        });
        if !stage.is_empty() {
            recorder.write_error(stage, &std::io::Error::other("boom"));
        }
        recorder.complete(Completion {
            status_code: status,
            result: result.into(),
            model: model.into(),
            ..Default::default()
        });
    }
    let filter = |f: RequestFilter| manager.list_requests(10, &f).entries;
    let got = filter(RequestFilter {
        status_class: "4xx".into(),
        ..Default::default()
    });
    assert!(got.len() == 1 && got[0].model == "m-b");
    // Status expressions: exact/negation/comparison/class/comma-OR; invalid
    // expressions match nothing.
    for (expr, want) in [
        ("400", 1),
        ("!200", 2),
        (">=400", 2),
        ("<300", 1),
        ("4xx", 1),
        ("400,500", 2),
        ("!2xx", 2),
        ("garbage", 0),
        (">=4xx", 0),
    ] {
        let got = filter(RequestFilter {
            status: expr.into(),
            ..Default::default()
        });
        assert_eq!(got.len(), want, "status={expr}");
    }
    assert_eq!(
        filter(RequestFilter {
            model: "m-a".into(),
            ..Default::default()
        })
        .len(),
        2
    );
    assert_eq!(
        filter(RequestFilter {
            error_stage: "provider_stream".into(),
            ..Default::default()
        })
        .len(),
        1
    );
    assert_eq!(
        filter(RequestFilter {
            result: "completed".into(),
            ..Default::default()
        })
        .len(),
        1
    );
    assert!(
        filter(RequestFilter {
            since: Some(zoned_after(SignedDuration::from_hours(1))),
            ..Default::default()
        })
        .is_empty()
    );
    // `until` pins the list inside a historical window: a past bound keeps
    // nothing, a future bound keeps all.
    assert!(
        filter(RequestFilter {
            until: Some(zoned_before(SignedDuration::from_hours(1))),
            ..Default::default()
        })
        .is_empty()
    );
    assert_eq!(
        filter(RequestFilter {
            until: Some(zoned_after(SignedDuration::from_hours(1))),
            ..Default::default()
        })
        .len(),
        3
    );
    // Exhausted limit signals remaining history.
    let got = manager.list_requests(1, &RequestFilter::default());
    assert!(got.entries.len() == 1 && got.has_more);
    assert!(
        !manager
            .list_requests(10, &RequestFilter::default())
            .has_more
    );
    manager.close();
}

/// `TestErrorOwnerAndSLA` — failure attribution and the SLA denominator:
/// client faults and 429s leave it; only upstream faults lose points.
#[test]
fn ErrorOwnerAndSLA() {
    for (status, result, stage, want) in [
        (200, "completed", "", ""),
        (429, "failed", "devin_connect", "business_limited"),
        (429, "failed", "rate_gate", "business_limited"),
        (200, "disconnected", "client_disconnected", "client"),
        (500, "aborted", "", "client"),
        (400, "failed", "http_decode", "client"),
        (413, "failed", "http_read", "client"),
        (500, "failed", "provider_stream", "upstream"),
        (502, "failed", "devin_transport", "upstream"),
        // 200 + in-stream error event: status 200 but result=failed → server.
        (200, "failed", "response_event", "upstream"),
        // Upstream 4xx outside request-body stages counts as server fault.
        (404, "failed", "devin_connect", "upstream"),
    ] {
        let entry = IndexEntry {
            status_code: status,
            result: result.into(),
            error_stage: stage.into(),
            ..Default::default()
        };
        assert_eq!(
            debuglog::error_owner(&entry),
            want,
            "error_owner({status}/{result}/{stage})"
        );
    }

    let agg = UsageAggregator::new();
    let started = || gotime::rfc3339_nano(&gotime::now());
    let add = |status: i64, result: &str, stage: &str| {
        agg.add(&IndexEntry {
            started_at: started(),
            duration_ms: 10,
            status_code: status,
            result: result.into(),
            error_stage: stage.into(),
            model: "m-x".into(),
            ..Default::default()
        });
    };
    add(200, "completed", "");
    add(200, "completed", "");
    add(429, "failed", "devin_connect"); // rate limit: leaves denominator
    add(200, "disconnected", ""); // client: leaves denominator
    add(400, "failed", "http_decode"); // client 4xx: leaves denominator
    add(500, "failed", "provider_stream"); // server fault: the only SLA loss
    let snap = agg.snapshot();
    let m = &snap.models[0];
    assert!(
        m.totals.client_faults == 2 && m.totals.upstream_faults == 1 && m.totals.rate_limited == 1,
        "faults: {:?}",
        m.totals
    );
    // slable = 6 - 2 - 1 = 3; SLA = (3-1)/3 ≈ 0.667.
    let want = 2.0 / 3.0;
    assert!(
        (m.sla_success_rate - want).abs() < 1e-9,
        "{}",
        m.sla_success_rate
    );
    assert!(snap.window.client_faults == 2 && snap.window.upstream_faults == 1);
    let sum_client: i64 = snap.points.iter().map(|p| p.totals.client_faults).sum();
    let sum_upstream: i64 = snap.points.iter().map(|p| p.totals.upstream_faults).sum();
    assert!(sum_client == 2 && sum_upstream == 1);
}

/// `TestRetryAttemptsInIndex` — resend counts land in the index and meta;
/// `note_retry_attempt` and 04's `retry_attempt` separator share one source.
#[test]
fn RetryAttemptsInIndex() {
    let root = support::work_dir("retries").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        ..Default::default()
    });
    let dir = recorder.dir_name();
    recorder.note_retry_attempt(2, "unauthenticated: token reloaded");
    recorder.note_retry_attempt(3, "transport: EOF");
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "m-x".into(),
        ..Default::default()
    });
    let entries = manager.list_requests(10, &RequestFilter::default()).entries;
    assert!(entries.len() == 1 && entries[0].retries == 2, "{entries:?}");
    let meta = read_json(&root.join(&dir).join("meta.json"));
    let attempts = meta["retry_attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[1]["attempt"], 3);
    assert_eq!(attempts[1]["cause"], "transport: EOF");
    manager.close();
}

/// `TestSetEnabledHotToggle` — the runtime switch takes effect immediately.
#[test]
fn SetEnabledHotToggle() {
    let root = support::work_dir("toggle").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    manager.set_enabled(false);
    assert!(!manager.start(&RequestMeta::default()).is_active());
    manager.set_enabled(true);
    let r = manager.start(&RequestMeta::default());
    assert!(r.is_active());
    r.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        ..Default::default()
    });
    manager.close();
}

/// `TestAbortActiveRequest` — abort cancels the attached hook and records
/// the result as `aborted`.
#[test]
fn AbortActiveRequest() {
    let root = support::work_dir("abort").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        ..Default::default()
    });
    let dir = recorder.dir_name();
    assert!(
        !manager.abort(&dir),
        "abort should fail before ctx attaches"
    );
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&cancelled);
    recorder.set_abort(Arc::new(move || {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }));
    let active = manager.active_requests();
    assert!(
        active.len() == 1 && active[0].abortable && active[0].state == "waiting_upstream",
        "{active:?}"
    );
    assert!(manager.abort(&dir));
    assert!(cancelled.load(std::sync::atomic::Ordering::Relaxed));
    recorder.complete(Completion {
        status_code: 200,
        result: "disconnected".into(),
        ..Default::default()
    });
    let result = manager.list_requests(
        10,
        &RequestFilter {
            result: "aborted".into(),
            ..Default::default()
        },
    );
    assert_eq!(result.entries.len(), 1);
    assert!(!manager.abort(&dir), "abort after complete should fail");
    manager.close();
}

/// `TestLayeredRetention` — payload stripping and error-dir exemption.
#[test]
fn LayeredRetention() {
    let root = support::work_dir("layered").join("logs");
    let manager = Manager::new(
        &root,
        &RetentionPolicy {
            payload_hours: 1,
            keep_error_dirs: 1,
            ..Default::default()
        },
    );
    // A 2-hour-old dir (name-embedded age): payloads strip, evidence stays.
    // The retry shard 03-devin-request.attempt2.json is payload too.
    let old = root.join("20200101-000000");
    std::fs::create_dir_all(old.join("attachments")).unwrap();
    let payloads = [
        "03-devin-request.json",
        "03-devin-request.attempt2.json",
        "04-devin-response.jsonl",
        "06-http-response.jsonl",
    ];
    for name in payloads.iter().chain(["meta.json", "error.json"].iter()) {
        std::fs::write(old.join(name), b"x").unwrap();
    }
    std::fs::write(old.join("attachments/a.bin"), b"x").unwrap();
    manager.clean_once();
    for gone in payloads.iter().chain(["attachments"].iter()) {
        assert!(
            !old.join(gone).exists(),
            "payload {gone} should be stripped"
        );
    }
    for keep in ["meta.json", "error.json"] {
        assert!(old.join(keep).exists(), "evidence {keep} should remain");
    }
    manager.close();
}

/// `TestUsageMinBucketWraparound` — ring wrap: an 8-day-old entry colliding
/// with the current bucket's slot must not reset counted data.
#[test]
fn UsageMinBucketWraparound() {
    let agg = UsageAggregator::new();
    let now_secs = Timestamp::now().as_second();
    let now_slot = now_secs - now_secs.rem_euclid(600);
    let now = Timestamp::from_second(now_slot)
        .unwrap()
        .to_zoned(TimeZone::system());
    agg.add(&IndexEntry {
        started_at: gotime::rfc3339_nano(&now),
        duration_ms: 5,
        result: "completed".into(),
        input_tokens: 7,
        ..Default::default()
    });
    // Exactly USAGE_MIN_BUCKETS (8 days) earlier maps to the same slot.
    let old = now
        .timestamp()
        .checked_sub(SignedDuration::from_secs(
            i64::try_from(debuglog::USAGE_MIN_BUCKETS).unwrap_or(i64::MAX) * 600,
        ))
        .unwrap()
        .to_zoned(TimeZone::system());
    agg.add(&IndexEntry {
        started_at: gotime::rfc3339_nano(&old),
        duration_ms: 9,
        result: "completed".into(),
        input_tokens: 3,
        ..Default::default()
    });
    let snap = agg.snapshot();
    assert!(snap.window.requests == 2 && snap.window.input_tokens == 10);
    let current = snap.points.last().unwrap();
    assert!(
        current.totals.requests == 1 && current.totals.input_tokens == 7,
        "current bucket: {:?}",
        current.totals
    );
}

/// `TestUsageReplayCountsOnce` — replay and live counting never overlap:
/// whether a request completes before or after the snapshot, its index line
/// counts exactly once.
#[test]
fn UsageReplayCountsOnce() {
    for i in 0..50 {
        let root = support::work_dir("once").join("logs");
        let manager = Manager::new(&root, &RetentionPolicy::default());
        let recorder = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/x".into(),
            ..Default::default()
        });
        recorder.complete(Completion {
            status_code: 200,
            result: "completed".into(),
            model: "m".into(),
            ..Default::default()
        });
        let snap = manager.usage_stats();
        manager.close();
        assert!(
            snap.entries == 1 && snap.window.requests == 1,
            "iter {i}: entries={} requests={}",
            snap.entries,
            snap.window.requests
        );
    }
}

/// `TestIndexSnapshottedGate` — the snapshot gate's boundary semantics:
/// pre-gate completions only write the index line (replay counts them);
/// post-gate ones count via the live path.
#[test]
fn IndexSnapshottedGate() {
    let root = support::work_dir("gate").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    manager.wait_replay();

    // Artificially return to pre-snapshot state: the completed request only
    // writes its index line without counting — replay would have counted it
    // (the test's snapshot already ran, so the total stays 0).
    manager.debug_set_index_snapshotted(false);
    start_completed(
        &manager,
        &RequestMeta {
            method: "POST".into(),
            path: "/x".into(),
            ..Default::default()
        },
        Completion {
            status_code: 200,
            result: "completed".into(),
            ..Default::default()
        },
    );
    assert_eq!(
        manager.usage_stats().entries,
        0,
        "pre-snapshot entry counted"
    );

    manager.debug_set_index_snapshotted(true);
    start_completed(
        &manager,
        &RequestMeta {
            method: "POST".into(),
            path: "/x".into(),
            ..Default::default()
        },
        Completion {
            status_code: 200,
            result: "completed".into(),
            ..Default::default()
        },
    );
    assert_eq!(manager.usage_stats().entries, 1, "post-snapshot entry lost");
    manager.close();
}

/// `TestRetentionAgesByDirName` — aging keys on the dir name's embedded
/// timestamp; a fresh mtime (e.g. after payload strip) does not defer
/// eviction.
#[test]
fn RetentionAgesByDirName() {
    let root = support::work_dir("aging").join("logs");
    let manager = Manager::new(
        &root,
        &RetentionPolicy {
            days: 7,
            payload_hours: 1,
            ..Default::default()
        },
    );
    let old = root.join("20200101-000000");
    std::fs::create_dir_all(&old).unwrap();
    for name in ["03-devin-request.json", "meta.json"] {
        std::fs::write(old.join(name), b"x").unwrap();
    }
    // mtime is now; the name says 2020 — the name wins.
    assert_eq!(manager.clean_once(), 1, "name-embedded age must win");
    assert!(!old.exists());
    manager.close();
}

/// `TestUsageAggregatorCounts` — one completed request counts into every
/// dimension.
#[test]
fn UsageAggregatorCounts() {
    let root = support::work_dir("usage").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());

    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        key_hash: "k1".into(),
        ..Default::default()
    });
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "swe-2-max".into(),
        requested_model: "swe-2".into(),
        usage: Usage {
            input: 100,
            output: 50,
            cache_read: 40,
            cache_write: 30,
            reasoning: Some(7),
            total_tokens: 187,
        },
        ..Default::default()
    });
    let failed = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        key_hash: "k1".into(),
        ..Default::default()
    });
    failed.write_error("provider_stream", &std::io::Error::other("boom"));
    failed.complete(Completion {
        status_code: 500,
        result: "failed".into(),
        model: "swe-2-max".into(),
        usage: Usage {
            input: 10,
            total_tokens: 10,
            ..Default::default()
        },
        ..Default::default()
    });

    let snap = manager.usage_stats();
    assert!(
        snap.today.requests == 2 && snap.today.errors == 1,
        "{:?}",
        snap.today
    );
    assert!(
        snap.today.input_tokens == 110
            && snap.today.output_tokens == 50
            && snap.today.cache_read_tokens == 40
            && snap.today.cache_write_tokens == 30
            && snap.today.reasoning_tokens == 7
            && snap.today.total_tokens == 197,
        "{:?}",
        snap.today
    );
    assert!(
        snap.models.len() == 1
            && snap.models[0].name == "swe-2-max"
            && snap.models[0].totals.requests == 2
            && snap.models[0].totals.errors == 1,
        "{:?}",
        snap.models
    );
    // model_days feeds the panel's per-day model filter: both requests land
    // on today.
    let today_key = gotime::day_key(&gotime::local_now());
    let md = &snap.model_days["swe-2-max"][&today_key];
    assert!(
        md.requests == 2 && md.input_tokens == 110,
        "{:?}",
        snap.model_days
    );
    assert!(snap.keys.len() == 1 && snap.keys[0].totals.requests == 2);
    assert_eq!(snap.error_stages["provider_stream"], 1);
    assert_eq!(snap.points.len(), debuglog::USAGE_MIN_BUCKETS);
    assert_eq!(snap.duration.samples, 2);
    manager.close();
}

/// `TestUsageReplayOnRestart` — rebuilding the manager (process restart)
/// keeps historical stats.
#[test]
fn UsageReplayOnRestart() {
    let root = support::work_dir("replay2").join("logs");
    let first = Manager::new(&root, &RetentionPolicy::default());
    let recorder = first.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        api: "openai-chat".into(),
        ..Default::default()
    });
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "glm-5-2".into(),
        usage: Usage {
            input: 1000,
            output: 200,
            total_tokens: 1200,
            ..Default::default()
        },
        ..Default::default()
    });
    first.close();

    let second = Manager::new(&root, &RetentionPolicy::default());
    let snap = second.usage_stats();
    assert!(
        snap.window.requests == 1
            && snap.window.input_tokens == 1000
            && snap.window.total_tokens == 1200,
        "{:?}",
        snap.window
    );
    assert!(snap.models.len() == 1 && snap.models[0].name == "glm-5-2");
    // Post-replay requests keep accumulating without double counting.
    let recorder2 = second.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        ..Default::default()
    });
    recorder2.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "glm-5-2".into(),
        usage: Usage {
            input: 5,
            output: 5,
            total_tokens: 10,
            ..Default::default()
        },
        ..Default::default()
    });
    let snap = second.usage_stats();
    assert!(snap.window.requests == 2 && snap.window.total_tokens == 1210);
    second.close();
}

/// `TestUsagePercentiles` — reservoir p50/p95/p99 and ring overwrite.
#[test]
fn UsagePercentiles() {
    let agg = UsageAggregator::new();
    for i in 1..=100i64 {
        agg.add(&IndexEntry {
            started_at: gotime::rfc3339_nano(&gotime::now()),
            duration_ms: i,
            result: "completed".into(),
            ..Default::default()
        });
    }
    let stats = agg.snapshot().duration;
    assert!(
        stats.p50 == 50 && stats.p95 == 95 && stats.p99 == 99 && stats.max == 100,
        "{stats:?}"
    );
    // Past capacity only the most recent samples remain.
    for _ in 0..debuglog::USAGE_SAMPLE_CAPACITY {
        agg.add(&IndexEntry {
            started_at: gotime::rfc3339_nano(&gotime::now()),
            duration_ms: 1,
            result: "completed".into(),
            ..Default::default()
        });
    }
    let stats = agg.snapshot().duration;
    assert_eq!(
        stats.samples,
        i64::try_from(debuglog::USAGE_SAMPLE_CAPACITY).unwrap_or(i64::MAX)
    );
}

/// `TestUsageRateLimitSampling` — upstream 429s count separately and sample
/// the send rate over the prior 60s; replay rebuilds the same events.
#[test]
fn UsageRateLimitSampling() {
    let base = Timestamp::now();
    let entry = |off_secs: i64, status: i64, dur_ms: i64| IndexEntry {
        started_at: gotime::rfc3339_nano(
            &base
                .checked_add(SignedDuration::from_secs(off_secs))
                .unwrap()
                .to_zoned(TimeZone::system()),
        ),
        duration_ms: dur_ms,
        status_code: status,
        result: "completed".into(),
        model: "m-a".into(),
        ..Default::default()
    };
    let mut entries = vec![
        entry(-120, 200, 100), // outside the 429's 60s window
        entry(-30, 200, 100),
        entry(-20, 200, 100),
        entry(-10, 200, 100),
        // end = -5s+2s = -3s; window (-63s,-3s] holds -30/-20/-10/-5 → 4 starts.
        entry(-5, 429, 2000),
    ];
    entries[4].error_stage = "rate_gate".into();

    let agg = UsageAggregator::new();
    for e in &entries {
        agg.add(e);
    }
    let snap = agg.snapshot();
    assert_eq!(snap.window.rate_limited, 1);
    assert_eq!(snap.rate_limit_events.len(), 1);
    let ev = &snap.rate_limit_events[0];
    assert!(
        ev.model == "m-a"
            && ev.rpm == 4
            && ev.at == base.as_second() - 3
            && ev.stage == "rate_gate",
        "{ev:?}"
    );
    assert_eq!(snap.models[0].totals.rate_limited, 1);

    // The same index content replayed from a file rebuilds identical
    // aggregates and samples after a restart.
    let work = support::work_dir("rl-replay");
    let path = work.join("index.jsonl");
    let mut buf = Vec::new();
    for e in &entries {
        buf.extend_from_slice(&e.to_go_json());
        buf.push(b'\n');
    }
    std::fs::write(&path, &buf).unwrap();
    let replayed = UsageAggregator::new();
    replayed.replay_lines(&std::fs::read(&path).unwrap());
    let rsnap = replayed.snapshot();
    assert!(
        rsnap.window.rate_limited == 1
            && rsnap.rate_limit_events.len() == 1
            && rsnap.rate_limit_events[0].rpm == 4,
        "replayed: {:?}",
        rsnap.rate_limit_events
    );
}

/// `TestIndexFileCapTruncates` — past the cap, `index.jsonl` rewrites keeping
/// the tail half; surviving lines are all complete entries.
#[test]
fn IndexFileCapTruncates() {
    let root = support::work_dir("indexcap").join("logs");
    let manager = Manager::new(&root, &RetentionPolicy::default());
    manager.set_index_file_cap(4 << 10); // 4KB: a few dozen summaries trigger it
    for i in 0..60 {
        let recorder = manager.start(&RequestMeta {
            method: "POST".into(),
            path: "/v1/responses".into(),
            ..Default::default()
        });
        assert!(recorder.is_active(), "request {i}: start returned disabled");
        recorder.complete(Completion {
            status_code: 200,
            result: "completed".into(),
            ..Default::default()
        });
    }
    let size = std::fs::metadata(root.join("index.jsonl")).unwrap().len();
    assert!(size <= 4 << 10, "index.jsonl size {size} exceeds cap");
    let lines = read_index(&root);
    assert!(
        !lines.is_empty() && lines.len() < 60,
        "{} lines",
        lines.len()
    );
    manager.close();
}
