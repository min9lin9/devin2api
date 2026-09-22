use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use devin2api::debuglog::Recorder;
use devin2api::domain::{Failure, RequestMessages};
use devin2api::metrics::{DiagnosticsListener, Metrics, RejectEvent, RejectReason};
use devin2api::server::http::{App, HttpBackend, HttpConfig, HttpEventStream};
use devin2api::upstream::catalog::ModelInfo;
use http::Request;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct NeverBackend;

impl HttpBackend for NeverBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ModelInfo>, Failure>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: RequestMessages,
        _cancel: CancellationToken,
        _recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn HttpEventStream>, Failure>> + Send + '_>> {
        Box::pin(async { unreachable!("malformed requests never reach the backend") })
    }
}

fn object<'a>(value: &'a Value, key: &str) -> &'a serde_json::Map<String, Value> {
    value.get(key).and_then(Value::as_object).unwrap()
}

fn metrics_at(now: i64) -> (Metrics, Arc<AtomicI64>) {
    let time = Arc::new(AtomicI64::new(now));
    let source = time.clone();
    let metrics = Metrics::with_clock(Arc::new(move || source.load(Ordering::Relaxed)));
    (metrics, time)
}

#[test]
fn lifecycle_rates_reject_ring_and_process_values_are_truthful() {
    let (metrics, _time) = metrics_at(1_800_000_005);
    let request = metrics.begin();
    assert_eq!(metrics.active(), 1);
    request.observe(true, 100);
    request.finish(200, 500, "completed");

    let failed = metrics.begin();
    failed.observe(false, 40);
    failed.finish(503, 20, "failed");
    metrics.reject(
        RejectReason::Draining,
        RejectEvent {
            status: 503,
            path: "/v1/messages".into(),
            ..RejectEvent::default()
        },
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot["runtime"], "rust");
    assert_eq!(snapshot["active_requests"], 0);
    assert_eq!(snapshot["completed_requests"], 2);
    assert_eq!(snapshot["rejected_requests"], 1);
    assert_eq!(snapshot["ok_responses"], 1);
    assert_eq!(snapshot["server_error_responses"], 1);
    assert_eq!(snapshot["streaming_requests"], 1);
    assert_eq!(snapshot["non_streaming_requests"], 1);
    assert_eq!(snapshot["request_body_bytes"], 140);
    assert_eq!(snapshot["response_body_bytes"], 520);

    let rates = object(&snapshot, "rates");
    assert_eq!(rates["rpm_current"], 3);
    assert_eq!(rates["rpm_peak"], 3);
    assert!(rates["qps_current"].as_f64().unwrap() > 0.0);
    let trend = snapshot["trend_minutes"].as_array().unwrap();
    assert_eq!(trend.len(), 360);
    assert_eq!(trend.last().unwrap()["requests"], 3);
    assert_eq!(trend.last().unwrap()["errors"], 2);

    let rejects = object(&snapshot, "rejects");
    assert_eq!(rejects["by_reason"]["draining"], 1);
    assert_eq!(rejects["recent"][0]["path"], "/v1/messages");
    assert_eq!(rejects["labels"].as_array().unwrap().len(), 6);

    let process = object(&snapshot, "process");
    assert!(process["rss_bytes"].as_u64().unwrap() > 0);
    assert!(process["cpu_seconds"].as_f64().unwrap() >= 0.0);
    let cpu_percent = process["cpu_percent"].as_f64().unwrap();
    // CPU counts are tiny; float math mirrors the Go diagnostic.
    #[allow(clippy::cast_precision_loss)]
    let physical_limit = process["num_cpu"].as_u64().unwrap() as f64 * 100.0;
    assert!(
        (0.0..=physical_limit).contains(&cpu_percent),
        "cpu_percent {cpu_percent} exceeds physical limit {physical_limit}"
    );
    for go_only in [
        "goroutines",
        "heap_alloc_bytes",
        "heap_sys_bytes",
        "stack_inuse",
        "alloc_total",
        "num_gc",
        "gc_pause_total_ms",
        "gc_cpu_fraction",
    ] {
        assert!(
            process[go_only].is_null(),
            "{go_only} must be null, not a bogus zero"
        );
    }
    assert!(
        snapshot["capabilities"]["process_memory"]["supported"]
            .as_bool()
            .unwrap()
    );
}

#[test]
fn trend_bucket_rollover_uses_the_injected_clock() {
    let (metrics, time) = metrics_at(1_800_000_009);
    metrics.begin().finish(200, 0, "completed");
    time.store(1_800_000_010, Ordering::Relaxed);
    metrics.begin().finish(500, 0, "failed");

    let snapshot = metrics.snapshot();
    let trend = snapshot["trend_minutes"].as_array().unwrap();
    assert_eq!(trend[trend.len() - 2]["requests"], 1);
    assert_eq!(trend[trend.len() - 2]["errors"], 0);
    assert_eq!(trend.last().unwrap()["requests"], 1);
    assert_eq!(trend.last().unwrap()["errors"], 1);
}

#[tokio::test]
async fn http_surface_records_real_reject_and_completed_response_bytes() {
    let app = App::with_backend(
        NeverBackend,
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    let unauthorized = app
        .router()
        .oneshot(
            Request::post("/v1/responses")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    let _ = to_bytes(unauthorized.into_body(), 1024 * 1024)
        .await
        .unwrap();

    let malformed = app
        .router()
        .oneshot(
            Request::post("/v1/responses")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from("{not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    let body = to_bytes(malformed.into_body(), 1024 * 1024).await.unwrap();
    assert!(!body.is_empty());

    let snapshot = app.metrics().snapshot();
    assert_eq!(snapshot["rejected_requests"], 1);
    assert_eq!(snapshot["completed_requests"], 1);
    assert_eq!(snapshot["client_error_responses"], 1);
    assert!(snapshot["response_body_bytes"].as_u64().unwrap() > 0);
    assert_eq!(snapshot["rejects"]["by_reason"]["missing_api_key"], 1);
}

#[test]
fn diagnostics_expose_instrumented_tasks_waits_and_spans() {
    let metrics = Metrics::new();
    let task = metrics.begin_task();
    let span = metrics.begin_span();
    metrics.observe_queue_wait(Duration::from_millis(7));
    let snapshot = metrics.diagnostics_snapshot();
    assert_eq!(snapshot["runtime"], "rust");
    assert_eq!(snapshot["tasks"]["active"], 1);
    assert_eq!(snapshot["tasks"]["spawned_total"], 1);
    assert_eq!(snapshot["queue_waits"]["count"], 1);
    assert!(snapshot["queue_waits"]["total_ms"].as_f64().unwrap() >= 7.0);
    assert_eq!(snapshot["tracing_spans"]["active"], 1);
    assert_eq!(snapshot["tracing_spans"]["opened_total"], 1);
    drop(task);
    drop(span);
    let after = metrics.diagnostics_snapshot();
    assert_eq!(after["tasks"]["active"], 0);
    assert_eq!(after["tracing_spans"]["active"], 0);
}

async fn get_json(address: std::net::SocketAddr, path: &str) -> (reqwest::StatusCode, Value) {
    let response = reqwest::get(format!("http://{address}{path}"))
        .await
        .unwrap();
    let status = response.status();
    let value = response.json().await.unwrap();
    (status, value)
}

#[tokio::test]
async fn loopback_diagnostic_listener_has_index_snapshots_and_legacy_501() {
    let metrics = Arc::new(Metrics::new());
    let listener = DiagnosticsListener::bind("127.0.0.1:0", metrics)
        .await
        .unwrap();
    let address = listener.local_addr();
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone();
    let server = tokio::spawn(async move { listener.serve(stopped).await });

    let (status, index) = get_json(address, "/").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(index["runtime"], "rust");
    assert_eq!(index["endpoints"]["runtime"], "/debug/diagnostics/runtime");

    let (status, runtime) = get_json(address, "/debug/diagnostics/runtime").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(runtime["runtime"], "rust");

    let (status, legacy) = get_json(address, "/debug/pprof/profile?seconds=1").await;
    assert_eq!(status, reqwest::StatusCode::NOT_IMPLEMENTED);
    assert_eq!(legacy["error"], "unsupported_go_profile");
    assert_eq!(legacy["replacement"], "/debug/diagnostics/profile");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn listener_rejects_non_loopback_and_conflicts_without_affecting_existing_listener() {
    let metrics = Arc::new(Metrics::new());
    let error = DiagnosticsListener::bind("0.0.0.0:0", metrics.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("loopback"));

    let first = DiagnosticsListener::bind("127.0.0.1:0", metrics.clone())
        .await
        .unwrap();
    let address = first.local_addr();
    let conflict = DiagnosticsListener::bind(&address.to_string(), metrics)
        .await
        .unwrap_err();
    assert_eq!(conflict.kind(), std::io::ErrorKind::AddrInUse);

    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone();
    let server = tokio::spawn(async move { first.serve(stopped).await });
    let (status, _) = get_json(address, "/debug/diagnostics/runtime").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    shutdown.cancel();
    server.await.unwrap().unwrap();
}
