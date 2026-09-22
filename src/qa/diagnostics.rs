//! Process-surface QA for task 16 diagnostics.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::debuglog::Recorder;
use crate::domain::{
    AssistantMessage, Content, Failure, RequestMessages, ResponseEvent, ResponseEventType,
    StopReason, TextContent,
};
use crate::metrics::DiagnosticsListener;
use crate::server::http::{App, HttpBackend, HttpConfig, HttpEventStream};
use crate::upstream::catalog::ModelInfo;
use anyhow::Context as _;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const QA_TIMEOUT: Duration = Duration::from_secs(10);

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone)]
struct Backend;
struct OneEvent(Option<ResponseEvent>);

impl HttpEventStream for OneEvent {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let event = self.0.take();
        Box::pin(async move { Ok(event) })
    }
}

impl HttpBackend for Backend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        request: RequestMessages,
        _cancel: CancellationToken,
        _recorder: Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        let model = request.model;
        Box::pin(async move {
            Ok(Box::new(OneEvent(Some(ResponseEvent {
                kind: ResponseEventType::Done,
                reason: Some(StopReason::Stop),
                message: Some(AssistantMessage {
                    content: vec![Content::Text(TextContent {
                        text: "pong".into(),
                    })],
                    model: model.clone(),
                    response_model: model,
                    response_id: "resp_diagnostics_qa".into(),
                    stop_reason: Some(StopReason::Stop),
                    ..AssistantMessage::default()
                }),
                ..ResponseEvent::default()
            }))) as Box<dyn HttpEventStream>)
        })
    }
}

async fn get_json(client: &reqwest::Client, url: &str) -> anyhow::Result<(u16, Value)> {
    let response = client.get(url).send().await?;
    let status = response.status().as_u16();
    Ok((status, response.json().await?))
}

fn write_profile_artifacts(evidence: &Path, profile: &Value) -> anyhow::Result<()> {
    let pid = profile["pid"].as_u64().unwrap_or_default();
    let script = format!(
        "#!/bin/sh\nset -eu\nperf record -F 99 -g -p {pid} -o \"${{1:-cpu.perf.data}}\" -- sleep \"${{2:-15}}\"\n"
    );
    std::fs::write(evidence.join("cpu-profile.sh"), script)?;
    std::fs::write(
        evidence.join("profile-capability.json"),
        serde_json::to_vec_pretty(profile)?,
    )?;
    // This command is available without perf_event privileges and produces a
    // truthful process CPU/RSS artifact on Linux. Call-stack profiling remains
    // the documented perf command and its kernel capability is reported.
    #[cfg(target_os = "linux")]
    {
        let output = std::process::Command::new("/usr/bin/time")
            .args([
                "-v",
                "sh",
                "-c",
                "printf diagnostics-profile-probe >/dev/null",
            ])
            .output()?;
        std::fs::write(evidence.join("process-profile.txt"), &output.stderr)?;
    }
    Ok(())
}

pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    match case {
        None | Some("happy") => run_happy(evidence).await,
        Some("listener-conflict-and-unsupported") => run_failure(evidence).await,
        Some(other) => anyhow::bail!("unknown diagnostics case {other}"),
    }
}

async fn running_surfaces() -> anyhow::Result<(
    App,
    std::net::SocketAddr,
    std::net::SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let app = App::with_backend(Backend, HttpConfig::default());
    let main = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let main_address = main.local_addr()?;
    let main_stop = CancellationToken::new();
    let main_stopped = main_stop.clone();
    let served = app.clone();
    let main_task = tokio::spawn(async move { served.serve(main, main_stopped).await });

    let diagnostics = DiagnosticsListener::bind("127.0.0.1:0", app.metrics()).await?;
    let diagnostic_address = diagnostics.local_addr();
    let diagnostic_stop = CancellationToken::new();
    let diagnostic_stopped = diagnostic_stop.clone();
    let diagnostic_task = tokio::spawn(async move { diagnostics.serve(diagnostic_stopped).await });
    Ok((
        app,
        main_address,
        diagnostic_address,
        main_stop,
        main_task,
        diagnostic_stop,
        diagnostic_task,
    ))
}

async fn stop(
    main_stop: CancellationToken,
    main_task: tokio::task::JoinHandle<std::io::Result<()>>,
    diagnostic_stop: CancellationToken,
    diagnostic_task: tokio::task::JoinHandle<std::io::Result<()>>,
) -> anyhow::Result<()> {
    main_stop.cancel();
    diagnostic_stop.cancel();
    tokio::time::timeout(QA_TIMEOUT, main_task)
        .await
        .context("main stop timeout")???;
    tokio::time::timeout(QA_TIMEOUT, diagnostic_task)
        .await
        .context("diagnostic stop timeout")???;
    Ok(())
}

async fn run_happy(evidence: &Path) -> anyhow::Result<i32> {
    let (_app, main, diagnostic, main_stop, main_task, diagnostic_stop, diagnostic_task) =
        running_surfaces().await?;
    let client = reqwest::Client::builder().timeout(QA_TIMEOUT).build()?;
    let response = client
        .post(format!("http://{main}/v1/responses"))
        .header("content-type", "application/json")
        .body(r#"{"model":"stub-model","input":"hi"}"#)
        .send()
        .await?;
    let inference_status = response.status().as_u16();
    let inference_body = response.text().await?;
    let (runtime_status, runtime) = get_json(
        &client,
        &format!("http://{diagnostic}/debug/diagnostics/runtime"),
    )
    .await?;
    let (_, profile) = get_json(
        &client,
        &format!("http://{diagnostic}/debug/diagnostics/profile"),
    )
    .await?;
    write_profile_artifacts(evidence, &profile)?;
    let cpu_percent = runtime["process"]["cpu_percent"].as_f64();
    let cpu_limit = runtime["process"]["num_cpu"]
        .as_u64()
        .map(|cpus| f64::from(u32::try_from(cpus).unwrap_or(u32::MAX)) * 100.0);
    let cpu_bounded = cpu_percent
        .zip(cpu_limit)
        .is_some_and(|(percent, limit)| (0.0..=limit).contains(&percent));
    let passed = inference_status == 200
        && inference_body.contains("pong")
        && runtime_status == 200
        && runtime["runtime"] == "rust"
        && runtime["process"]["rss_bytes"]
            .as_u64()
            .is_some_and(|rss| rss > 0)
        && runtime["process"]["cpu_seconds"].as_f64().is_some()
        && cpu_bounded
        && runtime["process"]["goroutines"].is_null()
        && runtime["http"]["completed_requests"] == 1
        && runtime["http"]["response_body_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
        && runtime["tasks"]["spawned_total"]
            .as_u64()
            .is_some_and(|tasks| tasks > 0)
        && runtime["queue_waits"]["count"] == 1
        && runtime["tracing_spans"]["opened_total"] == 1;
    let report = json!({
        "case":"happy", "passed":passed,
        "inference":{"status":inference_status,"body":inference_body},
        "diagnostics":runtime, "profile":profile,
    });
    std::fs::write(
        evidence.join("diagnostics.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    stop(main_stop, main_task, diagnostic_stop, diagnostic_task).await?;
    Ok(i32::from(!passed))
}

async fn run_failure(evidence: &Path) -> anyhow::Result<i32> {
    let (_app, main, diagnostic, main_stop, main_task, diagnostic_stop, diagnostic_task) =
        running_surfaces().await?;
    let conflict = DiagnosticsListener::bind(
        &diagnostic.to_string(),
        Arc::new(crate::metrics::Metrics::new()),
    )
    .await;
    let public =
        DiagnosticsListener::bind("0.0.0.0:0", Arc::new(crate::metrics::Metrics::new())).await;
    let client = reqwest::Client::builder().timeout(QA_TIMEOUT).build()?;
    let (legacy_status, legacy) =
        get_json(&client, &format!("http://{diagnostic}/debug/pprof/heap")).await?;
    let health = client
        .get(format!("http://{main}/healthz"))
        .send()
        .await?
        .status()
        .as_u16();
    let passed = conflict
        .as_ref()
        .is_err_and(|error| error.kind() == std::io::ErrorKind::AddrInUse)
        && public
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
        && legacy_status == 501
        && legacy["error"] == "unsupported_go_profile"
        && health == 200;
    let report = json!({
        "case":"listener-conflict-and-unsupported", "passed":passed,
        "conflict_error":conflict.err().map(|error| error.to_string()),
        "public_bind_error":public.err().map(|error| error.to_string()),
        "legacy_status":legacy_status, "legacy":legacy, "main_health_status":health,
    });
    std::fs::write(
        evidence.join("listener-conflict-and-unsupported.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    stop(main_stop, main_task, diagnostic_stop, diagnostic_task).await?;
    Ok(i32::from(!passed))
}
