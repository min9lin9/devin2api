//! Auxiliary binary `loadtest` — streaming load generator for the
//! service's `/v1/*` endpoints. Port of `G/cmd/`loadtest`/main.go`: fixed
//! concurrency, per-request TTFB (first response body byte) and total
//! duration, quantile + throughput report.
//!
//! Two measurements are added without changing the old TTFB meaning:
//! `wire_first_byte_ms` is the time to the first response *header* byte
//! (Go's TTFB conflates headers with the first body read; separating them
//! shows how long the upstream spent before committing a status), and
//! `semantic_first_content_ms` is the time to the first `SSE` `data:` frame
//! carrying non-empty `choices[].delta.content` — the first token the
//! client can render, which a raw byte timestamp cannot distinguish from
//! role/metadata frames.
//!
//! Usage: `loadtest -url <http://localhost:3003/v1/chat/completions> -c 8 -n 200`
//!        `loadtest` -duration 30s -c 16   # duration mode for profiling

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use devin2api::auxiliary::goflag::{ErrorHandling, FlagSet};
use futures_util::StreamExt;

/// One request's measurement: `wire_first_byte` is header arrival,
/// `ttfb` is the first body byte (Go's TTFB), `semantic_first_content`
/// the first renderable `SSE` content, `total` the full response.
#[derive(Default)]
struct ResultRow {
    wire_first_byte: Option<Duration>,
    ttfb: Option<Duration>,
    semantic_first_content: Option<Duration>,
    total: Duration,
    bytes: u64,
    error: Option<String>,
}

struct Opts {
    url: String,
    key: String,
    concurrency: usize,
    total: u64,
    duration: Duration,
    body: Vec<u8>,
}

fn parse_args() -> Opts {
    let mut fs = FlagSet::new("loadtest", ErrorHandling::ExitOnError);
    let url = fs.string(
        "url",
        "http://localhost:3003/v1/chat/completions",
        "目标端点",
    );
    let key = fs.string("key", "", "auth.api_key（空 = 不携带凭据）");
    let concurrency = fs.int("c", 8, "并发 worker 数");
    let total = fs.int(
        "n",
        100,
        "总请求数；与 -duration 互斥，-duration 非零时忽略",
    );
    let duration = fs.duration(
        "duration",
        Duration::ZERO,
        "持续压测时长（如 30s）；非零时忽略 -n",
    );
    let model = fs.string("model", "stub", "请求体 model 字段");
    let stream = fs.bool("stream", true, "请求体 stream 字段；false 测非流式路径");
    let body_file = fs.string("body", "", "自定义请求体文件；空用内置 chat 请求");
    fs.parse(&std::env::args().skip(1).collect::<Vec<_>>())
        .unwrap_or_else(|e| unreachable!("ExitOnError exits: {e}"));

    let body = if fs.str(body_file).is_empty() {
        format!(
            "{{\"model\":{},\"stream\":{},\"messages\":[{{\"role\":\"user\",\"content\":\"load test\"}}]}}",
            serde_json::to_string(&fs.str(model)).unwrap_or_default(),
            fs.get_bool(stream)
        )
        .into_bytes()
    } else {
        match std::fs::read(fs.str(body_file)) {
            Ok(data) => data,
            Err(err) => {
                eprintln!("read body file: {err}");
                std::process::exit(1);
            }
        }
    };
    Opts {
        url: fs.str(url),
        key: fs.str(key),
        concurrency: usize::try_from(fs.get_int(concurrency).max(0)).unwrap_or(0),
        total: fs.get_int(total).max(0).cast_unsigned(),
        duration: fs.get_duration(duration),
        body,
    }
}

/// Whether an `SSE` `data:` payload carries renderable content — the first
/// non-empty `choices[].delta.content` (`OpenAI` chat shape).
fn sse_has_content(payload: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return false;
    };
    value
        .pointer("/choices/0/delta/content")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.is_empty())
}

/// Whether a complete non-`SSE` body carries content — `OpenAI`
/// `choices[0].message.content` or a Responses-style `output` array.
fn body_has_content(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    value
        .pointer("/choices/0/message/content")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.is_empty())
        || value.get("output").is_some_and(|v| !v.is_null())
}

/// `doRequest` — one POST measuring wire-first-byte (headers), TTFB
/// (first body byte), semantic first content and total duration. A
/// mid-stream read failure counts as `stream truncated`, never success.
async fn do_request(client: &reqwest::Client, opts: &Opts, body: &[u8]) -> ResultRow {
    let started = Instant::now();
    let mut request = client
        .post(&opts.url)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body.to_vec());
    if !opts.key.is_empty() {
        request = request.header(http::header::AUTHORIZATION, format!("Bearer {}", opts.key));
    }
    let resp = match request.send().await {
        Ok(resp) => resp,
        Err(err) => {
            return ResultRow {
                total: started.elapsed(),
                error: Some(err.to_string()),
                ..Default::default()
            };
        }
    };
    let mut row = ResultRow {
        wire_first_byte: Some(started.elapsed()),
        ..Default::default()
    };
    let status = resp.status();
    let is_sse = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"));
    let mut stream = resp.bytes_stream();
    let mut all: Vec<u8> = Vec::new();
    let mut sse_line: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                if !bytes.is_empty() && row.ttfb.is_none() {
                    row.ttfb = Some(started.elapsed());
                }
                row.bytes += bytes.len() as u64;
                if is_sse && row.semantic_first_content.is_none() {
                    // Scan complete `data:` lines incrementally; SSE frames
                    // can straddle TCP chunks.
                    sse_line.extend_from_slice(&bytes);
                    while let Some(nl) = sse_line.iter().position(|b| *b == b'\n') {
                        let line: Vec<u8> = sse_line.drain(..=nl).collect();
                        let text = String::from_utf8_lossy(&line);
                        if let Some(payload) = text.trim_end().strip_prefix("data:")
                            && sse_has_content(payload.trim())
                        {
                            row.semantic_first_content = Some(started.elapsed());
                        }
                    }
                }
                all.extend_from_slice(&bytes);
            }
            Err(err) => {
                row.ttfb.get_or_insert_with(|| started.elapsed());
                row.total = started.elapsed();
                row.error = Some(format!("stream truncated: {err}"));
                return row;
            }
        }
    }
    row.total = started.elapsed();
    if row.ttfb.is_none() {
        // An empty body still produced a first-read (EOF) — Go records
        // TTFB at the first Read call regardless of n.
        row.ttfb = Some(row.total);
    }
    if !is_sse && row.semantic_first_content.is_none() && body_has_content(&all) {
        // Non-streamed content is only visible once the body completes.
        row.semantic_first_content = Some(row.total);
    }
    if status != http::StatusCode::OK {
        row.error = Some(format!("status {}", status.as_u16()));
    }
    row
}

/// `pct` — nearest-rank percentile (ms).
// Percentile rank arithmetic is float like Go's; counts are tiny.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn pct(values: &[f64], p: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

// Throughput math is float like Go's; counts are tiny.
#[allow(clippy::cast_precision_loss)]
fn avg(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn report_line(name: &str, values: &[f64]) {
    if values.is_empty() {
        return;
    }
    println!(
        "{name:<26} avg={:.1} p50={:.1} p90={:.1} p99={:.1}",
        avg(values),
        pct(values, 50.0),
        pct(values, 90.0),
        pct(values, 99.0)
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let opts = Arc::new(parse_args());
    // Shared client reuses the connection pool like real clients; the
    // 10-minute timeout only backstops a fully stuck request — long
    // streams are the thing being measured.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .unwrap_or_else(|e| {
            eprintln!("http client: {e}");
            std::process::exit(1);
        });

    let issued = Arc::new(AtomicU64::new(0));
    let deadline = if opts.duration.is_zero() {
        None
    } else {
        Some(Instant::now() + opts.duration)
    };
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(opts.concurrency);
    for _ in 0..opts.concurrency {
        let opts = Arc::clone(&opts);
        let client = client.clone();
        let issued = Arc::clone(&issued);
        tasks.push(tokio::spawn(async move {
            let mut results = Vec::with_capacity(256);
            loop {
                let should_issue = deadline.map_or_else(
                    || issued.fetch_add(1, Ordering::SeqCst) < opts.total,
                    |end| Instant::now() < end,
                );
                if !should_issue {
                    break;
                }
                results.push(do_request(&client, &opts, &opts.body).await);
            }
            results
        }));
    }
    let mut results: Vec<ResultRow> = Vec::new();
    for task in tasks {
        results.extend(task.await.unwrap_or_default());
    }
    let elapsed = started.elapsed();

    // `report` — quantiles + throughput + error summary.
    let mut ttfbs: Vec<f64> = Vec::new();
    let mut wire: Vec<f64> = Vec::new();
    let mut semantic: Vec<f64> = Vec::new();
    let mut totals: Vec<f64> = Vec::new();
    let mut bytes_total = 0u64;
    let mut errors: BTreeMap<String, usize> = BTreeMap::new();
    for row in &results {
        bytes_total += row.bytes;
        if let Some(msg) = &row.error {
            *errors.entry(msg.clone()).or_insert(0) += 1;
            continue;
        }
        if let Some(v) = row.wire_first_byte {
            wire.push(v.as_secs_f64() * 1000.0);
        }
        if let Some(v) = row.ttfb {
            ttfbs.push(v.as_secs_f64() * 1000.0);
        }
        if let Some(v) = row.semantic_first_content {
            semantic.push(v.as_secs_f64() * 1000.0);
        }
        totals.push(row.total.as_secs_f64() * 1000.0);
    }
    let ok = totals.len();
    // Throughput math is float like Go's; counts are tiny.
    #[allow(clippy::cast_precision_loss)]
    let rps = ok as f64 / elapsed.as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let body_mb = bytes_total as f64 / 1e6;
    println!(
        "requests={} ok={} errors={} elapsed={:.1}s rps={:.1} body_mb={:.1}",
        results.len(),
        ok,
        results.len() - ok,
        elapsed.as_secs_f64(),
        rps,
        body_mb
    );
    report_line("ttfb_ms", &ttfbs);
    report_line("wire_first_byte_ms", &wire);
    report_line("semantic_first_content_ms", &semantic);
    report_line("total_ms", &totals);
    for (msg, count) in &errors {
        println!("error x{count}: {msg}");
    }
}
