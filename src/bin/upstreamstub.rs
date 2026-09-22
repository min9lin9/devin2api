//! Auxiliary binary `upstreamstub` — upstream stream-cut fault-injection
//! stub. Port of `G/cmd/upstreamstub/main.go`: answers
//! `GetChatMessage` with real Connect streaming envelopes, cutting the
//! stream mid-envelope per scenario so clients see
//! `incomplete envelope: unexpected EOF` — isomorphic to a live `TCP` cut
//! from the frame reader's view. Also models silent `EOF`, upstream error
//! trailers, hangs, bad frames and a deterministic full stream
//! (`stream`) that backs perf snapshots without burning real quota.
//!
//! The server is a minimal `HTTP`/1.1 implementation over raw `TCP` so the
//! Go behaviors that matter survive: `Connection: close` framing,
//! per-flush incremental writes, hang scenarios that never terminate,
//! and the catch-all handler that hijacks and closes the connection for
//! unimplemented RPCs (the catalog-miss path).

use std::io::Write as _;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use buffa::Message;
use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::auxiliary::goflag::{ErrorHandling, FlagSet};

const CHAT_PATH: &str = "/exa.api_server_pb.ApiServerService/GetChatMessage";

const VALID_SCENARIOS: &[&str] = &[
    "precontent",
    "midcontent",
    "recover",
    "cleaneof",
    "cleaneof-content",
    "bare-end",
    "endstream-error",
    "badframe",
    "badflags",
    "end-hang",
    "heartbeat",
    "stall",
    "stream",
];

struct Opts {
    listen: String,
    scenario: String,
    recover_after: u64,
    deltas: usize,
    delta_bytes: usize,
    interval: Duration,
    ttfb: Duration,
}

/// `log.Printf` equivalent — timestamped stderr line.
fn log(args: std::fmt::Arguments<'_>) {
    let now = jiff::Zoned::now();
    eprintln!("{} {}", now.strftime("%Y/%m/%d %H:%M:%S"), args);
}

fn parse_args() -> Opts {
    let mut fs = FlagSet::new("upstreamstub", ErrorHandling::ExitOnError);
    let listen = fs.string("listen", "127.0.0.1:48090", "监听地址");
    let scenario = fs.string(
        "scenario",
        "precontent",
        "precontent|midcontent|recover|cleaneof|cleaneof-content|bare-end|endstream-error|badframe|badflags|end-hang|heartbeat|stall|stream",
    );
    let recover_after = fs.int(
        "recover-after",
        1,
        "recover 场景下前 N 次请求截断，之后返回完整流",
    );
    let deltas = fs.int("deltas", 200, "stream 场景的 delta 帧数");
    let delta_bytes = fs.int("delta-bytes", 32, "stream 场景每帧 delta 字节数");
    let interval = fs.duration(
        "interval",
        Duration::ZERO,
        "stream 场景帧间隔（0 = 连续吐帧）",
    );
    let ttfb = fs.duration(
        "ttfb",
        Duration::ZERO,
        "stream 场景首帧前延迟（模拟上游思考 TTFT）",
    );
    fs.parse(&std::env::args().skip(1).collect::<Vec<_>>())
        .unwrap_or_else(|e| unreachable!("ExitOnError exits: {e}"));
    let opts = Opts {
        listen: fs.str(listen),
        scenario: fs.str(scenario),
        recover_after: fs.get_int(recover_after).max(0).cast_unsigned(),
        deltas: usize::try_from(fs.get_int(deltas).max(0)).unwrap_or(0),
        delta_bytes: usize::try_from(fs.get_int(delta_bytes).max(0)).unwrap_or(0),
        interval: fs.get_duration(interval),
        ttfb: fs.get_duration(ttfb),
    };
    // A mistyped scenario must not silently fall into another one —
    // tests would judge the wrong behavior. Reject at startup.
    if !VALID_SCENARIOS.contains(&opts.scenario.as_str()) {
        log(format_args!("unknown scenario {:?}", opts.scenario));
        std::process::exit(1);
    }
    opts
}

/// `frame` — one Connect streaming envelope: flag byte + 4-byte big-endian
/// length + message body.
fn frame(msg: &pb::GetChatMessageResponse, json: bool) -> Vec<u8> {
    let payload = if json {
        serde_json::to_vec(msg).unwrap_or_default()
    } else {
        msg.encode_to_vec()
    };
    envelope(0x00, &payload)
}

/// `endStream` — the terminating envelope: 0x02 flag + `JSON` trailer body
/// (`EndStreamResponse`: `{"error":..,"metadata":..}` or `{}`).
fn end_stream(payload: &str) -> Vec<u8> {
    envelope(0x02, payload.as_bytes())
}

fn envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    out
}

fn join(frames: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        out.extend_from_slice(f);
    }
    out
}

fn meta_frame() -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        message_id: Some("bot-stub".to_string()),
        request_id: Some("stub-req".to_string()),
        timestamp: MessageField::some(pb::GoogleProtobuf_Timestamp {
            seconds: Some(jiff::Timestamp::now().as_second()),
            ..Default::default()
        }),
        usage: MessageField::some(pb::ExaCodeiumCommonPb_ModelUsageStats {
            model_uid: Some("swe-2-max".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn delta_text(text: &str) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_text: Some(text.to_string()),
        ..Default::default()
    }
}

fn stop_frame() -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        stop_reason: Some(pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN),
        ..Default::default()
    }
}

use buffa::MessageField;

/// The parsed request head: method, path, headers and how much of `buf`
/// the body still needs (`Content`-Length or chunked).
struct RequestHead {
    path: String,
    json_wire: bool,
}

/// Read one `HTTP`/1.1 request (head + body) like `io.Copy(Discard, r.`Body`)`
/// — the body is consumed so keep-alive framing stays aligned, then
/// discarded.
fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<RequestHead>> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 8192];
    // Read until the header terminator.
    let head_end = loop {
        let n = std::io::Read::read(stream, &mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 1 << 20 {
            return Ok(None);
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]);
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .to_string();
    let mut content_length = 0usize;
    let mut chunked = false;
    let mut json_wire = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = value.trim().parse().unwrap_or(0);
            }
            "transfer-encoding" => {
                chunked = value.to_ascii_lowercase().contains("chunked");
            }
            "content-type" => {
                json_wire = value.trim() == "application/connect+json";
            }
            _ => {}
        }
    }
    // Consume the body: Content-Length bytes, or chunked frames to the
    // zero chunk. Errors mid-body are ignored — the stub discards it.
    let mut rest = buf.split_off(head_end);
    if chunked {
        let _ = drain_chunked(stream, &mut rest);
    } else {
        while rest.len() < content_length {
            let n = std::io::Read::read(stream, &mut chunk)?;
            if n == 0 {
                break;
            }
            rest.extend_from_slice(&chunk[..n]);
        }
    }
    Ok(Some(RequestHead { path, json_wire }))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Consume a chunked body from `rest` + `stream` (best effort).
fn drain_chunked(stream: &mut TcpStream, rest: &mut Vec<u8>) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    loop {
        // Need a size line.
        while find(rest, b"\r\n").is_none() {
            let n = std::io::Read::read(stream, &mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        let line_end = find(rest, b"\r\n").unwrap_or(0);
        let size_text = String::from_utf8_lossy(&rest[..line_end]);
        let size = usize::from_str_radix(size_text.trim(), 16).unwrap_or(0);
        let need = line_end + 2 + size + 2;
        while rest.len() < need {
            let n = std::io::Read::read(stream, &mut chunk)?;
            if n == 0 {
                return Ok(());
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        rest.drain(..need);
        if size == 0 {
            return Ok(());
        }
    }
}

fn write_all(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(bytes)?;
    stream.flush()
}

/// Send response headers for a flushed/close-delimited stream — Go's
/// `w.WriteHeader(200)` + `Flush()` shape (no `Content`-Length; the
/// connection close terminates the body).
fn write_headers(stream: &mut TcpStream, content_type: &str) -> std::io::Result<()> {
    write_all(
        stream,
        format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n")
            .as_bytes(),
    )
}

fn write_body_response(
    stream: &mut TcpStream,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    write_all(
        stream,
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )?;
    write_all(stream, body)
}

/// One connection's service: read the request, dispatch on the path,
/// run the scenario, close.
// One sequential scenario script; splitting would scatter the timeline.
#[allow(clippy::too_many_lines)]
fn serve(stream: &mut TcpStream, opts: &Opts, n: u64) -> std::io::Result<()> {
    let Some(head) = read_request(stream)? else {
        return Ok(());
    };
    if head.path != CHAT_PATH {
        // Other RPCs are unimplemented: Go hijacks and closes the conn,
        // sending the caller down the catalog-miss path. Dropping the
        // socket without a response is the same observable behavior.
        return Ok(());
    }
    let json_wire = head.json_wire;
    let content_type = if json_wire {
        "application/connect+json"
    } else {
        "application/connect+proto"
    };
    match opts.scenario.as_str() {
        "stall" => {
            // Stream established then infinite silence: exercises the
            // silence watchdog and dead-stream resend. Sleep ~2x the
            // client timeout as a bound.
            write_headers(stream, content_type)?;
            log(format_args!(
                "request #{n} scenario=stall: headers sent, hanging"
            ));
            std::thread::sleep(Duration::from_secs(300));
            return Ok(());
        }
        "end-hang" => {
            // Full termination sequence (stopReason + EndStream) then the
            // transport never finishes: the semantic end frame should
            // complete the call without waiting for TCP close.
            write_headers(stream, content_type)?;
            write_all(
                stream,
                &join(&[
                    &frame(&meta_frame(), json_wire),
                    &frame(&delta_text("stub: done"), json_wire),
                    &frame(&stop_frame(), json_wire),
                    &end_stream("{}"),
                ]),
            )?;
            log(format_args!(
                "request #{n} scenario=end-hang: stream complete, hanging"
            ));
            std::thread::sleep(Duration::from_secs(300));
            return Ok(());
        }
        "heartbeat" => {
            // Periodic no-event frames forever: a watchdog keyed on "any
            // frame" never declares death. Write failures after a client
            // disconnect are ignored — the stub's lifetime is the test
            // process that spawned it.
            write_headers(stream, content_type)?;
            loop {
                if write_all(stream, &frame(&meta_frame(), json_wire)).is_err() {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_secs(3));
            }
        }
        "stream" => {
            // Normal full stream: meta → optional TTFT silence → N deltas
            // (flushed per frame, driving the real per-frame
            // projection/encode path) → stop → endStream.
            write_headers(stream, content_type)?;
            write_all(stream, &frame(&meta_frame(), json_wire))?;
            if !opts.ttfb.is_zero() {
                std::thread::sleep(opts.ttfb);
            }
            // Fixed frame content: marshal once so the stub adds no
            // per-frame serialization cost — the measured bottleneck
            // stays on the proxy's projection path.
            let delta_frame = frame(&delta_text(&"x".repeat(opts.delta_bytes)), json_wire);
            for _ in 0..opts.deltas {
                if write_all(stream, &delta_frame).is_err() {
                    return Ok(());
                }
                if !opts.interval.is_zero() {
                    std::thread::sleep(opts.interval);
                }
            }
            write_all(
                stream,
                &join(&[&frame(&stop_frame(), json_wire), &end_stream("{}")]),
            )?;
            return Ok(());
        }
        _ => {}
    }
    // Buffered scenarios: assemble the body, then one response.
    let body: Vec<u8> = match opts.scenario.as_str() {
        "midcontent" => {
            // Content first, then a cut: the client already produced
            // deltas — the non-resendable case.
            let mut body = join(&[
                &frame(&delta_text("stub: hello "), json_wire),
                &frame(&delta_text("world"), json_wire),
            ]);
            body.extend_from_slice(&[0x00, 0x00]); // half a frame prefix → ErrUnexpectedEOF
            body
        }
        "cleaneof" => {
            // Clean cut after the metadata frame (no trailer): silent
            // truncation, pre-content should resend.
            frame(&meta_frame(), json_wire)
        }
        "cleaneof-content" => {
            // Clean cut after content: silent truncation that must not
            // be resent.
            join(&[
                &frame(&meta_frame(), json_wire),
                &frame(&delta_text("stub: partial"), json_wire),
            ])
        }
        "bare-end" => {
            // EndStream trailer without stopReason: upstream "ended
            // normally without a reason" — reproduces the live
            // "Devin stream ended without stop reason".
            join(&[&frame(&meta_frame(), json_wire), &end_stream("{}")])
        }
        "endstream-error" => {
            // Trailer carries an error: upstream reports a semantic
            // error via EndStream (rate-limit shape).
            join(&[
                &frame(&meta_frame(), json_wire),
                &end_stream(
                    r#"{"error":{"code":"resource_exhausted","message":"stub: rate limited"}}"#,
                ),
            ])
        }
        "badframe" => {
            // Garbage bytes as an envelope: unmarshal/frame-parse path.
            vec![0xff, 0xff, 0xff, 0xff, 0xff]
        }
        "badflags" => {
            // Complete envelope with an illegal flag byte (0x04
            // undefined): connect-go reports CodeInternal
            // "protocol error: invalid envelope flags".
            let mut b = frame(&delta_text("stub: never read"), json_wire);
            b[0] = 0x04;
            b
        }
        "recover" if n <= opts.recover_after => {
            let mut body = frame(&meta_frame(), json_wire);
            body.extend_from_slice(&[0x00, 0x00]);
            body
        }
        "recover" => join(&[
            &frame(&meta_frame(), json_wire),
            &frame(&delta_text("stub: recovered reply"), json_wire),
            &frame(&stop_frame(), json_wire),
            &end_stream("{}"),
        ]),
        // precontent: startup validation leaves only this default.
        _ => {
            let mut body = frame(&meta_frame(), json_wire);
            body.extend_from_slice(&[0x00, 0x00]);
            body
        }
    };
    write_body_response(stream, content_type, &body)?;
    log(format_args!(
        "request #{n} scenario={} bytes={}",
        opts.scenario,
        body.len()
    ));
    Ok(())
}

fn main() {
    let opts = Arc::new(parse_args());
    let listener = match TcpListener::bind(&opts.listen) {
        Ok(l) => l,
        Err(e) => {
            log(format_args!("listen {}: {e}", opts.listen));
            std::process::exit(1);
        }
    };
    log(format_args!(
        "upstreamstub listening on {} scenario={}",
        listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or(opts.listen.clone()),
        opts.scenario
    ));
    let count = AtomicU64::new(0);
    for conn in listener.incoming() {
        let Ok(mut stream) = conn else { continue };
        let n = count.fetch_add(1, Ordering::SeqCst) + 1;
        let opts = Arc::clone(&opts);
        // Hang scenarios park their thread for the stub's lifetime — the
        // process is the test's fixture, so a thread per connection is
        // the honest port of Go's per-conn goroutines.
        std::thread::spawn(move || {
            let _ = serve(&mut stream, &opts, n);
        });
    }
}
