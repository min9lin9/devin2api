# Benchmarks

Measured same-host comparison against the Go implementation ([WncFht/devin2api](https://github.com/WncFht/devin2api)) at `1dd2bc5a15a672a24c826ffe6c90682e2885a333`. This document exists for transparency — it is reference data, not a usage guide.

## Method

4-core Ryzen 5 5600G, sequential isolated legs, same loopback `upstreamstub`, corpus, transport settings, logging and state filesystem. Go release used the checked-in PGO profile; Rust used the committed Cargo release profile. Primary cells: Chat SSE, 200 deltas × 32 bytes, zero injected delay, ten paired 30s samples in seeded ABBA order (seed 20260916), 10,000 bootstrap resamples. Raw data: `bench4/bench-report.json` in the task-24 evidence (post-optimization binary; the earlier `bench3` run of the pre-optimization binary is retained for comparison).

## Primary cells (Chat SSE, 200×32B deltas)

| cell           | Go rps | Rust rps | ratio | throughput lower-95% CI | nonregression gate ≥0.95 |
| -------------- | ------ | -------- | ----- | ----------------------- | ------------------------ |
| c1 debug-off   | 504.3  | 460.5    | 0.95  | 0.896                   | FAIL                     |
| c1 debug-on    | 173.5  | 194.0    | 1.03  | 0.889                   | FAIL (CI floor)          |
| c8 debug-off   | 773.8  | 547.9    | 0.74  | 0.701                   | FAIL                     |
| c8 debug-on    | 275.4  | 189.2    | 0.65  | 0.486                   | FAIL                     |
| c64 debug-off  | 787.0  | 506.7    | 0.64  | 0.621                   | FAIL                     |
| c64 debug-on   | 301.2  | 162.0    | 0.51  | 0.448                   | FAIL                     |
| c256 debug-off | 913.6  | 500.9    | 0.54  | 0.533                   | FAIL                     |
| c256 debug-on  | 292.8  | 138.0    | 0.45  | 0.381                   | FAIL                     |

Zero errors in every sample of every cell (both daemons).

## Secondary cells (3 paired samples, descriptive)

| cell                  | ratio | note                                |
| --------------------- | ----- | ----------------------------------- |
| chat-json c8          | 1.28  | Rust faster on buffered JSON        |
| ws c8                 | 1.47  | Rust faster on WebSocket turns      |
| responses-sse c8      | 0.66  | SSE path shares the Chat bottleneck |
| messages-sse c8       | 0.68  | same                                |
| chat-sse 4KiB×200 c64 | 0.76  | larger frames narrow the gap        |
| chat-sse 2000×32B c64 | 0.50  | more frames/request widen it        |
| chat-sse ttft1s c8    | 1.01  | upstream-bound cell: parity         |

## Resource and reliability gates

- **RSS: PASS** — worst upper-95% CI ratio 0.691; Rust uses 0.22–0.75× Go's peak RSS in every cell.
- **Local overhead, debug-on: PASS** — Rust's decode+transform+egress p99 is lower than Go's (e.g. c64-debug-on diff_hi −104.3ms); the debuglog pipeline is cheaper per request.
- **Local overhead, debug-off: FAIL on the ratio arm** — end-to-end p99 under the zero-delay stub is higher (c256-debug-off ratio_hi 1.92), the same per-frame pipeline cost as throughput.
- **Reliability: PASS** — 100,000 stub-backed requests across SSE/JSON/WS with zero unexpected failures, duplicate/missing terminal events or leaked permits (the WS leg recorded 16 "leg budget exhausted" entries against the 25-minute cap — a throughput shortfall, not a correctness failure); cancellation p99 15.0ms (limit 250ms); clean shutdown with zero owned tasks.

## Root cause of the SSE gap

A structural optimization pass (2026-09-22) removed the largest per-frame costs: `ResponseEvent.partial` is now a shared `Arc` instead of a per-delta deep clone, the producer task was fused into the response body stream (Go's single-writer shape), and the upstream transport gained a bounded 16-chunk read-ahead ferry. These closed the gap at low concurrency (c1 debug-on now favors Rust) and widened the JSON/WS wins, but a residual gap remains at c≥64: connectrpc's `ServerStream` still polls the response body per envelope, and tokio wakeup cost per frame exceeds Go's channel handoff, so write batches stay small under load.

**Bottom line: do not claim this build is faster.** It is slower on SSE streaming, faster on buffered JSON and WebSocket, dramatically more memory-efficient, and passes all reliability gates.

## Reproducing

```bash
cargo run --locked --release --features qa --bin qa -- bench --go-root <go-checkout> --evidence <dir>
cargo run --locked --release --features qa --bin qa -- stress --requests 100000 --evidence <dir>
```

`bench` runs the full paired matrix and writes `bench-report.json` with raw samples, per-cell CIs and build hashes. `stress` runs the reliability gate. Both spawn isolated sequential legs against the same loopback stub.
