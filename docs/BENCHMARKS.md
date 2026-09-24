# Benchmarks

Measured same-host comparison against the Go implementation ([WncFht/devin2api](https://github.com/WncFht/devin2api)) at `1dd2bc5a15a672a24c826ffe6c90682e2885a333`. This document exists for transparency — it is reference data, not a usage guide.

## Method

4-core Ryzen 5 5600G, sequential isolated legs, same loopback `upstreamstub`, corpus, transport settings, logging and state filesystem. Go release used the checked-in PGO profile; Rust used the committed Cargo release profile (a PGO pipeline exists — see `scripts/pgo-build.sh` — but the numbers below are the plain release build). Primary cells: Chat SSE, 200 deltas × 32 bytes, zero injected delay, ten paired 30s samples in seeded ABBA order (seed 20260916), 10,000 bootstrap resamples. Raw data: `bench8/bench-report.json` in the task-24 evidence. Caveat: a sibling build job overlapped the first ~10 minutes of the c1 legs; the affected pairs are flagged in the evidence and the c1 numbers should be read as approximate.

## Primary cells (Chat SSE, 200×32B deltas)

| cell           | ratio (point) | throughput lower-95% CI | nonregression gate ≥0.95 |
| -------------- | ------------- | ----------------------- | ------------------------ |
| c1 debug-off   | 1.18          | 0.916                   | FAIL (CI floor)          |
| c1 debug-on    | 0.96          | 0.749                   | FAIL                     |
| c8 debug-off   | 0.78          | 0.716                   | FAIL                     |
| c8 debug-on    | 0.77          | 0.667                   | FAIL                     |
| c64 debug-off  | 0.74          | 0.678                   | FAIL                     |
| c64 debug-on   | 0.64          | 0.558                   | FAIL                     |
| c256 debug-off | 0.63          | 0.597                   | FAIL                     |
| c256 debug-on  | 0.55          | 0.451                   | FAIL                     |

Zero errors in every sample of every cell (both daemons).

## Secondary cells (3 paired samples, descriptive)

| cell                  | ratio | note                                |
| --------------------- | ----- | ----------------------------------- |
| chat-json c8          | 1.27  | Rust faster on buffered JSON        |
| ws c8                 | 1.39  | Rust faster on WebSocket turns      |
| responses-sse c8      | 0.82  | SSE path shares the Chat bottleneck |
| messages-sse c8       | 0.78  | same                                |
| chat-sse 4KiB×200 c64 | 1.00  | larger frames reach parity          |
| chat-sse 2000×32B c64 | 0.57  | more frames/request widen the gap   |
| chat-sse ttft1s c8    | 1.00  | upstream-bound cell: parity         |

## Resource and reliability gates

- **RSS: PASS** — worst upper-95% CI ratio 0.72; Rust uses 0.22–0.69× Go's peak RSS in every cell.
- **Local overhead, debug-on: PASS** — Rust's decode+transform+egress p99 is far lower than Go's (c64-debug-on ratio 0.05, c256-debug-on 0.01); the debuglog pipeline is dramatically cheaper per request.
- **Local overhead, debug-off: FAIL on the ratio arm** — end-to-end p99 under the zero-delay stub is higher at c≥64 (c64-debug-off ratio_hi 2.79, c256-debug-off 1.56), the same contention-scaling cost as throughput.
- **Reliability: PASS** — 100,000 stub-backed requests across SSE/JSON/WS with zero unexpected failures, duplicate/missing terminal events or leaked permits; cancellation p99 15.0ms (limit 250ms); clean shutdown with zero owned tasks.

## What changed since the first measurement

Three optimization rounds (2026-09-23/24) attacked the per-event pipeline: batched envelope decode (`drain_ready`), zero-copy SSE write coalescing (`BytesMut` batch), allocation removal (conditional `Arc::make_mut`, fused single-pass escaping, `ResponseEvent` slimming 928B→~200B), a fused debuglog pipeline, and removal of a per-chunk terminal-error byte scan Go never performs. Net effect vs the first run: worst cell 0.27→0.45 (lower-95), low-concurrency cells at parity or better (c1-off 1.18, 4KiB 1.00, ttft1s 1.00), JSON/WS wins held.

The residual c≥64 gap is contention-scaling cost — per-request CPU is already at parity (c1), and the gap grows with concurrency, not with per-event work. A sharded-runtime experiment (single-thread runtimes + SO_REUSEPORT) measured neutral-to-worse and was reverted; allocator swaps measured within noise. Whether the gap is a 4-core artifact is an open question pending re-measurement on a larger host.

**Bottom line: do not claim this build is faster.** It is slower on SSE streaming at high concurrency, at parity or faster at low concurrency and on buffered JSON/WebSocket, dramatically more memory-efficient, and passes all reliability gates.

## Reproducing

```bash
cargo run --locked --release --features qa --bin qa -- bench --go-root <go-checkout> --evidence <dir>
cargo run --locked --release --features qa --bin qa -- stress --requests 100000 --evidence <dir>
./scripts/pgo-build.sh all   # optional: PGO-optimized binary (~10% cpu/request measured)
```

`bench` runs the full paired matrix and writes `bench-report.json` with raw samples, per-cell CIs and build hashes. `stress` runs the reliability gate. Both spawn isolated sequential legs against the same loopback stub.
