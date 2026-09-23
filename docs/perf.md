# Runtime diagnostics and profiling

Set `debug.pprof_listen` to a loopback address such as `127.0.0.1:6060` to enable the separate, unauthenticated diagnostics listener. Non-loopback addresses are rejected because the bind address is the trust boundary.

Machine-readable endpoints:

- `/debug/diagnostics/runtime` - process memory/CPU, threads, instrumented tasks, queue waits, allocator capability, and instrumented tracing spans.
- `/debug/diagnostics/tasks` and `/debug/diagnostics/tracing` - stable aliases for runtime-tool clients.
- `/debug/diagnostics/profile` - platform capability and copyable profiling commands.

The legacy `/debug/pprof/*` and `/debug/fgprof` routes return HTTP 501 JSON pointing to the replacement. Snapshot fields without an implemented truthful source report `null`; they are never reported as invented zero values.

## Capability matrix

| Capability                    | Linux                 | macOS                  | Windows                      |
| ----------------------------- | --------------------- | ---------------------- | ---------------------------- |
| Process RSS/CPU, thread count | `/proc` + process API | process API            | process API                  |
| Task/queue-wait snapshots     | yes                   | yes                    | yes                          |
| CPU call-graph profile        | `perf record` (below) | Instruments / `sample` | Windows Performance Recorder |
| Legacy pprof/fgprof routes    | 501 → replacement     | 501 → replacement      | 501 → replacement            |

## Linux CPU profile

The listener's profile endpoint supplies the current PID. Capture a call-graph artifact locally:

```sh
pid=$(curl -fsS http://127.0.0.1:6060/debug/diagnostics/profile | jq -r .pid)
perf record -F 99 -g -p "$pid" -o cpu.perf.data -- sleep 15
perf report -i cpu.perf.data
```

`perf` requires kernel `perf_event_open` permission (`CAP_PERFMON`, suitable `perf_event_paranoid`, or an equivalent administrator policy). A permissions failure is an unsupported host capability, not a zero-valued profile. For process-level CPU and peak-RSS evidence that does not require call-stack access:

```sh
/usr/bin/time -v devin-2api -config config.yaml 2>process-profile.txt
```

Linux exposes thread count and peak RSS through `/proc`; current RSS, virtual memory, and accumulated CPU come from the platform process API. Other platforms return explicit unsupported metadata for fields without an implemented truthful source. macOS operators can use Instruments/sample; Windows operators can use Windows Performance Recorder.

## Benchmarks

Measured performance data against the Go implementation lives in [BENCHMARKS.md](BENCHMARKS.md).
