# Documentation index

devin-2api (Rust port) translates Anthropic Messages / OpenAI Responses / Chat Completions requests into Connect-RPC `GetChatMessage` calls against the Devin upstream.

This directory is **living documentation**: it tracks the code as it is today and ships with the repository.

## Operating the service

| Document             | Contents                                                                                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `deployment.md`      | Platform layouts (launchd / systemd --user / Windows console), graceful drain, the one-writer state rule, rollback via a backed-up state copy, offline smoke |
| `compatibility.md`   | Go↔Rust compatibility contract: shared config/state/log formats, the five approved behavioral exceptions, migration and rollback rules                       |
| `troubleshooting.md` | Error quick-reference table, standard debugging workflow, verified upstream wire contracts, per-client pitfalls                                              |
| `perf.md`            | Measured Go-vs-Rust performance results and the Rust runtime diagnostics/profiling surface that replaces Go pprof                                            |
| `commands.md`        | Command reference: `devin-2api` flags/env, `probe`, `loadtest`, `upstreamstub`, `protoextract`, `protocensus`, scripts                                       |
| `toolchain.md`       | Rust toolchain pin, proto codegen pipeline, CI matrix, release/deploy tooling                                                                                |

## Upstream protocol

| Document      | Contents                                                                                                                                       |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `protocol.md` | Reverse-engineered upstream reference: `GetChatMessage` field contracts, response frame shapes, signature regimes, error taxonomy, RPC surface |

## Reading order for new operators

1. `README.md` quick start → running instance.
2. `deployment.md` → service management and rollback.
3. `troubleshooting.md` → when a client reports a failure.
4. `compatibility.md` → if migrating from the Go daemon.
5. `perf.md` → before drawing any performance conclusion.
