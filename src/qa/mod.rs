//! QA-only infrastructure (task 2): process management, the differential
//! comparator, the Go oracle runner and the contracts manifest.
//!
//! This module backs the feature-gated `qa` binary and the `oracle_harness`
//! integration test. It is dev tooling, not part of the released daemon.

pub mod bench;
pub mod cli;
pub mod compare;
pub mod contracts;
pub mod dashboard;
pub mod diagnostics;
pub mod docs;
pub mod http;
pub mod lifecycle;
pub mod oracle;
pub mod packaging;
pub mod panel;
pub mod parity;
pub mod process;
pub mod stress;
pub mod verdict;
pub mod ws;

/// One registered `qa` subcommand.
pub struct Subcommand {
    /// CLI name, e.g. `dashboard-api`.
    pub name: &'static str,
    /// One-line description shown in `--help`.
    pub summary: &'static str,
    /// Whether the subcommand runs today. Unimplemented subcommands are
    /// listed but fail with exit 2 — they never succeed as placeholders.
    pub implemented: bool,
    /// Owning plan task (for `--help` and coverage reporting).
    pub owner_task: u8,
}

/// The complete planned subcommand registry. Later tasks flip
/// `implemented` to true as they land; the `--help` contract requires every
/// planned name to be listed from task 2 onward.
pub const SUBCOMMANDS: &[Subcommand] = &[
    Subcommand {
        name: "baseline",
        summary: "build the Go oracle into the evidence dir and capture baseline transcripts",
        implemented: true,
        owner_task: 2,
    },
    Subcommand {
        name: "manifest",
        summary: "regenerate tests/contracts.json by scanning the Go reference",
        implemented: true,
        owner_task: 2,
    },
    Subcommand {
        name: "http",
        summary: "exercise the real Rust HTTP surface against a stub upstream",
        implemented: true,
        owner_task: 14,
    },
    Subcommand {
        name: "sse",
        summary: "compare SSE event streams between Go and Rust",
        implemented: false,
        owner_task: 14,
    },
    Subcommand {
        name: "websocket",
        summary: "exercise Responses WebSocket sessions with framed clients",
        implemented: true,
        owner_task: 15,
    },
    Subcommand {
        name: "dashboard-api",
        summary: "cover every admin API route in tests/contracts.json",
        implemented: true,
        owner_task: 17,
    },
    Subcommand {
        name: "diagnostics",
        summary: "drive requests then read runtime metrics and diagnostics",
        implemented: true,
        owner_task: 16,
    },
    Subcommand {
        name: "panel",
        summary: "byte-identical embedded assets, route fallbacks and desktop/mobile screenshots",
        implemented: true,
        owner_task: 18,
    },
    Subcommand {
        name: "parity",
        summary: "run the full Go/Rust process differential suite",
        implemented: true,
        owner_task: 23,
    },
    Subcommand {
        name: "faults",
        summary: "deterministic fault-injection and negative-control cases",
        implemented: true,
        owner_task: 23,
    },
    Subcommand {
        name: "lifecycle",
        summary: "reload, drain, handoff and version-resolution cases",
        implemented: true,
        owner_task: 19,
    },
    Subcommand {
        name: "cli",
        summary: "compare every auxiliary command/flag against the Go tools",
        implemented: true,
        owner_task: 20,
    },
    Subcommand {
        name: "bench",
        summary: "paired Go/Rust performance matrix with bootstrap CIs",
        implemented: true,
        owner_task: 24,
    },
    Subcommand {
        name: "stress",
        summary: "high-volume reliability run against the loopback stub",
        implemented: true,
        owner_task: 24,
    },
    Subcommand {
        name: "packaging",
        summary: "container and release-asset smoke on loopback",
        implemented: true,
        owner_task: 21,
    },
    Subcommand {
        name: "package",
        summary: "alias of packaging (task-21 acceptance spelling)",
        implemented: true,
        owner_task: 21,
    },
    Subcommand {
        name: "documented-smoke",
        summary: "execute the documented quickstart from a packaged artifact",
        implemented: true,
        owner_task: 25,
    },
    Subcommand {
        name: "coverage",
        summary: "verify every contracts.json case was executed; missing cases fail",
        implemented: false,
        owner_task: 25,
    },
    Subcommand {
        name: "live",
        summary: "opt-in live-upstream smoke (requires --allow-live and credentials)",
        implemented: false,
        owner_task: 25,
    },
    Subcommand {
        name: "final-surface",
        summary: "full user-surface QA from the packaged artifact",
        implemented: false,
        owner_task: 25,
    },
];

/// Look up a registered subcommand by name.
pub fn find_subcommand(name: &str) -> Option<&'static Subcommand> {
    SUBCOMMANDS.iter().find(|s| s.name == name)
}
