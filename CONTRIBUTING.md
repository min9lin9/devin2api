# Contributing

Thanks for your interest in contributing to devin2api. This project is a Rust port of the Go original ([WncFht/devin2api](https://github.com/WncFht/devin2api)) with a strict behavior-parity contract — please read this before opening a PR.

## Ground rules

- **Parity is the contract.** Observable behavior must match the Go reference implementation ([WncFht/devin2api](https://github.com/WncFht/devin2api)). Intentional differences are limited to the approved exceptions listed in [docs/deployment.md](docs/deployment.md#migrating-from-the-go-daemon); anything else is a regression.
- **Never commit secrets.** `config.yaml` is gitignored for a reason. Tests and QA use synthetic tokens only — no live upstream traffic in CI.
- **No "faster" claims without evidence.** Performance statements must cite measured data (see [docs/BENCHMARKS.md](docs/BENCHMARKS.md)).

## Development setup

```bash
git clone https://github.com/min9lin9/devin2api && cd devin2api
cargo build --locked
cargo test --locked --workspace --all-features
```

Requirements: stable Rust (see [docs/toolchain.md](docs/toolchain.md)), Node.js only for markdown lint (`npm ci`).

## Before submitting

All of these must pass — CI enforces them:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
npm run lint:md && npm run format:check   # if you touched *.md
```

## Testing conventions

- Integration tests live in `tests/`; the QA harness is `cargo run --locked --features qa --bin qa -- <subcommand>` (see [docs/commands.md](docs/commands.md)).
- Write regression tests for behavior changes; a fix without a failing-first test is incomplete.
- Do not weaken existing assertions to make a change pass.

## Commit style

- Small, focused commits; describe _why_, not just _what_.
- No merge commits on feature branches — rebase onto `main`.

## Reporting issues

- Bugs: include the version (`devin-2api -version`), config (redact tokens), and a minimal repro.
- Parity differences vs the Go original: label them clearly — they get priority.
- Security issues: see [SECURITY.md](SECURITY.md) — do not open a public issue.
