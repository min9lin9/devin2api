# Security Policy

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Report privately via GitHub's private vulnerability reporting (Security → Advisories → "Report a vulnerability") on this repository, or contact the maintainer through the channels listed on their GitHub profile.

Please include:

- Affected version (`devin-2api -version`) and platform
- Steps to reproduce or a proof of concept
- Impact assessment (what an attacker gains)

You can expect an acknowledgment within a few days. We will coordinate disclosure with you.

## Scope notes

devin2api is a local-first proxy that forwards requests to an upstream AI service with your credentials. Areas of particular security interest:

- **Credential handling** — the upstream token, panel password, and API keys must never leak into logs, error responses, or the panel. Redaction is enforced in code; bypasses are vulnerabilities.
- **The diagnostics listener** (`debug.pprof_listen`) is unauthenticated by design and therefore restricted to loopback binds — a non-loopback bind that succeeds is a bug.
- **State directory** — `logs/` may contain request/response bodies. File permissions and the one-writer rule are part of the security model.
- **Panel auth** — session handling, constant-time comparisons, and CSRF surface.

## Supported versions

Only the latest release receives security fixes. Pre-1.0 releases may introduce breaking changes; check release notes before upgrading.
