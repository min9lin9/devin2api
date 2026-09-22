# Troubleshooting upstream compatibility issues

This repository translates Anthropic / OpenAI protocol requests into Connect-RPC `GetChatMessage` calls against the Devin upstream. The upstream is a closed black box whose error messages are almost always uninformative wrappers like `permission_denied: an internal error occurred` or `invalid_argument: an internal error occurred`. This document collects verified debugging methods and upstream contracts for reuse when onboarding new clients.

## The pipeline

```text
client (cc / codex / kimi-cli / ...)
  → devin-2api :8080          (protocol translation + sanitize + wire construction)
    → server.codeium.com      (Devin upstream, Connect-RPC)
```

A failure at any hop surfaces to the client as a retry/failure. The first step of localization is always **determining which layer produced the error**.

## Error quick-reference

| Symptom                                                                                                                                                        | Layer           | Meaning                                                                                                                                                                                      | Action                                                                                                                                                                                                     |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| HTTP 401                                                                                                                                                       | devin-2api      | wrong `auth.api_key`                                                                                                                                                                         | check `auth.api_key` / request headers                                                                                                                                                                     |
| HTTP 401/502 `authentication_error` (upstream `unauthenticated`)                                                                                               | upstream        | `devin.token` empty or expired; the proxy doesn't intercept locally — `unauthenticated` first triggers a credential re-read and one transparent retry, and only surfaces if that still fails | add a token to any discovery source — self-heals without restart; or edit config.yaml then `POST /panel/api/config/reload`                                                                                 |
| `error_stage=request_build` (e.g. `tool_choice` pointing at a tool absent from `tools`)                                                                        | devin-2api      | local request-projection failure: rejected by wire-construction validation, never reached upstream                                                                                           | read `error.json`'s message to locate the field; a client request defect, not an upstream refusal                                                                                                          |
| `permission_denied` (no policy wording)                                                                                                                        | upstream        | model UID doesn't exist / not authorized                                                                                                                                                     | **first check whether the `devin.aliases` target is still alive** (a `model absent from upstream catalog` warning in stderr means exactly this), then check the model-name spelling                        |
| a model suddenly `not_found`/`permission_denied`                                                                                                               | upstream        | upstream may have added a version gate to that model                                                                                                                                         | bump `devin.client_version` to the latest CLI version and retry                                                                                                                                            |
| `permission_denied` + "blocked by our content policy"                                                                                                          | upstream        | hit a fingerprint-sentence rule                                                                                                                                                              | bisect the request body, add the triggering sentence to `src/upstream/sanitize.rs`                                                                                                                         |
| `invalid_argument` (without a `protocol error:` prefix)                                                                                                        | upstream        | **wire shape mismatch** (not a content problem)                                                                                                                                              | check item by item against "verified wire contracts" below                                                                                                                                                 |
| `incomplete envelope` / `unexpected EOF` / connection reset (incl. `unavailable:`, `invalid_argument: protocol error:` Connect wrappers)                       | transport       | upstream connection cut (TUN/proxy path change, upstream connection reaper)                                                                                                                  | connect-phase retried 3×; pre-content stream breaks are resent once (`meta.retry_attempts`/`index.retries` persisted); recorded `devin_transport`. If it still fails, inspect the connection path          |
| Connect-code error with no io/net error in the unwrap chain (`unavailable` fixed template, `invalid_argument` args, `permission_denied`, `resource_exhausted`) | upstream        | deterministic semantic error                                                                                                                                                                 | **not retried** — "try later" is a fixed template; recorded `devin_connect`                                                                                                                                |
| HTTP 200 + SSE `response.failed`/`error`                                                                                                                       | upstream        | upstream rejected after the stream was established                                                                                                                                           | same as above; classify by the code/status inside the event                                                                                                                                                |
| client sees 503 (`server_draining`) / 429 `server is busy` / 401, but index.jsonl and the requests page are all 200                                            | devin-2api      | pre-pipeline rejection: draining / concurrency overflow / auth failure — rejected before admission, no debug dir written                                                                     | check the "local rejects" table on the panel system page for reason+time; across restarts `grep 'request rejected' logs/stderr.log`; 503 is always draining (deploy window), 429 means the concurrency cap |
| client sees 429 with `rate limited by local gate` in the text                                                                                                  | devin-2api      | local rate-gate fast-fail (token queue exceeded `gate_max_hold_seconds`, non-probe inside a cooldown latch, or a retry storm blocked by the latch)                                           | filter the requests page by `error_stage=rate_gate`; the system page's "rate gate" section shows latch state; attribute separately from a real upstream 429 (`devin_connect`)                              |
| stream cut at exactly ~300s, `error_stage=client_disconnected`                                                                                                 | client          | client-side total timeout (each client's own request timeout) — the proxy has no 300s limit                                                                                                  | adjust the client's timeout config; proxy-side limits are only the drain cap (600s, exit window only) and the upstream watchdog                                                                            |
| process alive but the port refuses connections                                                                                                                 | launchd (macOS) | dyld/Gatekeeper stall                                                                                                                                                                        | confirm with `sample <pid>` then `kill -9`; KeepAlive relaunches                                                                                                                                           |
| logs all `completed` with no error_stage, but the session "thought for a long time and replied one line" or tools all show `[Tool use interrupted]`            | client          | interrupt–resume loop (see the Claude Code entry below) — the proxy delivered complete output; the break is in the client permission layer                                                   | `grep -l '"(no content)"' logs/*/01-http-request.json` — a hit means this failure; don't chase upstream                                                                                                    |

## Standard debugging workflow

### 1. Capture the raw request body

If a front gateway (e.g. ccload) sits in front of devin-2api, pull the exact body it sent — gateways may inject their own system prompt, so debug against the wire bytes, not the client's local file. If the response body is SSE, look at the **last event**: the `message`/`type` inside `response.failed` is the real error; HTTP 200 does not mean success.

### 2. Reproduce directly against devin-2api

```bash
curl -sN http://localhost:8080/v1/responses \
  -H "Authorization: Bearer <api_key>" -H "Content-Type: application/json" \
  --data-binary @/tmp/req.bin | tail -5
```

Transport-break failures (envelope truncation / connection reset) reproduce locally with `upstreamstub`: start the stub, point a test instance's `devin.base_url` at it, pick the failure shape with `-scenario`, and verify the retry chain and stage classification:

| Scenario                        | Stub behavior                                                                      | Expected classification                                                         |
| ------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `precontent`                    | truncated mid-envelope after the metadata frame                                    | transport, one pre-content resend                                               |
| `midcontent`                    | cut after a content frame                                                          | transport, no resend (content already emitted)                                  |
| `recover`                       | first N requests truncated, then a full stream (`-recover-after`)                  | transparent self-heal, `retries:1`                                              |
| `cleaneof` / `cleaneof-content` | clean close without a terminal frame (truncation equivalent)                       | transport; pre-content resend                                                   |
| `bare-end`                      | EndStream without stopReason                                                       | `provider_stream`, "ended without generated content"                            |
| `endstream-error`               | EndStream carrying a rate-limit error                                              | `devin_connect` semantic error + rate latch                                     |
| `stream`                        | normal full-stream baseline (`-deltas`/`-delta-bytes`/`-interval`/`-ttfb` tunable) | not a failure shape: completed, validates the normal path and latency breakdown |
| `badframe` / `badflags`         | truncated frame body / garbage flag byte                                           | transport, one pre-content resend                                               |
| `stall`                         | hangs with zero frames after stream open                                           | 120s watchdog kill → resend → transport                                         |
| `end-hang`                      | complete terminal sequence but the body never closes                               | 15s tail grace after stopReason → clean EOF finish                              |
| `heartbeat`                     | periodic event-free keepalive frames                                               | zero-event frames don't feed the no-progress deadline → 10min backstop finish   |

The watchdog is two-layered: `STALL_TIMEOUT` (120s, transport liveness — any frame counts) + `NO_PROGRESS_TIMEOUT` (10min, content progress — only semantic event frames count). After stopReason is consumed the wait window shrinks to `TAIL_GRACE` (15s) — the client drains the body waiting for transport EOF while reading the endstream envelope, and this layer finishes cleanly when upstream never closes the connection.

If the direct request fails the same way, the problem is devin-2api/upstream, unrelated to any front gateway.

### 3. Bisect the request body

Delete chunks of the failing JSON and retry to locate the triggering field. Typical cut order (by suspicion):

- history items in `input`/`messages` (cut down to the last one first)
- `tools` / function definitions
- `system` / `instructions`
- `reasoning` / thinking blocks
- `tool_use` / `tool_result` blocks (mind the pairing structure)

Quick verdict: the same request still fails without tools → the problem is in message history; still fails with only system + one user message → the problem is the system prompt (usually a fingerprint sentence).

### 4. Read the generated wire (debug logs)

```bash
# preferred hot toggle: POST /panel/api/debug/toggle, no restart
curl -s -X POST http://localhost:<port>/panel/api/debug/toggle \
  -H "Authorization: Bearer <dashboard.password>"
# editing config.yaml then POST /panel/api/config/reload also hot-applies;
# fields listed under requires_restart in the response need a supervised
# restart (cold path)
# reproduce one request, then read logs/<timestamp>/03-devin-request.json
```

Whether each message in `chatMessagePrompts` carries `source`/`prompt`/`toolCalls`/`toolCallId` maps directly onto the contract table below. In protojson output, **field absent** and **field empty-string** are different things — upstream behaves differently for each.

Every request also gets a one-line summary in the process log (`api`/`status`/`duration_ms`/`model`/`stream`/`client_ip`/`upstream_request_id`/token usage), and the debug dir's `meta.json` carries the same fields plus `user_agent`/`key_hash`/TTFB markers — scanning that line usually locates the failure class without unpacking protos.

### 5. Differential experiment

When you suspect a structural constraint, build two minimally different requests and fire both. The canonical case: the same history `call0, call1, result0, result1` was rejected; `call0, result0, call1, result1` passed — proving call→result must be adjacent.

### 6. Reverse-engineering references

- **`protocol.md`**: the upstream protocol conclusions organized by topic (field contracts, frame shapes, signature regimes, error taxonomy, RPC surface); the detailed evidence for this document's contract table lives there.
- **WindsurfAPI** (github.com/dwgx/WindsurfAPI) `src/devin-connect.js` comments mark which fields are `VERIFIED-FROM-WIRE` — their empirical conclusions are generally trustworthy.
- **Capturing real devin CLI traffic**: point `~/.local/share/devin/credentials.toml`'s `api_server_url` at a local capture server (Connect streaming bodies are enveloped: `flag(1B) + len(4B BE) + protobuf`), replay cached GetUserStatus / GetCliModelConfigs / GetCliTeamSettings so the CLI finishes startup and yields a GetChatMessage request body, decode it with the generated bindings in `crates/devin-proto`; `protoscope` / unknown-field checks reveal fields our proto is missing. **Restore `api_server_url` after experimenting** — running CLI sessions drop and reconnect because of it.

## Verified upstream wire contracts

Hard constraints probed one by one with real requests (details in `src/upstream/` comments):

1. **call→result adjacency**: every tool call an assistant emits must be immediately followed by its TOOL result message; a "all calls → all results" grouped sequence is a straight `invalid_argument`. (`pair_tool_calls_with_results` reorders.)
2. **An assistant turn merges into one message**: one assistant turn = one `ChatMessagePrompt`, carrying `prompt`+`thinking`+`signature`+`toolCalls` together (the real-client capture shape; adjacent SYSTEM pairs never appear); with no text the `prompt` field is omitted entirely — empty string and absent differ. Splitting into multiple messages inserts fake turn boundaries into the rendered context and significantly raises the model's EOS probability at declarative-sentence ends (the premature end_turn incident). The same split shape has a second cause: the `/v1/responses` decoder once turned a turn's flattened input items into separate messages (issue #2 — since `d53dfde` the decode layer merges adjacent AssistantMessages) — when you see adjacent SYSTEM pairs, suspect both the encode and decode layers.
3. **thinking hangs on each assistant message** (#11), signature #12 travels with thinking.
4. **Tool-result text must not be empty**; empty becomes the `[tool result]` placeholder.
5. **Completely empty assistant turns are skipped** (measured to induce repeated empty replies upstream).
6. **Tool-schema stripping**: `Description` is replaced by the tool name and annotations are stripped (`convert_tool_definition`, defending against Cursor-style MCP-gate fingerprints).
7. **Fingerprint-sentence rules** (`permission_denied`): semantically equivalent rewriting of system prompts / messages / tool descriptions; rules live in `src/upstream/sanitize.rs`, aligned with WindsurfAPI's full empirical rule set plus this project's added tool-call colon-sentence rule.
8. ~~empty system prompt + tools gets rejected~~: **no longer true as of 2026-09-12** — upstream no longer rejects it and the code no longer injects a fallback system prompt (only tool descriptions merge into the system field). Kept to explain old records.
9. **Prefix caching**: a content prefix hits without session state; stable `trajectory_id`/`cascade_id` + EPHEMERAL breakpoints raise the hit rate.
10. **stepType is always `USER_INPUT`**; the last message is **not required** to be USER (a TOOL ending passes when pairing is correct).
11. **The signature is a trailing frame, with per-provider regimes**: upstream sends `DeltaSignature`+`DeltaSignatureType` only after all content. Three regimes observed: `sealed` (swe-2, `sealed.v1.<b64>`), `anthropic` (claude-thinking, native base64 signature), `openai` (gpt-sol, signature is a serialized reasoning item). On replay the type must be stored/loaded paired with the provider — a mismatch triggers in-stream `invalid_argument`. The decoder merges the signature back into the previous thinking block (`decode_late_signature`); the encoder delays closing the thinking block until the signature arrives — it must never become a standalone empty thinking block (Claude Code drops the whole message, presenting as empty result with HTTP 200). Side effect: when the signature frame arrives across text/tool_use blocks, the emitted SSE is a "nested" block order — the thinking block's `signature_delta`/`content_block_stop` interleave inside tool_use deltas, two blocks open at once, deviating from Anthropic's strict block ordering; claude-cli 2.1.269 tolerated it in a full day of traffic, but parsers assuming strict single-block order may misread the late stop as ending the current block — a known trade-off (closing the block early loses the signature, which is worse).
12. **A clean EOF without stopReason is truncation, not a normal end**: a normal end always has a stopReason frame (swe-2/gemini/deepseek = `STOP_PATTERN`, claude = `MIN_LOG_PROB` — misleading name, actually the end_turn mapping; tool call = `FUNCTION_CALL`); EOF right after text with `deltaToolCalls`/`responseDimensionGroups` all absent is truncation. The decoder reports a stream error ("Devin stream ended without stop reason") instead of synthesizing end_turn — the only exception is `stopped_by_pattern` (local stop-sequence truncation).
13. **Tool-name charset ≈ `[A-Za-z0-9_-]`**: dot/colon/CJK tool names (`mcp::x`, `a.b`, `工具`) are rejected upstream with a vague `invalid_argument`; `mcp__a__b` is legal.
14. **freeform/custom tools have no native declaration channel**: `is_custom_tool`+`custom_tool_grammar` declaration → deterministic `unknown` (0 frames). The working shape is the "wrapper function": a schema with a single `input` string parameter, the model fills the raw text into `{"input":"…"}`, and the response side unwraps (`unwrap_custom_tool_arguments`). The history direction `invalid_json_str`+`is_custom_tool_call` works natively.

## New-client verification checklist

Run this order when onboarding a new client (cc, pi, kimi-code, kimi-cli…):

1. Single turn (confirm the basic path + whether the identity sentence is blocked):

    ```bash
    curl -s http://localhost:8080/v1/messages -H "Authorization: Bearer <api_key>" \
      -H "Content-Type: application/json" -H "anthropic-version: 2023-06-01" \
      -d '{"model":"swe-2-max","max_tokens":64,"messages":[{"role":"user","content":"Reply exactly: pong"}]}'
    ```

2. Feed the client's real system prompt wholesale (the highest fingerprint-risk step).
3. Multi-turn memory (does history replay work).
4. Single tool call → tool_result return → final answer.
5. Single-turn parallel multi-tool calls + multiple tool_results (the easiest place to hit the pairing constraint).
6. Run 3–5 again with `stream: true`.

Per-client specific risks:

- **Claude Code**: both the main-session and subagent system prompts are in the fingerprint set (the CC 2.1.236 main prompt's 7 sentences + the subagent prompt's emoji-ban sentence are in `src/upstream/sanitize.rs`; a new CC version with new wording gets blocked again); `metadata.user_id` is used as the SessionKey. When a subagent is rejected CC reports "issue with the selected model" and the main agent self-reports "subagent unavailable" — not a model problem; check `error.json`'s permission_denied. Two measured client-side pitfalls:
    - **Local model whitelist**: CC 2.1.x rejects unknown model names before sending (`swe-2-max` is blocked outright). Fix: a front gateway's `channel_models` adds a recognizable name like `claude-sonnet-4-6` → `redirect_model=swe-2-max`; on the CC side `ANTHROPIC_MODEL` takes the recognizable name. `modelOverrides`/`CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT=1` also work, but redirect is least invasive.
    - **settings env overrides shell**: the `env` block in `~/.claude/settings.json` beats shell env vars — an `ANTHROPIC_BASE_URL` there overrides your export (the symptom: the process talks to a different address, no output for ages). Injecting env via a project-level `.claude/settings.local.json` is cleanest.
    - **Interrupt–resume loop** (measured on claude-cli 2.1.269/agent-sdk, one 48-minute ~30-round incident on 2026-09-13): a tool call goes through a safety-review sub-call (non-streaming, `stop_sequences:["</block>"]`, prompt demands the response start with `<block>`, same model through this proxy) which judges `<block>no` (don't block), yet the turn is still programmatically interrupted at the permission layer after review (~0.2s from review end to interrupt, not a human Esc); the client records that turn as a thinking-only assistant (the whole tool_use block dropped) and auto-sends a `"(no content)"` user message to resume → the model can't see its own call was interrupted, blindly retries a new command → interrupted again, until a turn without tool_use (end_turn) or the user restarts the session. User view = `Thought for Xm` spins long then emits one line (that duration is the whole turn's wall clock, including the invisible loop). Proxy-side everything is normal (tool_use complete, `stop_reason=tool_use`, message_stop present); a companion symptom is the review sub-call occasionally burning out `max_tokens` and returning a zero-text empty verdict (thinking counts toward the budget; review max_tokens=2112 eaten by thinking). Log signature: `01-http-request.json` tail `user:"(no content)"` + consecutive thinking-only assistant messages in history.
- **Codex**: `apply_patch`'s FREEFORM bare words, "do not wrap the patch in JSON"; the 0.153.3 template has three more system-prompt fingerprints (the open-source definition sentence, the plan-status sentence pair, the ANSI-escape sentence — all in `src/upstream/sanitize.rs`); `type:"custom"` tools (apply_patch) are supported — wrapped upstream into a single-`input`-parameter function, unwrapped downstream back to `custom_tool_call` raw text (the upstream `is_custom_tool` declaration channel is broken; see `protocol.md`); `namespace`/`web_search`/`mcp`/`local_shell` types are still recorded `Dropped`.
- **pi** (`@mariozechner/pi-coding-agent`, 0.73.x fully passing): setup = custom provider in `~/.pi/agent/models.json`, `baseUrl` at the proxy, `api` = `anthropic-messages`, `apiKey` = the proxy key, model declared `id:"swe-2-max"` + `contextWindow`/`maxTokens`. **Key trait: pi's anthropic-messages provider prepends the complete Claude Code fingerprint prompt to the system array** (billing header + the full "You are Claude Code" text), putting its own real system prompt into a user message with a `[System Instructions]` prefix — so CC's fingerprint-rewrite rules cover pi automatically, a free ride on an already-working path. pi sends `thinking:{type:"enabled",budget_tokens:8192}`; upstream returns thinking+signature on demand. Has built-in compaction (`contextTokens > contextWindow - reserveTokens`, default 16k reserve/20k recent, `/compact` manual); no proxy-side compaction needed.
- **kimi-code**: works zero-modification (0.42.0 measured). Spoofs a CC request envelope (`claude-cli` UA, `X-Claude-Code-Session-Id`, CC beta headers); `metadata.user_id` carrying device_id JSON is used as SessionKey. Unknown model names need handwritten `capabilities` under `[models.X]` to get tool_use. Has built-in compaction.
- **kimi-cli**: officially deprecated (merged into kimi-code); not worth the investment.
- **General**: expected risk points = identity-sentence fingerprints + tool-call pairing constraints + per-client proprietary fields (capture a real request body first to check for nonstandard blocks).

On `permission_denied` → bisect per step 3 to locate the triggering sentence and add it to the rules in `src/upstream/sanitize.rs`; on `invalid_argument` → enable debug and read `03-devin-request.json` against the contract table.

## Operational pitfalls

- **Restart cutting in-flight streams**: supervised restarts (`kickstart -k`, `systemctl --user restart`) and `kill -9` cut all in-flight SSE responses immediately — the client sees "the answer suddenly stopped". Stop traffic at the front gateway or pick an idle window before changing config/binary; for debugging prefer a second instance on a spare port (`listen: ":3004"`) instead of touching the live one. The stop timeout is already 660s (launchd `ExitTimeOut` / systemd `TimeoutStopSec`, covering the binary's 600s drain cap) — in-flight streams finish during graceful exit; don't `kill -9` to save time.
- **Don't run `./devin-2api` manually to grab the listen port**: a manual instance and the supervisor's auto-relaunch (launchd KeepAlive / systemd Restart=always) fight over the port (crash-looping every 5s); whoever wins serves, and every alternation cuts all in-flight streams. All instances must be started/stopped through the supervisor. Consecutive bind failures (restart storms) write a `logs/bind-failure.json` marker (`first_at`/`last_at`/`count`/`holder`) surfaced at `/panel/api/stats` under `last_bind_failure` — check those two places first for "the service keeps failing to start".
- **`meta.json`'s `repairs` count has a baseline and is not a fault**: CC-style clients resend the same system-prompt set per request, so fingerprint rewrites and projection repairs hit every time — measured baseline ~6–17 hits/req. Watch for drift in the hit rule-id set (a new id = the client changed prompt wording, possibly needing a new fingerprint), not the total's normal fluctuation.
- **macOS-specific — launchd + freshly built binary**: overwriting the binary then immediately kickstarting can leave dyld stuck in a Gatekeeper check (process `S` state, no listener, no logs). Confirm with `sample <pid>` then `kill -9` and let KeepAlive relaunch; the safe order is build first, stop the old process after. See `deployment.md`.
- **CLI-capture experiment aftermath**: after restoring `credentials.toml`, open CLI sessions need any message sent to reconnect.

## Client context-window configuration (auto-compact prerequisite)

Upstream `GetCliModelConfigs` reports swe-2-max's real window as **262000**. If a client believes the window is larger, its auto-compact threshold sits beyond the limit and it hits prompt-too-long forever without compacting. Verified working configurations:

- **Codex** `~/.codex/config.toml`: `model_context_window = 262000`, `model_auto_compact_token_limit = 230000`. A resume experiment confirmed a 240k history triggers `context compacted` then continues normally.
- **Claude Code** `~/.claude/settings.json` env: `CLAUDE_CODE_MAX_CONTEXT_TOKENS=262000` (window declaration for non-`claude-` prefixed models), `CLAUDE_CODE_AUTO_COMPACT_WINDOW=230000`. Auto-compaction measured after ~202k usage (threshold ≈ window − 28k buffer), context drops to ~17k after compacting.
- CC also has a per-prompt ≤80%-of-window client-side guard (~209k tokens); over the limit it refuses with "Prompt is too long" without sending; `-c -p` resume whose projected total exceeds the window is likewise refused without auto-compaction — boundary protection, not a bug.
- Codex only triggers error-recovery compaction on an SSE `response.failed` event with `error.code=="context_length_exceeded"`; a bare HTTP 413 body doesn't trigger it (generic request error). So devin-2api deliberately emits a synthetic `start` then an error event for streaming requests (top-level `status:413` + `code=context_length_exceeded`). Path difference to note: **direct to the proxy** Codex receives `response.failed`; **through a gateway** `response.created`/`in_progress` don't count as semantic output and don't commit the gateway's response — the gateway still intercepts the error event before writing and materializes an HTTP 413 to the client — equivalent to a bare 413 (client-level, zero cooldown), just without the SSE shape. The Anthropic surface differs: `message_start` counts as semantic output and commits; the error event then passes through verbatim. Non-streaming requests uniformly get a clean HTTP 413.
