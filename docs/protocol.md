# Devin upstream protocol reference (reverse-engineered)

> Living document organized by topic: `GetChatMessage` field contracts, response frame shapes, error semantics. Conclusions are updated as code and re-probes evolve.
>
> Evidence has two tiers: **measured** = `probe` or the proxy chain against the real upstream; **static** = CLI binary strings / captures / proto bundle analysis. Measured wins on conflict. The dated raw probe logs live in the Go reference repo's `notes/archive/` (not published); this document absorbs their conclusions.
>
> Troubleshooting workflow and client onboarding → `troubleshooting.md`.

2026-09-15 full re-probe: tool-name charset, call↔result positional pairing, dirty-history tolerance (trailing-assistant/thinking-only/dup-result), `stopPatterns` ignored, `disable_parallel_tool_calls` ineffective, `sealed.v1` trailing signature frames, swe-2-max context boundary ~219K–262K tok, `CheckUserMessageRateLimit` constant −1, `is_custom_tool`+lark grammar deterministically `unknown` — all conclusions upheld. Changed items are noted inline.

## Probe tooling

`probe` (auxiliary binary): a Connect-RPC experiment tool hitting `server.codeium.com` directly, reusing the generated bindings in `crates/devin-proto`, with the same token and account as `config.yaml`.

- `chat`: per-field probing; flags cover `-model` `-max-tokens` `-temperature` `-top-p` `-top-k` `-stop-pattern` `-num-completions` `-system` `-prompt-id` `-provider-source` `-planner-mode` `-request-type` `-tool-choice` `-disable-parallel` `-custom-tool` `-raw-schema` `-tool-extras` `-images` `-language` `-chat-model-name` `-meta-extras` `-no-fingerprint` `-no-ids` `-trajectory-id` `-step-index` `-cascade-id` `-assign-jwt` `-internal-model` `-resolve`/`-router` etc.
- `edge <name>`: history-contract boundary cases (pairing/orphans/ordering — see the tool-call matrix); `hist`/`-shape` covers turn-shape variants.
- `replay -variant <name>`: signature replay A/B (`with-sig`/`no-sig`/`bogus-sig`/`bogus-sig-typed`/`with-ids`/`sig-only`/`mutated-thinking`/`no-thinking`).
- `rerun -file <03-devin-request.json>`: replay a captured wire request wholesale. Replay refreshes `metadata.api_key` to the current token and re-mints `execution_id` — not strictly byte-identical; the same body replayed 6/6 times was accepted. Large-history replay is slow: a 706KB real Codex session did not finish streaming within probe's 300s context (upstream accepts but does not close within the window).
- `configs`/`status`/`assign`/`misc`: `GetCliModelConfigs`, `CheckChatCapacity`/`CheckUserMessageRateLimit`/`GetModelStatuses`/`GetModelProviders`, `AssignModel`, `GetEmbeddings`/`GetStatus`/`GetConfig`/`GetCommandModelConfigs`/legacy chat surface.

Capturing real CLI traffic (point `credentials.toml`'s `api_server_url` at a local capture server + replay the startup RPCs) is described in `troubleshooting.md` under "reverse-engineering reference".

## Request surface: `GetChatMessageRequest`

### Top-level fields

| Field                                                                                         | Measured conclusion                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metadata`                                                                                    | `session_id`/`request_id`/`device_fingerprint`/`disable_telemetry`/`user_agent` all silently accepted; a missing `f` fingerprint is fine (the real CLI sends a random `f` per request — we match)                                                                                                                                                                                   |
| `chat_model_uid`                                                                              | **required**; absent → `unknown: an internal error occurred`; versioned uid (`swe-2-high-09102026`) → `permission_denied` (versionId is an internal reference)                                                                                                                                                                                                                      |
| `request_type`                                                                                | **only CASCADE works sessionless**; GENERAL/SMART_FRIEND/COMMAND/EVAL/CONTEXT_CHECK → `failed_precondition` (needs a StartCascade-created trajectory)                                                                                                                                                                                                                               |
| `trajectoryReference`                                                                         | `trajectory_id` is reusable as a session-continuation identifier; `step_index` is a monotonic counter the real CLI sends and upstream accepts                                                                                                                                                                                                                                       |
| `tool_choice`                                                                                 | `option_name` legal values = `none`/`auto`/`required` (**`any` → `invalid_argument`**; Anthropic `any` must map to `required`); `none` is really enforced — under coercion the model still emits no call and self-reports "tool calls are disabled"; `tool_name="X"` forces a call, naming a nonexistent tool → in-stream `invalid_argument`; `required` with no tools is tolerated |
| `disable_parallel_tool_calls`                                                                 | accepted but **no effect**: multiple calls still arrive in one turn — shape alignment only                                                                                                                                                                                                                                                                                          |
| `planner_mode`                                                                                | READ_ONLY/NO_TOOL/EXPLORE/PLANNING/AUTO all accepted but **purely advisory** (tool calls still emitted under NO_TOOL)                                                                                                                                                                                                                                                               |
| `prompt_id`                                                                                   | accepted; same id resent has no dedup — pure correlation field, the CLI itself doesn't send it                                                                                                                                                                                                                                                                                      |
| `provider_source` / `language` / `chat_model_name`                                            | all silently accepted, no observable difference; CLI captures confirm **none are sent** (the CLI top level only sends `metadata/prompt/chatMessagePrompts/chatModelUid/requestType/configuration/tools/trajectoryReference/cascadeId/plannerMode/executionId` — not even cache options; purely implicit caching)                                                                    |
| `use_internal_chat_model` + enum                                                              | `permission_denied` — the internal-enum channel is closed to free tokens                                                                                                                                                                                                                                                                                                            |
| `experiment_config` / `strict` / `read_only_hint` / `server_name` / `attribution_field_names` | all silently accepted                                                                                                                                                                                                                                                                                                                                                               |
| `system_prompt_cache_options` / per-message `prompt_cache_options`                            | EPHEMERAL, no side effects — see the Go reference's `upstream-cache.md`                                                                                                                                                                                                                                                                                                             |
| `cascade_id`                                                                                  | **carries no session state** — two requests on one cascade can't see each other (model self-reports "first message"); continuation only works by replaying `chat_message_prompts`; AssignModel's jwt binds cascade_id (see routing)                                                                                                                                                 |

### `ChatMessagePrompt` (messages)

| Field                                                                 | Measured conclusion                                                                                                                                                                                          |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `source`                                                              | SYSTEM_PROMPT as a message source → `unknown` provider error — system only works via the top-level `prompt`                                                                                                  |
| `output_id`                                                           | **issued on the OpenAI path** (gpt-5-6-sol family, `msg_*`, same prefix as the `rs_*` reasoning item inside the same frame's signature); replays fine. Not present on swe-2/glm/deepseek/gemini/claude paths |
| `signature` / `signature_type`                                        | see "signature regimes" — must be paired on replay                                                                                                                                                           |
| `thinking_id` / `phase`                                               | not observed on any provider                                                                                                                                                                                 |
| `prompt_annotation_ranges` / `safe_for_code_telemetry` / `num_tokens` | unset (IDE use), recorded only — `num_tokens`(#4)/`safe_for_code_telemetry`(#5) are message-level fields, not top-level                                                                                      |

### `ChatToolDefinition` / `ChatToolCall` (declarations and calls)

| Field                                                                                   | Measured conclusion                                                                                                                                                                                                                                                                                                                                                                                                      |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `is_custom_tool` + `custom_tool_grammar`(lark)                                          | **declaration channel is broken**: 3/3 deterministic `unknown` (0 frames, provider-layer error; still deterministic on the 2026-09-15 re-probe). The workaround is deployed: client custom tools are declared as a function with a single `input` string parameter, the model fills the raw text into `{"input":"…"}` (apply_patch patches confirmed delivered this way), and the response side unwraps back to raw text |
| `invalid_json_str` / `is_custom_tool_call` (history direction)                          | **works**: replaying a call with `is_custom_tool_call=true` + `invalid_json_str=<raw patch>` is consumed normally and the model reads the patch content. Decoders should pass it through verbatim, not swallow it into `{}`                                                                                                                                                                                              |
| `invalid_json_str` (response direction)                                                 | not inducible on swe-2-max — provider-constrained decoding wraps even freeform-style output into legal JSON (`{"path":"*** Begin Patch…"}`). Effectively a dead field on the free tier                                                                                                                                                                                                                                   |
| `json_schema_string` invalid JSON                                                       | `unknown` provider error (a bad schema blows up the provider layer)                                                                                                                                                                                                                                                                                                                                                      |
| `json_schema_string` minimal placeholder `{"type":"object"}`                            | accepted and the model really issues calls (FUNCTION_CALL finish) — the precondition for forwarding anthropic client tools (bash_/text_editor_ family without input_schema) in this shape holds                                                                                                                                                                                                                          |
| `arguments_json` invalid JSON in history                                                | **nondeterministic**: once in-stream `invalid_argument`, once a normal answer (the model complained about the bad arguments in thinking) — bad-argument history can blow up upstream                                                                                                                                                                                                                                     |
| `strict`/`read_only_hint`/`server_name`/`attribution_field_names`/`computer_use_config` | silently accepted, recorded only                                                                                                                                                                                                                                                                                                                                                                                         |

## Response frame shapes

### Frame order and provider comparison

swe-2-max (Fireworks) normal response frame order: ~40 heartbeat frames carrying only `latency`/`timestamp`/`usage` → **single-frame whole-chunk** `deltaThinking` → `deltaText`+`deltaTokens` in pieces → `deltaSignature`+`deltaSignatureType` → `stopReason` → `responseDimensionGroups`. The signature is a **trailing frame after all content**.

Cross-provider comparison (measured):

| Dimension                | swe-2-max (FIREWORKS_DEVIN) | claude-opus-4-6-thinking (ANTHROPIC / ANTHROPIC_BEDROCK_GLOBAL) | gpt-5-6-sol (OPENAI_SAFETY_RETENTION)                                                                    | gemini-3-1-pro-high (GEMINI_DATABRICKS) | deepseek-v4-pro-high (FIREWORKS_DEVIN) |
| ------------------------ | --------------------------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------- | -------------------------------------- |
| `signature_type`         | `sealed`                    | `anthropic`                                                     | `openai`                                                                                                 | no signature frame                      | no signature frame                     |
| signature shape          | `sealed.v1.<b64url>`        | Anthropic native base64                                         | **serialized reasoning item JSON** (`[{"id":"rs_*","type":"reasoning","encrypted_content":"gAAAAAB…"}]`) | —                                       | —                                      |
| `outputId`               | none                        | none                                                            | `msg_*`                                                                                                  | none                                    | none                                   |
| `usage.messageId`        | none                        | `msg_*` (Anthropic native id)                                   | none                                                                                                     | none                                    | none                                   |
| normal-finish stopReason | `STOP_PATTERN`              | **`MIN_LOG_PROB`** (end_turn mapping, misleading name)          | `UNSPECIFIED` (small sample)                                                                             | `STOP_PATTERN`                          | `STOP_PATTERN`                         |
| `usage.responseHeader`   | `x-request-id: chatcmpl-*`  | `Request-Id: req_*`                                             | `x-request-id: req_*` + `openai-version`                                                                 | `responseId` + `trafficType`            | `x-request-id: chatcmpl-*`             |
| thinking shape           | single whole-chunk frame    | multi-frame streaming                                           | summary via deltaThinking, reasoning body sealed in signature                                            | multi-frame summary                     | multi-frame                            |

Notes:

- `apiProvider` is a **per-request attribute**, not a model property: claude-opus-4-6-thinking hit `ANTHROPIC` then `ANTHROPIC_BEDROCK_GLOBAL` on the same uid (the latter with `msg_bdrk_*` + `X-Amzn-Requestid`). The local proto's APIProvider enum stops at `68=BEDROCK_MANTLE_FEDERAL` while the server keeps growing: **58 is undefined** (glm-5-3-max returned unnamed `"58"`), **69+ appeared after 68** (swe-1-7 production traffic measured `"69"` across 18864 frames).
- Missing `usage.responseHeader` is a **provider-58 trait, not a glm trait** — CLI captures show glm-5-2 on `FIREWORKS_DEVIN_SERVERLESS` carrying `x-request-id: chatcmpl-*`.
- The gemini family (incl. 3-7/3-8-flash-medium, provider now `GOOGLE_GENAI_VERTEX_GLOBAL`) sends no signature at all — `gemini_thought_signature` is a dead field on the free tier.
- The free tier never sends `creditCost`/`committed_*`/`actualModelUid`/`completionProfile`/`prompt`(echo)/`redact`/`provider_refusal` (text refusals are ordinary deltaText).
- Tool-call id format is per-model: swe-2-max → `read_file_0`, glm-5-2 → `chatcmpl-tool-<hex>` — **do not assume an id shape**.
- `cacheReadTokens` is the norm, not the exception: 82.4% of requests hit (74991/91048, 74612 of them >1000 tokens) — with EPHEMERAL breakpoints + stable trajectory IDs, cache hits are the default path.
- The `premature_end_turn` flag really exists (0.017% of traffic after the `d53dfde` fix, 15/91048) and is surfaced in the panel. It is a shape-candidate signal, not a verdict: a properly finishing turn (`tool_result` input → pure text → `STOP`) is indistinguishable from a real premature end; hits need per-case review. The confirmed root cause was responses-decode fragmentation (issue #2, `d53dfde`).

### stopReason vocabulary

- Normal finishes are per-provider: `STOP_PATTERN` (swe-2/gemini/deepseek — unrelated to stop_patterns hits, the enum name misleads), `MIN_LOG_PROB` (claude, 4/4 samples), `UNSPECIFIED` (gpt-sol, small sample).
- `FUNCTION_CALL` = tool call; `MAX_TOKENS` = budget burned (thinking counts); `CONTENT_FILTER` exists.
- **A clean EOF without `stopReason` is truncation, not a normal end** (measured incident: Codex treated a truncation as completion → task_complete). The decoder reports a stream error rather than synthesizing end_turn; the only exception is local `stopped_by_pattern`.

### Signature regimes and replay rules

Three regimes (`deltaSignatureType`): `sealed` (swe-2/Fireworks), `anthropic` (claude family, native signature), `openai` (gpt-sol family — the signature is a serialized Responses reasoning item containing `encrypted_content`; replaying it to `/v1/responses` clients restores a standard reasoning item, the key channel for multi-turn reasoning).

Replay strictness (measured A/B): **Anthropic (verifies the blob itself; a forgery → in-stream `invalid_argument`) > OpenAI (only checks `signature_type` pairing, content unverified) > Fireworks (no verification at all)**. None verify "signature binds to thinking content" (mutated body + original signature replays fine).

Rules:

- **`signature_type` must pair with the provider** — a mismatched type triggers in-stream `invalid_argument`, more sensitive than the signature content itself.
- A missing `signature_type` is tolerated (the OpenAI path ignores the signature field), but it is a provider-routing hint and the OpenAI path needs it to restore reasoning items — storing it is safer.
- When the trailing signature frame arrives the thinking block is usually already closed; the decoder merges it back into the previous block (`decode_late_signature`); it must never become a standalone empty thinking block (Claude Code drops the whole message).
- **Signatures always arrive in a single frame**: on 2026-09-15, 1826 streams were rechecked (20 live probes + 1806 historical `04-devin-response.jsonl`) — `deltaSignature` appeared exactly once per stream, in the same frame as `deltaSignatureType`; cross-frame signature fragmentation has never been observed.
- **Signature position is per-provider**: anthropic-type lands at the end of the thinking block (mid-stream, before content); sealed-type (swe-2/Fireworks) lands at stream end after all `deltaText` — the downstream encoder must hold the thinking block open until flush or the swe-2 signature has nowhere to go.
- Downstream encoding note (our implementation constraint, not an upstream fact): the official Anthropic SDK **assigns** `signature` rather than appending (`content.signature = delta.signature`) — only one `signature_delta` may be sent and it must carry the complete signature; incremental delivery keeps only the last fragment.

## Sessions and routing

### The AssignModel routing chain

```text
AssignModel{model_router_uid, cascade_id}
  → {assignment_jwt(JWE, A256GCMKW), model_uid, harness_uids}
GetChatMessage{chat_model_uid=assignment.model_uid, model_assignment_jwt, cascade_id=<same>}
```

- `subagent-default` → `swe-1-7-medium` (DECART); `session-titler`/`command-reviser` → `swe-1-7`.
- The jwt **binds `cascade_id`** (mismatch → `invalid_argument`) but **not `model_uid`** (a subagent jwt runs swe-2-max fine).
- Error classification: a non-router legit uid into AssignModel → `invalid_argument`; a nonexistent router name → `not_found`; hitting a router directly without AssignModel → `unavailable` (**looks transient, is actually permanent**).
- `session-titler`/`command-reviser`/`swe-1-7-medium` work directly as `chat_model_uid` (no AssignModel).
- This account's `GetCliModelConfigs` has zero routers (`is_model_router` all false) and empty `inference_config` — yet `subagent-default` still resolves via AssignModel despite being absent from the catalog (catalog absence ≠ unusable).

### Hidden usable uids / internal function models

Internal uids hardcoded in the binary (upstream knows them all): `session-titler`, `command-reviser`, `subagent-free-default`, `subagent-default`, `smart_friend_model_uid` (one per model). `request_type` is not necessarily CASCADE — the CLI uses other types for title/revise.

Model enum space excerpt (strings): swe-2-low/high/max/high-lite; pigeon-v4-vl/v4-devin/v6-low (routers); kimi-k2p6/k2p7-code/k3-low/high/max; glm-5-3-low/high/max, glm-5p2 (router); decart-swe-1.7, hestia2/3, chiron, swe-1-6(-fast), claude-opus-4-7-medium, gpt-5.4.

## Tool-call contract

### call↔result pairing matrix (`edge` cases, swe-2-max)

**The only two hard constraints: a result must follow an existing call; you cannot stack several calls then batch the results.** Everything else about "dirty history" is far more tolerant than expected.

| Shape                                                                      | Result                                                                                                                                                                                    |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| call,result,call,result (correct pairing)                                  | normal                                                                                                                                                                                    |
| call,call,result,result (grouped)                                          | **`invalid_argument`** — a result must be adjacent to "the most recent unpaired call"                                                                                                     |
| one assistant message containing two calls + two consecutive results       | normal — the fatal shape is not "grouping" itself but "a new call message introduces a new id before the previous call's result"; multiple calls in one message declared at once are fine |
| call,user,result (user in between)                                         | normal                                                                                                                                                                                    |
| two calls with the same id + one result                                    | normal                                                                                                                                                                                    |
| one call + two results with the same id                                    | normal, both texts delivered                                                                                                                                                              |
| call(c1) + result(zzz) id mismatch                                         | normal — bound positionally to the pending c1                                                                                                                                             |
| orphan result with no call at all (incl. with tool_call_id)                | **`invalid_argument`**                                                                                                                                                                    |
| history ending on an unanswered call                                       | normal (upstream doesn't answer it but doesn't error)                                                                                                                                     |
| history ending on a tool result                                            | normal                                                                                                                                                                                    |
| trailing-assistant / empty user / dup message id / thinking-only assistant | all normal                                                                                                                                                                                |
| redacted thinking + forged `sealed.v1.` signature                          | normal                                                                                                                                                                                    |
| empty assistant (SYSTEM source) + user "continue"                          | continues normally — the shape basis for the `continueEmpty` resend path                                                                                                                  |

Implication: `RequestMessages::demote_orphan_tool_results` (IR layer) + `pair_tool_calls_with_results` (wire layer) are load-bearing; orphan-result demotion is a safe degradation (with a pending call, upstream tolerates id mismatch positionally).

### Tool names and tool_choice

- Legal charset ≈ **`[A-Za-z0-9_-]`**: `a.b`/`mcp::x`/`a-b_c.d`/non-ASCII → all `invalid_argument` (vague "internal error" wording); `mcp__a__b` is legal. Local ingress validation turns the vague in-stream error into a readable 400.
- `tool_name` naming a nonexistent tool → in-stream `invalid_argument` (after frame 2).
- `required` with no tools → tolerated, normal text answer.
- MCP tool names (`mcp__ide__getDiagnostics`) can be declared and force-called; 50/150 tool declarations show no limit.

### custom/freeform tools (Codex apply_patch)

- **Declaration channel** `is_custom_tool`+grammar → deterministic `unknown` (broken).
- **Workaround (implemented)**: declare as a function with a single `input` string parameter (`{"properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}`), the lark grammar from `format.definition` is injected into the tool description; the model fills the complete patch into `{"input":"*** Begin Patch\n…"}`; the response side unwraps by declared-name set back to raw text, restoring `custom_tool_call`/`custom_tool_call_input.*` events downstream.
- **History channel** `invalid_json_str`+`is_custom_tool_call` works natively (see field table).
- When the model deviates from the wrapper schema (bare text/multi-key), the raw text passes through whole under freeform semantics.

## `CompletionConfiguration` semantics

| Field                              | Measured                                                                                                                                                                                                                                                                                                |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `maxTokens`                        | **effective**, thinking counts toward the budget (max=8 burns entirely on thinking, zero text, `MAX_TOKENS` finish). **Delivery can exceed the cap at chunk granularity while billing follows the cap**: max=1 delivers ~40 thinking tokens but `outputTokens=1`. `deltaTokens` only counts text frames |
| `stopPatterns`                     | **ineffective** (same on swe-2 and gpt-sol re-probes) — the field is silently accepted and ignored; the `STOP_PATTERN` enum is unrelated to stop hits. Local tail truncation is the only stop-sequence implementation                                                                                   |
| `maxNewlines`                      | ineffective (500 lines still sent); the CLI sending it is habit, not a constraint                                                                                                                                                                                                                       |
| `numCompletions`>1                 | `invalid_argument: protocol error: incomplete envelope: unexpected EOF` after 5 frames (2026-09-15 re-probe, deterministic ×2) — CASCADE only supports 1                                                                                                                                                |
| `temperature`/`topK`/`topP`/`seed` | pass through fine; `temp=0.5+top_p=0.5` works on glm-5-3-max(preserveThinking)/swe-2/claude-thinking — no "thinking forces sampling params" constraint                                                                                                                                                  |

An empty-text turn reproduces reliably: `max_tokens≤8` → all `MAX_TOKENS` + thinking-only + zero text — the real shape of "stopReason with zero content", distinct from emptyEndTurn (normal stop, zero content).

## Rate limiting and quota

- **Two independent systems**: `CheckUserMessageRateLimit` constantly reports `{messagesRemaining:-1}` (unlimited, upheld on 2026-09-15), but the real generation path is rate-limited — ~6 messages/minute trips `resource_exhausted: …reset in N seconds|minutes` (both second and minute granularities observed, both parsed), N grows with continued tripping (measured 2s→36s, minute-scale 1–2min — see the Go reference's `upstream-rate-limit.md`). "The capacity check says fine" ≠ "generation won't 429".
- The error carries **no Retry-After header and no RetryInfo detail** — the only machine-usable information is the seconds in the message text, which is parsed and passed through.
- Connect response headers/trailers carry only standard fields, **trailers are always empty** — upstream gives no HTTP-layer quota signal; the only provider-side anchor is `usage.responseHeader.x-request-id` (surfaced into diagnostics).
- **A transient total-outage state really exists**: all RPCs (incl. unary) return `unavailable: unexpected EOF` inside a ~3-minute window then self-heal; 10 rapid sends occasionally get 0-frame EOFs; streaming responses get cut mid-envelope-header (`invalid_argument: protocol error: incomplete envelope` — multiple independent connections died synchronously in the same wave, a TUN proxy stack was on the path) — `try_reopen`'s pre-content retry on transport breaks covers the correct classification.

## Error taxonomy (Connect code → semantics)

| code                  | Trigger                                                                                                                                                                                                                                                                                 | Semantics                                                                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `permission_denied`   | unauthorized uid, internal enums, versioned uid, content-fingerprint sentences                                                                                                                                                                                                          | permission/policy                                                                                                                                            |
| `invalid_argument`    | bad wire shape: orphan TOOL, grouped call/result, non-router uid into AssignModel, `tool_choice=any`, jwt without cascade, naming a nonexistent tool, over-long prompt (`"The prompt is too long for this model"` with real text), the in-stream truncation shape of `numCompletions>1` | request-shape error                                                                                                                                          |
| `failed_precondition` | non-CASCADE request_type; illegal `request_type` enum (999) → `"please update your editor"` version-gate wording                                                                                                                                                                        | missing precondition (needs a real cascade session)                                                                                                          |
| `not_found`           | nonexistent router name; **enum-valid router uids not provisioned on this account land here too** (pigeon-v4-devin/v6-low, glm-5p2 all measured `not_found`)                                                                                                                            | resource doesn't exist                                                                                                                                       |
| `unavailable`         | direct router hit, GetEmbeddings; **also genuinely transient** (0-frame EOF storms, connections cut by middleboxes)                                                                                                                                                                     | the "try later" wording is a fixed template — most cases are permanent semantic errors; unwrap chains carrying io/net errors are transport breaks, retryable |
| `unknown`             | provider-layer collapse: bad schema, `is_custom_tool` declaration, SYSTEM_PROMPT source, missing model uid; illegal `internal_model` enum (99999) **streams 105 normal frames then dies** — enum fields have no upfront validation                                                      | provider internal error, permanent                                                                                                                           |
| `internal`            | (old observation: numCompletions>1 once reported `INTERNAL_ERROR`; the 2026-09-15 re-probe shows `invalid_argument: incomplete envelope`)                                                                                                                                               | stream interruption                                                                                                                                          |
| `resource_exhausted`  | high-frequency requests                                                                                                                                                                                                                                                                 | real rate limit; the hint lives only in the `reset in N seconds/minutes` text (both granularities parsed)                                                    |

Implication: the "experiencing issues / try later" wording on `unavailable`/`unknown` is a misleading template — the real cause is a deterministic request/permission problem. Implemented: `is_transient_connect_error` discriminates by unwrap chain — transport breaks get wrapped uniformly (round-trip break → `unavailable` wrapping EOF, envelope truncation → `invalid_argument: protocol error:`, tcp reset → wrapping `*net.OpError`); chains carrying io.EOF/io.ErrUnexpectedEOF/net.Error are judged transport breaks (connect-phase retry + pre-content resend, recorded `devin_transport`); clean Connect errors are the semantic refusals above (recorded `devin_connect`, not retried).

Machine-readable surface of error bodies (full 2026-09-15 review): **all Connect errors have zero details, zero Retry-After, always-empty trailers** — code and message are the only signals; messages are mostly the opaque template `"an internal error occurred (trace ID: …)"` (shared by permission_denied/invalid_argument/unknown); real descriptive text appears only in a few classes (prompt-too-long, editor version gate). The `trace ID` suffix is the debugging anchor. Fields **silently swallowed without error**: bogus experiment strings in `experiment_config`, illegal `provider_source` enums — don't expect upstream-side validation.

## Other RPC surface

| RPC                                   | Measured                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GetCliModelConfigs`                  | 209 model rows (210 in the cascade variant, the delta is one legacy `MODEL_CHAT_GPT_4O_2024_08_06`); vs the 09-13 snapshot +18 uids, none removed: `gpt-5-3-codex-{low,medium,high,xhigh}[-priority]`×8, `gpt-5-4-{none,low,medium,high,xhigh}[-priority]`×10. Multiple `subagent_default_model_uid` (=`subagent-default`) and `default_override_model_config` (=`{swe-2-high, swe-2-high-09102026}`); this account has zero routers, `inference_config`/`smart_friend_model_uid` all empty |
| `CheckUserMessageRateLimit`           | constant `{hasCapacity:true, messagesRemaining:-1}` (upheld 2026-09-15) — independent of the real limit                                                                                                                                                                                                                                                                                                                                                                                     |
| `GetModelStatuses`                    | returns abnormal-model alerts — MODEL_8341 elevated error rate measured, the 09-15 snapshot added MODEL_15133 with the same alert (MODEL_8341 still present); usable as a health-check source                                                                                                                                                                                                                                                                                               |
| `GetModelProviders`                   | 12 vendors (xAI/DeepSeek/Qwen/NVIDIA/ThinkingMachines/Windsurf/OpenAI/Google/Moonshot/Z.ai/MiniMax/Anthropic)                                                                                                                                                                                                                                                                                                                                                                               |
| `GetStatus`/`GetConfig`               | empty responses                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `GetCommandModelConfigs`              | 6 legacy enum uids                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `GetEmbeddings`                       | `unavailable` — not open to this token                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GetStreamingExternalChatCompletions` | `invalid_argument` — the legacy Windsurf chat surface, keyed by `model_id` enum, closed                                                                                                                                                                                                                                                                                                                                                                                                     |

`ModelFeatures` capability bits (`supports_tool_calls`/`supports_parallel_tool_calls`/`supports_thinking`/`preserve_thinking`/`interleave_thinking`/`supports_images`/`supports_documents`/`summarize_thinking` etc.) are surfaced by `ListModels`; the glm/kimi/grok/inkling/deepseek/nemotron families all lack `supports_parallel_tool_calls`.

`InferenceConfig` (oneof openai/google/anthropic/zai/xai/thinking_machines) is a **server-side per-model config surface**, not a request parameter — reasoning effort doesn't go through `GetChatMessage`; you can only switch uid tiers.

## Images and documents

- The real vision channel works: swe-2-max and claude-opus-4-6 both read images correctly (counted in inputTokens); **images attached to TOOL messages are also consumed** (the tool_result image sub-channel works).
- 20 images in one turn all accepted — the CLI's `max_trailing_images` is client policy, not a wire constraint.
- `mime_type=application/pdf` via the Images channel → `invalid_argument`; `ChatMessagePrompt` has no document field — document blocks are a dead end.
- Degenerate images (1×1 PNG) → `invalid_argument` — upstream has a minimum-validity check.
- ~~non-vision model + image → `invalid_argument`~~: **refuted on 2026-09-15** — glm-5-3-low and deepseek-v4-pro-high accept images and answer directly; the upstream basis for `validate_images_for_model` is gone and the local gate is the only interceptor. Soft signal: those models answer image content all wrong — whether the image really reaches the model is doubtful; images on non-vision models may be silently ineffective even when not rejected.

## CLI-side intelligence (static)

### `InferenceRequest` shaping policy (the CLI's internal unified inference request)

`max_trailing_images` (attach images to the last N messages, over-limit writes an "Images omitted" placeholder), `disable_prompt_cache_writes`, `prefix_mismatch_behavior` (error/drop_block), `append_only_history` (cache-friendly), `system_prefix_len`, `hosted_tool_search` (server-hosted tool search, not exposed on the wire).

### Internal-only prompts (not via the GetChatMessage main chain, but expose expected shapes)

- `agent-ext/title`: plain title ≤80 chars, no quotes/punctuation wrapping.
- `agent-ext.revise-command`: outputs only the rewritten command.
- `agent-ext/looper`: review loop, forced `<ACCEPT>`/`<REJECT>` ending.
- `agent-ext/btw`: read-only side chat forking the main session, compact output.
- `smart_permission/classifier`, `hooks/evaluator`: local logic, no model call.
- `skills/*`, `rules/*`: local config injectors, serialized into the system prompt.

### CLI error enum

`Unauthenticated/Timeout/RateLimited/QuotaExhausted/UsageLimitReached/ServerError/ClientError/Refusal/ContextTooLong/PayloadTooLarge/Disconnected/MalformedResponse/AuthFlowError` — `ContextTooLong` and `PayloadTooLarge` are distinct errors. Upstream's measured `invalid_argument:"prompt is too long"` is normalized to 413; if upstream changes the code, the normalization table needs a new entry.

## Unobserved list (invisible on this account)

- Response side: `thinking_id`/`phase`/`credit_cost`/`committed_*`/`provider_refusal`/`redact`/`gemini_thought_signature`/`completion_profile`/`actual_model_uid`/`arena_*`; the live response-direction forms of `invalid_json_str`/`is_custom_tool_call`; `thinkingRedacted` has never been set (the redacted shape replayed by `edge thinking-empty-sig` is accepted, but upstream doesn't produce it).
- `deltaSignature` cross-frame fragmentation: all 1826 streams arrived single-frame — the hold-open the encoder reserves for fragments is defense in depth, not an observed need.
- `prompt` (#19 echo), full `response_dimension_groups` semantics (UI use, unimportant).
- protocensus note: request-side `file`/`size`/`sha256` unknown-key warnings are **false positives from debuglog attachment-reference envelopes**, not real proto gaps.

## Proxy-side landing points (implementation lookup)

| Contract                                                                      | Location                                                                                                                                            |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| call→result reordering / orphan-result demotion                               | `pair_tool_calls_with_results` (`src/upstream/request.rs`) / `RequestMessages::demote_orphan_tool_results` (`src/domain/request.rs`)                |
| merging an assistant turn into one ChatMessagePrompt                          | `build_request` (`src/upstream/request.rs`)                                                                                                         |
| local stop-sequence truncation                                                | decoder tail window + `stopped_by_pattern` (`src/upstream/response.rs`)                                                                             |
| missing stopReason → truncation                                               | decoder finish (`src/upstream/response.rs`)                                                                                                         |
| signature merge / signature_type paired replay                                | `decode_late_signature` / reasoning-signature classification (`src/upstream/response.rs`)                                                           |
| custom-tool wrap/unwrap                                                       | custom-tool input schema (`src/protocol/responses/request.rs`) / `unwrap_custom_tool_arguments` + the custom-tools set (`src/upstream/response.rs`) |
| tool-name charset validation                                                  | name-charset gate (`src/domain/request.rs`)                                                                                                         |
| `any`→`required` tool_choice mapping                                          | `parse_openai_tool_choice` (`src/protocol/common.rs`) + adapter `build_request`                                                                     |
| transport-break retry (unwrap-chain criterion) / no retry on semantic refusal | `is_transient_connect_error` / `try_reopen` (`src/upstream/retry.rs`)                                                                               |
| rate-limit text seconds → Retry-After                                         | error normalization layer (`src/protocol/common.rs`)                                                                                                |
| provider request id / api_provider into diagnostics                           | decoder metadata update (`src/upstream/response.rs`)                                                                                                |
| XML argument-leak repair                                                      | `repair_leaked_xml_arguments` (`src/upstream/response.rs`)                                                                                          |
