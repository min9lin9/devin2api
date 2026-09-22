//! `probe` — controlled experiments against the Devin upstream. Port of
//! `G/cmd/probe/main.go`: same subcommand dispatch, same flag spellings,
//! same request shapes and the same output markers (`== request:`,
//! `== N frames`, `== stopReason:`, per-frame dumps).
//!
//! Token resolution mirrors Go: `DEVIN_TOKEN` env, then `devin.token` from
//! the config chain (`DEVIN2API_CONFIG` → `./config.yaml` → platform
//! default), then the Devin CLI credentials file. The client identity
//! triple resolves from `devin.client_*` with the captured-CLI defaults,
//! so probe traffic shares the proxy's fingerprint.
//!
//! Known deviation: Go's `enumByName` accepts arbitrary numbers (proto
//! enums are open i32); the generated Rust enums are closed, so an
//! out-of-range numeric value reports `unknown enum` instead of sending
//! an unrepresentable wire value.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::anyhow;
use buffa::{Enumeration, MessageField};
use connectrpc::client::CallOptions;
use devin_proto::generated::exa::api_server_pb as pb;
use devin_proto::generated::exa::api_server_pb::__buffa::oneof;

use crate::config;
use crate::randid;
use crate::upstream::catalog::resolve_model_alias;
use crate::upstream::request::{ClientIdentity, build_metadata};
use crate::upstream::transport::{
    TransportConfig, UpstreamTransport, build_http_client, static_token,
};

use super::goflag::{ErrorHandling, FlagSet};

/// `defaultBaseURL` — Go's fallback upstream.
const DEFAULT_BASE_URL: &str = "https://server.codeium.com";

/// The probe's whole-context deadline (Go `context.WithTimeout(300s)`).
const CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// The generated Connect client over the shared upstream transport.
type Client = pb::ApiServerServiceClient<UpstreamTransport>;

fn call_options() -> CallOptions {
    CallOptions::default().with_timeout(CALL_TIMEOUT)
}

/// Process-wide probe context: config-derived aliases + client identity.
pub struct ProbeContext {
    /// `devin.aliases` (normalized by config load).
    pub aliases: BTreeMap<String, String>,
    /// Resolved client identity (`devin.client_*` or captured defaults).
    pub identity: ClientIdentity,
    /// Upstream credential.
    pub token: String,
}

/// `usage()` — the subcommand listing printed on unknown/missing commands.
pub fn usage() {
    eprintln!(
        r#"subcommands:
  configs                     dump GetCliModelConfigs (raw + router/feature summary)
  status                      CheckChatCapacity + CheckUserMessageRateLimit + GetModelStatuses + GetModelProviders
  assign <uid> [uid...]       AssignModel for each router uid
  chat [flags]                one GetChatMessage stream, dump all frames
    -model uid                chat_model_uid (default swe-2-max)
    -prompt text              user prompt (default "Reply exactly: pong")
    -system text              system prompt (default "You are a helpful assistant.")
    -system-as-message        send system prompt as SYSTEM_PROMPT-source message, drop top-level prompt
    -system-empty             send prompt field as explicit empty string
    -tool name                add a JSON-schema tool (repeatable: -tool a -tool b)
    -tool-schema json         schema for the corresponding -tool (positional)
    -custom-tool name         add is_custom_tool with lark grammar (name)
    -raw-schema               send invalid json_schema_string on tools
    -tool-extras              strict+read_only_hint+server_name+attribution on tools
    -tool-choice opt:v|tool:v tool_choice oneof
    -disable-parallel         disable_parallel_tool_calls=true
    -provider-source N|name   provider_source enum
    -prompt-id s              prompt_id field
    -num-tokens n             per-message num_tokens on last user msg
    -planner-mode N|name      planner_mode enum
    -step-type N|name         trajectory step_type enum
    -step-index n             trajectoryReference.step_index (session-monotonic counter)
    -request-type N|name      request_type enum
    -language N|name          language enum
    -chat-model-name s        chat_model_name field
    -no-fingerprint           omit metadata.f
    -no-ids                   omit trajectory/cascade ids
    -trajectory-id s          explicit trajectory_id (share across calls)
    -cascade-id s             explicit cascade_id (share across calls to test concurrency)
    -max-tokens n             configuration.max_tokens
    -num-completions n        configuration.num_completions
    -stop-pattern s           configuration.stop_patterns[0]
    -temperature f            configuration.temperature (default 1)
    -top-p f                  configuration.top_p (default 0.95)
    -top-k n                  configuration.top_k (default 40)
    -images n                 attach n copies of a tiny png to the user msg
    -internal-model N         use_internal_chat_model + internal_chat_model=N
    -assign-jwt s             model_assignment_jwt
    -resolve                  run AssignModel first, use returned uid+jwt
    -resolve-only             run AssignModel but keep chat_model_uid (jwt/model mismatch test)
    -router uid               router uid for -resolve (defaults to -model)
    -meta-extras              send session_id/request_id/device_fingerprint/disable_telemetry
    -frames                   print every frame protojson (default: field inventory + text)
    -dump dir                 write each frame protojson to dir/NN.json
  replay [flags]              two-step: call once, then replay assistant msg with variants
    -model uid                chat_model_uid (default swe-2-max)
    -prompt text              step-1 user prompt
    -variant name             with-sig|with-ids|no-sig|bogus-sig|bogus-sig-typed|sig-only|mutated-thinking|no-thinking
  hist [flags]                synthetic text+call+result history in a chosen wire shape
    -shape name               merged|merged-single|split|split-single (default merged)
    -model uid                chat_model_uid (default swe-2-max)
  rerun -file 03.json [-n N]  replay a captured GetChatMessageRequest N times,
                              print stop_reason + calls + text tail per run
  bigctx [flags]              send ~N KB single user message, observe error code
    -kb n                     payload size (default 1024)
    -model uid                chat_model_uid (default swe-2-max)
  misc                        adjacent endpoints: embeddings/extchat/status/config/command configs
  edge <case> [flags]         targeted edge-case histories (case names in source switch)
    -model uid                chat_model_uid (default swe-2-max)
    -image-file path          attach a real png instead of the tiny 1x1 blue png
    -prompt text              user prompt text for prompt-driven cases (e.g. user-image-prompt)"#
    );
}

/// The subcommand dispatch table — `usage()` stays in sync with it.
pub const COMMANDS: &[&str] = &[
    "configs", "status", "assign", "chat", "replay", "hist", "rerun", "bigctx", "misc", "edge",
];

/// `resolveToken` — `DEVIN_TOKEN` env, then `devin.token` from config,
/// then the Devin CLI credentials discovery chain.
#[must_use]
pub fn resolve_token(cfg: &config::Config) -> String {
    if let Ok(token) = std::env::var("DEVIN_TOKEN")
        && !token.is_empty()
    {
        return token;
    }
    if !cfg.devin.token.is_empty() {
        return cfg.devin.token.clone();
    }
    config::resolve_devin_token_with(&|k| std::env::var(k).ok(), config::Platform::current())
}

/// Load the probe config through the same chain the service uses
/// (`ResolveConfigPath` → `Load`); a missing or invalid config yields the
/// zero value — the env/credentials/default fallback chain still applies.
#[must_use]
pub fn load_probe_config() -> config::Config {
    config::resolve_config_path("")
        .ok()
        .and_then(|path| config::load(&path).ok())
        .unwrap_or_default()
}

/// Build the Connect client over the shared upstream transport — Go's
/// `devinprotoconnect.NewApiServerServiceClient` on the tuned transport.
///
/// # Errors
///
/// `TransportError` for invalid base URL/proxy.
pub fn build_client(cfg: &config::Config, token: &str) -> anyhow::Result<Client> {
    let base_url = if cfg.devin.base_url.is_empty() {
        DEFAULT_BASE_URL.to_string()
    } else {
        cfg.devin.base_url.clone()
    };
    let transport_cfg = TransportConfig {
        base_url,
        proxy: cfg.devin.proxy.clone(),
        force_http1: cfg.devin.force_http1.unwrap_or(config::DEFAULT_FORCE_HTTP1),
        extra_root_pems: Vec::new(),
    };
    let http = build_http_client(&transport_cfg).map_err(|e| anyhow!("{e}"))?;
    let uri: http::Uri = transport_cfg
        .base_url
        .parse()
        .map_err(|_| anyhow!("invalid devin.base_url: {}", transport_cfg.base_url))?;
    Ok(Client::new(
        UpstreamTransport::streaming(http, static_token(token.to_string())),
        connectrpc::client::ClientConfig::new(uri),
    ))
}

/// `aliasModel` — resolve `-model` through `devin.aliases` like the
/// proxy's stream path; a hit prints the rewrite so output is explainable.
fn alias_model(ctx: &ProbeContext, name: &str) -> String {
    let resolved = resolve_model_alias(&ctx.aliases, name);
    if resolved != name {
        println!("alias: {name} -> {resolved}");
    }
    resolved
}

/// `metadata` — the upstream Metadata header; `fingerprint=false` omits
/// the device fingerprint field.
fn metadata(ctx: &ProbeContext, fingerprint: bool) -> pb::ExaCodeiumCommonPb_Metadata {
    let resolved = ctx.identity.resolve();
    build_metadata(
        &ctx.token,
        &resolved.name,
        &resolved.version,
        &resolved.os,
        if fingerprint { 366 } else { 0 },
    )
}

fn j(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// `defaultCompletionConfig` — the captured-CLI completion defaults.
fn default_completion_config() -> pb::ExaCodeiumCommonPb_CompletionConfiguration {
    pb::ExaCodeiumCommonPb_CompletionConfiguration {
        num_completions: Some(1),
        max_tokens: Some(128_000),
        max_newlines: Some(400),
        temperature: Some(1.0),
        top_k: Some(40),
        top_p: Some(0.95),
        ..Default::default()
    }
}

// ---- message constructors (hist/edge share the wire shapes) -------------------

fn user_msg(text: &str) -> pb::ExaChatPb_ChatMessagePrompt {
    pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
        prompt: Some(text.to_string()),
        ..Default::default()
    }
}

fn assistant_msg() -> pb::ExaChatPb_ChatMessagePrompt {
    pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM),
        ..Default::default()
    }
}

fn assistant_text_msg(text: &str) -> pb::ExaChatPb_ChatMessagePrompt {
    let mut m = assistant_msg();
    m.prompt = Some(text.to_string());
    m
}

fn assistant_call_msg(id: &str, name: &str, args_json: &str) -> pb::ExaChatPb_ChatMessagePrompt {
    let mut m = assistant_msg();
    m.tool_calls.push(tool_call(id, name, args_json));
    m
}

fn tool_result_msg(call_id: &str, text: &str) -> pb::ExaChatPb_ChatMessagePrompt {
    pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL),
        prompt: Some(text.to_string()),
        tool_call_id: Some(call_id.to_string()),
        ..Default::default()
    }
}

fn tool_call(id: &str, name: &str, args_json: &str) -> pb::ExaCodeiumCommonPb_ChatToolCall {
    pb::ExaCodeiumCommonPb_ChatToolCall {
        id: Some(id.to_string()),
        name: Some(name.to_string()),
        arguments_json: Some(args_json.to_string()),
        ..Default::default()
    }
}

// ---- configs ------------------------------------------------------------------

async fn cmd_configs(ctx: &ProbeContext, client: &Client) -> anyhow::Result<()> {
    let resp = client
        .get_cli_model_configs_with_options(
            pb::GetCliModelConfigsRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                ..Default::default()
            },
            call_options(),
        )
        .await
        .map_err(|e| anyhow!("GetCliModelConfigs: {e}"))?
        .into_owned();
    let raw = serde_json::to_vec(&resp)?;
    std::fs::create_dir_all("outputs/probe")?;
    std::fs::write("outputs/probe/cli-model-configs.json", &raw)?;
    println!(
        "subagent_default_model_uid: {}",
        resp.subagent_default_model_uid.as_deref().unwrap_or("")
    );
    println!(
        "{:<28} {:<6} {:<7} {:<8} {:<9} {:<8} smart_friend",
        "uid", "router", "family", "tools", "thinking", "images"
    );
    for c in &resp.client_model_configs {
        let mut uid = c.model_uid.clone().unwrap_or_default();
        if uid.is_empty()
            && let Some(oneof::exa_codeium_common_pb_model_or_alias::Choice::ModelUid(alias)) =
                c.model_or_alias.as_option().and_then(|m| m.choice.as_ref())
        {
            uid.clone_from(alias);
        }
        let mut router = String::new();
        let mut family = String::new();
        let mut tools = String::new();
        let mut thinking = String::new();
        let mut images = String::new();
        if let Some(mi) = c.model_info.as_option() {
            router = mi.is_model_router.unwrap_or(false).to_string();
            family = mi.model_family_uid.clone().unwrap_or_default();
            if let Some(feat) = mi.model_features.as_option() {
                tools = feat.supports_tool_calls.unwrap_or(false).to_string();
                thinking = feat.supports_thinking.unwrap_or(false).to_string();
                images = feat.supports_images.unwrap_or(false).to_string();
            }
        }
        let friend = c.smart_friend_model_uid.clone().unwrap_or_default();
        println!("{uid:<28} {router:<6} {family:<7} {tools:<8} {thinking:<9} {images:<8} {friend}");
    }
    println!("raw -> outputs/probe/cli-model-configs.json");
    Ok(())
}

// ---- status -------------------------------------------------------------------

async fn cmd_status(ctx: &ProbeContext, client: &Client) -> anyhow::Result<()> {
    match client
        .check_chat_capacity_with_options(
            pb::CheckChatCapacityRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("CheckChatCapacity ERR: {e}"),
        Ok(r) => println!("CheckChatCapacity: {}", j(&r.into_owned())),
    }
    match client
        .check_user_message_rate_limit_with_options(
            pb::CheckUserMessageRateLimitRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                model_uid: Some("swe-2-max".to_string()),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("CheckUserMessageRateLimit ERR: {e}"),
        Ok(r) => println!("CheckUserMessageRateLimit: {}", j(&r.into_owned())),
    }
    match client
        .get_model_statuses_with_options(
            pb::GetModelStatusesRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("GetModelStatuses ERR: {e}"),
        Ok(r) => println!("GetModelStatuses: {}", j(&r.into_owned())),
    }
    match client
        .get_model_providers_with_options(pb::GetModelProvidersRequest::default(), call_options())
        .await
    {
        Err(e) => println!("GetModelProviders ERR: {e}"),
        Ok(r) => println!("GetModelProviders: {}", j(&r.into_owned())),
    }
    Ok(())
}

// ---- assign -------------------------------------------------------------------

async fn cmd_assign(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    if args.is_empty() {
        return Err(anyhow!("assign needs at least one uid"));
    }
    for uid in args {
        match client
            .assign_model_with_options(
                pb::AssignModelRequest {
                    metadata: MessageField::some(metadata(ctx, true)),
                    model_router_uid: Some(uid.clone()),
                    cascade_id: Some(randid::uuid()),
                    ..Default::default()
                },
                call_options(),
            )
            .await
        {
            Err(e) => println!("{uid:<28} ERR {e}"),
            Ok(resp) => println!("{uid:<28} -> {}", j(&resp.into_owned())),
        }
    }
    Ok(())
}

// ---- chat ---------------------------------------------------------------------

/// `enumByName` — number, full name, or unique `_`-suffix (Go semantics;
/// ambiguous suffixes are an explicit error, never a random pick).
fn enum_by_name<E: Enumeration + 'static>(s: &str) -> anyhow::Result<E> {
    if let Ok(n) = s.parse::<i32>() {
        return E::from_i32(n).ok_or_else(|| anyhow!("unknown enum {s:?}"));
    }
    let up = s.to_uppercase();
    if let Some(v) = E::from_proto_name(&up) {
        return Ok(v);
    }
    let suffix = format!("_{up}");
    let matches: Vec<E> = E::values()
        .iter()
        .copied()
        .filter(|v| v.proto_name().ends_with(&suffix))
        .collect();
    match matches.len() {
        1 => Ok(matches[0]),
        0 => Err(anyhow!("unknown enum {s:?}")),
        n => Err(anyhow!("ambiguous enum {s:?} matches {n} values")),
    }
}

/// `tinyPNG` — the 1x1 blue PNG, base64.
fn tiny_png() -> String {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
        .unwrap_or_default();
    base64::engine::general_purpose::STANDARD.encode(raw)
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn trunc(s: &str, n: usize) -> &str {
    if s.len() > n { &s[..n.min(s.len())] } else { s }
}

/// `runStream` — print the request, consume the stream, dump the field
/// inventory / usage / stop reason / text / calls. A stream error prints
/// the error and headers/trailers but still exits 0 (Go parity).
async fn run_stream(
    client: &Client,
    req: &pb::GetChatMessageRequest,
    show_frames: bool,
    dump_dir: &str,
) -> anyhow::Result<()> {
    let req_json = serde_json::to_string(req).unwrap_or_default();
    let shown = String::from_utf8_lossy(&req_json.as_bytes()[..req_json.len().min(2000)]);
    println!("== request: {shown}");
    let mut stream = client
        .get_chat_message_with_options(req.clone(), call_options())
        .await
        .map_err(|e| anyhow!("connect: {e}"))?;
    let mut field_seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut usage_seen: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut text = String::new();
    let mut thinking = String::new();
    let mut calls: Vec<serde_json::Value> = Vec::new();
    let mut n = 0usize;
    let mut stop_reason =
        pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_UNSPECIFIED;
    if !dump_dir.is_empty() {
        let _ = std::fs::create_dir_all(dump_dir);
    }
    let mut stream_err: Option<connectrpc::ConnectError> = None;
    loop {
        match stream.message::<pb::GetChatMessageResponse>().await {
            Ok(Some(frame)) => {
                let msg = frame.to_owned_message();
                n += 1;
                let body = serde_json::to_string(&msg).unwrap_or_default();
                if !dump_dir.is_empty() {
                    let _ = std::fs::write(Path::new(dump_dir).join(format!("{n:03}.json")), &body);
                }
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(&body)
                {
                    for k in map.keys() {
                        *field_seen.entry(k.clone()).or_insert(0) += 1;
                    }
                    if let Some(serde_json::Value::Object(usage)) = map.get("usage") {
                        for (k, v) in usage {
                            usage_seen.insert(k.clone(), v.clone());
                        }
                    }
                }
                if let Some(reason) = msg.stop_reason
                    && reason
                        != pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_UNSPECIFIED
                    {
                        stop_reason = reason;
                    }
                if show_frames {
                    println!("-- frame {n}: {body}");
                }
                if let Some(delta) = &msg.delta_text {
                    text.push_str(delta);
                }
                if let Some(delta) = &msg.delta_thinking {
                    thinking.push_str(delta);
                }
                for tc in &msg.delta_tool_calls {
                    calls.push(serde_json::json!({
                        "id": tc.id,
                        "name": tc.name,
                        "args": tc.arguments_json,
                        "invalid_json_str": tc.invalid_json_str,
                        "invalid_json_err": tc.invalid_json_err,
                        "is_custom_tool_call": tc.is_custom_tool_call,
                    }));
                }
            }
            Ok(None) => break,
            Err(e) => {
                stream_err = Some(e);
                break;
            }
        }
    }
    if let Some(err) = &stream_err {
        println!("== stream err after {n} frames: {err}");
        dump_connect_err(err);
    }
    println!("== headers: {}", j(&go_header_map(stream.headers())));
    // Go's stream.Trailer() returns an empty (non-nil) map → `{}`.
    let trailers = stream.trailers().map(go_header_map).unwrap_or_default();
    println!("== trailers: {}", j(&trailers));
    if stream_err.is_some() {
        return Ok(());
    }
    println!("== {n} frames");
    println!("== fields: {}", j(&field_seen));
    println!("== usage: {}", j(&usage_seen));
    println!("== stopReason: {}", stop_reason.proto_name());
    if !thinking.is_empty() {
        let shown = String::from_utf8_lossy(&thinking.as_bytes()[..thinking.len().min(300)]);
        println!("== thinking: {shown}");
    }
    println!("== text: {text}");
    for c in &calls {
        println!("== call: {}", j(c));
    }
    Ok(())
}

/// Go `http.Header` JSON form: `{"Canonical-Key": ["v1", "v2"]}`.
fn go_header_map(headers: &http::HeaderMap) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        out.entry(go_canonical_header(name.as_str()))
            .or_default()
            .push(value.to_str().unwrap_or_default().to_string());
    }
    out
}

/// Go's `textproto.CanonicalMIMEHeaderKey` for common headers.
fn go_canonical_header(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = true;
    for ch in name.chars() {
        if ch == '-' {
            upper = true;
            out.push(ch);
        } else if upper {
            out.push(ch.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}

/// `dumpConnectErr` — code, raw message, details and metadata of a
/// Connect error (decides whether structured details survive upstream).
fn dump_connect_err(err: &connectrpc::ConnectError) {
    println!("  code: {:?}", err.code);
    println!("  raw message: {err}");
    for (i, d) in err.details.iter().enumerate() {
        println!(
            "  detail[{i}]: type={} bytes={}",
            d.type_url.as_str(),
            d.value.as_deref().map_or(0, str::len)
        );
    }
    // Go prints `meta[Canonical-Key]=[v1 v2]` over merged header+trailer
    // maps; canonicalize the key and bracket-join values to match.
    let mut meta: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in err.response_headers().iter().chain(err.trailers().iter()) {
        meta.entry(go_canonical_header(k.as_str()))
            .or_default()
            .push(v.to_str().unwrap_or_default().to_string());
    }
    for (k, vs) in &meta {
        println!("  meta[{k}]=[{}]", vs.join(" "));
    }
}

#[allow(clippy::too_many_lines)]
async fn cmd_chat(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("chat", ErrorHandling::ContinueOnError);
    let model = fs.string("model", "swe-2-max", "");
    let user_prompt = fs.string("prompt", "Reply exactly: pong", "");
    let system = fs.string("system", "You are a helpful assistant.", "");
    let tools = fs.list("tool", "");
    let tool_schemas = fs.list(
        "tool-schema",
        "json schema for the corresponding -tool (positional)",
    );
    let custom_tool = fs.string(
        "custom-tool",
        "",
        "add is_custom_tool with lark grammar (name)",
    );
    let raw_schema = fs.bool(
        "raw-schema",
        false,
        "send invalid json_schema_string on tools",
    );
    let tool_extras = fs.bool(
        "tool-extras",
        false,
        "strict+read_only_hint+server_name+attribution on tools",
    );
    let sys_as_msg = fs.bool(
        "system-as-message",
        false,
        "send system prompt as SYSTEM_PROMPT-source message, drop top-level prompt",
    );
    let empty_sys = fs.bool(
        "system-empty",
        false,
        "send prompt field as explicit empty string",
    );
    let tool_choice = fs.string("tool-choice", "", "");
    let disable_parallel = fs.bool("disable-parallel", false, "");
    let provider_source = fs.string("provider-source", "", "");
    let prompt_id = fs.string("prompt-id", "", "");
    let num_tokens = fs.int("num-tokens", 0, "");
    let planner_mode = fs.string("planner-mode", "", "");
    let step_type = fs.string("step-type", "", "");
    let request_type = fs.string("request-type", "", "");
    let language = fs.string("language", "", "");
    let chat_model_name = fs.string("chat-model-name", "", "");
    let no_fingerprint = fs.bool("no-fingerprint", false, "");
    let no_ids = fs.bool("no-ids", false, "");
    let max_tokens = fs.int("max-tokens", 0, "");
    let temperature = fs.float(
        "temperature",
        -1.0,
        "configuration.temperature (<0 = keep default 1)",
    );
    let top_p = fs.float(
        "top-p",
        -1.0,
        "configuration.top_p (<0 = keep default 0.95)",
    );
    let top_k = fs.int("top-k", -1, "configuration.top_k (<0 = keep default 40)");
    let trajectory_id = fs.string(
        "trajectory-id",
        "",
        "explicit trajectory_id (share across calls for session continuation)",
    );
    let step_index = fs.int(
        "step-index",
        -1,
        "trajectoryReference.step_index (session-monotonic counter, real CLI sends it)",
    );
    let images = fs.int("images", 0, "");
    let internal_model = fs.int("internal-model", 0, "");
    let assign_jwt = fs.string("assign-jwt", "", "model_assignment_jwt");
    let resolve_model = fs.bool(
        "resolve",
        false,
        "run AssignModel first, use returned uid+jwt",
    );
    let resolve_only = fs.bool(
        "resolve-only",
        false,
        "run AssignModel but keep chat_model_uid (jwt/model mismatch test)",
    );
    let router_uid = fs.string("router", "", "router uid for -resolve (defaults to -model)");
    let meta_extras = fs.bool(
        "meta-extras",
        false,
        "send session_id/request_id/device_fingerprint/disable_telemetry",
    );
    let num_completions = fs.int("num-completions", 0, "configuration.num_completions");
    let stop_pattern = fs.string("stop-pattern", "", "configuration.stop_patterns[0]");
    let cascade_id = fs.string(
        "cascade-id",
        "",
        "explicit cascade_id (share across calls to test concurrency)",
    );
    let frames = fs.bool("frames", false, "");
    let dump_dir = fs.string("dump", "", "");
    fs.parse(args).map_err(|e| anyhow!("{e}"))?;

    let mut model = alias_model(ctx, &fs.str(model));
    let mut assign_jwt = fs.str(assign_jwt);
    let mut shared_cascade = String::new();
    if fs.get_bool(resolve_model) {
        shared_cascade = randid::uuid();
        let router = {
            let r = fs.str(router_uid);
            if r.is_empty() { model.clone() } else { r }
        };
        let ar = client
            .assign_model_with_options(
                pb::AssignModelRequest {
                    metadata: MessageField::some(metadata(ctx, true)),
                    model_router_uid: Some(router),
                    cascade_id: Some(shared_cascade.clone()),
                    ..Default::default()
                },
                call_options(),
            )
            .await
            .map_err(|e| anyhow!("AssignModel({model}): {e}"))?
            .into_owned();
        let a = ar.assignment.as_option().cloned().unwrap_or_default();
        println!(
            "== assigned: uid={} harness={:?} jwt_len={}",
            a.model_uid.as_deref().unwrap_or(""),
            a.harness_uids,
            a.assignment_jwt.as_deref().map_or(0, str::len)
        );
        if !fs.get_bool(resolve_only) {
            model = a.model_uid.clone().unwrap_or_default();
        }
        assign_jwt = a.assignment_jwt.clone().unwrap_or_default();
    }

    let mut sys_prompt = fs.str(system);
    if fs.get_bool(sys_as_msg) {
        sys_prompt = String::new();
    }
    let mut m = metadata(ctx, !fs.get_bool(no_fingerprint));
    if fs.get_bool(meta_extras) {
        m.session_id = Some(randid::uuid());
        m.request_id = Some(42);
        m.device_fingerprint = Some(randid::hex(32));
        m.disable_telemetry = Some(true);
        m.user_agent = Some(format!("devin/{}", ctx.identity.resolve().version));
    }
    let mut req = pb::GetChatMessageRequest {
        metadata: MessageField::some(m),
        prompt: non_empty(&sys_prompt),
        chat_model_uid: non_empty(&model),
        ..Default::default()
    };
    if fs.get_bool(empty_sys) {
        req.prompt = Some(String::new());
    }
    req.request_type = Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE);
    req.configuration = MessageField::some(default_completion_config());
    req.planner_mode = Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT);
    req.execution_id = Some(randid::uuid());
    if fs.get_int(num_completions) > 0
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.num_completions = Some(u64::try_from(fs.get_int(num_completions)).unwrap_or_default());
    }
    if !fs.str(stop_pattern).is_empty()
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.stop_patterns = vec![fs.str(stop_pattern)];
    }
    if fs.get_int(max_tokens) > 0
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.max_tokens = Some(u64::try_from(fs.get_int(max_tokens)).unwrap_or_default());
    }
    if fs.get_float(temperature) >= 0.0
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.temperature = Some(fs.get_float(temperature));
    }
    if fs.get_float(top_p) >= 0.0
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.top_p = Some(fs.get_float(top_p));
    }
    if fs.get_int(top_k) >= 0
        && let Some(c) = req.configuration.as_option_mut()
    {
        c.top_k = Some(u64::try_from(fs.get_int(top_k)).unwrap_or_default());
    }
    if !fs.get_bool(no_ids) {
        let cascade = fs.str(cascade_id);
        req.cascade_id = Some(if !cascade.is_empty() {
            cascade
        } else if !shared_cascade.is_empty() {
            shared_cascade.clone()
        } else {
            randid::uuid()
        });
        let traj = fs.str(trajectory_id);
        let traj_id = if traj.is_empty() {
            randid::uuid()
        } else {
            traj
        };
        req.trajectory_reference = MessageField::some(pb::ExaCortexPb_CortexTrajectoryReference {
            trajectory_id: Some(traj_id),
            trajectory_type: Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE),
            step_type: Some(pb::ExaCortexPb_CortexStepType::ExaCortexPb_CortexStepType_CORTEX_STEP_TYPE_USER_INPUT),
            ..Default::default()
        });
        if fs.get_int(step_index) >= 0
            && let Some(t) = req.trajectory_reference.as_option_mut()
        {
            t.step_index = Some(i32::try_from(fs.get_int(step_index)).unwrap_or_default());
        }
    }
    let mut msg = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
        prompt: Some(fs.str(user_prompt)),
        ..Default::default()
    };
    if fs.get_int(num_tokens) > 0 {
        msg.num_tokens = Some(u32::try_from(fs.get_int(num_tokens)).unwrap_or_default());
    }
    for _ in 0..fs.get_int(images).max(0) {
        msg.images.push(pb::ExaCodeiumCommonPb_ImageData {
            base64_data: Some(tiny_png()),
            mime_type: Some("image/png".to_string()),
            ..Default::default()
        });
    }
    if fs.get_bool(sys_as_msg) {
        req.chat_message_prompts.push(pb::ExaChatPb_ChatMessagePrompt {
            message_id: Some(randid::uuid()),
            source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM_PROMPT),
            prompt: Some(fs.str(system)),
            ..Default::default()
        });
    }
    req.chat_message_prompts.push(msg);

    let tool_schemas = fs.get_list(tool_schemas);
    for (schema_idx, name) in fs.get_list(tools).iter().enumerate() {
        let mut schema =
            r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#
                .to_string();
        if fs.get_bool(raw_schema) {
            schema = "this is not json".to_string();
        }
        if schema_idx < tool_schemas.len() && !tool_schemas[schema_idx].is_empty() {
            schema.clone_from(&tool_schemas[schema_idx]);
        }
        let mut td = pb::ExaChatPb_ChatToolDefinition {
            name: Some(name.clone()),
            description: Some(format!("{name} tool")),
            json_schema_string: Some(schema),
            ..Default::default()
        };
        if fs.get_bool(tool_extras) {
            td.strict = Some(true);
            td.read_only_hint = Some(true);
            td.server_name = Some("mcp-server".to_string());
            td.attribution_field_names = vec!["path".to_string()];
        }
        req.tools.push(td);
    }
    if !fs.str(custom_tool).is_empty() {
        req.tools.push(pb::ExaChatPb_ChatToolDefinition {
            name: Some(fs.str(custom_tool)),
            description: Some("apply a patch".to_string()),
            is_custom_tool: Some(true),
            custom_tool_grammar: Some(r#"start: "PATCH" /[a-zA-Z0-9_.\/-]+/ "END""#.to_string()),
            custom_tool_grammar_syntax: Some("lark".to_string()),
            ..Default::default()
        });
    }
    if !fs.str(tool_choice).is_empty() {
        let value = fs.str(tool_choice);
        let choice = if let Some(v) = value.strip_prefix("tool:") {
            oneof::exa_chat_pb_chat_tool_choice::Choice::ToolName(v.to_string())
        } else {
            let v = value.strip_prefix("opt:").unwrap_or(&value);
            oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(v.to_string())
        };
        req.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
            choice: Some(choice),
            ..Default::default()
        });
    }
    if fs.get_bool(disable_parallel) {
        req.disable_parallel_tool_calls = Some(true);
    }
    if !fs.str(provider_source).is_empty() {
        req.provider_source = Some(enum_by_name::<pb::ExaCodeiumCommonPb_ProviderSource>(
            &fs.str(provider_source),
        )?);
    }
    if !fs.str(prompt_id).is_empty() {
        req.prompt_id = Some(fs.str(prompt_id));
    }
    if !fs.str(planner_mode).is_empty() {
        req.planner_mode = Some(enum_by_name::<
            pb::ExaCodeiumCommonPb_ConversationalPlannerMode,
        >(&fs.str(planner_mode))?);
    }
    if !fs.str(step_type).is_empty() {
        let v = enum_by_name::<pb::ExaCortexPb_CortexStepType>(&fs.str(step_type))?;
        if req.trajectory_reference.is_unset() {
            req.trajectory_reference =
                MessageField::some(pb::ExaCortexPb_CortexTrajectoryReference {
                    trajectory_id: Some(randid::uuid()),
                    trajectory_type: Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE),
                    ..Default::default()
                });
        }
        if let Some(t) = req.trajectory_reference.as_option_mut() {
            t.step_type = Some(v);
        }
    }
    if !fs.str(request_type).is_empty() {
        req.request_type = Some(enum_by_name::<pb::ChatMessageRequestType>(
            &fs.str(request_type),
        )?);
    }
    if !fs.str(language).is_empty() {
        req.language = Some(enum_by_name::<pb::ExaCodeiumCommonPb_Language>(
            &fs.str(language),
        )?);
    }
    if !fs.str(chat_model_name).is_empty() {
        req.chat_model_name = Some(fs.str(chat_model_name));
    }
    if fs.get_int(internal_model) != 0 {
        req.use_internal_chat_model = Some(true);
        req.internal_chat_model = Some(
            pb::ExaCodeiumCommonPb_Model::from_i32(
                i32::try_from(fs.get_int(internal_model)).unwrap_or_default(),
            )
            .ok_or_else(|| anyhow!("unknown enum {:?}", fs.get_int(internal_model)))?,
        );
    }
    if !assign_jwt.is_empty() {
        req.model_assignment_jwt = Some(assign_jwt);
    }
    run_stream(client, &req, fs.get_bool(frames), &fs.str(dump_dir)).await
}

// ---- replay --------------------------------------------------------------------

// Mirrors the Go probe replay flow step-for-step for parity review.
#[allow(clippy::too_many_lines)]
async fn cmd_replay(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("replay", ErrorHandling::ContinueOnError);
    let model = fs.string("model", "swe-2-max", "");
    let variant = fs.string("variant", "with-sig", "");
    let q1_text = fs.string(
        "prompt",
        "Think briefly, then reply with the single word: zebra",
        "",
    );
    fs.parse(args).map_err(|e| anyhow!("{e}"))?;
    let model = alias_model(ctx, &fs.str(model));

    let mk = |msgs: Vec<pb::ExaChatPb_ChatMessagePrompt>| {
        pb::GetChatMessageRequest {
        metadata: MessageField::some(metadata(ctx, true)),
        prompt: Some("You are a helpful assistant.".to_string()),
        chat_model_uid: Some(model.clone()),
        request_type: Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE),
        configuration: MessageField::some(default_completion_config()),
        cascade_id: Some(randid::uuid()),
        planner_mode: Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT),
        execution_id: Some(randid::uuid()),
        trajectory_reference: MessageField::some(pb::ExaCortexPb_CortexTrajectoryReference {
            trajectory_id: Some(randid::uuid()),
            trajectory_type: Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE),
            step_type: Some(pb::ExaCortexPb_CortexStepType::ExaCortexPb_CortexStepType_CORTEX_STEP_TYPE_USER_INPUT),
            ..Default::default()
        }),
        chat_message_prompts: msgs,
        ..Default::default()
    }
    };

    // step 1: ask a question that triggers thinking
    let q1 = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
        prompt: Some(fs.str(q1_text)),
        ..Default::default()
    };
    let mut stream = client
        .get_chat_message_with_options(mk(vec![q1.clone()]), call_options())
        .await
        .map_err(|e| anyhow!("step1 connect: {e}"))?;
    let mut a_text = String::new();
    let mut a_thinking = String::new();
    let mut a_sig = String::new();
    let mut a_sig_type = String::new();
    let mut a_output_id = String::new();
    let mut a_thinking_id = String::new();
    let mut a_phase = String::new();
    let mut a_redacted = false;
    let mut frames1 = 0usize;
    loop {
        match stream.message::<pb::GetChatMessageResponse>().await {
            Ok(Some(frame)) => {
                let m = frame.to_owned_message();
                frames1 += 1;
                if let Some(v) = &m.delta_text {
                    a_text.push_str(v);
                }
                if let Some(v) = &m.delta_thinking {
                    a_thinking.push_str(v);
                }
                if let Some(v) = &m.delta_signature {
                    a_sig.push_str(v);
                }
                if let Some(v) = &m.delta_signature_type
                    && !v.is_empty()
                {
                    a_sig_type.clone_from(v);
                }
                if let Some(v) = &m.output_id
                    && !v.is_empty()
                {
                    a_output_id.clone_from(v);
                }
                if let Some(v) = &m.thinking_id
                    && !v.is_empty()
                {
                    a_thinking_id.clone_from(v);
                }
                if let Some(v) = &m.phase
                    && !v.is_empty()
                {
                    a_phase.clone_from(v);
                }
                a_redacted = a_redacted || m.thinking_redacted.unwrap_or(false);
            }
            Ok(None) => break,
            Err(e) => return Err(anyhow!("step1 stream: {e}")),
        }
    }
    println!(
        "step1: frames={frames1} text={a_text:?} thinking_len={} sig_len={} sig_type={a_sig_type:?} output_id={a_output_id:?} thinking_id={a_thinking_id:?} phase={a_phase:?} redacted={a_redacted}",
        a_thinking.len(),
        a_sig.len()
    );

    // step 2: replay the assistant message with the requested variant
    let mut asst = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM),
        prompt: Some(a_text.clone()),
        ..Default::default()
    };
    match fs.str(variant).as_str() {
        "with-sig" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some(a_thinking.clone());
            }
            if !a_sig.is_empty() {
                asst.signature = Some(a_sig.clone());
            }
        }
        "no-sig" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some(a_thinking.clone());
            }
        }
        "bogus-sig" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some(a_thinking.clone());
            }
            if !a_sig.is_empty() {
                asst.signature = Some(format!("bogus-{}", trunc(&a_sig, 16)));
            }
        }
        "bogus-sig-typed" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some(a_thinking.clone());
            }
            if !a_sig.is_empty() {
                asst.signature = Some(format!("bogus-{}", trunc(&a_sig, 16)));
                asst.signature_type = Some(a_sig_type.clone());
            }
        }
        "with-ids" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some(a_thinking.clone());
            }
            if !a_sig.is_empty() {
                asst.signature = Some(a_sig.clone());
            }
            asst.output_id = Some(a_output_id.clone());
            asst.thinking_id = Some(a_thinking_id.clone());
            if !a_sig_type.is_empty() {
                asst.signature_type = Some(a_sig_type.clone());
            }
            if !a_phase.is_empty() {
                asst.phase = Some(a_phase.clone());
            }
        }
        "sig-only" => {
            if !a_sig.is_empty() {
                asst.signature = Some(a_sig.clone());
            }
            if !a_sig_type.is_empty() {
                asst.signature_type = Some(a_sig_type.clone());
            }
        }
        "mutated-thinking" => {
            if !a_thinking.is_empty() {
                asst.thinking = Some("COMPLETELY DIFFERENT reasoning about bananas.".to_string());
            }
            if !a_sig.is_empty() {
                asst.signature = Some(a_sig.clone());
            }
        }
        "no-thinking" => {}
        other => return Err(anyhow!("unknown variant {other:?}")),
    }
    let q2 = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
        prompt: Some("What word did you just say? One word only.".to_string()),
        ..Default::default()
    };
    let q1_copy = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: q1.source,
        prompt: q1.prompt.clone(),
        ..Default::default()
    };
    run_stream(client, &mk(vec![q1_copy, asst, q2]), false, "").await
}

// ---- hist -----------------------------------------------------------------------

// Mirrors the Go probe hist flow for parity review.
#[allow(clippy::too_many_lines)]
async fn cmd_hist(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("hist", ErrorHandling::ContinueOnError);
    let shape = fs.string("shape", "merged", "");
    let model = fs.string("model", "swe-2-max", "");
    fs.parse(args).map_err(|e| anyhow!("{e}"))?;
    let model = alias_model(ctx, &fs.str(model));
    let call1 = tool_call("chatcmpl-tool-aaa1", "exec", r#"{"command":"ls"}"#);
    let call2 = tool_call("chatcmpl-tool-bbb2", "read_file", r#"{"path":"README.md"}"#);
    let thinking = "I should list the directory and read the readme in parallel.";
    let asst: Vec<pb::ExaChatPb_ChatMessagePrompt> = match fs.str(shape).as_str() {
        "merged" => {
            let mut m = assistant_msg();
            m.prompt = Some("I'll list files and read the readme at once:".to_string());
            m.thinking = Some(thinking.to_string());
            m.tool_calls = vec![call1.clone(), call2.clone()];
            vec![m]
        }
        "merged-single" => {
            let mut m = assistant_msg();
            m.prompt = Some("I'll list files first:".to_string());
            m.thinking = Some(thinking.to_string());
            m.tool_calls = vec![call1.clone()];
            vec![m]
        }
        "split" => {
            let mut t = assistant_msg();
            t.prompt = Some("I'll list files and read the readme at once:".to_string());
            t.thinking = Some(thinking.to_string());
            let mut c1 = assistant_msg();
            c1.thinking = Some(thinking.to_string());
            c1.tool_calls = vec![call1.clone()];
            let mut c2 = assistant_msg();
            c2.thinking = Some(thinking.to_string());
            c2.tool_calls = vec![call2.clone()];
            vec![t, c1, c2]
        }
        "split-single" => {
            let mut t = assistant_msg();
            t.prompt = Some("I'll list files first:".to_string());
            t.thinking = Some(thinking.to_string());
            let mut c1 = assistant_msg();
            c1.thinking = Some(thinking.to_string());
            c1.tool_calls = vec![call1.clone()];
            vec![t, c1]
        }
        other => return Err(anyhow!("unknown shape {other:?}")),
    };
    let mut msgs: Vec<pb::ExaChatPb_ChatMessagePrompt> =
        vec![user_msg("list the files and read README.md")];
    if fs.str(shape) == "split" {
        // Production shape: text prompt then call→result interleaved
        // (upstream rejects grouped layouts).
        msgs.push(asst[0].clone());
        msgs.push(asst[1].clone());
        msgs.push(tool_result_msg(
            "chatcmpl-tool-aaa1",
            "a.txt\nb.txt\nREADME.md",
        ));
        msgs.push(asst[2].clone());
        msgs.push(tool_result_msg("chatcmpl-tool-bbb2", "# hello\n"));
    } else {
        msgs.extend(asst.iter().cloned());
        msgs.push(tool_result_msg(
            "chatcmpl-tool-aaa1",
            "a.txt\nb.txt\nREADME.md",
        ));
        if fs.str(shape) == "merged" {
            msgs.push(tool_result_msg("chatcmpl-tool-bbb2", "# hello\n"));
        }
    }
    msgs.push(user_msg("What files did you see? One line."));
    let req = pb::GetChatMessageRequest {
        metadata: MessageField::some(metadata(ctx, true)),
        prompt: Some("You are a helpful assistant.".to_string()),
        chat_model_uid: Some(model),
        request_type: Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE),
        configuration: MessageField::some(default_completion_config()),
        cascade_id: Some(randid::uuid()),
        planner_mode: Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT),
        execution_id: Some(randid::uuid()),
        trajectory_reference: MessageField::some(pb::ExaCortexPb_CortexTrajectoryReference {
            trajectory_id: Some(randid::uuid()),
            trajectory_type: Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE),
            step_type: Some(pb::ExaCortexPb_CortexStepType::ExaCortexPb_CortexStepType_CORTEX_STEP_TYPE_USER_INPUT),
            ..Default::default()
        }),
        chat_message_prompts: msgs,
        tools: vec![
            pb::ExaChatPb_ChatToolDefinition {
                name: Some("exec".to_string()),
                description: Some("run a command".to_string()),
                json_schema_string: Some(r#"{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}"#.to_string()),
                ..Default::default()
            },
            pb::ExaChatPb_ChatToolDefinition {
                name: Some("read_file".to_string()),
                description: Some("read a file".to_string()),
                json_schema_string: Some(r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#.to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    run_stream(client, &req, false, "").await
}

// ---- rerun ----------------------------------------------------------------------

async fn cmd_rerun(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("rerun", ErrorHandling::ContinueOnError);
    let file = fs.string(
        "file",
        "",
        "protojson GetChatMessageRequest (logs/*/03-devin-request.json)",
    );
    let n = fs.int("n", 8, "");
    fs.parse(args).map_err(|e| anyhow!("{e}"))?;
    let raw = std::fs::read(fs.str(file))?;
    let mut base: pb::GetChatMessageRequest =
        serde_json::from_slice(&raw).map_err(|e| anyhow!("unmarshal {}: {e}", fs.str(file)))?;
    // The captured apiKey may have rotated; override with the live token.
    if let Some(m) = base.metadata.as_option_mut() {
        m.api_key = Some(ctx.token.clone());
    }
    for run in 0..fs.get_int(n) {
        let mut req = base.clone();
        req.execution_id = Some(randid::uuid());
        let mut stream = match client
            .get_chat_message_with_options(req, call_options())
            .await
        {
            Ok(s) => s,
            Err(e) => {
                println!("run {run}: connect: {e}");
                continue;
            }
        };
        let mut stop = "UNSPECIFIED".to_string();
        let mut text = String::new();
        let mut calls = 0usize;
        loop {
            match stream.message::<pb::GetChatMessageResponse>().await {
                Ok(Some(frame)) => {
                    let m = frame.to_owned_message();
                    if let Some(reason) = m.stop_reason {
                        let s = reason.proto_name();
                        if !s.ends_with("UNSPECIFIED") {
                            // Enum names may lack the STOP_REASON_ prefix
                            // for unknown values — fall back to the raw
                            // string like Go's LastIndex guard.
                            stop = s.rfind("STOP_REASON_").map_or_else(
                                || s.to_string(),
                                |i| s[i + "STOP_REASON_".len()..].to_string(),
                            );
                        }
                    }
                    if let Some(v) = &m.delta_text {
                        text.push_str(v);
                    }
                    calls += m.delta_tool_calls.len();
                }
                Ok(None) => break,
                Err(e) => {
                    println!("run {run}: stream: {e}");
                    break;
                }
            }
        }
        if stream.error().is_some() {
            continue;
        }
        let tail = if text.len() > 90 {
            String::from_utf8_lossy(&text.as_bytes()[text.len() - 90..]).to_string()
        } else {
            text.clone()
        };
        println!("run {run}: stop={stop} calls={calls} tail={tail:?}");
    }
    Ok(())
}

// ---- bigctx ---------------------------------------------------------------------

async fn cmd_bigctx(ctx: &ProbeContext, client: &Client, args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("bigctx", ErrorHandling::ContinueOnError);
    let kb = fs.int("kb", 1024, "");
    let model = fs.string("model", "swe-2-max", "");
    fs.parse(args).map_err(|e| anyhow!("{e}"))?;
    let model = alias_model(ctx, &fs.str(model));
    let filler = "lorem ipsum dolor sit amet "
        .repeat(usize::try_from(fs.get_int(kb).max(0)).unwrap_or_default() * 1024 / 27);
    let req = pb::GetChatMessageRequest {
        metadata: MessageField::some(metadata(ctx, true)),
        prompt: Some("You are a helpful assistant.".to_string()),
        chat_model_uid: Some(model),
        request_type: Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE),
        configuration: MessageField::some(default_completion_config()),
        execution_id: Some(randid::uuid()),
        chat_message_prompts: vec![pb::ExaChatPb_ChatMessagePrompt {
            message_id: Some(randid::uuid()),
            source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
            prompt: Some(format!("{filler}\nReply: ok")),
            ..Default::default()
        }],
        ..Default::default()
    };
    run_stream(client, &req, false, "").await
}

// ---- misc -----------------------------------------------------------------------

// Mirrors the Go probe misc flow for parity review.
#[allow(clippy::too_many_lines)]
async fn cmd_misc(ctx: &ProbeContext, client: &Client) -> anyhow::Result<()> {
    match client
        .get_embeddings_with_options(
            pb::GetEmbeddingsRequest {
                request: MessageField::some(pb::ExaCodeiumCommonPb_EmbeddingsRequest {
                    prompts: vec!["hello world".to_string()],
                    model: Some(
                        pb::ExaCodeiumCommonPb_Model::ExaCodeiumCommonPb_Model_MODEL_EMBED_6591,
                    ),
                    ..Default::default()
                }),
                embedding_model: Some(
                    pb::ExaCodeiumCommonPb_Model::ExaCodeiumCommonPb_Model_MODEL_EMBED_6591,
                ),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => {
            println!("GetEmbeddings ERR: {e}");
            dump_connect_err(&e);
        }
        Ok(emb) => {
            let body = j(&emb.into_owned());
            println!("GetEmbeddings: {}", trunc(&body, 400));
        }
    }

    match client
        .get_streaming_external_chat_completions_with_options(
            pb::GetChatCompletionsRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                chat_message_prompts: vec![pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER),
                    prompt: Some("say hi".to_string()),
                    ..Default::default()
                }],
                system_prompt: Some("You are helpful.".to_string()),
                completions_request: MessageField::some(pb::ExaCodeiumCommonPb_CompletionsRequest {
                    configuration: MessageField::some(pb::ExaCodeiumCommonPb_CompletionConfiguration {
                        num_completions: Some(1),
                        max_tokens: Some(64),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("GetStreamingExternalChatCompletions connect ERR: {e}"),
        Ok(mut st) => {
            let mut n = 0;
            loop {
                match st
                    .message::<pb::GetStreamingExternalChatCompletionsResponse>()
                    .await
                {
                    Ok(Some(frame)) => {
                        n += 1;
                        let body = j(&frame.to_owned_message());
                        println!("extchat frame {n}: {}", trunc(&body, 300));
                        if n > 8 {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        println!("extchat stream err: {e}");
                        break;
                    }
                }
            }
        }
    }

    match client
        .get_status_with_options(
            pb::GetStatusRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("GetStatus ERR: {e}"),
        Ok(r) => println!("GetStatus: {}", trunc(&j(&r.into_owned()), 600)),
    }
    match client
        .get_config_with_options(pb::GetConfigRequest::default(), call_options())
        .await
    {
        Err(e) => println!("GetConfig ERR: {e}"),
        Ok(r) => println!("GetConfig: {}", trunc(&j(&r.into_owned()), 600)),
    }
    match client
        .get_command_model_configs_with_options(
            pb::GetCommandModelConfigsRequest {
                metadata: MessageField::some(metadata(ctx, true)),
                ..Default::default()
            },
            call_options(),
        )
        .await
    {
        Err(e) => println!("GetCommandModelConfigs ERR: {e}"),
        Ok(r) => {
            let uids: Vec<String> = r
                .into_owned()
                .client_model_configs
                .iter()
                .map(|c| c.model_uid.clone().unwrap_or_default())
                .collect();
            println!("GetCommandModelConfigs uids: [{}]", uids.join(" "));
        }
    }
    Ok(())
}

// ---- edge ------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn cmd_edge(ctx: &ProbeContext, client: &Client, argv: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("edge", ErrorHandling::ContinueOnError);
    let model = fs.string("model", "swe-2-max", "");
    let image_file = fs.string("image-file", "", "png file to attach instead of tinyPNG");
    let prompt = fs.string("prompt", "", "user prompt text for prompt-driven cases");
    fs.parse(argv).map_err(|e| anyhow!("{e}"))?;
    let model = alias_model(ctx, &fs.str(model));
    let mut image_b64 = tiny_png();
    if !fs.str(image_file).is_empty() {
        use base64::Engine;
        let raw = std::fs::read(fs.str(image_file))?;
        image_b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    }
    let case_args = fs.args().to_vec();
    if case_args.is_empty() {
        return Err(anyhow!("edge needs a case name"));
    }
    let mut req = pb::GetChatMessageRequest {
        metadata: MessageField::some(metadata(ctx, true)),
        prompt: Some("You are a helpful assistant.".to_string()),
        chat_model_uid: Some(model),
        request_type: Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE),
        configuration: MessageField::some(default_completion_config()),
        planner_mode: Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT),
        execution_id: Some(randid::uuid()),
        ..Default::default()
    };
    let user = pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER;
    let system_src = pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM;
    let tool_src = pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL;
    match case_args[0].as_str() {
        "orphan-tool-result" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(tool_src),
                    prompt: Some("orphan result text".to_string()),
                    ..Default::default()
                },
                user_msg("what did the tool return?"),
            ];
        }
        "unknown-source" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_UNKNOWN),
                    prompt: Some("mystery".to_string()),
                    ..Default::default()
                },
                user_msg("continue"),
            ];
        }
        "dup-message-id" => {
            let dup = randid::uuid();
            let mut first = user_msg("hi");
            let mut second = user_msg("second message");
            first.message_id = Some(dup.clone());
            second.message_id = Some(dup);
            req.chat_message_prompts = vec![first, second];
        }
        "empty-user-prompt" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(user),
                    ..Default::default()
                },
                user_msg("continue"),
            ];
        }
        "empty-assistant" => {
            req.chat_message_prompts = vec![
                user_msg("Say hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(system_src),
                    prompt: Some(String::new()),
                    ..Default::default()
                },
                user_msg("continue"),
            ];
        }
        "experiment" => {
            req.experiment_config = MessageField::some(pb::ExaCodeiumCommonPb_ExperimentConfig {
                force_enable_experiment_strings: vec!["bogus_exp_xyz".to_string()],
                force_disable_experiment_strings: vec!["another_bogus".to_string()],
                ..Default::default()
            });
            req.chat_message_prompts = vec![user_msg("Reply exactly: pong")];
        }
        "trailing-assistant" => {
            req.chat_message_prompts =
                vec![user_msg("List two colors."), assistant_text_msg("1. Blue")];
        }
        "trailing-tool-result" => {
            req.chat_message_prompts = vec![
                user_msg("Read file a.txt"),
                assistant_call_msg("call_1", "read_file", r#"{"path":"a.txt"}"#),
                tool_result_msg("call_1", "file contents here"),
            ];
        }
        "thinking-only-assistant" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(system_src),
                    thinking: Some("I should greet politely.".to_string()),
                    ..Default::default()
                },
                user_msg("continue"),
            ];
        }
        "thinking-empty-sig" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(system_src),
                    prompt: Some("sure".to_string()),
                    signature: Some("sealed.v1.ZmFrZSBmb3IgdGVzdA".to_string()),
                    thinking_redacted: Some(true),
                    ..Default::default()
                },
                user_msg("continue"),
            ];
        }
        "interleaved-calls" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt then b.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                tool_result_msg("c1", "aaa"),
                assistant_call_msg("c2", "read_file", r#"{"path":"b.txt"}"#),
                tool_result_msg("c2", "bbb"),
                user_msg("what did you find?"),
            ];
        }
        "grouped-calls-results" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt then b.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                assistant_call_msg("c2", "read_file", r#"{"path":"b.txt"}"#),
                tool_result_msg("c1", "aaa"),
                tool_result_msg("c2", "bbb"),
                user_msg("what did you find?"),
            ];
        }
        "trailing-call-no-result" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
            ];
        }
        "dup-tool-result" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                tool_result_msg("c1", "first"),
                tool_result_msg("c1", "second"),
                user_msg("ok?"),
            ];
        }
        "tool-result-mismatch-call" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                tool_result_msg("zzz", "orphan"),
                user_msg("ok?"),
            ];
        }
        "orphan-result-with-id" => {
            req.chat_message_prompts = vec![
                user_msg("hi"),
                tool_result_msg("zzz", "orphan"),
                user_msg("what did the tool return?"),
            ];
        }
        "tool-call-invalid-json-arg" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", "{bad json"),
                user_msg("ok?"),
            ];
        }
        "tool-name" => {
            if case_args.len() < 2 {
                return Err(anyhow!(
                    "tool-name needs a name argument (use empty string for empty)"
                ));
            }
            req.chat_message_prompts = vec![user_msg("call the tool now")];
            req.tools = vec![pb::ExaChatPb_ChatToolDefinition {
                name: Some(case_args[1].clone()),
                description: Some("test tool".to_string()),
                json_schema_string: Some(r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#.to_string()),
                ..Default::default()
            }];
            req.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
                choice: Some(oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(
                    "required".to_string(),
                )),
                ..Default::default()
            });
        }
        "gap-tool-result" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                user_msg("[system] reminder: be concise"),
                tool_result_msg("c1", "file contents here"),
                user_msg("what did you find?"),
            ];
        }
        "dup-call-id" => {
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                assistant_call_msg("c1", "read_file", r#"{"path":"a.txt"}"#),
                tool_result_msg("c1", "file contents"),
                user_msg("ok?"),
            ];
        }
        "tool-result-image" => {
            let mut tr = tool_result_msg("c1", "screenshot attached");
            tr.images = vec![pb::ExaCodeiumCommonPb_ImageData {
                base64_data: Some(image_b64.clone()),
                mime_type: Some("image/png".to_string()),
                ..Default::default()
            }];
            req.chat_message_prompts = vec![
                user_msg("Take a screenshot then tell me the dominant color."),
                assistant_call_msg("c1", "take_screenshot", "{}"),
                tr,
                user_msg("What color is it? Answer in one word."),
            ];
        }
        "user-image-file" => {
            let mut m =
                user_msg("What is the dominant color of the attached image? Answer in one word.");
            m.images = vec![pb::ExaCodeiumCommonPb_ImageData {
                base64_data: Some(image_b64.clone()),
                mime_type: Some("image/png".to_string()),
                ..Default::default()
            }];
            req.chat_message_prompts = vec![m];
        }
        "user-image-prompt" => {
            let mut text = fs.str(prompt);
            if text.is_empty() {
                text = "What is the dominant color of the attached image? Answer in one word."
                    .to_string();
            }
            let mut m = user_msg(&text);
            m.images = vec![pb::ExaCodeiumCommonPb_ImageData {
                base64_data: Some(image_b64.clone()),
                mime_type: Some("image/png".to_string()),
                ..Default::default()
            }];
            req.chat_message_prompts = vec![m];
        }
        "pdf-as-image" => {
            let mut m = user_msg("What is in this document? One sentence.");
            m.images = vec![pb::ExaCodeiumCommonPb_ImageData {
                base64_data: Some(tiny_png()),
                mime_type: Some("application/pdf".to_string()),
                ..Default::default()
            }];
            req.chat_message_prompts = vec![m];
        }
        "custom-tool-call-flag" => {
            req.chat_message_prompts = vec![
                user_msg(
                    "Apply this patch: *** Begin Patch\n*** Update File: x.go\n@@\n+x\n*** End Patch",
                ),
                pb::ExaChatPb_ChatMessagePrompt {
                    message_id: Some(randid::uuid()),
                    source: Some(system_src),
                    tool_calls: vec![pb::ExaCodeiumCommonPb_ChatToolCall {
                        id: Some("c1".to_string()),
                        name: Some("apply_patch".to_string()),
                        is_custom_tool_call: Some(true),
                        invalid_json_str: Some(
                            "*** Begin Patch\n*** Update File: x.go\n@@\n+x\n*** End Patch"
                                .to_string(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                tool_result_msg("c1", "applied"),
                user_msg("did it apply?"),
            ];
        }
        "parallel-call-id-frames" => {
            req.chat_message_prompts = vec![user_msg(
                "Call read_file twice in the same turn: once with path a.txt, once with path b.txt. Issue both tool calls together, not sequentially.",
            )];
            req.tools = vec![pb::ExaChatPb_ChatToolDefinition {
                name: Some("read_file".to_string()),
                description: Some("read_file".to_string()),
                json_schema_string: Some(r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#.to_string()),
                ..Default::default()
            }];
            req.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
                choice: Some(oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(
                    "auto".to_string(),
                )),
                ..Default::default()
            });
            return run_stream(client, &req, true, "").await;
        }
        "n-tools-limit" => {
            if case_args.len() < 2 {
                return Err(anyhow!("n-tools-limit needs a count argument"));
            }
            let n: usize = case_args[1]
                .parse()
                .ok()
                .filter(|n: &usize| *n >= 1)
                .ok_or_else(|| anyhow!("n-tools-limit bad count {:?}", case_args[1]))?;
            req.chat_message_prompts = vec![user_msg("Call tool_0 now.")];
            for i in 0..n {
                req.tools.push(pb::ExaChatPb_ChatToolDefinition {
                    name: Some(format!("tool_{i}")),
                    description: Some("t".to_string()),
                    json_schema_string: Some(r#"{"type":"object"}"#.to_string()),
                    ..Default::default()
                });
            }
            req.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
                choice: Some(oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(
                    "required".to_string(),
                )),
                ..Default::default()
            });
        }
        "history-tool-name" => {
            let name = if case_args.len() >= 2 {
                case_args[1].as_str()
            } else {
                "a.b"
            };
            req.chat_message_prompts = vec![
                user_msg("Read a.txt"),
                assistant_call_msg("c1", name, r#"{"path":"a.txt"}"#),
                tool_result_msg("c1", "file contents"),
                user_msg("ok?"),
            ];
        }
        other => return Err(anyhow!("unknown edge case {other:?}")),
    }
    run_stream(client, &req, false, "").await
}

// ---- dispatch ----------------------------------------------------------------------

/// Run one probe subcommand. `args` are the arguments after the
/// subcommand name.
///
/// # Errors
///
/// Propagates flag-parse, request-build and connect errors; the caller
/// prints `ERR: <e>` and exits 1.
pub async fn run(
    ctx: &ProbeContext,
    client: &Client,
    command: &str,
    args: &[String],
) -> anyhow::Result<()> {
    match command {
        "configs" => cmd_configs(ctx, client).await,
        "status" => cmd_status(ctx, client).await,
        "assign" => cmd_assign(ctx, client, args).await,
        "chat" => cmd_chat(ctx, client, args).await,
        "replay" => cmd_replay(ctx, client, args).await,
        "hist" => cmd_hist(ctx, client, args).await,
        "rerun" => cmd_rerun(ctx, client, args).await,
        "bigctx" => cmd_bigctx(ctx, client, args).await,
        "misc" => cmd_misc(ctx, client).await,
        "edge" => cmd_edge(ctx, client, args).await,
        other => Err(anyhow!("unknown subcommand {other:?}")),
    }
}
