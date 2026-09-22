//! Upstream request projection and transcript repair.
//!
//! Port of `G/internal/adapter/devin/request_encoder.go` plus the
//! `G/internal/upstream` metadata builder: projects the intermediate
//! `RequestMessages` into a Devin Connect `GetChatMessageRequest` —
//! metadata/completion parameters, session trajectory ID derivation,
//! per-message content conversion (text/thinking/tool calls/tool
//! results/images) and tool call→result pairing repair. Response-side
//! decoding lives in `response.rs` (task 9).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{LazyLock, Mutex};

use devin_proto::buffa::MessageField;
use devin_proto::generated::exa::api_server_pb as pb;
use devin_proto::generated::exa::api_server_pb::__buffa::oneof;
use sha2::Digest as _;

use crate::domain::failure::Failure;
use crate::domain::request::{Content, Message, RequestMessages, RequestRepairs, ToolChoiceMode};
use crate::randid;

/// Default client identity constants, field-aligned with real Devin CLI
/// captures; `devin.client_*` config keys can override if upstream starts
/// gating on `extension_version`.
pub const DEFAULT_CLIENT_NAME: &str = "chisel";
pub const DEFAULT_CLIENT_VERSION: &str = "3000.2.17";
pub const DEFAULT_CLIENT_OS: &str = "mac";

/// Client identity carried in request metadata; empty fields fall back to
/// the captured CLI defaults — port of `Config.ClientIdentity`.
#[derive(Debug, Clone, Default)]
pub struct ClientIdentity {
    /// `extension_name`/`ide_name`; empty falls back to `chisel`.
    pub name: String,
    /// `extension_version`/`ide_version`; empty falls back to `3000.2.17`.
    pub version: String,
    /// `os`; empty falls back to `mac`.
    pub os: String,
}

impl ClientIdentity {
    /// Resolves the identity to send: trimmed fields with captured-CLI
    /// fallbacks — Go `Config.ClientIdentity()`.
    pub fn resolve(&self) -> ResolvedIdentity {
        let name = self.name.trim();
        let version = self.version.trim();
        let os = self.os.trim();
        ResolvedIdentity {
            name: if name.is_empty() {
                DEFAULT_CLIENT_NAME
            } else {
                name
            }
            .to_string(),
            version: if version.is_empty() {
                DEFAULT_CLIENT_VERSION
            } else {
                version
            }
            .to_string(),
            os: if os.is_empty() { DEFAULT_CLIENT_OS } else { os }.to_string(),
        }
    }
}

/// The resolved (non-empty) client identity for one request.
#[derive(Debug, Clone)]
pub struct ResolvedIdentity {
    /// Client name (`chisel`).
    pub name: String,
    /// Client version (`3000.2.17`).
    pub version: String,
    /// Client OS (`mac`).
    pub os: String,
}

impl Default for ResolvedIdentity {
    fn default() -> Self {
        Self {
            name: DEFAULT_CLIENT_NAME.to_string(),
            version: DEFAULT_CLIENT_VERSION.to_string(),
            os: DEFAULT_CLIENT_OS.to_string(),
        }
    }
}

/// Per-call binding for one `GetChatMessage` invocation: `model` is the
/// upstream model uid after alias/router rewriting, `token` is the
/// credential fetched at call time (unauthenticated self-heal swaps it),
/// `model_assignment_jwt` is the `AssignModel` router jwt bound to this
/// request. All three vary per call, separate from static config — a retry
/// only needs to swap `binding.token`.
#[derive(Debug, Clone, Default)]
pub struct CallBinding {
    /// Upstream credential for this attempt.
    pub token: String,
    /// Resolved upstream model uid.
    pub model: String,
    /// `AssignModel` router jwt bound to this request's cascade id.
    pub model_assignment_jwt: String,
}

/// `upstream.BuildMetadata`: the common request `Metadata` header. `os`
/// and `fingerprint_bytes` come from the caller's mimicked client shape
/// (chisel CLI is mac/366-byte fingerprint, Windsurf shape is win/32, 0
/// omits the fingerprint). `f` is device telemetry only.
pub fn build_metadata(
    token: &str,
    client_name: &str,
    client_version: &str,
    os: &str,
    fingerprint_bytes: usize,
) -> pb::ExaCodeiumCommonPb_Metadata {
    let mut metadata = pb::ExaCodeiumCommonPb_Metadata {
        api_key: Some(token.to_string()),
        extension_name: Some(client_name.to_string()),
        extension_version: Some(client_version.to_string()),
        ide_name: Some(client_name.to_string()),
        ide_version: Some(client_version.to_string()),
        locale: Some("en".to_string()),
        os: Some(os.to_string()),
        ..Default::default()
    };
    if fingerprint_bytes > 0 {
        metadata.f = Some(randid::hex(fingerprint_bytes));
    }
    metadata
}

/// Projects an intermediate request into the upstream wire format: static
/// identity comes from `identity`, per-call credentials/routing from
/// `binding`. Returns the repairs recorded during conversion (silent
/// repair counts persisted with the request log).
// One projection pass mirroring Go's requestEncoder for parity review.
#[allow(clippy::too_many_lines)]
pub fn build_request(
    request: &RequestMessages,
    identity: &ClientIdentity,
    binding: &CallBinding,
) -> Result<(pb::GetChatMessageRequest, RequestRepairs), Failure> {
    let mut repairs = RequestRepairs::default();
    // Upstream trajectory identity is reused per session: consecutive
    // requests of one session share stable trajectory/cascade IDs for
    // steadier cache hits (measured ~7/8 vs all-random variance); cache
    // matching itself is keyed on "account + content prefix" — the IDs do
    // not participate in matching.
    let (trajectory_id, cascade_id) = derive_session_ids(request);
    let execution_id = randid::uuid();
    let resolved = identity.resolve();
    let metadata = build_metadata(
        &binding.token,
        &resolved.name,
        &resolved.version,
        &resolved.os,
        366,
    );
    // tool_choice=none is a real upstream execution disable (the model
    // self-reports "tools are disabled"): tool declarations and
    // description injection are unusable noise for the model and stay off
    // the wire.
    let no_tools = request
        .tool_choice
        .as_ref()
        .is_some_and(|choice| choice.mode == ToolChoiceMode::None);
    let system_prompt = if no_tools {
        request.system_prompt.clone()
    } else {
        super::tool_definition::with_tool_descriptions(&request.system_prompt, &request.tools)
    };
    let mut completion = pb::ExaCodeiumCommonPb_CompletionConfiguration {
        num_completions: Some(1),
        max_tokens: Some(128_000),
        max_newlines: Some(400),
        temperature: Some(1.0),
        top_k: Some(40),
        top_p: Some(0.95),
        ..Default::default()
    };
    // Client-provided sampling parameters pass through; absent values keep
    // the CLI defaults.
    if let Some(max_tokens) = request.max_tokens
        && max_tokens > 0
    {
        completion.max_tokens = Some(max_tokens.cast_unsigned());
    }
    if let Some(temperature) = request.temperature {
        completion.temperature = Some(temperature);
    }
    if let Some(top_p) = request.top_p {
        completion.top_p = Some(top_p);
    }
    if let Some(top_k) = request.top_k {
        completion.top_k = Some(top_k.cast_unsigned());
    }
    if !request.stop_sequences.is_empty() {
        completion.stop_patterns.clone_from(&request.stop_sequences);
    }
    if let Some(seed) = request.seed {
        completion.seed = Some(seed.cast_unsigned());
    }
    let mut result = pb::GetChatMessageRequest {
        metadata: MessageField::some(metadata),
        prompt: Some(system_prompt),
        // Upstream prompt prefix cache: the system prompt is a stable
        // prefix, marked with an EPHEMERAL breakpoint.
        system_prompt_cache_options: MessageField::some(ephemeral_cache_options()),
        chat_model_uid: Some(binding.model.clone()),
        request_type: Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE),
        configuration: MessageField::some(completion),
        trajectory_reference: MessageField::some(pb::ExaCortexPb_CortexTrajectoryReference {
            trajectory_id: Some(trajectory_id.clone()),
            // step_index is the real CLI's in-session monotonic step
            // (packet capture); omitting it is a residual wire-shape
            // difference. Accounted per trajectory_id.
            step_index: Some(next_step_index(&trajectory_id)),
            trajectory_type: Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE),
            step_type: Some(pb::ExaCortexPb_CortexStepType::ExaCortexPb_CortexStepType_CORTEX_STEP_TYPE_USER_INPUT),
            ..Default::default()
        }),
        cascade_id: Some(cascade_id),
        planner_mode: Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT),
        execution_id: Some(execution_id),
        ..Default::default()
    };
    // Upstream-verified: option_name accepts none/auto/required;
    // Anthropic's "any" is already normalized to required at this layer.
    // auto is not sent, matching upstream default behavior.
    if let Some(choice) = &request.tool_choice {
        match choice.mode {
            ToolChoiceMode::None | ToolChoiceMode::Required => {
                result.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
                    choice: Some(oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(
                        choice.mode.as_str().to_string(),
                    )),
                    ..Default::default()
                });
            }
            ToolChoiceMode::Named => {
                // A named call must hit the tools table: upstream only
                // answers a vague invalid_argument for a missing target —
                // report a readable error locally first.
                let found = request
                    .tools
                    .iter()
                    .any(|tool| tool.name == choice.tool_name);
                if !found {
                    return Err(Failure::invalid_argument(format!(
                        "tool_choice names tool {:?} which is not in the tools list",
                        choice.tool_name
                    )));
                }
                result.tool_choice = MessageField::some(pb::ExaChatPb_ChatToolChoice {
                    choice: Some(oneof::exa_chat_pb_chat_tool_choice::Choice::ToolName(
                        choice.tool_name.clone(),
                    )),
                    ..Default::default()
                });
            }
            ToolChoiceMode::Auto => {}
        }
    }
    // Upstream accepts but does not enforce this constraint (parallel
    // calls still arrive); shape-aligned only.
    if request.disable_parallel_tool_calls {
        result.disable_parallel_tool_calls = Some(true);
    }
    // Devin/Cascade reliably accepts only "current turn" images; history
    // images in Images get invalid_argument. Current turn = every
    // user/tool message after the last AssistantMessage. Anthropic
    // clients often put image and tool_result in one user message, which
    // decodes into separate UserMessage + ToolResultMessage; attaching
    // only to the last would lose the image.
    let last_assistant_index = request
        .messages
        .iter()
        .rposition(|message| matches!(message, Message::Assistant(_)))
        .map_or(-1i64, |index| i64::try_from(index).unwrap_or(i64::MAX));
    for (index, message) in request.messages.iter().enumerate() {
        let converted = convert_message(
            message,
            i64::try_from(index).unwrap_or(i64::MAX) > last_assistant_index,
            &mut repairs,
        );
        // A completely empty assistant message is skipped (upstream
        // degrades on empty replies) and counted as a repair.
        if matches!(message, Message::Assistant(_)) && converted.is_empty() {
            repairs.dropped_empty_assistant += 1;
        }
        result.chat_message_prompts.extend(converted);
    }
    // Upstream requires call→result adjacency: every tool call an
    // assistant issued must be followed immediately by its TOOL result or
    // invalid_argument. Client histories (OpenAI/Anthropic) are grouped
    // "all calls → all results"; reorder by call id into interleaved
    // pairs.
    let (paired, reordered) = pair_tool_calls_with_results(&result.chat_message_prompts);
    result.chat_message_prompts = paired;
    repairs.reordered_prompts = reordered;
    if !no_tools {
        for tool in &request.tools {
            let converted = super::tool_definition::convert_tool_definition(tool)?;
            result.tools.push(converted);
        }
    }
    // The last message gets an EPHEMERAL breakpoint: the cache covers the
    // whole history prefix up to it, so next turn's new messages appended
    // after the breakpoint hit the cache.
    if let Some(last) = result.chat_message_prompts.last_mut() {
        last.prompt_cache_options = MessageField::some(ephemeral_cache_options());
    }
    // The router uid's AssignModel jwt binds this cascade_id, matching the
    // real CLI's GetChatMessage shape (see resolveModelRouting).
    if !binding.model_assignment_jwt.is_empty() {
        result.model_assignment_jwt = Some(binding.model_assignment_jwt.clone());
    }
    Ok((result, repairs))
}

/// The upstream prompt-cache EPHEMERAL breakpoint marker.
fn ephemeral_cache_options() -> pb::ExaChatPb_PromptCacheOptions {
    pb::ExaChatPb_PromptCacheOptions {
        r#type: Some(
            pb::ExaChatPb_CacheControlType::ExaChatPb_CacheControlType_CACHE_CONTROL_TYPE_EPHEMERAL,
        ),
        ..Default::default()
    }
}

/// Derives the upstream trajectory/cascade IDs for one request.
/// `session_key` (CC `metadata.user_id` containing `session_id`, Codex
/// `prompt_cache_key` at thread level) is itself session-scoped and seeds
/// directly — compaction rewriting message content does not break
/// trajectory continuity. Without a session key, falls back to a content
/// hash of "system prompt head 4KB + first message text head 1KB":
/// multi-turn replays of one session share the prefix → stable; distinct
/// sessions → naturally spread.
///
/// Truncation is at byte offsets exactly like Go (`head[:4096]`,
/// `text[:1024]`), which can cut a multi-byte rune — the seed is bytes,
/// not chars.
pub fn derive_session_ids(request: &RequestMessages) -> (String, String) {
    let mut seed: Vec<u8> = Vec::new();
    if request.session_key.is_empty() {
        let head = request.system_prompt.as_bytes();
        let head = &head[..head.len().min(4096)];
        seed.extend_from_slice(head);
        for message in &request.messages {
            let text = first_message_text(message);
            if text.is_empty() {
                continue;
            }
            let text = text.as_bytes();
            let text = &text[..text.len().min(1024)];
            seed.push(0);
            seed.extend_from_slice(text);
            break;
        }
    } else {
        seed.extend_from_slice(request.session_key.as_bytes());
    }
    let sum: [u8; 32] = sha2::Sha256::digest(&seed).into();
    (uuid_from_bytes(&sum[..16]), uuid_from_bytes(&sum[16..32]))
}

/// Extracts a message's first text block for the session seed.
fn first_message_text(message: &Message) -> &str {
    let content = match message {
        Message::User(typed) => &typed.content,
        Message::Assistant(typed) => &typed.content,
        Message::ToolResult(typed) => &typed.content,
    };
    for block in content {
        if let Content::Text(text) = block
            && !text.text.is_empty()
        {
            return &text.text;
        }
    }
    ""
}

/// Formats 16 bytes as a UUID string (version/variant bits forced).
fn uuid_from_bytes(bytes: &[u8]) -> String {
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes[..16]);
    randid::format_uuid(out)
}

/// Per-trajectory count of upstream steps already sent: the real CLI
/// sends a monotonically increasing in-session `step_index` per request
/// (packet capture); omitting it is a residual wire-shape difference.
/// Counts reset on process restart, matching CLI restart behavior; the
/// capacity cap prevents session counts accumulating into an unbounded
/// map — hitting it clears the whole table, a harmless bookkeeping reset.
static STEP_INDEX_REGISTRY: LazyLock<Mutex<HashMap<String, i32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The next in-session monotonic step for this trajectory; when the table
/// grows unboundedly it is cleared wholesale and counting restarts (65536
/// entries ≈ tens of thousands of concurrent sessions; clearing only
/// restarts steps from 1 — upstream does not verify cross-request
/// continuity).
fn next_step_index(trajectory_id: &str) -> i32 {
    let mut counts = STEP_INDEX_REGISTRY
        .lock()
        .expect("step index registry poisoned");
    if counts.len() >= 65536 {
        counts.clear();
    }
    let count = counts.entry(trajectory_id.to_string()).or_insert(0);
    *count += 1;
    *count
}

/// Converts an intermediate message into Devin `ChatMessagePrompt`s.
/// `attach_images` gates whether `ImageContent` lands in `Images` (latest
/// user turn only); history images become text placeholders. `repairs`
/// accumulates silent repairs made during conversion (history image
/// stripping etc.).
///
/// Go's `convertMessage` error arm covers message kinds outside the
/// interface — unreachable for the Rust enum, so this is infallible.
fn convert_message(
    message: &Message,
    attach_images: bool,
    repairs: &mut RequestRepairs,
) -> Vec<pb::ExaChatPb_ChatMessagePrompt> {
    match message {
        Message::User(typed) => vec![prompt_for_content(
            pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER,
            &typed.content,
            attach_images,
            repairs,
        )],
        Message::Assistant(typed) => {
            // Wire evidence (chisel 3000.2.17 capture): one assistant turn
            // merges into a single prompt — prompt/thinking/signature/
            // toolCalls carried in one body, the prompt field absent when
            // there is no text; the real client never produces adjacent
            // SYSTEM messages. Splitting introduces turn boundaries in the
            // rendered context and the model samples EOS early after
            // "announcement" text.
            let mut signature = String::new();
            let mut signature_type = String::new();
            let mut redacted = false;
            let mut text = String::new();
            let mut thinking = String::new();
            let mut calls: Vec<&crate::domain::request::ToolCall> = Vec::new();
            for block in &typed.content {
                match block {
                    Content::Text(typed) => text.push_str(&typed.text),
                    Content::Thinking(typed) => {
                        // One assistant message can carry multiple thinking
                        // blocks (interleaved); the wire model has a single
                        // thinking per prompt — concatenate in order, keep
                        // the last non-empty signature.
                        if !thinking.is_empty() && !typed.thinking.is_empty() {
                            thinking.push('\n');
                        }
                        thinking.push_str(&typed.thinking);
                        if !typed.thinking_signature.is_empty() {
                            signature.clone_from(&typed.thinking_signature);
                            signature_type.clone_from(&typed.signature_type);
                        }
                        redacted = redacted || typed.redacted;
                    }
                    Content::ToolCall(call) => calls.push(call),
                    Content::Image(_) => {}
                }
            }
            // A completely empty assistant message induces repeated empty
            // replies upstream — skip it. The emptiness check covers every
            // replayable artifact: thinking/signature/redacted/output_id/
            // calls — any one present is non-empty; under the openai
            // regime a "signature-only thinking block" is a legal payload
            // (what decodeLateSignature synthesizes).
            if text.is_empty()
                && thinking.is_empty()
                && calls.is_empty()
                && signature.is_empty()
                && !redacted
                && typed.output_id.is_empty()
            {
                return Vec::new();
            }
            let mut prompt = pb::ExaChatPb_ChatMessagePrompt {
                message_id: Some(randid::uuid()),
                source: Some(assistant_source()),
                ..Default::default()
            };
            if !text.is_empty() {
                prompt.prompt = Some(text);
            }
            // signature_type and output_id must replay verbatim with the
            // signature: a mismatched signature_type triggers upstream
            // invalid_argument (verified).
            if !thinking.is_empty()
                || redacted
                || !signature.is_empty()
                || !typed.output_id.is_empty()
            {
                if !thinking.is_empty() {
                    prompt.thinking = Some(thinking);
                }
                if !signature.is_empty() {
                    prompt.signature = Some(signature);
                }
                if !signature_type.is_empty() {
                    prompt.signature_type = Some(signature_type);
                }
                if !typed.output_id.is_empty() {
                    prompt.output_id = Some(typed.output_id.clone());
                }
                prompt.thinking_redacted = Some(redacted);
            }
            for call in calls {
                let mut tool_call = pb::ExaCodeiumCommonPb_ChatToolCall {
                    id: Some(call.id.clone()),
                    // History call names go up verbatim: the upstream
                    // charset gate only checks the tools declaration —
                    // history names with . / : / CJK are accepted
                    // (probe edge history-tool-name); ToolCall::validate
                    // does not check charset either, so names arriving
                    // here can contain characters declarations reject.
                    name: Some(call.name.clone()),
                    ..Default::default()
                };
                if call.custom {
                    // Custom/freeform call argument bodies are not JSON —
                    // replayed verbatim through the invalid_json_str
                    // channel (upstream tolerates non-JSON text there).
                    tool_call.is_custom_tool_call = Some(true);
                    tool_call.invalid_json_str = Some(call.arguments.clone());
                } else {
                    tool_call.arguments_json = Some(call.arguments.clone());
                }
                prompt.tool_calls.push(tool_call);
            }
            vec![prompt]
        }
        Message::ToolResult(typed) => {
            let mut prompt = prompt_for_content(
                pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL,
                &typed.content,
                attach_images,
                repairs,
            );
            if prompt.prompt.as_deref().unwrap_or_default().is_empty() {
                // Upstream rejects empty tool-result text; aligned with
                // WindsurfAPI's placeholder.
                prompt.prompt = Some("[tool result]".to_string());
            }
            prompt.tool_call_id = Some(typed.tool_call_id.clone());
            prompt.tool_result_is_error = Some(typed.is_error);
            vec![prompt]
        }
    }
}

/// The assistant message's source enum on the Devin wire (upstream names
/// it SYSTEM, value 2).
fn assistant_source() -> pb::ExaCodeiumCommonPb_ChatMessageSource {
    pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM
}

/// Reorders a grouped "consecutive call prompts + consecutive result
/// prompts" sequence into interleaved `call_i`, `result_i`, `call_j`, `result_j`.
/// Already-interleaved sequences pass through unchanged; calls with no
/// matching result keep their position. Returns the reordered prompts and
/// the number of prompts whose position changed (0 for interleaved
/// histories).
fn pair_tool_calls_with_results(
    prompts: &[pb::ExaChatPb_ChatMessagePrompt],
) -> (Vec<pb::ExaChatPb_ChatMessagePrompt>, i64) {
    let tool_source =
        pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL;
    let is_call_prompt = |prompt: &pb::ExaChatPb_ChatMessagePrompt| {
        prompt.source == Some(assistant_source()) && !prompt.tool_calls.is_empty()
    };
    let is_result_prompt =
        |prompt: &pb::ExaChatPb_ChatMessagePrompt| prompt.source == Some(tool_source);
    let mut out: Vec<pb::ExaChatPb_ChatMessagePrompt> = Vec::with_capacity(prompts.len());
    // Each prompt's original position, for the moved-count — the Rust
    // equivalent of Go's pointer-identity map (every prompt is uniquely
    // constructed).
    let mut original_index: HashMap<usize, usize> = HashMap::with_capacity(prompts.len());
    let mut i = 0usize;
    while i < prompts.len() {
        if !is_call_prompt(&prompts[i]) {
            original_index.insert(out.len(), i);
            out.push(prompts[i].clone());
            i += 1;
            continue;
        }
        let calls_start = i;
        while i < prompts.len() && is_call_prompt(&prompts[i]) {
            i += 1;
        }
        let calls_end = i;
        let mut by_id: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        let results_start = i;
        let mut j = i;
        while j < prompts.len() && is_result_prompt(&prompts[j]) {
            let id = prompts[j].tool_call_id.as_deref().unwrap_or_default();
            by_id.entry(id).or_default().push(j);
            j += 1;
        }
        // consumed records paired results by position, not id: multiple
        // results with the same id are consumed in arrival order
        // (duplicate call-ids are tolerated upstream but bound
        // positionally); a single-value byID would let a later result
        // overwrite and lose the earlier one.
        let mut consumed: HashSet<usize> = HashSet::new();
        for call_index in calls_start..calls_end {
            original_index.insert(out.len(), call_index);
            out.push(prompts[call_index].clone());
            for call in &prompts[call_index].tool_calls {
                let id = call.id.as_deref().unwrap_or_default();
                let Some(queue) = by_id.get_mut(id) else {
                    continue;
                };
                if queue.is_empty() {
                    continue;
                }
                let result_index = queue.remove(0);
                original_index.insert(out.len(), result_index);
                out.push(prompts[result_index].clone());
                consumed.insert(result_index);
            }
        }
        // Unpaired orphan results keep their order — no message is lost.
        for (k, prompt) in prompts.iter().enumerate().take(j).skip(results_start) {
            if !consumed.contains(&k) {
                original_index.insert(out.len(), k);
                out.push(prompt.clone());
            }
        }
        i = j;
    }
    let moved = original_index
        .iter()
        .filter(|&(&position, &original)| position != original)
        .count();
    let moved = i64::try_from(moved).unwrap_or(i64::MAX);
    (out, moved)
}

/// Projects a `UserMessage`/`ToolResultMessage` content block list into a
/// single prompt. Both message kinds' `validate` already restricts
/// content to text/image — thinking/tool calls never arrive here;
/// assistant-side artifacts are assembled separately in `convert_message`.
fn prompt_for_content(
    source: pb::ExaCodeiumCommonPb_ChatMessageSource,
    content: &[Content],
    attach_images: bool,
    repairs: &mut RequestRepairs,
) -> pb::ExaChatPb_ChatMessagePrompt {
    let mut prompt = pb::ExaChatPb_ChatMessagePrompt {
        message_id: Some(randid::uuid()),
        source: Some(source),
        ..Default::default()
    };
    let mut text = String::new();
    for block in content {
        match block {
            Content::Text(block) => text.push_str(&block.text),
            Content::Image(block) => {
                if !attach_images {
                    // Aligned with WindsurfAPI: history images do not enter
                    // Images, avoiding upstream invalid_argument.
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str("[Image omitted from history]");
                    repairs.omitted_history_images += 1;
                    continue;
                }
                // Devin/Windsurf ImageData: bare base64 (no data: prefix)
                // + mime_type.
                let mut data = block.data.as_str();
                if data.starts_with("data:")
                    && let Some((_, encoded)) = data.split_once(',')
                {
                    data = encoded;
                }
                let mime_type = if block.mime_type.is_empty() {
                    "image/png"
                } else {
                    block.mime_type.as_str()
                };
                prompt.images.push(pb::ExaCodeiumCommonPb_ImageData {
                    base64_data: Some(data.to_string()),
                    mime_type: Some(mime_type.to_string()),
                    ..Default::default()
                });
            }
            Content::Thinking(_) | Content::ToolCall(_) => {}
        }
    }
    prompt.prompt = Some(text);
    prompt
}
