//! Upstream response frame decoding and semantic event ordering.
//!
//! Port of `G/internal/adapter/devin/response_decoder.go`: one
//! [`ResponseDecoder`] reduces `GetChatMessageResponse` frames into ordered
//! [`ResponseEvent`]s — thinking before text before tool deltas before the
//! stop reason — while accumulating the final [`AssistantMessage`].
//!
//! Contracts carried over from the Go decoder:
//!
//! - `decode` borrows the frame; the caller keeps the raw message for the
//!   debug log (`recordProtoJSON` equivalent) — nothing is consumed.
//! - A non-empty return from `decode` is the semantic-progress signal the
//!   stream's no-progress watchdog feeds on; metadata-only frames (usage,
//!   latency keepalives) return empty and do not count as progress.
//! - A stop reason alone does not finish the stream: trailing usage and
//!   signature frames still merge into the aggregate until `finish`.
//! - `finish(None)` never fabricates success: EOF without a stop reason is
//!   a truncation error, and EOF with no content at all is an empty-stream
//!   error.
//! - proto3-open enum semantics: the original descriptor is proto3 (the
//!   flattened schema is proto2), so Go keeps undeclared enum values in the
//!   field — `GetStopReason()` returns the raw number. buffa routes them to
//!   `__buffa_unknown_fields` instead; [`ResponseDecoder::effective_stop_reason`]
//!   recovers them so the undeclared-value warning and stop mapping match
//!   Go, and the drift check excludes those enum-field varints because Go's
//!   `GetUnknown()` never contains them.

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;

use devin_proto::buffa::{Enumeration, UnknownFieldData, UnknownFields};
use devin_proto::generated::exa::api_server_pb::__buffa::view::{
    ExaCodeiumCommonPb_ChatToolCallView, ExaCodeiumCommonPb_ModelUsageStatsView,
    GetChatMessageResponseView, GoogleProtobuf_TimestampView,
};
use devin_proto::generated::exa::api_server_pb::{
    ExaCodeiumCommonPb_APIProvider, ExaCodeiumCommonPb_ChatToolCall,
    ExaCodeiumCommonPb_ModelUsageStats, ExaCodeiumCommonPb_StopReason, GetChatMessageResponse,
    GoogleProtobuf_Timestamp,
};

/// Zero-copy frame access shared by the owned message and the decoded
/// view. The hot path decodes straight from the wire view — materializing
/// an owned `GetChatMessageResponse` per delta was a measured per-frame
/// cost (task-24 profiling); tests and the debug-log path still use the
/// owned type through the same accessors.
pub trait FrameAccess {
    /// `timestamp` field view type.
    type Timestamp<'a>: TimestampAccess
    where
        Self: 'a;
    /// `usage` field view type.
    type Usage<'a>: UsageAccess<'a>
    where
        Self: 'a;
    /// `delta_tool_calls` element view type.
    type ToolCall<'a>: ToolCallAccess<'a>
    where
        Self: 'a;

    fn message_id(&self) -> Option<&str>;
    fn output_id(&self) -> Option<&str>;
    fn request_id(&self) -> Option<&str>;
    fn actual_model_uid(&self) -> Option<&str>;
    fn timestamp(&self) -> Option<&Self::Timestamp<'_>>;
    fn usage(&self) -> Option<&Self::Usage<'_>>;
    fn delta_text(&self) -> Option<&str>;
    fn delta_thinking(&self) -> Option<&str>;
    fn delta_signature(&self) -> Option<&str>;
    fn delta_signature_type(&self) -> Option<&str>;
    fn thinking_redacted(&self) -> bool;
    fn stop_reason(&self) -> Option<ExaCodeiumCommonPb_StopReason>;
    fn tool_call_count(&self) -> usize;
    fn tool_call(&self, index: usize) -> Option<&Self::ToolCall<'_>>;
    /// Whether the frame carries fields outside our compiled schema.
    fn has_unknown_fields(&self) -> bool;
    /// The decoded unknown-field set. Materializing it may allocate on
    /// the view path — call only after `has_unknown_fields` returned true
    /// (schema drift is the exceptional case).
    fn unknown_fields(&self) -> UnknownFields;
}

/// `timestamp` field access (owned and view share the same shape).
pub trait TimestampAccess {
    fn seconds(&self) -> i64;
    fn nanos(&self) -> i32;
}

/// `usage` field access.
pub trait UsageAccess<'a> {
    fn model_uid(&'a self) -> Option<&'a str>;
    fn input_tokens(&self) -> u64;
    fn output_tokens(&self) -> u64;
    fn cache_read_tokens(&self) -> u64;
    fn cache_write_tokens(&self) -> u64;
    fn provider_refusal(&self) -> bool;
    fn api_provider(&self) -> Option<ExaCodeiumCommonPb_APIProvider>;
    fn message_id(&'a self) -> Option<&'a str>;
    fn billing_model_uid(&'a self) -> Option<&'a str>;
    fn response_header_get(&'a self, key: &str) -> Option<&'a str>;
    fn has_unknown_fields(&self) -> bool;
    fn unknown_fields(&self) -> UnknownFields;
}

/// `delta_tool_calls` element access.
pub trait ToolCallAccess<'a> {
    fn id(&'a self) -> Option<&'a str>;
    fn name(&'a self) -> Option<&'a str>;
    fn is_custom_tool_call(&self) -> bool;
    fn arguments_json(&'a self) -> Option<&'a str>;
    fn invalid_json_str(&'a self) -> Option<&'a str>;
    fn has_unknown_fields(&self) -> bool;
    fn unknown_fields(&self) -> UnknownFields;
}

impl TimestampAccess for GoogleProtobuf_Timestamp {
    fn seconds(&self) -> i64 {
        self.seconds.unwrap_or(0)
    }
    fn nanos(&self) -> i32 {
        self.nanos.unwrap_or(0)
    }
}

impl TimestampAccess for GoogleProtobuf_TimestampView<'_> {
    fn seconds(&self) -> i64 {
        self.seconds.unwrap_or(0)
    }
    fn nanos(&self) -> i32 {
        self.nanos.unwrap_or(0)
    }
}

impl<'a> UsageAccess<'a> for ExaCodeiumCommonPb_ModelUsageStats {
    fn model_uid(&'a self) -> Option<&'a str> {
        self.model_uid.as_deref()
    }
    fn input_tokens(&self) -> u64 {
        self.input_tokens.unwrap_or(0)
    }
    fn output_tokens(&self) -> u64 {
        self.output_tokens.unwrap_or(0)
    }
    fn cache_read_tokens(&self) -> u64 {
        self.cache_read_tokens.unwrap_or(0)
    }
    fn cache_write_tokens(&self) -> u64 {
        self.cache_write_tokens.unwrap_or(0)
    }
    fn provider_refusal(&self) -> bool {
        self.provider_refusal.unwrap_or(false)
    }
    fn api_provider(&self) -> Option<ExaCodeiumCommonPb_APIProvider> {
        self.api_provider
    }
    fn message_id(&'a self) -> Option<&'a str> {
        self.message_id.as_deref()
    }
    fn billing_model_uid(&'a self) -> Option<&'a str> {
        self.billing_model_uid.as_deref()
    }
    fn response_header_get(&'a self, key: &str) -> Option<&'a str> {
        self.response_header.get(key).map(String::as_str)
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.clone()
    }
}

impl<'a> UsageAccess<'a> for ExaCodeiumCommonPb_ModelUsageStatsView<'a> {
    fn model_uid(&'a self) -> Option<&'a str> {
        self.model_uid
    }
    fn input_tokens(&self) -> u64 {
        self.input_tokens.unwrap_or(0)
    }
    fn output_tokens(&self) -> u64 {
        self.output_tokens.unwrap_or(0)
    }
    fn cache_read_tokens(&self) -> u64 {
        self.cache_read_tokens.unwrap_or(0)
    }
    fn cache_write_tokens(&self) -> u64 {
        self.cache_write_tokens.unwrap_or(0)
    }
    fn provider_refusal(&self) -> bool {
        self.provider_refusal.unwrap_or(false)
    }
    fn api_provider(&self) -> Option<ExaCodeiumCommonPb_APIProvider> {
        self.api_provider
    }
    fn message_id(&'a self) -> Option<&'a str> {
        self.message_id
    }
    fn billing_model_uid(&'a self) -> Option<&'a str> {
        self.billing_model_uid
    }
    fn response_header_get(&'a self, key: &str) -> Option<&'a str> {
        self.response_header.get(key).copied()
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.to_owned().unwrap_or_default()
    }
}

impl<'a> ToolCallAccess<'a> for ExaCodeiumCommonPb_ChatToolCall {
    fn id(&'a self) -> Option<&'a str> {
        self.id.as_deref()
    }
    fn name(&'a self) -> Option<&'a str> {
        self.name.as_deref()
    }
    fn is_custom_tool_call(&self) -> bool {
        self.is_custom_tool_call.unwrap_or(false)
    }
    fn arguments_json(&'a self) -> Option<&'a str> {
        self.arguments_json.as_deref()
    }
    fn invalid_json_str(&'a self) -> Option<&'a str> {
        self.invalid_json_str.as_deref()
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.clone()
    }
}

impl<'a> ToolCallAccess<'a> for ExaCodeiumCommonPb_ChatToolCallView<'a> {
    fn id(&'a self) -> Option<&'a str> {
        self.id
    }
    fn name(&'a self) -> Option<&'a str> {
        self.name
    }
    fn is_custom_tool_call(&self) -> bool {
        self.is_custom_tool_call.unwrap_or(false)
    }
    fn arguments_json(&'a self) -> Option<&'a str> {
        self.arguments_json
    }
    fn invalid_json_str(&'a self) -> Option<&'a str> {
        self.invalid_json_str
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.to_owned().unwrap_or_default()
    }
}

impl FrameAccess for GetChatMessageResponse {
    type Timestamp<'a>
        = GoogleProtobuf_Timestamp
    where
        Self: 'a;
    type Usage<'a>
        = ExaCodeiumCommonPb_ModelUsageStats
    where
        Self: 'a;
    type ToolCall<'a>
        = ExaCodeiumCommonPb_ChatToolCall
    where
        Self: 'a;

    fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }
    fn output_id(&self) -> Option<&str> {
        self.output_id.as_deref()
    }
    fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    fn actual_model_uid(&self) -> Option<&str> {
        self.actual_model_uid.as_deref()
    }
    fn timestamp(&self) -> Option<&Self::Timestamp<'_>> {
        self.timestamp.as_option()
    }
    fn usage(&self) -> Option<&Self::Usage<'_>> {
        self.usage.as_option()
    }
    fn delta_text(&self) -> Option<&str> {
        self.delta_text.as_deref()
    }
    fn delta_thinking(&self) -> Option<&str> {
        self.delta_thinking.as_deref()
    }
    fn delta_signature(&self) -> Option<&str> {
        self.delta_signature.as_deref()
    }
    fn delta_signature_type(&self) -> Option<&str> {
        self.delta_signature_type.as_deref()
    }
    fn thinking_redacted(&self) -> bool {
        self.thinking_redacted.unwrap_or(false)
    }
    fn stop_reason(&self) -> Option<ExaCodeiumCommonPb_StopReason> {
        self.stop_reason
    }
    fn tool_call_count(&self) -> usize {
        self.delta_tool_calls.len()
    }
    fn tool_call(&self, index: usize) -> Option<&Self::ToolCall<'_>> {
        self.delta_tool_calls.get(index)
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.clone()
    }
}

impl FrameAccess for GetChatMessageResponseView<'_> {
    type Timestamp<'a>
        = GoogleProtobuf_TimestampView<'a>
    where
        Self: 'a;
    type Usage<'a>
        = ExaCodeiumCommonPb_ModelUsageStatsView<'a>
    where
        Self: 'a;
    type ToolCall<'a>
        = ExaCodeiumCommonPb_ChatToolCallView<'a>
    where
        Self: 'a;

    fn message_id(&self) -> Option<&str> {
        self.message_id
    }
    fn output_id(&self) -> Option<&str> {
        self.output_id
    }
    fn request_id(&self) -> Option<&str> {
        self.request_id
    }
    fn actual_model_uid(&self) -> Option<&str> {
        self.actual_model_uid
    }
    fn timestamp(&self) -> Option<&Self::Timestamp<'_>> {
        self.timestamp.as_option()
    }
    fn usage(&self) -> Option<&Self::Usage<'_>> {
        self.usage.as_option()
    }
    fn delta_text(&self) -> Option<&str> {
        self.delta_text
    }
    fn delta_thinking(&self) -> Option<&str> {
        self.delta_thinking
    }
    fn delta_signature(&self) -> Option<&str> {
        self.delta_signature
    }
    fn delta_signature_type(&self) -> Option<&str> {
        self.delta_signature_type
    }
    fn thinking_redacted(&self) -> bool {
        self.thinking_redacted.unwrap_or(false)
    }
    fn stop_reason(&self) -> Option<ExaCodeiumCommonPb_StopReason> {
        self.stop_reason
    }
    fn tool_call_count(&self) -> usize {
        self.delta_tool_calls.len()
    }
    fn tool_call(&self, index: usize) -> Option<&Self::ToolCall<'_>> {
        self.delta_tool_calls.get(index)
    }
    fn has_unknown_fields(&self) -> bool {
        !self.__buffa_unknown_fields.is_empty()
    }
    fn unknown_fields(&self) -> UnknownFields {
        self.__buffa_unknown_fields.to_owned().unwrap_or_default()
    }
}

use crate::domain::failure::{Failure, classify};
use crate::domain::request::{Content, TextContent, ThinkingContent, ToolCall, ToolDefinition};
use crate::domain::response::{
    AssistantMessage, AssistantMessageDiagnostic, ResponseEvent, ResponseEventType, StopReason,
};

/// `GetChatMessageResponse.stop_reason` field number — undeclared enum
/// values land in `__buffa_unknown_fields` under this number.
const STOP_REASON_FIELD: u32 = 5;
/// `ExaCodeiumCommonPb_ModelUsageStats.model_deprecated` (enum) field
/// number; undeclared values stay in-field under Go's proto3-open enums.
const USAGE_MODEL_DEPRECATED_FIELD: u32 = 1;
/// `ExaCodeiumCommonPb_ModelUsageStats.api_provider` (enum) field number.
const USAGE_API_PROVIDER_FIELD: u32 = 6;

/// One observed schema-drift site: fields the upstream sent outside our
/// compiled proto definition. Go surfaces this as a single warning per
/// stream; the record keeps every observation so the debug log can retain
/// the full drift evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDrift {
    /// Where the unknown fields were seen: `frame`, `usage` or `tool_call`.
    pub scope: &'static str,
    /// Top-level field numbers found in the unknown set, in wire order.
    pub field_numbers: Vec<u32>,
}

/// Accumulated state of one in-flight Devin tool call.
struct ToolState {
    /// The call as accumulated so far; copied into `partial.content` at
    /// `content_idx` whenever it changes.
    call: ToolCall,
    /// Position of the call in `partial.content`; `-1` until the start
    /// event emitted it (always non-negative by the time it is read).
    content_idx: i32,
    /// Fixed call identifier for emitted events: whatever `ToolCallStart`
    /// used, later deltas reuse — a late real id backfills `call`/the
    /// content block but never the event id (clients reconcile on start's).
    event_id: String,
    /// Accumulated `arguments_json` fragments (or the verbatim custom body).
    arguments: String,
    /// The raw call was appended to `partial` and produced a start event.
    emitted: bool,
    /// `call.id` is still the synthesized placeholder (first frame had no
    /// id): id-less continuation frames and the late real-id frame both
    /// merge into this call positionally.
    placeholder_id: bool,
    /// The call is a custom-declared tool's function wrapper: fragments are
    /// the `{"input":"<verbatim>"}` JSON wrapper, not the argument body.
    wrapped: bool,
}

/// Response frame decoder: reduces one Devin stream's frames into ordered
/// intermediate events and the final aggregated message.
///
/// Single-consumer contract like Go's: `start` once, `decode` per frame,
/// `finish` exactly once at stream end (EOF or error). All state is plain
/// owned data — no locking, matching the Go decoder's goroutine-local use.
// The open-block bools mirror the Go decoder's per-channel state
// flags one-for-one.
#[allow(clippy::struct_excessive_bools)]
pub struct ResponseDecoder {
    /// Devin model identifier the request used.
    model: String,
    /// The assistant message accumulated so far. Shared with emitted
    /// events through `Arc` (Go reuses one `*AssistantMessage` pointer per
    /// frame — response.go:247): events `Arc::clone` it, mutations go
    /// through `Arc::make_mut`, which clones only while a previously
    /// emitted event still holds the snapshot — an O(1) emit instead of a
    /// deep clone of the whole accumulated message per event.
    partial: Arc<AssistantMessage>,
    /// Current open text block state.
    text: TextContent,
    text_builder: String,
    text_idx: usize,
    text_open: bool,
    /// Current open thinking block state; body and signature accumulate in
    /// separate builders (upstream reports them on separate channels).
    thinking: ThinkingContent,
    thinking_builder: String,
    thinking_sig_builder: String,
    think_idx: usize,
    thinking_open: bool,
    /// Tool calls accumulating argument fragments.
    tools: Vec<ToolState>,
    /// `start` already produced its event.
    started: bool,
    /// A done or error event was already produced.
    finished: bool,
    /// Upstream explicitly returned a stop reason.
    has_stop_reason: bool,
    /// The declared final stop reason, applied when the stream ends.
    stop_reason: Option<StopReason>,
    /// Client-requested stop sequences; upstream `stopPatterns` does not
    /// actually trigger (the model generates past them), so the decode
    /// layer truncates accumulated text locally.
    stop_patterns: Vec<String>,
    /// Longest stop pattern in bytes; bounds the withheld tail window.
    max_pattern_len: usize,
    /// Bytes of `text_builder` already emitted as deltas.
    text_emitted: usize,
    /// Generation was truncated by a stop sequence; later frames only
    /// update usage metadata.
    stopped_by_pattern: bool,
    /// The stop sequence that matched.
    stop_sequence: String,
    /// Provider-side tracing info was written to diagnostics.
    provider_logged: bool,
    /// Upstream declared the provider refused the request
    /// (`usage.provider_refusal`).
    provider_refusal: bool,
    /// Tool names declared with freeform/custom semantics; their wire form
    /// is a single-parameter function wrapper unwrapped back to verbatim
    /// text on the response side.
    custom_tools: HashSet<String>,
    /// This stream already warned about schema drift (unknown fields);
    /// repeating the same warning per frame adds no information.
    drift_warned: bool,
    /// This stream already warned about an undeclared `stop_reason` value.
    stop_reason_warned: bool,
    /// Every observed drift site, for the debug log.
    drift: Vec<SchemaDrift>,
}

impl ResponseDecoder {
    /// Creates a decoder: empty stop patterns are filtered out and the
    /// maximum length recorded (the scan bound of the truncation tail
    /// window); `custom_tools` is the set of tool names declared with
    /// freeform semantics whose calls unwrap from the wrapper format.
    pub fn new(model: &str, stop_patterns: &[String], custom_tools: HashSet<String>) -> Self {
        let mut patterns = Vec::with_capacity(stop_patterns.len());
        let mut max_len = 0usize;
        for pattern in stop_patterns {
            if pattern.is_empty() {
                continue;
            }
            if pattern.len() > max_len {
                max_len = pattern.len();
            }
            patterns.push(pattern.clone());
        }
        Self {
            model: model.to_string(),
            partial: Arc::new(AssistantMessage::default()),
            text: TextContent::default(),
            text_builder: String::new(),
            text_idx: 0,
            text_open: false,
            thinking: ThinkingContent::default(),
            thinking_builder: String::new(),
            thinking_sig_builder: String::new(),
            think_idx: 0,
            thinking_open: false,
            tools: Vec::new(),
            started: false,
            finished: false,
            has_stop_reason: false,
            stop_reason: None,
            stop_patterns: patterns,
            max_pattern_len: max_len,
            text_emitted: 0,
            stopped_by_pattern: false,
            stop_sequence: String::new(),
            provider_logged: false,
            provider_refusal: false,
            custom_tools,
            drift_warned: false,
            stop_reason_warned: false,
            drift: Vec::new(),
        }
    }

    /// The assistant message accumulated so far (test/introspection view;
    /// the stream layer reads terminal events, not this). An open thinking
    /// block is materialized into `partial` on read — Go syncs it per
    /// frame, so the introspection view must see the in-flight signature;
    /// the hot path defers that write to `end_thinking`/`fail` instead.
    pub fn partial(&mut self) -> &AssistantMessage {
        if self.thinking_open {
            self.thinking.thinking_signature = self.thinking_sig_builder.clone();
            Arc::make_mut(&mut self.partial).content[self.think_idx] =
                Content::Thinking(self.thinking.clone());
        }
        &self.partial
    }

    /// Number of tool calls seen so far.
    pub fn tool_count(&self) -> usize {
        self.tools.len()
    }

    /// Whether upstream explicitly returned a stop reason — the stream
    /// layer shrinks its silence deadline to the tail grace once set.
    pub fn has_stop_reason(&self) -> bool {
        self.has_stop_reason
    }

    /// Whether a terminal (done/error) event was already produced.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Every schema-drift observation so far, in arrival order.
    pub fn drift(&self) -> &[SchemaDrift] {
        &self.drift
    }

    /// Initializes `partial` and produces the `start` event. The stream
    /// layer withholds it (`pending_start`) and releases it with the first
    /// real events; a retried stream that already emitted a start does not
    /// produce another. Repeated calls return empty.
    pub fn start(&mut self) -> Vec<ResponseEvent> {
        if self.started || self.finished {
            return Vec::new();
        }
        self.started = true;
        self.partial = Arc::new(AssistantMessage {
            api: "connect".to_string(),
            provider: "devin".to_string(),
            model: self.model.clone(),
            stop_reason: Some(StopReason::Pending),
            timestamp_ms: unix_millis_now(),
            ..AssistantMessage::default()
        });
        vec![ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        }]
    }

    /// The shared `partial` handle for event payloads: an `Arc::clone`,
    /// not a deep copy. The decoder's next mutation uses `Arc::make_mut`,
    /// so an event still holding this handle keeps its snapshot semantics
    /// (the clone cost moves to the mutation site and only fires while a
    /// prior event is alive — matching Go's shared-pointer shape with
    /// snapshot semantics preserved).
    fn snapshot(&self) -> Arc<AssistantMessage> {
        Arc::clone(&self.partial)
    }

    /// Interprets one upstream frame as zero or more intermediate events:
    /// metadata/usage first, then thinking, text, tool deltas and the stop
    /// reason in that fixed order, all appending to one events vector.
    /// Frames after `finished` or after stop-sequence truncation only
    /// update metadata. An empty return means no semantic progress — the
    /// stream's no-progress watchdog must not be fed by such frames.
    pub fn decode<F: FrameAccess>(&mut self, response: &F) -> Vec<ResponseEvent> {
        if self.finished {
            return Vec::new();
        }
        self.note_schema_drift(response);
        self.update_metadata(response);
        if self.stopped_by_pattern {
            // Output was already truncated by a stop sequence; remaining
            // frames are consumed only so usage accounting stays complete.
            return Vec::new();
        }
        // Most frames produce 1-2 events; one shared vector keeps the
        // per-frame small allocations near one. `Vec::new` defers the
        // allocation to the first push so metadata-only frames (usage,
        // latency keepalives) never pay it.
        let mut events = Vec::new();
        // Upstream sends the signature as a trailing frame after all body
        // content; with the thinking block already closed it must merge
        // back into the last thinking block, not open a new one.
        let delta_signature = response.delta_signature().unwrap_or("");
        let delta_thinking = response.delta_thinking().unwrap_or("");
        if !delta_signature.is_empty() && delta_thinking.is_empty() && !self.thinking_open {
            self.decode_late_signature(&mut events, response);
        } else if !delta_thinking.is_empty()
            || !delta_signature.is_empty()
            || response.thinking_redacted()
        {
            self.end_text(&mut events);
            self.decode_thinking(&mut events, response);
        }
        if let Some(delta) = response.delta_text()
            && !delta.is_empty()
        {
            self.end_thinking(&mut events);
            self.decode_text(&mut events, delta);
        }
        for index in 0..response.tool_call_count() {
            let Some(delta) = response.tool_call(index) else {
                continue;
            };
            self.end_thinking(&mut events);
            self.end_text(&mut events);
            self.decode_tool(&mut events, delta);
        }
        match Self::effective_stop_reason(response) {
            EffectiveStop::Declared(reason) => {
                self.has_stop_reason = true;
                self.stop_reason = Some(map_stop_reason(reason));
            }
            EffectiveStop::Undeclared(raw) => {
                // The value is not in our compiled proto definition:
                // upstream added a stop-reason shape. mapStopReason's
                // default silently folds it into Stop — warn with the
                // number so the new semantics are not swallowed into a
                // normal ending without a trace.
                self.has_stop_reason = true;
                self.stop_reason = Some(StopReason::Stop);
                if !self.stop_reason_warned {
                    self.stop_reason_warned = true;
                    tracing::warn!(
                        value = raw,
                        "upstream sent undeclared stop_reason value; mapped to stop"
                    );
                }
            }
            EffectiveStop::Absent => {}
        }
        events
    }

    /// The frame's effective stop reason under Go's proto3-open enum
    /// semantics: a declared value wins; an undeclared varint recovered
    /// from the unknown set is reported separately; anything else (absent
    /// or explicit UNSPECIFIED) is no stop reason.
    fn effective_stop_reason<F: FrameAccess>(response: &F) -> EffectiveStop {
        if let Some(reason) = response.stop_reason()
            && reason
                != ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_UNSPECIFIED
        {
            return EffectiveStop::Declared(reason);
        }
        if !response.has_unknown_fields() {
            return EffectiveStop::Absent;
        }
        for field in &response.unknown_fields() {
            if field.number == STOP_REASON_FIELD
                && let UnknownFieldData::Varint(raw) = field.data
                && raw != 0
            {
                return EffectiveStop::Undeclared(raw);
            }
        }
        EffectiveStop::Absent
    }

    /// Checks the frame for fields outside our proto definition: upstream
    /// schema evolution lands silently in the unknown set, so "upstream
    /// added a field we cannot see" is only observable here. Warns at most
    /// once per stream but records every site; covers the frame top level
    /// and the two most semantic nestings (usage, `tool_call` deltas) —
    /// drift there affects billing and calls.
    fn note_schema_drift<F: FrameAccess>(&mut self, response: &F) {
        if response.has_unknown_fields() {
            self.record_drift("frame", &response.unknown_fields(), &[STOP_REASON_FIELD]);
        }
        if let Some(usage) = response.usage()
            && usage.has_unknown_fields()
        {
            self.record_drift(
                "usage",
                &usage.unknown_fields(),
                &[USAGE_MODEL_DEPRECATED_FIELD, USAGE_API_PROVIDER_FIELD],
            );
        }
        for index in 0..response.tool_call_count() {
            if let Some(delta) = response.tool_call(index)
                && delta.has_unknown_fields()
            {
                self.record_drift("tool_call", &delta.unknown_fields(), &[]);
            }
        }
    }

    /// Records one drift site and emits the once-per-stream warning.
    /// `enum_fields` are field numbers whose varint unknowns are undeclared
    /// enum values — Go's proto3-open enums keep those in the field, so
    /// they are drift only in storage location, not in schema.
    fn record_drift(&mut self, scope: &'static str, unknown: &UnknownFields, enum_fields: &[u32]) {
        let numbers: Vec<u32> = unknown
            .iter()
            .filter(|field| {
                !(enum_fields.contains(&field.number)
                    && matches!(field.data, UnknownFieldData::Varint(_)))
            })
            .map(|field| field.number)
            .collect();
        if numbers.is_empty() {
            return;
        }
        self.drift.push(SchemaDrift {
            scope,
            field_numbers: numbers.clone(),
        });
        if !self.drift_warned {
            self.drift_warned = true;
            tracing::warn!(
                scope,
                fields = ?numbers,
                "upstream response carried fields outside our proto schema; decode may be drifting"
            );
        }
    }

    /// Produces the closing events at stream end (clean EOF or error): an
    /// error passes through verbatim into `fail`; a clean end first closes
    /// open thinking/text/tool blocks and then emits done. Repeated calls
    /// return empty.
    pub fn finish(&mut self, upstream_err: Option<&(dyn Error + 'static)>) -> Vec<ResponseEvent> {
        if self.finished {
            return Vec::new();
        }
        if let Some(err) = upstream_err {
            // Mid-stream or terminal transport errors pass through
            // verbatim; `fail` classifies them into the record.
            return self.fail(err);
        }
        if !self.has_stop_reason && self.partial.content.is_empty() && self.tools.is_empty() {
            return self.fail(&Failure::plain(
                "devin stream ended without generated content",
            ));
        }
        let reason = if self.stopped_by_pattern {
            Arc::make_mut(&mut self.partial).stop_sequence = self.stop_sequence.clone();
            StopReason::StopSequence
        } else if !self.has_stop_reason {
            // A normal Devin ending always carries a stopReason frame
            // (observed order: delta → stopReason → usage →
            // responseDimensionGroups). A clean EOF without one means the
            // stream was truncated at the application layer; synthesizing
            // Stop would disguise the truncation as end_turn and a
            // downstream agent would treat a half-finished task as done.
            return self.fail(&Failure::plain("devin stream ended without stop reason"));
        } else {
            // has_stop_reason implies stop_reason is set.
            self.stop_reason.unwrap_or(StopReason::Stop)
        };
        if reason == StopReason::Error {
            if self.provider_refusal {
                return self.fail(&Failure::plain(
                    "upstream provider refused the request (provider_refusal)",
                ));
            }
            return self.fail(&Failure::plain("devin stopped with an error"));
        }
        self.complete(reason)
    }

    /// Whether the frame would change any `partial` field — the
    /// `Arc::make_mut` guard: while a prior event still holds the shared
    /// snapshot (the normal case during burst drains), `make_mut`
    /// deep-clones the whole accumulated message, so calling it
    /// unconditionally makes every frame O(total content). Metadata-only
    /// frames and repeat-value frames skip the clone entirely.
    fn metadata_changes<F: FrameAccess>(&self, response: &F) -> bool {
        let partial = &*self.partial;
        if response
            .message_id()
            .is_some_and(|id| partial.response_id != id)
        {
            return true;
        }
        if response
            .output_id()
            .is_some_and(|id| !id.is_empty() && partial.output_id != id)
        {
            return true;
        }
        if response
            .request_id()
            .is_some_and(|id| !id.is_empty() && partial.upstream_request_id.is_empty())
        {
            return true;
        }
        if response
            .actual_model_uid()
            .is_some_and(|uid| partial.response_model != uid)
        {
            return true;
        }
        if let Some(timestamp) = response.timestamp() {
            let millis = timestamp
                .seconds()
                .saturating_mul(1000)
                .saturating_add(i64::from(timestamp.nanos()).div_euclid(1_000_000));
            if partial.timestamp_ms != millis {
                return true;
            }
        }
        let Some(usage) = response.usage() else {
            return false;
        };
        if partial.response_model.is_empty() && usage.model_uid().is_some_and(|uid| !uid.is_empty())
        {
            return true;
        }
        // The write conditions mirror `update_metadata` exactly: a counter
        // writes when the frame value is non-zero or nothing was booked
        // yet, and only then does a differing value count as a change.
        let input = u64_as_i64(usage.input_tokens());
        if (usage.input_tokens() != 0 || partial.usage.input == 0) && partial.usage.input != input {
            return true;
        }
        let output = u64_as_i64(usage.output_tokens());
        if (usage.output_tokens() != 0 || partial.usage.output == 0)
            && partial.usage.output != output
        {
            return true;
        }
        let cache_read = u64_as_i64(usage.cache_read_tokens());
        if (usage.cache_read_tokens() != 0 || partial.usage.cache_read == 0)
            && partial.usage.cache_read != cache_read
        {
            return true;
        }
        let cache_write = u64_as_i64(usage.cache_write_tokens());
        if (usage.cache_write_tokens() != 0 || partial.usage.cache_write == 0)
            && partial.usage.cache_write != cache_write
        {
            return true;
        }
        // Reached only when every counter is already at its post-write
        // value, so the recomputed total compares against current fields.
        if partial.usage.total_tokens
            != partial.usage.input
                + partial.usage.output
                + partial.usage.cache_read
                + partial.usage.cache_write
        {
            return true;
        }
        // The provider diagnostic is a `partial` write too: it lands once
        // per stream when the frame carries provider tracing info.
        if !self.provider_logged {
            let api_provider = api_provider_name(usage);
            let api_provider = api_provider
                .strip_prefix("API_PROVIDER_")
                .unwrap_or(&api_provider);
            if usage
                .response_header_get("x-request-id")
                .is_some_and(|id| !id.is_empty())
                || (!api_provider.is_empty() && api_provider != "UNSPECIFIED")
            {
                return true;
            }
        }
        false
    }

    /// Flushes frame metadata (id/model/timestamp/usage/diagnostics) into
    /// `partial`; fields only overwrite when they carry a value that is
    /// more complete — upstream reports usage incrementally across frames
    /// and the first value must not be lost. `Arc::make_mut` runs only
    /// when a field would actually change ([`Self::metadata_changes`]) —
    /// while a prior event still holds the shared snapshot, an
    /// unconditional `make_mut` deep-clones the whole accumulated message
    /// per frame, the O(frames × content) cost this guard removes.
    fn update_metadata<F: FrameAccess>(&mut self, response: &F) {
        // `provider_refusal` is decoder state, not a `partial` field: it
        // must latch even when nothing else changed.
        if let Some(usage) = response.usage()
            && usage.provider_refusal()
        {
            self.provider_refusal = true;
        }
        if !self.metadata_changes(response) {
            return;
        }
        let partial = Arc::make_mut(&mut self.partial);
        if let Some(message_id) = response.message_id() {
            partial.response_id = message_id.to_string();
        }
        if let Some(output_id) = response.output_id()
            && !output_id.is_empty()
        {
            partial.output_id = output_id.to_string();
        }
        if let Some(request_id) = response.request_id()
            && !request_id.is_empty()
            && partial.upstream_request_id.is_empty()
        {
            partial.upstream_request_id = request_id.to_string();
        }
        if let Some(actual_model_uid) = response.actual_model_uid() {
            partial.response_model = actual_model_uid.to_string();
        }
        if let Some(timestamp) = response.timestamp() {
            let seconds = timestamp.seconds();
            let nanos = i64::from(timestamp.nanos());
            // Go's time.Unix(sec, nsec).UnixMilli(): Euclidean division
            // matches its floor behavior for negative nanos too.
            partial.timestamp_ms = seconds
                .saturating_mul(1000)
                .saturating_add(nanos.div_euclid(1_000_000));
        }
        if let Some(usage) = response.usage() {
            if partial.response_model.is_empty()
                && let Some(model_uid) = usage.model_uid()
            {
                partial.response_model = model_uid.to_string();
            }
            // An explicit zero field must not overwrite an already-booked
            // non-zero value — placeholder-zero upstreams/relays exist;
            // usage is a cumulative snapshot, so zero is only accepted as
            // the placeholder when nothing was booked yet.
            let input = usage.input_tokens();
            if input != 0 || partial.usage.input == 0 {
                partial.usage.input = u64_as_i64(input);
            }
            let output = usage.output_tokens();
            if output != 0 || partial.usage.output == 0 {
                partial.usage.output = u64_as_i64(output);
            }
            let cache_read = usage.cache_read_tokens();
            if cache_read != 0 || partial.usage.cache_read == 0 {
                partial.usage.cache_read = u64_as_i64(cache_read);
            }
            let cache_write = usage.cache_write_tokens();
            if cache_write != 0 || partial.usage.cache_write == 0 {
                partial.usage.cache_write = u64_as_i64(cache_write);
            }
            partial.usage.total_tokens = partial.usage.input
                + partial.usage.output
                + partial.usage.cache_read
                + partial.usage.cache_write;
            // Provider-side tracing info (api_provider + the vendor HTTP
            // request id) is recorded once — it can be handed to
            // upstream/provider support directly when debugging.
            if !self.provider_logged {
                // Go's TrimPrefix("API_PROVIDER_"): a no-op on the full
                // proto name, kept for parity with the reference.
                let api_provider = api_provider_name(usage);
                let api_provider = api_provider
                    .strip_prefix("API_PROVIDER_")
                    .unwrap_or(&api_provider)
                    .to_string();
                let provider_request_id = usage
                    .response_header_get("x-request-id")
                    .unwrap_or_default()
                    .to_string();
                if !provider_request_id.is_empty()
                    || (!api_provider.is_empty() && api_provider != "UNSPECIFIED")
                {
                    let details = serde_json::json!({
                        "api_provider": api_provider,
                        "provider_request_id": provider_request_id,
                        "provider_message_id": usage.message_id().unwrap_or_default(),
                        "billing_model_uid": usage.billing_model_uid().unwrap_or_default(),
                    })
                    .to_string();
                    partial.diagnostics.push(AssistantMessageDiagnostic {
                        kind: "upstream_provider".to_string(),
                        timestamp_ms: unix_millis_now(),
                        details,
                    });
                    self.provider_logged = true;
                }
            }
        }
    }

    /// Handles thinking deltas: opens a block and emits `ThinkingStart`
    /// when none is open; body and signature accumulate in separate
    /// builders (upstream reports them on separate channels) and the
    /// signature is written back to `partial` every frame — a trailing
    /// signature arriving after the block closed is handled by the late
    /// path in `decode`.
    fn decode_thinking<F: FrameAccess>(&mut self, events: &mut Vec<ResponseEvent>, response: &F) {
        if !self.thinking_open {
            self.thinking = ThinkingContent {
                redacted: response.thinking_redacted(),
                ..ThinkingContent::default()
            };
            self.thinking_builder.clear();
            self.thinking_sig_builder.clear();
            Arc::make_mut(&mut self.partial)
                .content
                .push(Content::Thinking(self.thinking.clone()));
            self.think_idx = self.partial.content.len() - 1;
            self.thinking_open = true;
            events.push(ResponseEvent {
                kind: ResponseEventType::ThinkingStart,
                content_index: content_index(self.think_idx),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
        let delta = response.delta_thinking().unwrap_or("");
        if !delta.is_empty() {
            self.thinking_builder.push_str(delta);
        }
        if let Some(sig) = response.delta_signature()
            && !sig.is_empty()
        {
            self.thinking_sig_builder.push_str(sig);
        }
        if let Some(sig_type) = response.delta_signature_type()
            && !sig_type.is_empty()
        {
            // signature_type decides the signature payload's format
            // (sealed/anthropic/openai); it must be replayed verbatim —
            // a mismatch triggers upstream invalid_argument.
            self.thinking.signature_type = sig_type.to_string();
        }
        self.thinking.redacted = self.thinking.redacted || response.thinking_redacted();
        // The thinking body AND signature materialize at endThinking (and
        // at `fail` for an error mid-block): syncing them into `partial`
        // per frame costs an `Arc::make_mut` deep clone whenever a prior
        // event still holds the snapshot — O(frames × content) — while no
        // reader consumes the in-block copy mid-block (encoders read it
        // at the start/end boundaries, `decode_late_signature` only runs
        // after the block closed).
        if !delta.is_empty() {
            events.push(ResponseEvent {
                kind: ResponseEventType::ThinkingDelta,
                content_index: content_index(self.think_idx),
                delta: delta.to_string(),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
    }

    /// Handles text deltas: opens a block and emits `TextStart` when none
    /// is open; on a stop-sequence hit truncates the emitted text and
    /// records `stopped_by_pattern` so later frames only flush metadata.
    fn decode_text(&mut self, events: &mut Vec<ResponseEvent>, delta: &str) {
        if !self.text_open {
            self.text = TextContent::default();
            self.text_builder.clear();
            self.text_emitted = 0;
            Arc::make_mut(&mut self.partial)
                .content
                .push(Content::Text(self.text.clone()));
            self.text_idx = self.partial.content.len() - 1;
            self.text_open = true;
            events.push(ResponseEvent {
                kind: ResponseEventType::TextStart,
                content_index: content_index(self.text_idx),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
        // Builder accumulation avoids ever-larger per-frame strings.
        self.text_builder.push_str(delta);
        if self.stop_patterns.is_empty() {
            self.text_emitted += delta.len();
            events.push(ResponseEvent {
                kind: ResponseEventType::TextDelta,
                content_index: content_index(self.text_idx),
                delta: delta.to_string(),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
            return;
        }
        self.scan_text_for_stops(events);
    }

    /// Searches the accumulated text for stop sequences and decides which
    /// delta may be emitted this frame. The emitted prefix is guaranteed
    /// free of match starts (see the safe-window derivation in
    /// `emit_text_delta`), so scanning starts at `text_emitted`. On a hit
    /// the text is truncated, the block closed and `stopped_by_pattern`
    /// set; without a hit the last `max_pattern_len - 1` bytes stay
    /// withheld — they may be an incomplete cross-frame pattern prefix.
    fn scan_text_for_stops(&mut self, events: &mut Vec<ResponseEvent>) {
        // Borrow the builder instead of cloning it: the accumulated text
        // grows over the block, so a per-frame clone was quadratic.
        let text = &self.text_builder;
        let mut earliest: Option<usize> = None;
        let mut matched = "";
        for pattern in &self.stop_patterns {
            if let Some(idx) = text[self.text_emitted..].find(pattern.as_str()) {
                let pos = self.text_emitted + idx;
                if earliest.is_none_or(|current| pos < current) {
                    earliest = Some(pos);
                    matched = pattern.as_str();
                }
            }
        }
        if let Some(earliest) = earliest {
            self.stop_sequence = matched.to_string();
            if earliest > self.text_emitted {
                let delta = text[self.text_emitted..earliest].to_string();
                events.push(self.emit_text_delta(delta));
            }
            self.stopped_by_pattern = true;
            self.text_builder.truncate(earliest);
            self.end_text(events);
            return;
        }
        // `safe` is a byte lower bound and may land inside a multi-byte
        // rune: back off to the nearest char boundary and leave the
        // continuation bytes in the holdback window for the next frame —
        // emitting half a UTF-8 sequence would be replaced by U+FFFD at
        // the encoder and the leftover bytes would produce another,
        // permanently losing the character for the client.
        let safe = text
            .len()
            .saturating_sub(self.max_pattern_len.saturating_sub(1));
        if safe > self.text_emitted {
            let mut safe = safe;
            while safe < text.len() && !text.is_char_boundary(safe) {
                safe -= 1;
            }
            if safe > self.text_emitted {
                let delta = text[self.text_emitted..safe].to_string();
                events.push(self.emit_text_delta(delta));
            }
        }
    }

    /// Emits one text delta and advances the `text_emitted` counter.
    fn emit_text_delta(&mut self, delta: String) -> ResponseEvent {
        self.text_emitted += delta.len();
        ResponseEvent {
            kind: ResponseEventType::TextDelta,
            content_index: content_index(self.text_idx),
            delta,
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        }
    }

    /// Handles a tool-call delta: locates or creates the `ToolState` by id
    /// (an id-less first frame gets a synthesized placeholder id), then
    /// routes custom-declared tools' wrapped arguments and native calls
    /// separately; the argument body materializes once at `complete`.
    fn decode_tool<'a, T: ToolCallAccess<'a>>(
        &mut self,
        events: &mut Vec<ResponseEvent>,
        delta: &'a T,
    ) {
        let index = if let Some(index) = self.find_tool(delta) {
            index
        } else {
            let id = delta.id().unwrap_or("");
            let placeholder = id.is_empty();
            // Upstream occasionally sends a first frame without an
            // id: synthesize a stable placeholder so ToolCallStart
            // passes Validate, then merge argument deltas
            // positionally.
            let id = if placeholder {
                format!("call_{}", self.tools.len())
            } else {
                id.to_string()
            };
            self.tools.push(ToolState {
                call: ToolCall {
                    id: id.clone(),
                    name: delta.name().unwrap_or_default().to_string(),
                    arguments: "{}".to_string(),
                    custom: false,
                },
                content_idx: -1,
                event_id: id,
                arguments: String::new(),
                emitted: false,
                placeholder_id: placeholder,
                wrapped: false,
            });
            self.tools.len() - 1
        };
        if let Some(name) = delta.name()
            && !name.is_empty()
        {
            self.tools[index].call.name = name.to_string();
            if self.custom_tools.contains(name) {
                // A custom-declared tool's wire form is a function
                // wrapper: mark it custom_tool_call-shaped; fragments
                // unwrap at complete.
                self.tools[index].call.custom = true;
                self.tools[index].wrapped = true;
            }
        }
        if delta.is_custom_tool_call() {
            self.tools[index].call.custom = true;
        }
        if self.tools[index].placeholder_id
            && let Some(real_id) = delta.id()
            && !real_id.is_empty()
        {
            // The placeholder call's real id arrived late: backfill the
            // emitted content block so the final message carries the real
            // id for the next replay round; the event ToolCallID keeps
            // the first (placeholder) value — clients already reconcile
            // on start's id and swapping mid-stream would orphan deltas.
            self.tools[index].placeholder_id = false;
            self.tools[index].call.id = real_id.to_string();
            let content_idx = usize::try_from(self.tools[index].content_idx)
                .expect("emitted tool state has a content index");
            Arc::make_mut(&mut self.partial).content[content_idx] =
                Content::ToolCall(self.tools[index].call.clone());
        }
        let mut fragment = delta.arguments_json().unwrap_or("").to_string();
        let mut has_fragment = delta.arguments_json().is_some();
        if let Some(invalid) = delta.invalid_json_str()
            && !invalid.is_empty()
        {
            // A custom/freeform tool's argument body is not JSON to begin
            // with (e.g. patch text): pass it through verbatim instead of
            // swallowing it into {}.
            self.tools[index].call.custom = true;
            fragment = invalid.to_string();
            has_fragment = true;
        }
        if has_fragment {
            self.tools[index].arguments.push_str(&fragment);
        }
        self.decode_native_tool(events, index, fragment, has_fragment);
    }

    /// Preserves Devin's native tool name and argument-delta semantics.
    /// `fragment` moves into the delta event — one copy total per frame.
    fn decode_native_tool(
        &mut self,
        events: &mut Vec<ResponseEvent>,
        index: usize,
        fragment: String,
        has_fragment: bool,
    ) {
        if !self.tools[index].emitted {
            self.tools[index].content_idx =
                i32::try_from(self.partial.content.len()).unwrap_or(i32::MAX);
            self.tools[index].emitted = true;
            Arc::make_mut(&mut self.partial)
                .content
                .push(Content::ToolCall(self.tools[index].call.clone()));
            events.push(ResponseEvent {
                kind: ResponseEventType::ToolCallStart,
                content_index: self.tools[index].content_idx,
                tool_call_id: self.tools[index].event_id.clone(),
                tool_name: self.tools[index].call.name.clone(),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
        // Tool arguments parse once in complete, avoiding per-frame O(n)
        // copy/validation. A wrapped call's fragments are JSON-wrapper
        // shards, not the argument text — no deltas; the unwrapped input
        // is re-emitted as one full delta at complete.
        if has_fragment && !self.tools[index].wrapped {
            events.push(ResponseEvent {
                kind: ResponseEventType::ToolCallDelta,
                content_index: self.tools[index].content_idx,
                tool_call_id: self.tools[index].event_id.clone(),
                delta: fragment,
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
    }

    /// Merges a signature frame arriving after the thinking block closed
    /// back into the last thinking block. With no thinking block to attach
    /// to, synthesizes an empty one: under the openai regime the signature
    /// is the only thinking artifact (no deltaThinking — the reasoning
    /// content is sealed inside the signature's reasoning item), and
    /// dropping it would leave /v1/responses downstream without a
    /// reasoning item forever.
    fn decode_late_signature<F: FrameAccess>(
        &mut self,
        events: &mut Vec<ResponseEvent>,
        response: &F,
    ) {
        let signature = response.delta_signature().unwrap_or_default().to_string();
        // Find the last thinking block first so the mutation borrow ends
        // before `snapshot()` borrows `self.partial` for the event.
        let mut merge_index = None;
        for index in (0..self.partial.content.len()).rev() {
            if matches!(self.partial.content[index], Content::Thinking(_)) {
                merge_index = Some(index);
                break;
            }
        }
        if let Some(index) = merge_index {
            {
                let Content::Thinking(thinking) =
                    &mut Arc::make_mut(&mut self.partial).content[index]
                else {
                    unreachable!("merge_index selects a thinking block");
                };
                thinking.thinking_signature.push_str(&signature);
                if let Some(sig_type) = response.delta_signature_type()
                    && !sig_type.is_empty()
                {
                    thinking.signature_type = sig_type.to_string();
                }
                thinking.redacted = thinking.redacted || response.thinking_redacted();
            }
            events.push(ResponseEvent {
                kind: ResponseEventType::ThinkingSignature,
                content_index: content_index(index),
                delta: signature,
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
            return;
        }
        let thinking = ThinkingContent {
            thinking_signature: signature,
            signature_type: response
                .delta_signature_type()
                .unwrap_or_default()
                .to_string(),
            redacted: response.thinking_redacted(),
            ..ThinkingContent::default()
        };
        Arc::make_mut(&mut self.partial)
            .content
            .push(Content::Thinking(thinking));
        let index = self.partial.content.len() - 1;
        // The events share the same Partial snapshot, so the encoder reads
        // the in-block signature at the start/end boundary; also emitting
        // thinking_signature would double-count it, and a synthesized
        // block never gets a thinking_end — a signature-only event would
        // leave the encoder-side item dangling until stream end.
        events.push(ResponseEvent {
            kind: ResponseEventType::ThinkingStart,
            content_index: content_index(index),
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        });
        events.push(ResponseEvent {
            kind: ResponseEventType::ThinkingEnd,
            content_index: content_index(index),
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        });
    }

    /// Closes the current thinking block: the builder's full body and
    /// signature materialize into `partial` once, then `ThinkingEnd`
    /// emits. A thinking block can be reopened by a cross-block trailing
    /// signature — see `decode_late_signature`.
    fn end_thinking(&mut self, events: &mut Vec<ResponseEvent>) {
        if !self.thinking_open {
            return;
        }
        self.thinking_open = false;
        // Full body and signature materialize only at block end.
        self.thinking.thinking = std::mem::take(&mut self.thinking_builder);
        self.thinking.thinking_signature = std::mem::take(&mut self.thinking_sig_builder);
        let block = std::mem::take(&mut self.thinking);
        let content = block.thinking.clone();
        Arc::make_mut(&mut self.partial).content[self.think_idx] = Content::Thinking(block);
        events.push(ResponseEvent {
            kind: ResponseEventType::ThinkingEnd,
            content_index: content_index(self.think_idx),
            content,
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        });
    }

    /// Closes the current text block: the builder's un-emitted tail (after
    /// a stop-sequence truncation the builder was reset to the truncated
    /// text, so nothing extra emits) flushes as the last delta, then
    /// `TextEnd` emits.
    fn end_text(&mut self, events: &mut Vec<ResponseEvent>) {
        if !self.text_open {
            return;
        }
        self.text_open = false;
        // With stop sequences the tail window may still hold un-emitted
        // content; on truncation the builder was already reset to the
        // truncated text and text_emitted never exceeds its length.
        if self.text_emitted < self.text_builder.len() {
            let pending = self.text_builder[self.text_emitted..].to_string();
            events.push(self.emit_text_delta(pending));
        }
        // The complete text materializes only at block end, avoiding
        // O(n^2) copies; the builder moves into the block (it is cleared
        // on the next open anyway).
        self.text.text = std::mem::take(&mut self.text_builder);
        let block = std::mem::take(&mut self.text);
        let content = block.text.clone();
        Arc::make_mut(&mut self.partial).content[self.text_idx] = Content::Text(block);
        events.push(ResponseEvent {
            kind: ResponseEventType::TextEnd,
            content_index: content_index(self.text_idx),
            content,
            partial: Some(self.snapshot()),
            ..ResponseEvent::default()
        });
    }

    /// Locates the `ToolState` a tool-call delta belongs to: a frame with
    /// an id matches exactly; on no match and a last call still holding a
    /// placeholder id, it is that placeholder's late real id (upstream
    /// sends one call's frames consecutively). An id-less frame merges
    /// positionally into the last call — unless it carries a different
    /// name, which makes it the next call whose first frame lacked an id;
    /// merging would glue two independent calls' arguments together.
    fn find_tool<'a, T: ToolCallAccess<'a>>(&self, delta: &'a T) -> Option<usize> {
        let id = delta.id().unwrap_or("");
        if !id.is_empty() {
            for (index, state) in self.tools.iter().enumerate() {
                if state.call.id == id {
                    return Some(index);
                }
            }
        }
        let last = self.tools.len().checked_sub(1)?;
        if !id.is_empty() {
            if self.tools[last].placeholder_id {
                return Some(last);
            }
            return None;
        }
        if let Some(name) = delta.name()
            && !name.is_empty()
            && !self.tools[last].call.name.is_empty()
            && name != self.tools[last].call.name
        {
            return None;
        }
        Some(last)
    }

    /// Produces the normal closing sequence: set `stop_reason`, close open
    /// thinking/text/tool blocks (tool arguments materialize and repair
    /// here, once), then emit done — `partial` is the final message.
    fn complete(&mut self, reason: StopReason) -> Vec<ResponseEvent> {
        if self.finished {
            return Vec::new();
        }
        let mut events = Vec::with_capacity(self.tools.len() * 3 + 3);
        Arc::make_mut(&mut self.partial).stop_reason = Some(reason);
        self.end_thinking(&mut events);
        self.end_text(&mut events);
        for index in 0..self.tools.len() {
            // The accumulated fragments become the arguments once, at the
            // end, avoiding repeated mid-stream parse/copy; the builder
            // moves out — it is never read again after this point.
            let arguments = std::mem::take(&mut self.tools[index].arguments);
            self.tools[index].call.arguments = arguments;
            if self.tools[index].wrapped {
                // A custom-declared tool's wrapped argument body: unwrap
                // the input verbatim; when the model deviates from the
                // wrapper schema (bare text/multi-key) the whole body
                // passes through as freeform text.
                self.tools[index].call.custom = true;
                self.tools[index].call.arguments =
                    unwrap_custom_tool_arguments(&self.tools[index].call.arguments);
                // Re-emit one complete delta so downstream state that
                // accumulates inputs from deltas converges to the same
                // value.
                events.push(ResponseEvent {
                    kind: ResponseEventType::ToolCallDelta,
                    content_index: self.tools[index].content_idx,
                    tool_call_id: self.tools[index].event_id.clone(),
                    delta: self.tools[index].call.arguments.clone(),
                    partial: Some(self.snapshot()),
                    ..ResponseEvent::default()
                });
            } else if self.tools[index].call.custom {
                // The verbatim body is the arguments (invalid_json_str
                // channel or client-replayed malformed JSON): no JSON
                // validation and no XML repair.
            } else if !crate::domain::is_json_object(&self.tools[index].call.arguments) {
                // swe-family models occasionally leak XML parameter
                // syntax into arguments_json (observed via CLI): first
                // try to recover <parameter name="X">v</parameter> back
                // into JSON, then fall back to {}.
                match repair_leaked_xml_arguments(&self.tools[index].call.arguments) {
                    Some(repaired) => self.tools[index].call.arguments = repaired,
                    None => self.tools[index].call.arguments = "{}".to_string(),
                }
            }
            let content_idx = usize::try_from(self.tools[index].content_idx)
                .expect("emitted tool state has a content index");
            Arc::make_mut(&mut self.partial).content[content_idx] =
                Content::ToolCall(self.tools[index].call.clone());
            events.push(ResponseEvent {
                kind: ResponseEventType::ToolCallEnd,
                content_index: self.tools[index].content_idx,
                tool_call: Some(Box::new(self.tools[index].call.clone())),
                partial: Some(self.snapshot()),
                ..ResponseEvent::default()
            });
        }
        events.push(ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(reason),
            message: Some(self.snapshot()),
            ..ResponseEvent::default()
        });
        self.finished = true;
        events
    }

    /// Produces the error terminal event: `partial` is marked error,
    /// carries the verbatim text and the classified record (consumers
    /// take type/status/retry facts from `failure`, not by re-parsing the
    /// text); done semantics are mapped by consumers from reason=error
    /// into protocol error frames. Repeated calls return empty.
    fn fail(&mut self, err: &(dyn Error + 'static)) -> Vec<ResponseEvent> {
        if self.finished {
            return Vec::new();
        }
        // Go's per-frame signature sync means an error mid-thinking-block
        // carries the accumulated signature in the error snapshot; the
        // deferred materialization must catch up here (end_thinking is
        // skipped on the error path).
        if self.thinking_open {
            self.thinking.thinking_signature = self.thinking_sig_builder.clone();
            Arc::make_mut(&mut self.partial).content[self.think_idx] =
                Content::Thinking(self.thinking.clone());
        }
        {
            let partial = Arc::make_mut(&mut self.partial);
            partial.stop_reason = Some(StopReason::Error);
            partial.error_message = err.to_string();
            partial.failure = Some(Box::new(classify(err)));
        }
        self.finished = true;
        vec![ResponseEvent {
            kind: ResponseEventType::Error,
            reason: Some(StopReason::Error),
            error: Some(self.snapshot()),
            ..ResponseEvent::default()
        }]
    }
}

/// The frame's effective stop reason under proto3-open enum semantics.
enum EffectiveStop {
    /// A declared, non-UNSPECIFIED enum value.
    Declared(ExaCodeiumCommonPb_StopReason),
    /// An undeclared varint recovered from the unknown set (Go keeps it in
    /// the field; buffa routes it to unknown fields).
    Undeclared(u64),
    /// Absent or explicit UNSPECIFIED.
    Absent,
}

/// The tool names declared with freeform/custom semantics in the request,
/// for the decoder to recognize wrapped function calls and unwrap them
/// back to verbatim text.
pub fn custom_tool_names(tools: &[ToolDefinition]) -> HashSet<String> {
    tools
        .iter()
        .filter(|tool| tool.custom)
        .map(|tool| tool.name.clone())
        .collect()
}

/// Maps the upstream `stop_reason` enum onto the intermediate model; the
/// enum still carries completion-era termination shapes, so same-semantics
/// values fold together. Undeclared values (Go's default arm) map to Stop.
pub fn map_stop_reason(reason: ExaCodeiumCommonPb_StopReason) -> StopReason {
    use ExaCodeiumCommonPb_StopReason as R;
    match reason {
        // INCOMPLETE/PARTIAL both mean the model did not produce a complete
        // reply; treat as length truncation.
        R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MAX_TOKENS
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MAX_NEWLINES
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_INCOMPLETE
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_PARTIAL => StopReason::Length,
        R::ExaCodeiumCommonPb_StopReason_STOP_REASON_FUNCTION_CALL => StopReason::ToolUse,
        R::ExaCodeiumCommonPb_StopReason_STOP_REASON_CONTENT_FILTER => StopReason::ContentFilter,
        R::ExaCodeiumCommonPb_StopReason_STOP_REASON_ERROR
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_NONFINITE_LOGIT_OR_PROB => StopReason::Error,
        // STOP_PATTERN is the normal ending of upstream's own stop-pattern
        // mechanism (observed to close normal replies); the rest are
        // completion-era normal terminations.
        R::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MIN_LOG_PROB
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_EXIT_SCOPE
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_FIRST_NON_WHITESPACE_LINE
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_NON_INSERTION
        | R::ExaCodeiumCommonPb_StopReason_STOP_REASON_UNSPECIFIED => StopReason::Stop,
    }
}

/// The `api_provider` name as Go's `usage.GetApiProvider().String()`
/// produces it: the declared variant's proto name, or the raw number for
/// an undeclared value recovered from the unknown set (proto3-open enum
/// semantics — Go keeps the value in the field and prints the number).
fn api_provider_name<'a, U: UsageAccess<'a>>(usage: &'a U) -> String {
    if let Some(provider) = usage.api_provider() {
        return provider.proto_name().to_string();
    }
    if usage.has_unknown_fields() {
        for field in &usage.unknown_fields() {
            if field.number == USAGE_API_PROVIDER_FIELD
                && let UnknownFieldData::Varint(raw) = field.data
            {
                return raw.to_string();
            }
        }
    }
    // The Go getter's default for an unset field.
    ExaCodeiumCommonPb_APIProvider::default()
        .proto_name()
        .to_string()
}

/// Unwraps the `{"input":"<verbatim>"}` wrapper schema's payload; when the
/// model deviates from the wrapper (bare text or a non-string input) the
/// whole body passes through under freeform semantics.
fn unwrap_custom_tool_arguments(raw: &str) -> String {
    let Ok(object) = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(raw) else {
        return raw.to_string();
    };
    if let Some(input) = object.get("input")
        && let Some(text) = input.as_str()
    {
        return text.to_string();
    }
    raw.to_string()
}

/// Extracts `<parameter name="X">v</parameter>` tags leaked into
/// `arguments_json` back into a JSON object; returns `None` when no
/// parameter tags exist so the caller keeps its original fallback path.
/// Hand-rolled equivalent of Go's
/// `<(?:antml:)?parameter\s+name="([A-Za-z_][\w-]*)"[^>]*>([\s\S]*?)</(?:antml:)?parameter>`
/// (Go `\s` is `[\t\n\f\r ]`, `\w` is ASCII `[0-9A-Za-z_]`).
fn repair_leaked_xml_arguments(raw: &str) -> Option<String> {
    const OPEN_PREFIXES: &[&str] = &["<parameter", "<antml:parameter"];
    const CLOSE_TAGS: &[&str] = &["</parameter>", "</antml:parameter>"];
    let bytes = raw.as_bytes();
    let mut object: BTreeMap<String, String> = BTreeMap::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        // Find the next '<' that could start an open tag.
        let Some(open) = raw[cursor..].find('<').map(|idx| cursor + idx) else {
            break;
        };
        let rest = &raw[open..];
        let Some(name_start) = OPEN_PREFIXES
            .iter()
            .find(|prefix| rest.starts_with(**prefix))
            .map(|prefix| open + prefix.len())
        else {
            cursor = open + 1;
            continue;
        };
        // `\s+` — at least one Go-regexp whitespace byte.
        let mut pos = name_start;
        while pos < bytes.len() && matches!(bytes[pos], b'\t' | b'\n' | b'\x0C' | b'\r' | b' ') {
            pos += 1;
        }
        if pos == name_start || !rest[pos - open..].starts_with("name=\"") {
            cursor = open + 1;
            continue;
        }
        pos += "name=\"".len();
        // Name: [A-Za-z_][0-9A-Za-z_-]*
        let name_begin = pos;
        if pos >= bytes.len() || !matches!(bytes[pos], b'A'..=b'Z' | b'a'..=b'z' | b'_') {
            cursor = open + 1;
            continue;
        }
        pos += 1;
        while pos < bytes.len()
            && matches!(bytes[pos], b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'_' | b'-')
        {
            pos += 1;
        }
        let name = &raw[name_begin..pos];
        // Closing quote, then [^>]* up to the tag's '>'.
        if pos >= bytes.len() || bytes[pos] != b'"' {
            cursor = open + 1;
            continue;
        }
        pos += 1;
        let Some(tag_end) = raw[pos..].find('>').map(|idx| pos + idx) else {
            break;
        };
        let content_start = tag_end + 1;
        // Non-greedy content: ends at the earliest close tag of either
        // spelling.
        let mut close = None;
        for tag in CLOSE_TAGS {
            if let Some(idx) = raw[content_start..].find(tag) {
                let end = content_start + idx;
                if close.is_none_or(|(current, _)| end < current) {
                    close = Some((end, tag.len()));
                }
            }
        }
        let Some((content_end, tag_len)) = close else {
            break;
        };
        object.insert(
            name.to_string(),
            raw[content_start..content_end].trim().to_string(),
        );
        cursor = content_end + tag_len;
    }
    if object.is_empty() {
        return None;
    }
    // Go's json.Marshal on map[string]string: sorted keys (BTreeMap) and
    // the default HTML-safe string escaping.
    let mut out = String::from("{");
    for (index, (key, value)) in object.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&go_json_string(key));
        out.push(':');
        out.push_str(&go_json_string(value));
    }
    out.push('}');
    Some(out)
}

/// Escapes a string the way Go's `encoding/json` does by default
/// (`SetEscapeHTML(true)`): `"` and `\`, control characters as `\n`-style
/// shorts or `\u00XX`, `<`/`>`/`&` as `\u003c`/`\u003e`/`\u0026`, and
/// U+2028/U+2029 escaped. Other characters pass through unescaped.
fn go_json_string(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            ch if (ch as u32) < 0x20 => {
                let code = ch as u32;
                out.push_str("\\u00");
                out.push(HEX[(code >> 4) as usize] as char);
                out.push(HEX[(code & 0xF) as usize] as char);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// `content_index` is `i32` in the domain event (Go's signed int); block
/// indexes are `usize` here — saturate rather than wrap on absurd input.
fn content_index(index: usize) -> i32 {
    i32::try_from(index).unwrap_or(i32::MAX)
}

/// Go's `int64(uint64)` conversion, which wraps on overflow the same way.
#[allow(clippy::cast_possible_wrap)]
fn u64_as_i64(value: u64) -> i64 {
    value as i64
}

/// Current Unix time in milliseconds (Go `time.Now().UnixMilli()`).
fn unix_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// Byte stream alias used by the streaming decoder (task 11 call site).
pub type FrameStream<'a> =
    futures_util::stream::BoxStream<'a, Result<bytes::Bytes, std::io::Error>>;
