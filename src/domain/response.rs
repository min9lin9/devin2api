//! Aggregated assistant responses, usage, diagnostics and incremental
//! response events.
//!
//! Port of `G/internal/llm/response.go` and `stream.go`. Go's optional
//! `StopReason` (empty string = unset) becomes `Option<StopReason>`; Go's
//! `*AssistantMessage` event fields become `Option<AssistantMessage>`.

use std::future::Future;
use std::sync::Arc;

use super::failure::Failure;
use super::request::{Content, ContentType, ToolCall};

/// Why generation stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// `pending` — generation still in progress.
    Pending,
    /// `stop` — natural completion.
    Stop,
    /// `stopSequence` — generation was cut by a client-provided stop
    /// sequence. The Devin upstream does not evaluate `stop_patterns`; the
    /// adapter truncates locally at the decode layer.
    StopSequence,
    /// `length` — token limit reached.
    Length,
    /// `toolUse` — the model wants to call tools.
    ToolUse,
    /// `contentFilter` — an upstream content filter ended generation.
    ContentFilter,
    /// `error` — generation failed.
    Error,
    /// `aborted` — generation was aborted.
    Aborted,
}

impl StopReason {
    /// Wire value used in Go error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Stop => "stop",
            Self::StopSequence => "stopSequence",
            Self::Length => "length",
            Self::ToolUse => "toolUse",
            Self::ContentFilter => "contentFilter",
            Self::Error => "error",
            Self::Aborted => "aborted",
        }
    }
}

/// Provider-independent assistant message; also the final response message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AssistantMessage {
    /// Generated text, thinking and tool-call blocks.
    pub content: Vec<Content>,
    /// Upstream API protocol used to produce the message.
    pub api: String,
    /// Model provider that produced the message.
    pub provider: String,
    /// Model identifier selected at request time.
    pub model: String,
    /// Actual model identifier returned by the provider.
    pub response_model: String,
    /// Provider-assigned response identifier, usable to continue a session or
    /// for diagnostics.
    pub response_id: String,
    /// Provider-assigned output item identifier (Devin upstream `outputId`,
    /// `msg_*` in `OpenAI` shape). Replayed verbatim as the wire `output_id`
    /// with this message — the cross-provider output-item identity anchor.
    pub output_id: String,
    /// Tracing identifier the upstream service assigned to this call (Devin
    /// Connect `request_id`); can be handed to upstream support directly.
    pub upstream_request_id: String,
    /// Non-primary-response diagnostics collected during conversion or
    /// streaming.
    pub diagnostics: Vec<AssistantMessageDiagnostic>,
    /// Accumulated token and cost usage for this response.
    pub usage: Usage,
    /// Current or final generation state; `None` mirrors Go's unset
    /// `StopReason == ""`.
    pub stop_reason: Option<StopReason>,
    /// The stop sequence actually hit when `stop_reason` is `StopSequence`.
    pub stop_sequence: String,
    /// Human-readable error when generation failed or was aborted.
    pub error_message: String,
    /// Classification record carried by the producer; when `None`, consumers
    /// fall back to classifying `error_message` text (see `failure_of`).
    pub failure: Option<Box<Failure>>,
    /// This proxy's request-log reference (debug directory name), injected by
    /// the app layer before error events go out — clients/agents can locate
    /// the full evidence chain with it.
    pub debug_ref: String,
    /// Unix-millisecond creation timestamp.
    pub timestamp_ms: i64,
}

impl AssistantMessage {
    /// Checks the assistant message.
    ///
    /// Error strings mirror the Go `Validate` messages. Go's invalid
    /// `StopReason` arm is unreachable: `Option<StopReason>` cannot hold an
    /// undefined value.
    pub fn validate(&self) -> Result<(), String> {
        validate_assistant_content(&self.content)?;
        self.usage.validate()
    }
}

fn validate_assistant_content(content: &[Content]) -> Result<(), String> {
    const ALLOWED: &[ContentType] = &[
        ContentType::Text,
        ContentType::Thinking,
        ContentType::ToolCall,
    ];
    for (index, block) in content.iter().enumerate() {
        if !ALLOWED.contains(&block.content_type()) {
            return Err(format!(
                "content block {index} has disallowed type {:?}",
                block.content_type().as_str()
            ));
        }
        if let Err(err) = block.validate() {
            return Err(format!(
                "content block {index} ({}): {err}",
                block.content_type().as_str()
            ));
        }
    }
    Ok(())
}

/// Accumulated usage of one assistant response or tool execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage {
    /// Input token count.
    pub input: i64,
    /// Output token count.
    pub output: i64,
    /// Tokens read from the prompt cache.
    pub cache_read: i64,
    /// Tokens written to the prompt cache.
    pub cache_write: i64,
    /// Subset of output tokens spent on reasoning; `None` means the provider
    /// did not report it. Currently no producer: Devin usage does not split
    /// reasoning tokens; reserved for upstreams that report
    /// `reasoning_tokens` — downstream encoders (`OpenAI` usage output, index
    /// column) are already in place.
    pub reasoning: Option<i64>,
    /// Total token count reported by the provider or computed by the adapter.
    pub total_tokens: i64,
}

impl Usage {
    /// Checks that all usage fields are non-negative.
    pub fn validate(&self) -> Result<(), String> {
        if self.input < 0
            || self.output < 0
            || self.cache_read < 0
            || self.cache_write < 0
            || self.total_tokens < 0
        {
            return Err("usage values cannot be negative".to_string());
        }
        if self.reasoning.is_some_and(|reasoning| reasoning < 0) {
            return Err("reasoning usage cannot be negative".to_string());
        }
        Ok(())
    }
}

/// A diagnostic record that does not change the main response result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssistantMessageDiagnostic {
    /// Stable diagnostic event type identifier (`type` in Go).
    pub kind: String,
    /// Unix-millisecond timestamp when the diagnostic was produced.
    pub timestamp_ms: i64,
    /// Optional structured diagnostic details (verbatim JSON text).
    pub details: String,
}

/// Kind of an incremental event in the response stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResponseEventType {
    /// `start`
    #[default]
    Start,
    /// `text_start`
    TextStart,
    /// `text_delta`
    TextDelta,
    /// `text_end`
    TextEnd,
    /// `thinking_start`
    ThinkingStart,
    /// `thinking_delta`
    ThinkingDelta,
    /// `thinking_end`
    ThinkingEnd,
    /// `thinking_signature` — a signature delta arriving after the thinking
    /// block ended (the Devin upstream sends signatures as trailing frames).
    /// `content_index` points at the already-ended block.
    ThinkingSignature,
    /// `toolcall_start`
    ToolCallStart,
    /// `toolcall_delta`
    ToolCallDelta,
    /// `toolcall_end`
    ToolCallEnd,
    /// `done`
    Done,
    /// `error`
    Error,
}

impl ResponseEventType {
    /// Wire value used in Go error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::TextStart => "text_start",
            Self::TextDelta => "text_delta",
            Self::TextEnd => "text_end",
            Self::ThinkingStart => "thinking_start",
            Self::ThinkingDelta => "thinking_delta",
            Self::ThinkingEnd => "thinking_end",
            Self::ThinkingSignature => "thinking_signature",
            Self::ToolCallStart => "toolcall_start",
            Self::ToolCallDelta => "toolcall_delta",
            Self::ToolCallEnd => "toolcall_end",
            Self::Done => "done",
            Self::Error => "error",
        }
    }
}

/// A provider-independent incremental assistant-response event.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResponseEvent {
    /// This event's kind.
    pub kind: ResponseEventType,
    /// Assistant content-block index this event applies to. Signed like Go's
    /// `int` so the negative-index contract is checkable.
    pub content_index: i32,
    /// Newly added text, thinking, or not-yet-complete tool-argument JSON
    /// fragment.
    pub delta: String,
    /// Complete content when a text or thinking block ends.
    pub content: String,
    /// Assistant message accumulated after this event, including current
    /// usage. Shared with the decoder through an `Arc` (Go keeps one
    /// `*AssistantMessage` per stream and reuses the pointer per frame):
    /// emitting an event is an `Arc::clone`, and the decoder's next
    /// mutation goes through `Arc::make_mut` — in-place once the consumer
    /// has dropped the previous events, one clone per frame when events
    /// are still queued.
    pub partial: Option<Arc<AssistantMessage>>,
    /// Stable call identifier for tool-call start and argument-delta events.
    pub tool_call_id: String,
    /// Tool name declared by the tool-call start event.
    pub tool_name: String,
    /// Fully parsed call when a tool-call event ends.
    pub tool_call: Option<ToolCall>,
    /// Stop reason of done/error events; `None` mirrors Go's unset reason.
    pub reason: Option<StopReason>,
    /// Final assistant message on normal completion.
    pub message: Option<AssistantMessage>,
    /// Final error assistant message on failure or abort.
    pub error: Option<AssistantMessage>,
}

impl ResponseEvent {
    /// Checks the event against the fields its kind requires.
    ///
    /// Error strings mirror the Go `Validate` messages.
    pub fn validate(&self) -> Result<(), String> {
        match self.kind {
            ResponseEventType::Start => self.require_partial(),
            ResponseEventType::TextStart
            | ResponseEventType::ThinkingStart
            | ResponseEventType::TextEnd
            | ResponseEventType::ThinkingEnd => self.require_indexed_partial(),
            ResponseEventType::ToolCallStart => {
                self.require_indexed_partial()?;
                if self.tool_call_id.is_empty() {
                    return Err("tool call start event requires a tool call ID".to_string());
                }
                if self.tool_name.is_empty() {
                    return Err("tool call start event requires a tool name".to_string());
                }
                Ok(())
            }
            ResponseEventType::TextDelta
            | ResponseEventType::ThinkingDelta
            | ResponseEventType::ThinkingSignature => {
                self.require_indexed_partial()?;
                if self.delta.is_empty() {
                    return Err("delta event requires a delta".to_string());
                }
                Ok(())
            }
            ResponseEventType::ToolCallDelta => {
                self.require_indexed_partial()?;
                if self.tool_call_id.is_empty() {
                    return Err("tool call delta event requires a tool call ID".to_string());
                }
                Ok(())
            }
            ResponseEventType::ToolCallEnd => {
                self.require_indexed_partial()?;
                let Some(call) = &self.tool_call else {
                    return Err("tool call end event requires a tool call".to_string());
                };
                call.validate()
            }
            ResponseEventType::Done => {
                match self.reason {
                    Some(
                        StopReason::Stop
                        | StopReason::StopSequence
                        | StopReason::Length
                        | StopReason::ToolUse
                        | StopReason::ContentFilter,
                    ) => {}
                    other => {
                        return Err(format!(
                            "invalid done reason {:?}",
                            other.map_or("", StopReason::as_str)
                        ));
                    }
                }
                let Some(message) = &self.message else {
                    return Err("done event requires a final message".to_string());
                };
                message.validate()
            }
            ResponseEventType::Error => {
                match self.reason {
                    Some(StopReason::Error | StopReason::Aborted) => {}
                    other => {
                        return Err(format!(
                            "invalid error reason {:?}",
                            other.map_or("", StopReason::as_str)
                        ));
                    }
                }
                let Some(error) = &self.error else {
                    return Err("error event requires a final error message".to_string());
                };
                error.validate()
            }
        }
    }

    /// Requires a `partial` message that itself validates.
    fn require_partial(&self) -> Result<(), String> {
        let Some(partial) = &self.partial else {
            return Err(format!(
                "{} event requires a partial message",
                self.kind.as_str()
            ));
        };
        partial.validate()
    }

    /// Checks only that `content_index` is non-negative and `partial` is
    /// present, without deep-validating the message: `partial` is the same
    /// accumulated object reused frame by frame, and a full `validate` per
    /// frame would be O(frames × blocks). The deep contract is covered by
    /// the start/done/error boundary events, where encoders read the full
    /// semantics.
    fn require_indexed_partial(&self) -> Result<(), String> {
        if self.content_index < 0 {
            return Err(format!(
                "{} event requires a non-negative content index",
                self.kind.as_str()
            ));
        }
        if self.partial.is_none() {
            return Err(format!(
                "{} event requires a partial message",
                self.kind.as_str()
            ));
        }
        Ok(())
    }
}

/// An ordered stream of incremental events for one or more assistant
/// responses, as returned by provider adapters.
///
/// Port of Go's `ResponseStream` interface: `recv` yields the next event;
/// `Ok(None)` is end-of-stream (Go's `io.EOF` from `Recv`), `Err` carries a
/// classified [`Failure`].
pub trait ResponseStream {
    /// Returns the next response event, `None` at end of stream.
    fn recv(&mut self) -> impl Future<Output = Result<Option<ResponseEvent>, Failure>> + Send;

    /// Non-blocking receive: an event already decoded or a frame already
    /// delivered by the pump, converted synchronously. `None` means
    /// "nothing ready right now" (the caller falls back to `recv`), never
    /// end-of-stream. Default: no ready-event fast path.
    ///
    /// This is the cancel-safe equivalent of Go's `select { case item :=
    /// <-items: ... default: flush }` burst drain: it never polls a
    /// future, so nothing can be dropped mid-await.
    fn try_recv(&mut self) -> Option<ResponseEvent> {
        None
    }
}
