//! Protocol-neutral domain types shared by the three public APIs.
//!
//! Port of `G/internal/llm` (request/response/failure/stream). The Go
//! package uses interface types (`Message`, `Content`); the port uses
//! enums with the same variant set so exhaustive matching replaces the
//! Go type switches.

pub mod failure;
pub mod request;
pub mod response;

pub use failure::{
    Canceled, DeadlineExceeded, Failure, classify, classify_text, failure_of,
    is_http2_transport_error, is_idle_conn_closed_error,
};
pub use request::{
    Content, ContentType, ImageContent, Message, MessageRole, RequestMessages, RequestRepairs,
    TextContent, ThinkingContent, ToolCall, ToolChoice, ToolChoiceMode, ToolDefinition,
    ToolResultMessage, UserMessage, is_json_object,
};
pub use response::{
    AssistantMessage, AssistantMessageDiagnostic, ResponseEvent, ResponseEventType, ResponseStream,
    StopReason, Usage,
};
