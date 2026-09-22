#!/usr/bin/env python3
"""Generates cases.json for task-12 output-protocol fixtures.

Each case mirrors one Go test's inputs (or a dispatch-layer scenario). The Go
oracle harness replays the same inputs through the Go encoders and records
expected.json; the Rust test replays them through the Rust port and compares
after normalizing generated ids and wall-clock timestamps.
"""
import json, os

def text(t): return {"kind": "text", "text": t}
def thinking(t="", sig="", sig_type="", redacted=False):
    return {"kind": "thinking", "thinking": t, "signature": sig,
            "signature_type": sig_type, "redacted": redacted}
def call(id, name, args, custom=False):
    return {"kind": "tool_call",
            "tool_call": {"id": id, "name": name, "arguments": args, "custom": custom}}

def msg(content=None, stop="pending", **kw):
    m = {"content": content or [], "stop_reason": stop}
    m.update(kw)
    return m

def usage(i=0, o=0, cr=0, cw=0, total=0, reasoning=None):
    u = {"input": i, "output": o, "cache_read": cr, "cache_write": cw,
         "total_tokens": total}
    if reasoning is not None:
        u["reasoning"] = reasoning
    return u

def ev(type, ci=0, delta="", content="", partial=None, tcid="", tname="",
       tool_call=None, reason=None, message=None, error=None):
    e = {"type": type}
    if ci: e["content_index"] = ci
    if delta: e["delta"] = delta
    if content: e["content"] = content
    if partial is not None: e["partial"] = partial
    if tcid: e["tool_call_id"] = tcid
    if tname: e["tool_name"] = tname
    if tool_call is not None: e["tool_call"] = tool_call
    if reason is not None: e["reason"] = reason
    if message is not None: e["message"] = message
    if error is not None: e["error"] = error
    return e

PEND = msg()  # bare pending partial for start events
cases = []

def case(name, protocol, mode, **kw):
    c = {"name": name, "protocol": protocol, "mode": mode}
    c.update(kw)
    cases.append(c)

# ---------------- OpenAI Responses ----------------

# TestStreamEncoderEncodesReasoningAndToolItems
th = thinking("inspect", "encrypted")
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([th, tc])
final = msg([th, tc], "toolUse", usage=usage(10, 5, 2, 0, 17))
case("responses_stream_reasoning_and_tool_items", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("toolcall_start", 1, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 1, delta='{"city":"', partial=partial, tcid="call-1"),
        ev("toolcall_delta", 1, delta='Shanghai"}', partial=partial, tcid="call-1"),
        ev("toolcall_end", 1, partial=partial,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestStreamEncoderEncodesFinalTextMessage
t = text("final answer")
partial = msg([t])
final = msg([t], "stop")
case("responses_stream_final_text_message", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("text_start", 0, partial=partial),
        ev("text_delta", 0, delta="final ", partial=partial),
        ev("text_delta", 0, delta="answer", partial=partial),
        ev("text_end", 0, content="final answer", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderHoldsReasoningForLateSignature
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([thinking("inspect"), tc])
partial_sig = msg([thinking("inspect", "sig"), tc])
final = msg([thinking("inspect", "sig"), tc], "toolUse")
case("responses_stream_late_signature", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("toolcall_start", 1, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 1, delta='{"city":"Shanghai"}', partial=partial, tcid="call-1"),
        ev("thinking_signature", 0, delta="sig", partial=partial_sig),
        ev("toolcall_end", 1, partial=partial_sig,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestStreamEncoderAccumulatesSignatureFragments
partial = msg([thinking("inspect")])
final = msg([thinking("inspect", "AAABBB")], "stop")
case("responses_stream_signature_fragments", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("thinking_signature", 0, delta="AAA", partial=final),
        ev("thinking_signature", 0, delta="BBB", partial=final),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderEncodesSignatureOnlyReasoning
th = thinking("", "sig")
partial = msg([th])
final = msg([th], "stop")
case("responses_stream_signature_only_reasoning", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_end", 0, partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderRejectsUnknownEvent — Go's Type is a free string; Rust's
# ResponseEventType is a closed enum so "unknown" is unrepresentable. The
# fixture runner asserts Go rejected it and skips the Rust encode.
case("responses_stream_rejects_unknown_event", "responses", "stream",
     model="gpt-test", events=[ev("unknown")])

# TestStreamEncoderRejectsDoneWithOpenItem
partial = msg([text("partial")])
final = msg([text("partial")], "stop")
case("responses_stream_rejects_done_with_open_item", "responses", "stream",
     model="gpt-test", events=[
        ev("text_start", 0, partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderEncodesLengthAsIncomplete
case("responses_stream_length_incomplete", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("done", reason="length", message=msg([], "length")),
     ])

# TestResponseUsageIncludesCachedTokensInInputTotal (via encode_response)
case("responses_final_cached_usage", "responses", "final",
     model="gpt-test",
     message=msg([text("hi")], "stop", response_model="gpt-test",
                 usage=usage(167, 61, 12195, 0, 12423)))

# TestStreamEncoderOpenAISignatureRestoresItemID
blob = '[{"id":"rs_real1","type":"reasoning","encrypted_content":"gAAA","summary":[],"content":[],"status":""}]'
th = thinking("", blob, "openai", True)
partial = msg([th])
final = msg([th], "stop")
case("responses_stream_openai_signature_item_id", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_end", 0, partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderMessageItemUsesOutputID
partial = msg([text("")], output_id="msg_up1")
final = msg([text("hi")], "stop", output_id="msg_up1")
case("responses_stream_message_item_output_id", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("text_start", 0, partial=partial),
        ev("text_delta", 0, delta="hi", partial=partial),
        ev("text_end", 0, content="hi", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderCustomToolCall
tc = call("c1", "apply_patch", "*** Begin Patch", True)
partial = msg([tc])
final = msg([tc], "toolUse")
case("responses_stream_custom_tool_call", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 0, partial=partial, tcid="c1", tname="apply_patch"),
        ev("toolcall_delta", 0, delta="*** Begin Patch", partial=partial, tcid="c1"),
        ev("toolcall_end", 0, partial=partial,
           tool_call={"id": "c1", "name": "apply_patch", "arguments": "*** Begin Patch", "custom": True}),
        ev("done", reason="toolUse", message=final),
     ])

# QA scenario: failure mid-stream after a partial tool call — the terminal
# error shape must be a real response.failed, not a fake completion.
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([tc])
failed = msg([tc], "error", provider="devin",
             error_message="resource_exhausted: rate limit exceeded")
case("responses_stream_failure_after_partial_tool", "responses", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 0, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 0, delta='{"city":"', partial=partial, tcid="call-1"),
        ev("error", reason="error", error=failed),
     ])

# ---------------- OpenAI Chat Completions ----------------

# TestStreamEncoderEmitsRoleAndText
t = text("final answer")
partial = msg([t])
final = msg([t], "stop")
case("chat_stream_role_and_text", "chat", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("text_start", 0, partial=partial),
        ev("text_delta", 0, delta="final ", partial=partial),
        ev("text_delta", 0, delta="answer", partial=partial),
        ev("text_end", 0, content="final answer", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderEmitsToolCalls
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([tc])
final = msg([tc], "toolUse")
case("chat_stream_tool_calls", "chat", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 0, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 0, delta='{"city":"', partial=partial, tcid="call-1"),
        ev("toolcall_delta", 0, delta='Shanghai"}', partial=partial, tcid="call-1"),
        ev("toolcall_end", 0, partial=partial,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestStreamEncoderToolCallAfterOtherBlocks — rekeyed tool_call_id on deltas.
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([thinking("t"), text("x"), tc])
final = msg([thinking("t"), text("x"), tc], "toolUse")
case("chat_stream_tool_call_after_other_blocks", "chat", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 2, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 2, delta='{"city":"', partial=partial, tcid="call-rekeyed"),
        ev("toolcall_delta", 2, delta='Shanghai"}', partial=partial, tcid="call-rekeyed"),
        ev("toolcall_end", 2, partial=partial,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestEncodeResponseFinal
case("chat_final", "chat", "final", model="",
     message=msg([text("hello")], "stop", response_id="chatcmpl-1",
                 response_model="gpt-test",
                 usage=usage(10, 5, 3, 0, 18)))

# TestEncodeResponseFinalWithReasoning
case("chat_final_with_reasoning", "chat", "final", model="",
     message=msg([thinking("think"), text("hello")], "stop",
                 response_id="chatcmpl-2", response_model="gpt-test"))

# TestMessageToChatKeepsTextWithToolCalls (via encode_response)
case("chat_final_text_with_tool_calls", "chat", "final", model="",
     message=msg([text("let me check"),
                  call("c1", "read_file", '{"path":"a"}')], "toolUse",
                 response_model="gpt-test"))
case("chat_final_tool_call_only", "chat", "final", model="",
     message=msg([call("c1", "read_file", '{"path":"a"}')], "toolUse",
                 response_model="gpt-test"))

# TestStreamEncoderEmitsError
failed = msg([], "error", provider="devin",
             error_message="resource_exhausted: rate limit exceeded")
case("chat_stream_error", "chat", "stream", model="gpt-test", events=[
    ev("error", reason="error", error=failed),
])

# TestStreamEncoderEmitsThinkingAsReasoningContent
th, t = thinking("think"), text("hello")
partial = msg([th, t])
final = msg([th, t], "stop")
case("chat_stream_thinking_as_reasoning_content", "chat", "stream",
     model="gpt-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="think", partial=partial),
        ev("thinking_end", 0, content="think", partial=partial),
        ev("text_start", 1, partial=partial),
        ev("text_delta", 1, delta="hello", partial=partial),
        ev("text_end", 1, content="hello", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# include_usage=true: the finish chunk carries a usage object (Go's
# stream_options.include_usage path) — exercises chat_usage mid-stream.
t = text("hi")
partial = msg([t])
final = msg([t], "stop", usage=usage(10, 5, 0, 0, 15))
case("chat_stream_include_usage", "chat", "stream",
     model="gpt-test", include_usage=True, events=[
        ev("start", partial=PEND),
        ev("text_start", 0, partial=partial),
        ev("text_delta", 0, delta="hi", partial=partial),
        ev("text_end", 0, content="hi", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# ---------------- Anthropic Messages ----------------

# TestStreamEncoderEmitsMessageStartAndText
t = text("hello")
partial = msg([t])
final = msg([t], "stop")
case("messages_stream_message_start_and_text", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=msg([], response_id="msg-1")),
        ev("text_start", 0, partial=partial),
        ev("text_delta", 0, delta="hello", partial=partial),
        ev("text_end", 0, content="hello", partial=partial),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderEmitsToolUse
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([tc])
final = msg([tc], "toolUse")
case("messages_stream_tool_use", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 0, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 0, delta='{"city":"', partial=partial, tcid="call-1"),
        ev("toolcall_delta", 0, delta='Shanghai"}', partial=partial, tcid="call-1"),
        ev("toolcall_end", 0, partial=partial,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestStreamEncoderHoldsThinkingForLateSignature
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([thinking("inspect"), tc])
partial_sig = msg([thinking("inspect", "sig"), tc])
final = msg([thinking("inspect", "sig"), tc], "toolUse")
case("messages_stream_late_signature", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("toolcall_start", 1, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 1, delta='{"city":"Shanghai"}', partial=partial, tcid="call-1"),
        ev("thinking_signature", 0, delta="sig", partial=partial_sig),
        ev("toolcall_end", 1, partial=partial_sig,
           tool_call={"id": "call-1", "name": "lookup", "arguments": '{"city":"Shanghai"}'}),
        ev("done", reason="toolUse", message=final),
     ])

# TestStreamEncoderClosesBlocksInIndexOrder
partial = msg([thinking("inspect"), text("4")])
final = msg([thinking("inspect", "sig"), text("4")], "stop")
case("messages_stream_closes_blocks_in_index_order", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("text_start", 1, partial=partial),
        ev("text_delta", 1, delta="4", partial=partial),
        ev("text_end", 1, content="4", partial=partial),
        ev("thinking_signature", 0, delta="sig", partial=final),
        ev("done", reason="stop", message=final),
     ])

# TestStreamEncoderBuffersSignatureFragments
partial = msg([thinking("inspect")])
with_sig = msg([thinking("inspect", "AAABBB")], "stop")
case("messages_stream_signature_fragments", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
        ev("thinking_signature", 0, delta="AAA", partial=with_sig),
        ev("thinking_signature", 0, delta="BBB", partial=with_sig),
        ev("done", reason="stop", message=with_sig),
     ])

# TestEncodeResponseFinal
case("messages_final", "messages", "final", model="",
     message=msg([text("hello")], "stop", response_id="msg-1",
                 response_model="claude-test",
                 usage=usage(10, 5, 3, 2, 0)))

# TestStreamEncoderEmitsError
failed = msg([], "error", provider="devin",
             error_message="permission_denied: not allowed")
case("messages_stream_error", "messages", "stream", model="claude-test", events=[
    ev("error", reason="error", error=failed),
])

# TestStreamEncoderSignatureReadyAtThinkingEnd
partial = msg([thinking("inspect", "sig")])
case("messages_stream_signature_ready_at_thinking_end", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_delta", 0, delta="inspect", partial=partial),
        ev("thinking_end", 0, content="inspect", partial=partial),
     ])

# TestStreamEncoderRedactedThinkingDeferredStart
partial = msg([thinking("hidden", "sealed-payload", "", True)])
case("messages_stream_redacted_thinking_deferred_start", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("thinking_start", 0, partial=partial),
        ev("thinking_end", 0, partial=partial),
     ])

# QA scenario: failure mid-stream after a partial tool call.
tc = call("call-1", "lookup", '{"city":"Shanghai"}')
partial = msg([tc])
failed = msg([tc], "error", provider="devin",
             error_message="resource_exhausted: rate limit exceeded")
case("messages_stream_failure_after_partial_tool", "messages", "stream",
     model="claude-test", events=[
        ev("start", partial=PEND),
        ev("toolcall_start", 0, partial=partial, tcid="call-1", tname="lookup"),
        ev("toolcall_delta", 0, delta='{"city":"', partial=partial, tcid="call-1"),
        ev("error", reason="error", error=failed),
     ])

# ---------------- dispatch layer: encode_error / encode_http_error ----------------

def failure(message, **kw):
    f = {"message": message}
    f.update(kw)
    return f

# encode_error: in-stream error body after a committed 200.
case("responses_encode_error_rate_limit", "responses", "error",
     debug_ref="dbg-1",
     error=failure("rate limit exceeded, reset in 30 seconds",
                   code="resource_exhausted"))
case("chat_encode_error_internal", "chat", "error",
     error=failure("invalid_argument: an internal error occurred (trace ID: abc)"))
case("messages_encode_error_permission", "messages", "error",
     debug_ref="dbg-2",
     error=failure("not allowed", code="permission_denied"))

# encode_http_error: pre-commit HTTP error bodies.
case("responses_http_error_rate_limit", "responses", "http_error",
     http_error={"failure": failure("quota exhausted",
                                    code="resource_exhausted",
                                    retry_after_seconds=30),
                 "stage": "provider_stream", "debug_ref": "dbg-3"})
case("chat_http_error_client_fixable", "chat", "http_error",
     http_error={"failure": failure("invalid_argument: model does not support image"),
                 "client_fixable": True, "stage": "http_decode"})
case("messages_http_error_upstream", "messages", "http_error",
     http_error={"failure": failure("unavailable: upstream offline"),
                 "stage": "provider_stream", "debug_ref": "dbg-4"})
case("messages_http_error_client_fixable", "messages", "http_error",
     http_error={"failure": failure("invalid_argument: The prompt is too long for this model"),
                 "client_fixable": True, "stage": "http_decode"})

out = {"cases": cases}
path = os.path.join(os.path.dirname(__file__), "cases.json")
with open(path, "w") as f:
    json.dump(out, f, indent=1)
    f.write("\n")
print(f"wrote {len(cases)} cases -> {path}")
