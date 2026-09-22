//! Rewrites of known upstream content-policy fingerprint text.
//!
//! Port of `G/internal/adapter/devin/sanitize.go`. The Devin upstream has
//! two gates: competitor-fingerprint and content-policy
//! (`permission_denied`). Fingerprints match on whole signature
//! sentences/paragraphs; the rule set mirrors `WindsurfAPI`'s
//! identity-neutralize.js plus locally verified triggers, rewriting to
//! equivalent neutral text without changing system-prompt semantics.
//!
//! Upstream policy is nondeterministic (the same prompt can be blocked then
//! allowed), so rules cover only proven triggers — no speculative rewrites
//! that could mangle user content.
//!
//! Regex porting notes: Go's `regexp` is RE2 with ASCII `\s`/`[\w]`/`\b` and
//! Unicode-aware `(?i)`/`.`/`[^...]`. `regex-automata` defaults to Unicode
//! classes, so Go's `\s` is written out as `[ \t\n\x0C\r]` (Go's `\s` lacks
//! `\v`), `\w` as `[0-9A-Za-z_]`, `\d` as `[0-9]`, and `\b` as `(?-u:\b)`.
//! Replacement `$`-expansion ports `regexp.Expand` semantics exactly.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex_automata::meta::Regex;
use regex_automata::util::captures::Captures;

use crate::domain::request::{Content, RequestMessages};

/// Matches text that may span a single newline but not a blank line —
/// RE2 has no lookahead, so "newline must be followed by non-blank content"
/// approximates `WindsurfAPI`'s `\n(?!\s*\n)`.
const WITHIN_PARAGRAPH: &str = "(?:[^\\n]|\\n[ \\t]*[^\\n \\t])*?";

const SECURITY_BENIGN: &str = "Decline requests that facilitate clearly malicious or harmful activity, and otherwise help the user with their software engineering task.";

/// One verified upstream-blocked-text rewrite rule.
struct SanitizeRule {
    id: &'static str,
    pattern: Regex,
    replacement: &'static str,
    /// `prompt_only` bare-word rules apply only to the system prompt and
    /// tool descriptions, never to user message bodies (e.g. a `FREEFORM`
    /// literal in code).
    prompt_only: bool,
    /// Lowercase literal substring every match of `pattern` must contain,
    /// used for `contains` pre-screening: text without `trigger` cannot
    /// match. Must be short but not generic — a wrong value silently
    /// disables the rule.
    trigger: &'static str,
}

fn rule(
    id: &'static str,
    pattern: &str,
    replacement: &'static str,
    trigger: &'static str,
) -> SanitizeRule {
    SanitizeRule {
        id,
        pattern: Regex::new(pattern).expect("sanitize rule pattern must compile"),
        replacement,
        prompt_only: false,
        trigger,
    }
}

fn prompt_rule(
    id: &'static str,
    pattern: &str,
    replacement: &'static str,
    trigger: &'static str,
) -> SanitizeRule {
    SanitizeRule {
        id,
        pattern: Regex::new(pattern).expect("sanitize rule pattern must compile"),
        replacement,
        prompt_only: true,
        trigger,
    }
}

/// Rule order matches the reference: whole paragraphs/sentences first,
/// single-line fallbacks after.
static SANITIZE_RULES: LazyLock<Vec<SanitizeRule>> = LazyLock::new(|| {
    vec![
        // (a1) Claude Code self-identity sentences (competitor fingerprint),
        // straight/curly apostrophe, full-sentence and noun-phrase forms.
        rule(
            "a1-cc-full",
            "(?i)You are Claude Code,[ \\t\\n\\x0C\\r]*Anthropic['’]?s official CLI for Claude\\.?",
            "You are an AI coding assistant.",
            "claude code",
        ),
        rule(
            "a1-cc-noun",
            "(?i)Claude Code,[ \\t\\n\\x0C\\r]*Anthropic['’]?s official CLI for Claude\\.?",
            "an AI coding assistant.",
            "claude code",
        ),
        // (a2) Claude Agent SDK self-identity sentence (content-policy).
        rule(
            "a2-sdk-full",
            "(?i)You are a Claude agent, built on Anthropic['’]?s Claude Agent SDK\\.?",
            "You are an AI coding assistant.",
            "claude agent",
        ),
        rule(
            "a2-sdk-noun",
            "(?i)(?-u:\\b)a Claude agent, built on Anthropic['’]?s Claude Agent SDK\\.?",
            "an AI coding assistant.",
            "claude agent",
        ),
        // (a3) Claude Code injected billing header line (competitor
        // fingerprint), whole line stripped.
        rule(
            "a3-billing",
            "(?im)^[ \\t\\n\\x0C\\r]*x-anthropic-billing-header:[^\\n]*\\n?",
            "",
            "x-anthropic-billing-header",
        ),
        // (b) Security-policy paragraph (abuse gate); the single-line
        // fallback covers cross-paragraph failures.
        rule(
            "b-security",
            &format!(
                "(?i)IMPORTANT:[ \\t\\n\\x0C\\r]*Assist with authorized security testing{WITHIN_PARAGRAPH}(?:defensive use cases\\.|security research[^.]*\\.)"
            ),
            SECURITY_BENIGN,
            "authorized security testing",
        ),
        rule(
            "b-security-line",
            "(?i)IMPORTANT:[ \\t\\n\\x0C\\r]*Assist with authorized security testing[^\\n]*",
            SECURITY_BENIGN,
            "authorized security testing",
        ),
        // The dual-use sentence is itself a fingerprint (locally verified);
        // without the IMPORTANT prefix the fallback rewrites it.
        rule(
            "b-dualluse",
            "(?i)Dual-use security tools \\(C2 frameworks, credential testing, exploit development\\) require clear authorization context:[^\\n]*",
            "Dual-use security tooling (e.g. C2 frameworks, credential testing, exploit development) needs explicit authorization context such as pentesting engagements, CTF competitions, security research, or defensive use cases.",
            "dual-use security tools",
        ),
        // (a4) Claude Code Environment brand block and model catalogue,
        // both paragraph-level fingerprints.
        rule(
            "a4-brand-span",
            &format!(
                "(?i)Claude Code is available as a CLI{WITHIN_PARAGRAPH}available on Opus [0-9./]+\\."
            ),
            "This coding assistant runs in a terminal.",
            "claude code is available",
        ),
        rule(
            "a4-fastmode",
            "(?im)(?:^|\\n)[ \\t\\n\\x0C\\r]*-?[ \\t\\n\\x0C\\r]*Fast mode for Claude Code[^\\n]*\\n?",
            "\n",
            "fast mode for claude code",
        ),
        rule(
            "a4-cli-line",
            "(?i)Claude Code is available as a CLI[^\\n]*\\n?",
            "This coding assistant runs in a terminal.\n",
            "claude code is available",
        ),
        rule(
            "a4-catalogue",
            &format!(
                "(?i)The most recent Claude models are{WITHIN_PARAGRAPH}most capable Claude models\\."
            ),
            "",
            "the most recent claude models",
        ),
        rule(
            "a4-catalogue-line",
            "(?im)The most recent Claude models are[^\\n]*\\n?",
            "",
            "the most recent claude models",
        ),
        // "You are powered by the model …" and "The exact model ID is …"
        // self-model fingerprints: the period must be followed by
        // whitespace or end-of-line (RE2 has no lookahead; `(?:\.(\s|$)|$)`
        // with (?m) makes `$` match line ends).
        rule(
            "a4-poweredby",
            "(?im)You are powered by the model[^\\n]*?(?:\\.(?:[ \\t\\n\\x0C\\r]|$)|$)\\n?",
            "",
            "powered by the model",
        ),
        rule(
            "a4-modelid",
            "(?im)The exact model ID is[^\\n]*?(?:\\.(?:[ \\t\\n\\x0C\\r]|$)|$)\\n?",
            "",
            "the exact model id is",
        ),
        // (a5) Cline capability boast: the trigger is the sentence shape,
        // the name is kept.
        rule(
            "a5-cline-boast",
            "You are ([A-Z][0-9A-Za-z_.-]*), a highly skilled software engineer with extensive knowledge in many programming languages, frameworks, design patterns,? and best practices\\.",
            "You are $1, a software engineer.",
            "a highly skilled software engineer",
        ),
        // (a6) Grok/xAI self-identity sentence + executing_actions_with_care
        // block.
        rule(
            "a6-grok-full",
            "(?i)You are Grok[0-9A-Za-z_ .-]* released by xAI\\.?",
            "You are an AI coding assistant.",
            "released by xai",
        ),
        rule(
            "a6-grok-noun",
            "(?i)(?-u:\\b)Grok[0-9A-Za-z_ .-]* released by xAI\\.?",
            "an AI coding assistant.",
            "released by xai",
        ),
        rule(
            "a6-grok2-care",
            "(?is)<executing_actions_with_care>.*?</executing_actions_with_care>",
            "",
            "executing_actions_with_care",
        ),
        // (a7) codex apply_patch tool-description FREEFORM bare word and
        // JSON-wrap sentence. The bare word could hit user text (SQL/code
        // identifiers), so it is prompt/tool-description only.
        prompt_rule("a7-freeform", "FREEFORM", "free-form", "freeform"),
        prompt_rule(
            "a7-json-wrap",
            "do not wrap the patch in JSON\\.",
            "provide the patch as plain text.",
            "do not wrap the patch in json",
        ),
        // Locally verified: Claude Code's tool-call colon sentence is also
        // a fingerprint.
        rule(
            "cc-colon-toolcall",
            "(?i)Do not use a colon before tool calls\\.[^\\n]*?with a period\\.",
            "Never put a colon before a tool call; write text like \"Let me read the file.\" ending with a period instead of a colon before the call.",
            "colon before tool calls",
        ),
        // CC 2.1.x prompt fingerprint lines (locally bisected):
        // auto-compact sentence, /help brand line, Agent-tool guidance,
        // CLAUDE.md line, memory mandate.
        rule(
            "cc-autocompact",
            "(?i)The system will automatically compress prior messages in your conversation as it approaches context limits\\.[^\\n]*",
            "Earlier messages may be automatically summarized as the conversation grows long, so the conversation is not bounded by the context window.",
            "automatically compress prior messages",
        ),
        // cc-help-line/cc-feedback fingerprints are the bare sentences
        // themselves (upstream blocks even without a line-start/list
        // prefix), so no line anchor; a "- " list prefix is preserved.
        rule(
            "cc-help-line",
            "(?i)/help:[ \\t\\n\\x0C\\r]*Get help with using Claude Code[^\\n]*",
            "/help: Get help with using this CLI",
            "/help:",
        ),
        rule(
            "cc-agent-tool",
            "(?i)Use the Agent tool with specialized agents when the task at hand matches the agent's description\\.[^\\n]*",
            "Use the Agent tool with specialized agents when the task matches the agent's description. Delegation is useful for parallelizing independent queries and for keeping the main context window free of excessive results, but avoid using it when not needed, and do not repeat work already delegated to a subagent.",
            "use the agent tool with specialized agents",
        ),
        rule(
            "cc-claudemd",
            "(?i)Anything already documented in CLAUDE\\.md files\\.",
            "Anything already documented in project instruction files.",
            "claude.md",
        ),
        rule(
            "cc-memory-must",
            "(?i)You MUST access memory when the user explicitly asks you to check, recall, or remember\\.",
            "Always consult memory when the user explicitly asks you to check, recall, or remember.",
            "must access memory",
        ),
        rule(
            "cc-feedback",
            "(?i)To give feedback, users should report the issue at https://github\\.com/anthropics/claude-code/issues[^\\n]*",
            "To give feedback, users should report issues to the maintainers of this CLI.",
            "claude-code",
        ),
        rule(
            "cc-blast-radius",
            "(?i)Carefully consider the reversibility and blast radius of actions\\.",
            "Carefully consider the reversibility and impact of actions.",
            "blast radius",
        ),
        rule(
            "cc-claudemd-2",
            "(?i)durable instructions like CLAUDE\\.md files",
            "durable instructions like project instruction files",
            "claude.md",
        ),
        // CC 2.1.x subagent system-prompt emoji ban line (locally bisected:
        // the fingerprint is the whole sentence — both the
        // "For clear communication…" prefix and "MUST avoid" are required).
        rule(
            "cc-subagent-emojis",
            "(?i)For clear communication with the user the assistant MUST avoid using emojis\\.",
            "Keep communication with the user clear and free of emojis.",
            "avoid using emojis",
        ),
        // Codex CLI prompt fingerprints (codex 0.153.x template bisected):
        // the trigger is the full "Codex refers to … interface" span —
        // truncating at open-source or dropping the Codex subject passes;
        // middle words are part of the fingerprint, covered by [^\n.]*
        // without crossing sentences.
        rule(
            "codex-opensource-def",
            "(?i)Codex refers to the open-source[^\\n.]*interface",
            "Codex is the open-source coding interface",
            "codex refers to the open-source",
        ),
        // plan status sentence pair: only "batch-complete" immediately
        // followed by "Finish with all items…" triggers — either alone,
        // reversed, or separated passes.
        rule(
            "codex-plan-statuses",
            "(?i)Do not batch-complete multiple items after the fact\\. Finish with all items completed or explicitly canceled/deferred before ending the turn\\.",
            "Do not batch-complete multiple items after the fact. Before ending the turn, leave all items completed or explicitly canceled/deferred.",
            "do not batch-complete multiple items",
        ),
        // ANSI escape sentence: "Don't output ANSI escape codes directly"
        // and "the CLI renderer applies them" must co-occur in one
        // sentence; either alone or a different subject passes.
        rule(
            "codex-ansi-escapes",
            "Don['’]t output ANSI escape codes directly — the CLI renderer applies them\\.",
            "Never output ANSI escape codes directly — the CLI renderer applies them.",
            "ansi escape codes directly",
        ),
        // codex-injected <permissions instructions> authorization block:
        // verified to trigger content-policy in 2026-08; re-verified
        // passing on 2026-09-15 with a synthetic block (real codex block
        // text not archived; assertion kept per historical evidence).
        // Previously stripped during common.DecodeContent — moved into
        // this table so 01/02 logs keep the client original, hits land in
        // repairs counters, and the anthropic entry (which skips
        // DecodeContent) is covered too.
        rule(
            "codex-permissions",
            "(?s)<permissions instructions>.*?</permissions instructions>",
            "",
            "permissions instructions",
        ),
    ]
});

/// Trigger buckets for the byte-level prescreen: triggers bucketed by first
/// byte folded lowercase (`b | 0x20`). Built once for all rules
/// (`include_prompt_only = true`) and once for message-body rules only.
static BUCKETS_ALL: LazyLock<TriggerBuckets> = LazyLock::new(|| trigger_buckets(true));
static BUCKETS_MESSAGES: LazyLock<TriggerBuckets> = LazyLock::new(|| trigger_buckets(false));

type TriggerBuckets = [Vec<&'static str>; 256];

fn trigger_buckets(include_prompt_only: bool) -> TriggerBuckets {
    let mut buckets: TriggerBuckets = std::array::from_fn(|_| Vec::new());
    for rule in SANITIZE_RULES.iter() {
        if rule.prompt_only && !include_prompt_only {
            continue;
        }
        let first = rule.trigger.as_bytes()[0] | 0x20;
        buckets[first as usize].push(rule.trigger);
    }
    buckets
}

/// Byte-scan prescreen: on a bucket hit, an ASCII case-insensitive prefix
/// compare decides. Any trigger present returns true (a rule *may* match);
/// all-negative means the text is certainly clean — rules cannot bypass
/// their bucket since buckets index by trigger first byte.
fn has_sanitize_trigger(text: &str, buckets: &TriggerBuckets) -> bool {
    let bytes = text.as_bytes();
    for (index, &byte) in bytes.iter().enumerate() {
        for trigger in &buckets[(byte | 0x20) as usize] {
            let trigger = trigger.as_bytes();
            if bytes.len() - index >= trigger.len()
                && bytes[index..index + trigger.len()].eq_ignore_ascii_case(trigger)
            {
                return true;
            }
        }
    }
    false
}

/// Rewrites `text` per the rule set, accumulating per-rule-id hit counts
/// into `hits`. The replacement may contain `$` capture-group references
/// (Go `regexp.Expand` semantics), so hits are counted via a find pass
/// rather than a replace callback. Returns `Cow::Borrowed` untouched text
/// when clean — the common path allocates nothing.
pub fn sanitize_upstream_text<'a>(
    text: &'a str,
    include_prompt_only: bool,
    hits: &mut BTreeMap<String, i64>,
) -> Cow<'a, str> {
    if text.is_empty() {
        return Cow::Borrowed(text);
    }
    let buckets = if include_prompt_only {
        &*BUCKETS_ALL
    } else {
        &*BUCKETS_MESSAGES
    };
    if !has_sanitize_trigger(text, buckets) {
        return Cow::Borrowed(text);
    }
    // Each rule's trigger is a literal word every match of its pattern must
    // contain; after the prescreen, rules are still skipped individually by
    // trigger before the regex rewrite.
    let lower = text.to_lowercase();
    let mut current: Cow<'a, str> = Cow::Borrowed(text);
    for rule in SANITIZE_RULES.iter() {
        if rule.prompt_only && !include_prompt_only {
            continue;
        }
        if !lower.contains(rule.trigger) {
            continue;
        }
        let matches: Vec<_> = rule.pattern.find_iter(&*current).collect();
        if matches.is_empty() {
            continue;
        }
        *hits.entry(rule.id.to_string()).or_insert(0) +=
            i64::try_from(matches.len()).unwrap_or(i64::MAX);
        current = Cow::Owned(replace_all(&rule.pattern, &current, rule.replacement));
    }
    current
}

/// Go `regexp.ReplaceAllString`: replaces every non-overlapping match,
/// expanding `$` references in `template` per `regexp.Expand`.
fn replace_all(pattern: &Regex, text: &str, template: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last = 0usize;
    for caps in pattern.captures_iter(text) {
        let Some(whole) = caps.get_group(0) else {
            continue;
        };
        out.push_str(&text[last..whole.start]);
        expand(&mut out, template, text, &caps);
        last = whole.end;
    }
    out.push_str(&text[last..]);
    out
}

/// Port of Go `regexp.(*Regexp).expand`: `$$` is a literal `$`, `$name` /
/// `${name}` / `$N` / `${N}` substitute group contents (empty when the
/// group did not participate or is out of range), malformed `$` sequences
/// are copied literally. Named groups are unused by this rule set — the
/// name lookup is kept for completeness.
fn expand(out: &mut String, template: &str, text: &str, caps: &Captures) {
    let mut rest = template;
    while !rest.is_empty() {
        let Some(dollar) = rest.find('$') else {
            break;
        };
        out.push_str(&rest[..dollar]);
        rest = &rest[dollar + 1..];
        if rest.starts_with('$') {
            // Treat $$ as $.
            out.push('$');
            rest = &rest[1..];
            continue;
        }
        let Some((name, num, after)) = extract(rest) else {
            // Malformed; treat $ as raw text.
            out.push('$');
            continue;
        };
        rest = after;
        if let Some(index) = num {
            if let Some(span) = caps.get_group(index) {
                out.push_str(&text[span]);
            }
        } else if let Some(span) = caps.get_group_by_name(name) {
            out.push_str(&text[span]);
        }
    }
    out.push_str(rest);
}

/// Port of Go `extract`: parses a leading `name` or `{name}` (letters,
/// digits, `_`); a pure number yields `Some(num)` — `None` for names,
/// leading zeros, or values ≥ 1e8. Returns `(name, num, rest)`.
fn extract(input: &str) -> Option<(&str, Option<usize>, &str)> {
    if input.is_empty() {
        return None;
    }
    let (body, brace) = match input.strip_prefix('{') {
        Some(rest) => (rest, true),
        None => (input, false),
    };
    let mut end = 0usize;
    for (offset, ch) in body.char_indices() {
        if !is_go_name_char(ch) {
            break;
        }
        end = offset + ch.len_utf8();
    }
    if end == 0 {
        // Empty name is not okay.
        return None;
    }
    let name = &body[..end];
    let mut rest = &body[end..];
    if brace {
        if !rest.starts_with('}') {
            // Missing closing brace.
            return None;
        }
        rest = &rest[1..];
    }
    // Parse number: all digits, no leading zeros, below 1e8.
    let bytes = name.as_bytes();
    let mut num: i64 = 0;
    for &byte in bytes {
        if !byte.is_ascii_digit() || num >= 100_000_000 {
            num = -1;
            break;
        }
        num = num * 10 + i64::from(byte - b'0');
    }
    if bytes[0] == b'0' && bytes.len() > 1 {
        num = -1;
    }
    let num = usize::try_from(num).ok();
    Some((name, num, rest))
}

/// Go `unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'`.
fn is_go_name_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

/// Rewrites all upstream-blocked known text in the request, returning
/// per-rule-id hit counts — the rewrite itself is silent, the counts land
/// in the request log so "what the proxy touched" stays auditable.
///
/// Mutates `Messages`/`Tools` elements in place (the 02 log must snapshot
/// before this call or it records rewritten content).
pub fn sanitize_request(request: &mut RequestMessages) -> BTreeMap<String, i64> {
    let mut hits = BTreeMap::new();
    request.system_prompt =
        sanitize_upstream_text(&request.system_prompt, true, &mut hits).into_owned();
    for message in &mut request.messages {
        let content = match message {
            crate::domain::request::Message::User(typed) => &mut typed.content,
            crate::domain::request::Message::Assistant(typed) => &mut typed.content,
            crate::domain::request::Message::ToolResult(typed) => &mut typed.content,
        };
        sanitize_contents(content, &mut hits);
    }
    for tool in &mut request.tools {
        tool.description = sanitize_upstream_text(&tool.description, true, &mut hits).into_owned();
    }
    // "tools declared but empty system prompt" triggered permission_denied
    // in early probes; the 2026-09-15 retest (explicit empty and absent
    // forms, with tools) was accepted — the assertion no longer
    // reproduces, but the fallback injection stays as a harmless neutral
    // default.
    if request.system_prompt.trim().is_empty() && !request.tools.is_empty() {
        request.system_prompt = "You are an AI coding assistant.".to_string();
        *hits.entry("inject-empty-system".to_string()).or_insert(0) += 1;
    }
    hits
}

/// Rewrites text/thinking bodies inside content blocks in place; images
/// and other blocks carry no blockable text and are skipped.
fn sanitize_contents(content: &mut [Content], hits: &mut BTreeMap<String, i64>) {
    for block in content.iter_mut() {
        match block {
            Content::Text(typed) => {
                typed.text = sanitize_upstream_text(&typed.text, false, hits).into_owned();
            }
            Content::Thinking(typed) => {
                typed.thinking = sanitize_upstream_text(&typed.thinking, false, hits).into_owned();
            }
            Content::Image(_) | Content::ToolCall(_) => {}
        }
    }
}
