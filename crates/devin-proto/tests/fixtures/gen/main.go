// Fixture generator + verifier for the devin-proto wire-compat suite.
//
// This is a DEVELOPMENT oracle tool only — it is never compiled into any
// shipped artifact. It uses the Go reference's checked-in generated module
// (outputs/devin-proto-go, module local/devinproto) so the fixtures are
// produced by the exact Go protobuf stack the reference daemon uses.
//
// Modes:
//   gen <fixture-dir>     write <name>.bin (proto.Marshal) + <name>.json
//                         (protojson) + manifest.json for every fixture
//   verify <fixture-dir> <rust-dir>
//                         decode <rust-dir>/<name>.bin with Go, marshal to
//                         protojson, and compare against the checked-in
//                         <name>.json oracle; also compare <rust-dir>/<name>.json
//                         (Rust serde output) against the same oracle.
//
// JSON comparison is semantic (encoding/json maps), not textual.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	devinproto "local/devinproto"
)

type fixture struct {
	name string
	msg  proto.Message
	// deterministicBytes is true when proto.Marshal output is stable across
	// runs (no map fields anywhere in the message tree).
	deterministicBytes bool
}

func str(s string) *string    { return &s }
func b(v bool) *bool          { return &v }
func u32(v uint32) *uint32    { return &v }
func u64(v uint64) *uint64    { return &v }
func i32(v int32) *int32      { return &v }
func i64(v int64) *int64      { return &v }
func f64(v float64) *float64  { return &v }

func metadata() *devinproto.ExaCodeiumCommonPb_Metadata {
	return &devinproto.ExaCodeiumCommonPb_Metadata{
		IdeName:          str("vscode"),
		IdeVersion:       str("1.94.0"),
		ExtensionName:    str("devin"),
		ExtensionVersion: str("2.1.0"),
		ApiKey:           str("synthetic-fixture-token"),
		Locale:           str("en-US"),
		Os:               str("linux"),
		SessionId:        str("00000000-0000-4000-8000-000000000001"),
		RequestId:        u64(42),
		UserAgent:        str("devin2api-qa/0.1"),
		AuthSource:       devinproto.ExaCodeiumCommonPb_AuthSource_ExaCodeiumCommonPb_AuthSource_AUTH_SOURCE_CODEIUM.Enum(),
		UserId:           str("user-fixture-1"),
		SupportedModelDisplays: []devinproto.ExaCodeiumCommonPb_DisplayOption{
			devinproto.ExaCodeiumCommonPb_DisplayOption_ExaCodeiumCommonPb_DisplayOption_DISPLAY_OPTION_ARENA,
			devinproto.ExaCodeiumCommonPb_DisplayOption_ExaCodeiumCommonPb_DisplayOption_DISPLAY_OPTION_MODEL_ROUTER,
		},
	}
}

func fixtures() []fixture {
	full := &devinproto.GetChatMessageRequest{
		Metadata: metadata(),
		Prompt:   str("explain the retry gate"),
		ChatMessagePrompts: []*devinproto.ExaChatPb_ChatMessagePrompt{
			{
				MessageId: str("m-1"),
				Source:    devinproto.ExaCodeiumCommonPb_ChatMessageSource_ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER.Enum(),
				Prompt:    str("first user turn"),
				NumTokens: u32(128),
				ToolCalls: []*devinproto.ExaCodeiumCommonPb_ChatToolCall{
					{
						Id:           str("call-1"),
						Name:         str("read_file"),
						ArgumentsJson: str(`{"path":"src/main.rs"}`),
					},
				},
				Images: []*devinproto.ExaCodeiumCommonPb_ImageData{
					{Base64Data: str("aGVsbG8="), MimeType: str("image/png"), Caption: str("shot")},
				},
				Thinking:  str("prior reasoning"),
				Signature: str("sig-abc"),
				PromptAnnotationRanges: []*devinproto.ExaCodeiumCommonPb_PromptAnnotationRange{
					{
						Kind:           devinproto.ExaCodeiumCommonPb_PromptAnnotationKind_ExaCodeiumCommonPb_PromptAnnotationKind_PROMPT_ANNOTATION_KIND_COPY.Enum(),
						ByteOffsetStart: u64(0),
						ByteOffsetEnd:   u64(15),
					},
				},
			},
			{
				MessageId:         str("m-2"),
				Source:            devinproto.ExaCodeiumCommonPb_ChatMessageSource_ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL.Enum(),
				Prompt:            str("tool result payload"),
				ToolCallId:        str("call-1"),
				ToolResultIsError: b(false),
			},
		},
		UseInternalChatModel: b(false),
		InternalChatModel:    devinproto.ExaCodeiumCommonPb_Model_ExaCodeiumCommonPb_Model_MODEL_GOOGLE_GEMINI_2_5_FLASH_PREVIEW_05_20_THINKING.Enum(),
		ChatModelUid:         str("model-uid-1"),
		RequestType:          devinproto.ChatMessageRequestType_CHAT_MESSAGE_REQUEST_TYPE_CASCADE.Enum(),
		Configuration: &devinproto.ExaCodeiumCommonPb_CompletionConfiguration{
			MaxTokens:    u64(8192),
			Temperature:  f64(0.25),
			TopK:         u64(40),
			TopP:         f64(0.95),
			StopPatterns: []string{"\n\nUSER:", "</done>"},
			Seed:         u64(20260916),
		},
		Tools: []*devinproto.ExaChatPb_ChatToolDefinition{
			{
				Name:             str("read_file"),
				Description:      str("read a file"),
				JsonSchemaString: str(`{"type":"object","properties":{"path":{"type":"string"}}}`),
				ReadOnlyHint:     b(true),
			},
			{
				Name:                  str("apply_patch"),
				Description:           str("custom tool"),
				IsCustomTool:          b(true),
				CustomToolGrammar:     str("patch := .*"),
				CustomToolGrammarSyntax: str("regex"),
				Strict:                b(false),
			},
		},
		DisableParallelToolCalls: b(true),
		ToolChoice: &devinproto.ExaChatPb_ChatToolChoice{
			Choice: &devinproto.ExaChatPb_ChatToolChoice_ToolName{ToolName: "read_file"},
		},
		SystemPromptCacheOptions: &devinproto.ExaChatPb_PromptCacheOptions{
			Type: devinproto.ExaChatPb_CacheControlType_ExaChatPb_CacheControlType_CACHE_CONTROL_TYPE_EPHEMERAL.Enum(),
		},
		ChatModelName:       str("devin-chat"),
		CascadeId:           str("cascade-7"),
		PromptId:            str("prompt-9"),
		PlannerMode:         devinproto.ExaCodeiumCommonPb_ConversationalPlannerMode_ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_PLANNING.Enum(),
		ExecutionId:         str("exec-3"),
		ArenaConvergeCount:  i32(2),
		ModelAssignmentJwt:  str("header.payload.signature"),
	}

	minimal := &devinproto.GetChatMessageRequest{
		Metadata: &devinproto.ExaCodeiumCommonPb_Metadata{ApiKey: str("synthetic-fixture-token")},
		Prompt:   str("hi"),
	}

	explicitDefaults := &devinproto.GetChatMessageRequest{
		Prompt:                   str(""),
		UseInternalChatModel:     b(false),
		DisableParallelToolCalls: b(false),
		ArenaConvergeCount:       i32(0),
		RequestType:              devinproto.ChatMessageRequestType_CHAT_MESSAGE_REQUEST_TYPE_UNSPECIFIED.Enum(),
	}

	streamDelta := &devinproto.GetChatMessageResponse{
		MessageId:   str("msg-1"),
		Timestamp:   &devinproto.GoogleProtobuf_Timestamp{Seconds: i64(1758000000), Nanos: i32(123456789)},
		DeltaText:   str("Hello"),
		DeltaTokens: u32(3),
	}

	streamThinking := &devinproto.GetChatMessageResponse{
		MessageId:       str("msg-1"),
		DeltaThinking:   str("thinking..."),
		DeltaSignature:  str("sig-1"),
		OutputId:        str("out-1"),
		ThinkingId:      str("think-1"),
		DeltaSignatureType: str("type-a"),
	}

	streamTool := &devinproto.GetChatMessageResponse{
		MessageId: str("msg-1"),
		DeltaToolCalls: []*devinproto.ExaCodeiumCommonPb_ChatToolCall{
			{
				Id:              str("call-9"),
				Name:            str("apply_patch"),
				ArgumentsJson:   str(`{"diff":"@@"}`),
				IsCustomToolCall: b(true),
			},
		},
	}

	streamStop := &devinproto.GetChatMessageResponse{
		MessageId:  str("msg-1"),
		StopReason: devinproto.ExaCodeiumCommonPb_StopReason_ExaCodeiumCommonPb_StopReason_STOP_REASON_FUNCTION_CALL.Enum(),
		Usage: &devinproto.ExaCodeiumCommonPb_ModelUsageStats{
			ModelUid:      str("model-uid-1"),
			InputTokens:   u64(1024),
			OutputTokens:  u64(256),
			ApiProvider:   devinproto.ExaCodeiumCommonPb_APIProvider_ExaCodeiumCommonPb_APIProvider_API_PROVIDER_ANTHROPIC.Enum(),
			MessageId:     str("msg-1"),
			ResponseHeader: map[string]string{"x-upstream": "devin", "x-attempt": "1"},
		},
		CreditCost:                     i32(7),
		CommittedCreditCost:            i32(7),
		CommittedQuotaCostBasisPoints:  i64(12345),
		CommittedOverageCostCents:      i64(0),
		ActualModelUid:                 str("model-uid-actual"),
		Phase:                          str("done"),
		ResponseDimensionGroups: []*devinproto.ExaCodeiumCommonPb_ResponseDimensionGroup{
			{
				Title: str("metrics"),
				Dimensions: []*devinproto.ExaCodeiumCommonPb_ResponseDimension{
					{
						Uid: str("dim-1"),
						Dimension: &devinproto.ExaCodeiumCommonPb_ResponseDimension_CopyableCode{
							CopyableCode: &devinproto.ExaCodeiumCommonPb_ResponseDimensionCopyableCode{
								Label: str("lines"),
								Value: str("42"),
							},
						},
					},
				},
			},
		},
	}

	queryResult := &devinproto.QueryResult{
		Record: map[string]string{"alpha": "1", "beta": "two", "gamma": ""},
	}

	deployMeta := &devinproto.DeployWindsurfJSAppRequest{
		Data: &devinproto.DeployWindsurfJSAppRequest_DeploymentMetadata_{
			DeploymentMetadata: &devinproto.DeployWindsurfJSAppRequest_DeploymentMetadata{
				Metadata:    metadata(),
				ProjectPath: str("/tmp/app"),
				ProjectId:   str("proj-1"),
				Framework:   str("nextjs"),
				DeploymentProvider: devinproto.ExaCodeiumCommonPb_DeploymentProvider_ExaCodeiumCommonPb_DeploymentProvider_DEPLOYMENT_PROVIDER_VERCEL.Enum(),
				DeployTarget: &devinproto.ExaCodeiumCommonPb_DeployTarget{
					IsSandbox:        b(false),
					ProviderTeamSlug: str("team-slug"),
					Domain:           str("example.dev"),
				},
			},
		},
	}

	deployChunk := &devinproto.DeployWindsurfJSAppRequest{
		Data: &devinproto.DeployWindsurfJSAppRequest_FileChunk{
			FileChunk: &devinproto.DeployWindsurfJSAppRequest_DeploymentFileChunk{
				FilePath:     str("pages/index.tsx"),
				FileContents: []byte{0x00, 0x01, 0x02, 0xff, 0x7f},
			},
		},
	}

	packedEnums := &devinproto.ExaChatPb_ChatMentionsSearchRequest{
		Query: str("main"),
		AllowedTypes: []devinproto.ExaCodeiumCommonPb_CodeContextType{
			devinproto.ExaCodeiumCommonPb_CodeContextType_ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_FILE,
			devinproto.ExaCodeiumCommonPb_CodeContextType_ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_FUNCTION,
			devinproto.ExaCodeiumCommonPb_CodeContextType_ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_REFERENCE_FUNCTION,
		},
		IncludeRepoInfo: b(true),
	}

	mapHeaders := &devinproto.GetAccountManagedPluginBundleResponse{
		Exists:        b(true),
		SignedUrl:     str("https://example.invalid/bundle.tgz"),
		RevisionToken: str("rev-3"),
		Headers:       map[string]string{"x-amz-meta": "v", "content-type": "application/gzip"},
	}

	// proto2 [default = UNVERIFIED] on verification: presence must survive.
	defaultPresenceAbsent := &devinproto.GoogleProtobuf_ExtensionRangeOptions{
		Declaration: []*devinproto.GoogleProtobuf_ExtensionRangeOptions_Declaration{
			{Number: i32(1), FullName: str("ext.one"), Type: str(".pkg.Msg")},
		},
	}
	defaultPresenceExplicit := &devinproto.GoogleProtobuf_ExtensionRangeOptions{
		Verification: devinproto.GoogleProtobuf_ExtensionRangeOptions_UNVERIFIED.Enum(),
	}

	return []fixture{
		{"get_chat_message_request_full", full, true},
		{"get_chat_message_request_minimal", minimal, true},
		{"get_chat_message_request_explicit_defaults", explicitDefaults, true},
		{"get_chat_message_response_stream_delta", streamDelta, true},
		{"get_chat_message_response_stream_thinking", streamThinking, true},
		{"get_chat_message_response_stream_tool", streamTool, true},
		{"get_chat_message_response_stream_stop", streamStop, false},
		{"query_result_map", queryResult, false},
		{"deploy_request_oneof_metadata", deployMeta, true},
		{"deploy_request_oneof_file_chunk", deployChunk, true},
		{"chat_mentions_search_packed_enums", packedEnums, true},
		{"plugin_bundle_map_headers", mapHeaders, false},
		{"extension_range_options_default_absent", defaultPresenceAbsent, true},
		{"extension_range_options_default_explicit", defaultPresenceExplicit, true},
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: gen <fixture-dir> | verify <fixture-dir> <rust-dir>")
		os.Exit(2)
	}
	mode, dir := os.Args[1], os.Args[2]

	switch mode {
	case "gen":
		if err := gen(dir); err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
	case "verify":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: verify <fixture-dir> <rust-dir>")
			os.Exit(2)
		}
		if err := verify(dir, os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, "verify:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", mode)
		os.Exit(2)
	}
}

func gen(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type entry struct {
		Name               string `json:"name"`
		Type               string `json:"type"`
		Bin                string `json:"bin"`
		JSON               string `json:"json"`
		DeterministicBytes bool   `json:"deterministic_bytes"`
	}
	var manifest []entry
	for _, f := range fixtures() {
		bin, err := proto.MarshalOptions{Deterministic: true}.Marshal(f.msg)
		if err != nil {
			return fmt.Errorf("%s: marshal: %w", f.name, err)
		}
		js, err := protojson.MarshalOptions{}.Marshal(f.msg)
		if err != nil {
			return fmt.Errorf("%s: protojson: %w", f.name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, f.name+".bin"), bin, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, f.name+".json"), js, 0o644); err != nil {
			return err
		}
		manifest = append(manifest, entry{
			Name:               f.name,
			Type:               string(f.msg.ProtoReflect().Descriptor().FullName()),
			Bin:                f.name + ".bin",
			JSON:               f.name + ".json",
			DeterministicBytes: f.deterministicBytes,
		})
	}
	raw, err := rawWireFixtures()
	if err != nil {
		return err
	}
	for _, r := range raw {
		if err := os.WriteFile(filepath.Join(dir, r.name+".bin"), r.bin, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, r.name+".json"), r.json, 0o644); err != nil {
			return err
		}
		manifest = append(manifest, entry{
			Name:               r.name,
			Type:               r.typ,
			Bin:                r.name + ".bin",
			JSON:               r.name + ".json",
			DeterministicBytes: true,
		})
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].Name < manifest[j].Name })
	out, err := json.MarshalIndent(map[string]any{
		"generated_by": "tests/fixtures/gen (Go protobuf oracle)",
		"fixtures":     manifest,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), out, 0o644)
}

// rawWireFixture is a hand-built wire payload plus its Go-decoded protojson.
type rawWireFixture struct {
	name string
	typ  string
	bin  []byte
	json []byte
}

func rawWireFixtures() ([]rawWireFixture, error) {
	var out []rawWireFixture

	// 1. Unknown wire tag: GetChatMessageResponse{delta_text:"x", field 90 varint 7}.
	//    Go keeps tag 90 in unknownFields; protojson drops it.
	unknownTag := []byte{
		0x1a, 0x01, 'x', // field 3 (delta_text), LEN, len 1, "x"
		0xd0, 0x05, 0x07, // field 90, varint, value 7
	}
	m := &devinproto.GetChatMessageResponse{}
	if err := proto.Unmarshal(unknownTag, m); err != nil {
		return nil, fmt.Errorf("unknown_tag decode: %w", err)
	}
	js, err := protojson.MarshalOptions{}.Marshal(m)
	if err != nil {
		return nil, err
	}
	out = append(out, rawWireFixture{
		name: "response_unknown_wire_tag",
		typ:  "exa.api_server_pb.GetChatMessageResponse",
		bin:  unknownTag,
		json: js,
	})

	// 2. Unknown enum value: GetChatMessageResponse{stop_reason: 999}.
	//    proto2 closed enum: Go routes the value to unknownFields.
	unknownEnum := []byte{
		0x28, 0xe7, 0x07, // field 5 (stop_reason), varint, 999
	}
	m2 := &devinproto.GetChatMessageResponse{}
	if err := proto.Unmarshal(unknownEnum, m2); err != nil {
		return nil, fmt.Errorf("unknown_enum decode: %w", err)
	}
	js2, err := protojson.MarshalOptions{}.Marshal(m2)
	if err != nil {
		return nil, err
	}
	out = append(out, rawWireFixture{
		name: "response_unknown_enum_value",
		typ:  "exa.api_server_pb.GetChatMessageResponse",
		bin:  unknownEnum,
		json: js2,
	})

	return out, nil
}

func verify(fixtureDir, rustDir string) error {
	byName := map[string]proto.Message{}
	for _, f := range fixtures() {
		byName[f.name] = f.msg
	}
	// Raw fixtures decode into the same message types.
	rawTypes := map[string]func() proto.Message{
		"response_unknown_wire_tag":   func() proto.Message { return &devinproto.GetChatMessageResponse{} },
		"response_unknown_enum_value": func() proto.Message { return &devinproto.GetChatMessageResponse{} },
	}

	manifestBytes, err := os.ReadFile(filepath.Join(fixtureDir, "manifest.json"))
	if err != nil {
		return err
	}
	var manifest struct {
		Fixtures []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}

	failures := 0
	for _, f := range manifest.Fixtures {
		oracleJSON, err := os.ReadFile(filepath.Join(fixtureDir, f.Name+".json"))
		if err != nil {
			return err
		}

		var msg proto.Message
		if mk, ok := rawTypes[f.Name]; ok {
			msg = mk()
		} else if src, ok := byName[f.Name]; ok {
			msg = src.ProtoReflect().New().Interface()
		} else {
			fmt.Printf("FAIL %s: no Go type for fixture\n", f.Name)
			failures++
			continue
		}

		// Rust-produced binary -> Go decode -> protojson == oracle.
		rustBin, err := os.ReadFile(filepath.Join(rustDir, f.Name+".bin"))
		if err != nil {
			fmt.Printf("FAIL %s: read rust bin: %v\n", f.Name, err)
			failures++
			continue
		}
		if err := proto.Unmarshal(rustBin, msg); err != nil {
			fmt.Printf("FAIL %s: Go cannot decode Rust bytes: %v\n", f.Name, err)
			failures++
			continue
		}
		goJSON, err := protojson.MarshalOptions{}.Marshal(msg)
		if err != nil {
			return err
		}
		if !jsonEqual(oracleJSON, goJSON) {
			fmt.Printf("FAIL %s: Go protojson of Rust bytes != oracle\n  oracle: %s\n  got:    %s\n", f.Name, oracleJSON, goJSON)
			failures++
			continue
		}

		// Rust-produced JSON == oracle (semantic compare).
		rustJSON, err := os.ReadFile(filepath.Join(rustDir, f.Name+".json"))
		if err != nil {
			fmt.Printf("FAIL %s: read rust json: %v\n", f.Name, err)
			failures++
			continue
		}
		// Known projection divergence: Go keeps an unknown enum value in the
		// field and protojson prints it as a bare number; buffa routes it to
		// unknown_fields so the field is absent from Rust proto-JSON. The
		// wire bytes are byte-identical (proven by the binary leg above and
		// by the Rust-side unknown_fields test), so drop the key from the
		// oracle for the Rust-JSON comparison only.
		cmpOracle := oracleJSON
		if drop, ok := rustJSONOracleDrops[f.Name]; ok {
			cmpOracle = dropJSONKeys(oracleJSON, drop)
		}
		if !jsonEqual(cmpOracle, rustJSON) {
			fmt.Printf("FAIL %s: Rust serde_json != Go protojson oracle\n  oracle: %s\n  got:    %s\n", f.Name, oracleJSON, rustJSON)
			failures++
			continue
		}
		fmt.Printf("PASS %s (%s)\n", f.Name, f.Type)
	}
	if failures > 0 {
		return fmt.Errorf("%d fixture(s) failed", failures)
	}
	return nil
}

// rustJSONOracleDrops lists oracle keys absent from Rust proto-JSON for
// documented codec divergences (see verify loop).
var rustJSONOracleDrops = map[string][]string{
	"response_unknown_enum_value": {"stopReason"},
}

// dropJSONKeys returns a copy of the JSON document with the named
// top-level keys removed.
func dropJSONKeys(doc []byte, keys []string) []byte {
	var v map[string]any
	if err := json.Unmarshal(doc, &v); err != nil {
		return doc
	}
	for _, k := range keys {
		delete(v, k)
	}
	out, _ := json.Marshal(v)
	return out
}

func jsonEqual(a, b []byte) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return deepEqual(va, vb)
}

// deepEqual compares decoded JSON with numeric tolerance for 1 vs 1.0.
func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, ok := bv[k]
			if !ok || !deepEqual(x, y) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	default:
		return bytes.Equal(mustJSON(a), mustJSON(b))
	}
}

func mustJSON(v any) []byte {
	out, _ := json.Marshal(v)
	return out
}
