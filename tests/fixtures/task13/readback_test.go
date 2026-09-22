package debuglog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestT13Readback replays a logs tree produced by the Rust port (copied to
// $T13_READBACK) and prints the aggregate + the newest index entry as
// `T13READBACK:{json}` on stdout. Injected via `go test -overlay` so the Go
// tree stays read-only; the Rust test parses the marker line.
func TestT13Readback(t *testing.T) {
	root := os.Getenv("T13_READBACK")
	if root == "" {
		t.Skip("T13_READBACK unset")
	}
	m := NewManager(root, RetentionPolicy{})
	defer m.Close()
	snap := m.UsageStats()
	list := m.ListRequests(10, RequestFilter{})
	var newest string
	if len(list.Entries) > 0 {
		if data, err := json.Marshal(list.Entries[0]); err == nil {
			// String field: the Rust test compares these bytes verbatim
			// against the raw line Rust appended.
			newest = string(data)
		}
	}
	// Byte-parity probe: Go re-marshals the newest index entry; the Rust
	// test compares it against the raw line Rust appended — a field-order
	// or omitempty drift shows up as a byte diff.
	var rawLast string
	if data, err := os.ReadFile(filepath.Join(root, "index.jsonl")); err == nil {
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) > 0 {
			rawLast = lines[len(lines)-1]
		}
	}
	out, err := json.Marshal(map[string]any{
		"entries":        snap.Entries,
		"window":         snap.Window,
		"error_stages":   snap.ErrorStages,
		"newest":         newest,
		"raw_last_line":  rawLast,
		"list_has_more":  list.HasMore,
		"list_len":       len(list.Entries),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("T13READBACK:%s", out)
}
