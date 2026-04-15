package toolidentity

import "testing"

func TestCanonicalName(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":                                   "",
		"bash":                               "bash",
		"Bash":                               "bash",
		"web_search":                         "web_search",
		"WebFetch":                           "web_search",
		"WebSearch":                          "web_search",
		"Read":                               "read_file",
		"read_file":                          "read_file",
		"Write":                              "write_file",
		"Edit":                               "write_file",
		"mcp__runtime-tools__read_file":      "read_file",
		"mcp__runtime-tools__write_file":     "write_file",
		"mcp__runtime-tools__emit_scan_done": "emit_scan_done",
	}

	for raw, want := range tests {
		if got := CanonicalName(raw); got != want {
			t.Fatalf("CanonicalName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestIsEmitToolName(t *testing.T) {
	t.Parallel()

	tests := map[string]bool{
		"emit_scan_done":                     true,
		"mcp__runtime-tools__emit_scan_done": true,
		"read_file":                          false,
		"mcp__runtime-tools__read_file":      false,
		"Write":                              false,
		"mcp__runtime-tools__write_file":     false,
	}

	for raw, want := range tests {
		if got := IsEmitToolName(raw); got != want {
			t.Fatalf("IsEmitToolName(%q) = %v, want %v", raw, got, want)
		}
	}
}
