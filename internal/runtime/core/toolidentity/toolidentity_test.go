package toolidentity

import (
	"reflect"
	"testing"
)

func TestDeclarationNamesRetainsRoutingIdentities(t *testing.T) {
	for _, row := range []struct {
		name string
		want []string
	}{
		{"", []string{}},
		{" read_file ", []string{"read_file"}},
		{"Read", []string{"Read", "read_file"}},
		{"mcp__runtime-tools__Read", []string{"mcp__runtime-tools__Read", "Read", "read_file"}},
		{"mcp__runtime-tools__emit_probe", []string{"mcp__runtime-tools__emit_probe", "emit_probe"}},
		{"mcp__runtime-tools__read_file", []string{"mcp__runtime-tools__read_file", "read_file"}},
	} {
		if got := DeclarationNames(row.name); !reflect.DeepEqual(got, row.want) {
			t.Fatalf("DeclarationNames(%q) = %v, want %v", row.name, got, row.want)
		}
	}
}

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
