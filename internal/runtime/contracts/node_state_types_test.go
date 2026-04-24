package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNormalizeNodeStateFieldType_AllowsCanonicalNodeStateTypes(t *testing.T) {
	cases := map[string]string{
		"text":             "text",
		"string":           "string",
		"integer":          "integer",
		"bool":             "boolean",
		"boolean":          "boolean",
		"float":            "float",
		"numeric(8, 2)":    "numeric(8,2)",
		"jsonb":            "jsonb",
		"timestamptz":      "timestamptz",
		"uuid":             "uuid",
		"text[]":           "text[]",
		"numeric(5,2)[]":   "numeric(5,2)[]",
		"DimensionScore":   "DimensionScore",
		"[DimensionScore]": "[DimensionScore]",
		"DimensionScore[]": "[DimensionScore]",
	}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			got, err := NormalizeNodeStateFieldType(raw)
			if err != nil {
				t.Fatalf("NormalizeNodeStateFieldType(%q): %v", raw, err)
			}
			if got != want {
				t.Fatalf("NormalizeNodeStateFieldType(%q) = %q, want %q", raw, got, want)
			}
		})
	}
}

func TestNormalizeNodeStateFieldType_RejectsPseudoTypes(t *testing.T) {
	cases := []string{
		"uuid (primary key)",
		"text[] (scanner types dispatched)",
		"timestamptz (null until done)",
		"dimension score receipts keyed by dimension name",
		"integer default 0",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, err := NormalizeNodeStateFieldType(raw); err == nil {
				t.Fatalf("expected pseudo-type rejection for %q", raw)
			}
		})
	}
}

func TestDecodeNodeStateFields_RejectsPseudoTypesInSequenceForm(t *testing.T) {
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(`
- name: dimensions_received
  type: dimension score receipts keyed by dimension name
`), &node); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	_, err := decodeNodeStateFields(node.Content[0])
	if err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("expected pseudo-type error, got %v", err)
	}
}
