package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNodeValueFieldsPreserveNestedSourceAndRejectAuthoredPresence(t *testing.T) {
	source, err := yamlsource.Load([]byte("node:\n  handler: &row\n    from: null\n  other: *row\n"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := nodeValueFields(source.Document("nodes.yaml").Root(), "nodes", map[string]struct{}{"node": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeValueFields(root["node"], "node", map[string]struct{}{"handler": {}, "other": {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodeValueFields(node["other"], "handler", map[string]struct{}{}, map[string]string{"from": "not executable"})
	if err == nil || !strings.Contains(err.Error(), "nodes.yaml:") || !strings.Contains(err.Error(), "from") {
		t.Fatalf("expected source-located authored-presence rejection, got %v", err)
	}
}

func TestNodeValueFieldsRejectDuplicateEffectiveAliasKeys(t *testing.T) {
	source, err := yamlsource.Load([]byte("node:\n  handler: &row {emit: one}\n  duplicate:\n    <<: *row\n    emit: two\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = nodeValueFields(source.Document("nodes.yaml").Root(), "nodes", map[string]struct{}{"node": {}}, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate effective YAML key") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("expected source-located duplicate effective key, got %v", err)
	}
}
