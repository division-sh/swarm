package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeExpressionValueUsesExistingR2Semantics(t *testing.T) {
	for _, test := range []struct {
		name, source string
		wantCEL      bool
	}{
		{"literal", "value: hello\n", false},
		{"whole interpolation", "value: ${payload.name}\n", true},
		{"nested interpolation", "value: {name: '${payload.name}', fixed: null}\n", true},
		{"escaped literal", "value: {literal: '${payload.name}'}\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(test.source))
			if err != nil {
				t.Fatal(err)
			}
			lookup, err := snapshot.Document("nodes.yaml").Root().Lookup("value")
			if err != nil {
				t.Fatal(err)
			}
			got, err := projectNodeExpressionValue(lookup.Value)
			if err != nil {
				t.Fatal(err)
			}
			if got.HasCELValue() != test.wantCEL {
				t.Fatalf("wrong R2 projection: %#v", got)
			}
		})
	}
}

func TestProjectNodeExpressionValueRejectsNonStringObjectKey(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("value: {1: '${payload.name}'}\n"))
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := snapshot.Document("nodes.yaml").Root().Lookup("value")
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeExpressionValue(lookup.Value)
	if err == nil || !strings.Contains(err.Error(), "must be a string") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("expected source-located non-string-key error, got %v", err)
	}
}
