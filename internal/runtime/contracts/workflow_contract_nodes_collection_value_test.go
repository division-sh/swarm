package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeCollectionValueRejectsInertFields(t *testing.T) {
	for _, test := range []struct {
		name, body string
		project    func(yamlsource.Value) error
	}{
		{"filter predicate", "filter: {source: payload.items, predicate: null}\n", func(v yamlsource.Value) error { _, err := projectNodeFilterValue(v); return err }},
		{"reduce params", "reduce: {source: payload.items, params: {mode: anything}}\n", func(v yamlsource.Value) error { _, err := projectNodeReduceValue(v); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			field, err := snapshot.Document("nodes.yaml").Root().Lookup(strings.Fields(test.name)[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := test.project(field.Value); err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "nodes.yaml:") {
				t.Fatalf("expected source-located retirement, got %v", err)
			}
		})
	}
}
