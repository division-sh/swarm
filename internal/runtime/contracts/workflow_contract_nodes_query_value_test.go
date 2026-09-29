package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeQueryValueRetiresInertFormsAndPreservesActiveSelectors(t *testing.T) {
	for _, test := range []struct {
		name, body, errorText string
	}{
		{"entity source", "query: {entities: Product, filter: 'state == 1', store_as: computed.items, count: true}\n", ""},
		{"collection source", "query: {source: payload.items, select: [id], store_as: computed.items}\n", ""},
		{"inert operation", "query: {source: payload.items, operation: count}\n", "operation"},
		{"sequence", "query: [{source: payload.items}]\n", "must be a mapping"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			query, err := snapshot.Document("nodes.yaml").Root().Lookup("query")
			if err != nil {
				t.Fatal(err)
			}
			projected, err := projectNodeQueryValue(query.Value)
			if test.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), test.errorText) || !strings.Contains(err.Error(), "nodes.yaml") {
					t.Fatalf("expected source-located %q error, got %v", test.errorText, err)
				}
				return
			}
			if err != nil || projected == nil || projected.StoreAs != "computed.items" {
				t.Fatalf("wrong query: %#v, %v", projected, err)
			}
		})
	}
}
