package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeQueryValueRetiresInertFormsAndPreservesActiveSelectors(t *testing.T) {
	for _, test := range []struct {
		name, body, errorText string
	}{
		{"entity source", "query: {entities: Product, filter: state == 1, store_as: computed.items, count: true}\n", ""},
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

func TestNodeQueryAdmissionConsumesCollectionDataflowOwner(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")
	for _, tc := range []struct{ name, source, wantError string }{
		{"path", "query: {source: payload.items, select: [id, note]}\nfilter: {items_from: computed.query, condition: true}\n", ""},
		{"table", "query: {entities: items}\n", ""},
		{"dual source", "query: {source: payload.items, entities: items}\n", "exactly one collection source"},
		{"missing source", "query: {count: false}\n", "exactly one collection source"},
		{"future source", "query: {source: computed.filter}\nfilter: {items_from: payload.items, condition: true}\n", "same or a later execution phase"},
		{"duplicate output", "query: {source: payload.items, store_as: computed.rows}\ncount: {items_from: payload.items, store_as: computed.rows}\n", "overlapping ownership"},
		{"nested output", "query: {source: payload.items, store_as: computed.rows}\nfilter: {items_from: payload.items, store_as: computed.rows.filtered}\n", "overlapping ownership"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			if err := decodeNodeTestYAML([]byte(tc.source), &handler); err != nil {
				t.Fatal(err)
			}
			_, err := bundle.ResolveHandlerCollectionPlan(node, "work.received", handler)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("want %q, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			plan, err := bundle.ResolveHandlerQueryCollectionPlan(node, "work.received", handler)
			if err != nil || plan.StoreAs != "computed.query" {
				t.Fatalf("query default/output changed: %#v, %v", plan, err)
			}
		})
	}
}
