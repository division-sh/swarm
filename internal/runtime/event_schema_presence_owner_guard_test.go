package runtime_test

import (
	"go/ast"
	"reflect"
	"sort"
	"testing"
)

func TestEventSchemaRequiredProjectionHasNoBehavioralConsumers(t *testing.T) {
	want := []string{
		"internal/providertriggers/normalized_events.go",
		"internal/runtime/contracts/event_catalog_admission.go",
		"internal/runtime/contracts/event_schema_ownership.go",
		"internal/runtime/contracts/platform_event_catalog.go",
		"internal/runtime/contracts/schema_registry.go",
		"internal/runtime/contracts/tool_input_schema.go",
		"internal/runtime/provider_trigger_source.go",
	}
	seen := map[string]struct{}{}
	inspectProductionGo(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Required" {
				return true
			}
			payload, ok := selector.X.(*ast.SelectorExpr)
			if !ok || payload.Sel.Name != "Payload" {
				return true
			}
			seen[path] = struct{}{}
			return true
		})
	})

	got := make([]string, 0, len(seen))
	for path := range seen {
		got = append(got, path)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EventCatalogEntry.Payload.Required owners = %v, want finite projection-only ledger %v", got, want)
	}
}
