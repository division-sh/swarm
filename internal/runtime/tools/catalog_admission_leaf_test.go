package tools

import (
	"reflect"
	"testing"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestCatalogAdmissionToolLeafSelectors(t *testing.T) {
	contract := entityruntime.Contract{
		Types: contracts.TypeCatalogDocument{
			Scalars: map[string]contracts.ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}},
			Enums:   map[string]contracts.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}},
			Types:   map[string]contracts.NamedTypeDecl{"Item": {Fields: map[string]contracts.TypeFieldSpec{"label": {Type: "Label"}}}},
		},
		Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
			"label": {Type: "Label"}, "identity": {Type: "ID"}, "status": {Type: "Status"}, "nested": {Type: "Item"},
			"items": {Type: "[Label]"}, "lookup": {Type: "map[Label]Item"}, "opaque": {Type: "json"},
		}},
	}
	if got := entityToolLeafSelectorNames(contract); !reflect.DeepEqual(got, []string{"identity", "label", "nested.label", "status"}) {
		t.Fatalf("generated read selectors omit admitted text aliases: %v", got)
	}
	for _, name := range []string{"label", "nested.label"} {
		if got := entityToolReadFilterPropertySchema([]entityruntime.Contract{contract}, name); !reflect.DeepEqual(got, map[string]any{"type": "string"}) {
			t.Fatalf("generated alias filter schema %s: %#v", name, got)
		}
	}
}
