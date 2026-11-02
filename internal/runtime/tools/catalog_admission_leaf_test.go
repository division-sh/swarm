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
			Enums: map[string]contracts.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}}},
			Types: map[string]contracts.NamedTypeDecl{"Item": {Fields: map[string]contracts.TypeFieldSpec{"label": {Type: "text"}}}},
		},
		Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
			"label": {Type: "text"}, "identity": {Type: "uuid"}, "status": {Type: "Status"}, "nested": {Type: "Item"},
			"items": {Type: "[text]"}, "lookup": {Type: "map[text]Item"}, "opaque": {Type: "json"},
		}},
	}
	if got := entityToolLeafSelectorNames(contract); !reflect.DeepEqual(got, []string{"identity", "label", "nested.label", "status"}) {
		t.Fatalf("generated read selectors omit admitted text fields: %v", got)
	}
	for _, name := range []string{"label", "nested.label"} {
		if got := entityToolReadFilterPropertySchema([]entityruntime.Contract{contract}, name); !reflect.DeepEqual(got, map[string]any{"type": "string"}) {
			t.Fatalf("generated filter schema %s: %#v", name, got)
		}
	}
}
