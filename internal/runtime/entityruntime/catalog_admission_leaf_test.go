package entityruntime

import (
	"testing"

	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestCatalogAdmissionLeafDomains(t *testing.T) {
	contract := Contract{Types: contracts.TypeCatalogDocument{
		Scalars: map[string]contracts.ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}, "Time": {Base: "timestamp"}},
		Enums:   map[string]contracts.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}},
		Types:   map[string]contracts.NamedTypeDecl{"Item": {Fields: map[string]contracts.TypeFieldSpec{"label": {Type: "Label"}}}},
	}}
	for _, tc := range []struct{ ref, kind string }{
		{"Label", "scalar"}, {"ID", "scalar"}, {"Time", "scalar"}, {"Status", "enum"},
		{"text", "scalar"}, {"integer", "scalar"}, {"numeric", "scalar"}, {"boolean", "scalar"},
		{"json", "json"}, {"[Label]", "list"}, {"map[Label]Item", "map"}, {"Item", "object"},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			contract.Entity.Fields = map[string]contracts.EntityFieldDecl{"value": {Type: tc.ref}, "nested": {Type: "Item"}}
			field, err := ResolveFieldPath(contract, "value")
			if err != nil || field.LeafKind != tc.kind || field.Type != tc.ref {
				t.Fatalf("declared field domain lost: %+v %v, want %s", field, err, tc.kind)
			}
			_, err = ResolveLeafField(contract, "value")
			if (err == nil) != (tc.kind == "scalar" || tc.kind == "enum") {
				t.Fatalf("scalar/enum role for %s: %v", tc.ref, err)
			}
			if nested, err := ResolveLeafField(contract, "nested.label"); err != nil || nested.Type != "Label" || nested.LeafKind != "scalar" {
				t.Fatalf("nested text alias refused: %+v %v", nested, err)
			}
		})
	}
}
