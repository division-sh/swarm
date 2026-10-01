package bootverify

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestA2CatalogEntityPathPreservesAdmittedLeafShapes(t *testing.T) {
	for _, tc := range []struct {
		name, path, kind, typeRef string
		optional                  bool
	}{
		{name: "text", path: "entity.name", kind: "scalar", typeRef: "text"},
		{name: "integer", path: "entity.total", kind: "scalar", typeRef: "integer"},
		{name: "scalar alias", path: "entity.score", kind: "scalar", typeRef: "Score"},
		{name: "enum", path: "entity.decision", kind: "enum", typeRef: "Decision"},
		{name: "named record", path: "entity.profile", kind: "named", typeRef: "Profile"},
		{name: "list", path: "entity.ids", kind: "list", typeRef: "[text]"},
		{name: "list size", path: "entity.ids.size", kind: "scalar", typeRef: "integer"},
		{name: "nested lists", path: "entity.groups", kind: "list", typeRef: "[[text]]"},
		{name: "meaningful map", path: "entity.items", kind: "map", typeRef: "map[text][integer]"},
		{name: "record values", path: "entity.records", kind: "map", typeRef: "map[text]Record"},
		{name: "nested map values", path: "entity.by_region", kind: "map", typeRef: "map[text]map[text]Decision"},
		{name: "record scalar", path: "entity.profile.name", kind: "scalar", typeRef: "text"},
		{name: "optional record field", path: "entity.profile.note", kind: "scalar", typeRef: "text", optional: true},
		{name: "record list", path: "entity.profile.ids", kind: "list", typeRef: "[integer]"},
		{name: "record list size", path: "entity.profile.ids.size", kind: "scalar", typeRef: "integer"},
		{name: "optional root map", path: "entity.optional_items", kind: "map", typeRef: "map[text][integer]", optional: true},
		{name: "optional root list size", path: "entity.optional_ids.size", kind: "scalar", typeRef: "integer", optional: true},
		{name: "record map", path: "entity.profile.items", kind: "map", typeRef: "map[text][integer]"},
		{name: "optional ancestor scalar", path: "entity.profile.maybe.label", kind: "scalar", typeRef: "text", optional: true},
		{name: "optional ancestor map", path: "entity.profile.maybe.items", kind: "map", typeRef: "map[text]Score", optional: true},
		{name: "optional ancestor list size", path: "entity.profile.maybe.ids.size", kind: "scalar", typeRef: "integer", optional: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := a2CatalogPathBundle()
			if _, err := (runtimecontracts.CatalogTypeReference{Type: tc.typeRef, Catalog: bundle.RootTypes}).Resolve(); err != nil {
				t.Fatalf("catalog refuses expected leaf %s: %v", tc.typeRef, err)
			}
			before := a2CatalogPathSnapshot(t, bundle)
			got, owner, err := wave1ResolveEntityPathWithOwner(semanticview.Wrap(bundle), ".", tc.path)
			if err != nil {
				t.Fatalf("resolve admitted %s: %v", tc.path, err)
			}
			if got != (wave1ResolvedType{Kind: tc.kind, Type: tc.typeRef, IsOptional: tc.optional}) || owner != "." {
				t.Fatalf("%s = %#v, owner %q; want kind=%s type=%s optional=%t owner=root", tc.path, got, owner, tc.kind, tc.typeRef, tc.optional)
			}
			if after := a2CatalogPathSnapshot(t, bundle); after != before {
				t.Fatalf("resolving %s changed the source declaration", tc.path)
			}
		})
	}
}

func TestA2CatalogEntityPathRejectsMissingDeclaredTypes(t *testing.T) {
	for _, tc := range []struct {
		name, path, typeRef string
	}{
		{name: "empty root type", path: "entity.name", typeRef: ""},
		{name: "unknown root type", path: "entity.name", typeRef: "Missing"},
		{name: "empty scalar alias", path: "entity.name", typeRef: "Empty"},
		{name: "empty list element alias", path: "entity.name", typeRef: "[Empty]"},
		{name: "empty list element alias size", path: "entity.name.size", typeRef: "[Empty]"},
		{name: "empty map value alias", path: "entity.name", typeRef: "map[text]Empty"},
		{name: "cyclic scalar alias", path: "entity.name", typeRef: "Cycle"},
		{name: "empty list element", path: "entity.name", typeRef: "[]"},
		{name: "unknown list element", path: "entity.name.size", typeRef: "[Missing]"},
		{name: "missing map value", path: "entity.name", typeRef: "map[text]"},
		{name: "unknown map value", path: "entity.name", typeRef: "map[text]Missing"},
		{name: "empty nested field", path: "entity.profile.blank", typeRef: "text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := a2CatalogPathBundle()
			contract := bundle.RootEntities["work"]
			contract.Fields["name"] = runtimecontracts.EntityFieldDecl{Type: tc.typeRef}
			bundle.RootTypes.Scalars["Empty"] = runtimecontracts.ScalarTypeDecl{}
			bundle.RootTypes.Scalars["Cycle"] = runtimecontracts.ScalarTypeDecl{Base: "Cycle"}
			profile := bundle.RootTypes.Types["Profile"]
			profile.Fields["blank"] = runtimecontracts.TypeFieldSpec{}
			before := a2CatalogPathSnapshot(t, bundle)
			got, owner, err := wave1ResolveEntityPathWithOwner(semanticview.Wrap(bundle), ".", tc.path)
			if err == nil || got != (wave1ResolvedType{}) || owner != "" {
				t.Fatalf("missing type admitted %s as %#v owner=%q err=%v", tc.path, got, owner, err)
			}
			if after := a2CatalogPathSnapshot(t, bundle); after != before {
				t.Fatal("missing type refusal changed the source declaration")
			}
		})
	}
}

func TestA2CatalogEntityPathRejectsUnownedAndInvalidTraversal(t *testing.T) {
	for _, tc := range []struct {
		name, flow, path, diagnostic string
	}{
		{name: "unknown field", flow: ".", path: "entity.missing", diagnostic: "does not declare field"},
		{name: "unknown nested field", flow: ".", path: "entity.profile.missing", diagnostic: "undeclared nested field"},
		{name: "empty segment", flow: ".", path: "entity.profile..name", diagnostic: "empty segment"},
		{name: "trailing segment", flow: ".", path: "entity.profile.", diagnostic: "empty segment"},
		{name: "scalar traversal", flow: ".", path: "entity.name.value", diagnostic: "non-composite"},
		{name: "list indexing", flow: ".", path: "entity.ids.0", diagnostic: "unsupported segment"},
		{name: "list size is terminal", flow: ".", path: "entity.ids.size.value", diagnostic: "unsupported segment"},
		{name: "map key is not a record field", flow: ".", path: "entity.items.A", diagnostic: "non-composite"},
		{name: "map has no list size pseudo field", flow: ".", path: "entity.items.size", diagnostic: "non-composite"},
		{name: "foreign flow cannot borrow root fields", flow: "unrelated", path: "entity.items", diagnostic: "no declared entity contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := a2CatalogPathBundle()
			before := a2CatalogPathSnapshot(t, bundle)
			got, owner, err := wave1ResolveEntityPathWithOwner(semanticview.Wrap(bundle), tc.flow, tc.path)
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("%s in %q: result=%#v owner=%q error=%v, want %q", tc.path, tc.flow, got, owner, err, tc.diagnostic)
			}
			if got != (wave1ResolvedType{}) || owner != "" {
				t.Fatalf("rejected path retained partial admission: %#v, owner %q", got, owner)
			}
			if after := a2CatalogPathSnapshot(t, bundle); after != before {
				t.Fatalf("rejecting %s changed the source declaration", tc.path)
			}
		})
	}
}

func TestA2CatalogEntityPathConsumersRetainMapOwner(t *testing.T) {
	for _, tc := range []struct {
		name, path, field, typeRef string
		optional                   bool
	}{
		{name: "root map reader", path: "entity.items", field: "items", typeRef: "map[text][integer]"},
		{name: "record map", path: "entity.profile.items", field: "profile", typeRef: "map[text][integer]"},
		{name: "optional ancestor", path: "entity.profile.maybe.items", field: "profile", typeRef: "map[text]Score", optional: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := a2CatalogPathBundle()
			before := a2CatalogPathSnapshot(t, bundle)
			source := semanticview.Wrap(bundle)
			want := wave1ResolvedType{Kind: "map", Type: tc.typeRef, IsOptional: tc.optional}
			t.Run("expression reader census", func(t *testing.T) {
				refs := wave1ResolvedExpressionRefs(source, ".", "reader", "item.reported", expressionReference{Expression: tc.path})
				if len(refs) != 1 || refs[0].OwnerFlowID != "." || refs[0].Field != tc.field || refs[0].Leaf != want {
					t.Fatalf("resolved %s reader refs = %#v; want one exact root-owned %s map reference", tc.path, refs, tc.field)
				}
			})
			if tc.field == "profile" {
				t.Run("nested writer target", func(t *testing.T) {
					got, owner, field, err := wave1ResolveWriteTargetPath(source, wave1WriteTarget{
						Node: identitytest.RootNode(t, "writer"), Target: tc.path, Entity: true,
					})
					if err != nil || got != want || owner != "." || field != tc.field {
						t.Fatalf("resolved %s write target = %#v owner=%q field=%q error=%v; want exact declared map", tc.path, got, owner, field, err)
					}
				})
			}
			if after := a2CatalogPathSnapshot(t, bundle); after != before {
				t.Fatalf("path consumers changed the source declaration for %s", tc.path)
			}
		})
	}
}

// These are already-lowered contract components, not constructor or public-boot proof.
func a2CatalogPathBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"work": {Fields: map[string]runtimecontracts.EntityFieldDecl{
				"name":           {Type: "text"},
				"total":          {Type: "integer"},
				"score":          {Type: "Score"},
				"decision":       {Type: "Decision"},
				"profile":        {Type: "Profile"},
				"ids":            {Type: "[text]"},
				"groups":         {Type: "[[text]]"},
				"items":          {Type: "map[text][integer]"},
				"records":        {Type: "map[text]Record"},
				"by_region":      {Type: "map[text]map[text]Decision"},
				"optional_items": {Type: "map[text][integer]", IsOptional: true},
				"optional_ids":   {Type: "[integer]", IsOptional: true},
			}},
		},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Scalars: map[string]runtimecontracts.ScalarTypeDecl{"Score": {Base: "integer"}},
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"Decision": {Values: []string{"accept", "reject"}, Default: "accept"},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Record": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "integer"}}},
				"Profile": {Fields: map[string]runtimecontracts.TypeFieldSpec{
					"name":  {Type: "text"},
					"note":  {Type: "text", IsOptional: true},
					"ids":   {Type: "[integer]"},
					"items": {Type: "map[text][integer]"},
					"maybe": {Type: "Nested", IsOptional: true},
				}},
				"Nested": {Fields: map[string]runtimecontracts.TypeFieldSpec{
					"label": {Type: "text"},
					"items": {Type: "map[text]Score"},
					"ids":   {Type: "[integer]"},
				}},
			},
		},
	}
}

func a2CatalogPathSnapshot(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) string {
	t.Helper()
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("snapshot source declaration: %v", err)
	}
	return string(data)
}
