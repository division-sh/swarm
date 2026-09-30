package accprojection

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestA2ProjectionTreatsMetadataNamesAsCatalogBusinessFields(t *testing.T) {
	binding := Binding{SourceItemType: "Business", TargetItemType: "Projected", Project: map[string]any{}}
	text := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}
	for _, name := range []string{"event_id", "event_type", "source", "received_at"} {
		field := runtimecontracts.ResolvedCatalogField{Name: name, TypeRef: "text", Type: text}
		binding.SourceType.Fields = append(binding.SourceType.Fields, field)
		binding.TargetType.Fields = append(binding.TargetType.Fields, field)
		binding.Project[name] = "source." + name
	}
	if issues := validateProject(nil, materializedFieldTarget{}, binding); len(issues) != 0 {
		t.Fatalf("catalog-admitted business fields were reserved: %#v", issues)
	}
	binding.Project["source"] = "source.undeclared"
	if issues := validateProject(nil, materializedFieldTarget{}, binding); !issuesContain(issues, "not a field") {
		t.Fatalf("undeclared field escaped catalog admission: %#v", issues)
	}
}
