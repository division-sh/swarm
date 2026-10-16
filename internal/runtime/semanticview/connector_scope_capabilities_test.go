package semanticview

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestS1ConnectorCapabilitiesRetainExactIndependentFlowCoordinates(t *testing.T) {
	const name = "team-bot.send"
	imports := map[string]ConnectorImportSource{name: MustConnectorImportSource("team-bot/connector.yaml")}
	root := Capabilities{}.WithConnectorPackImports(".", map[string]ConnectorGenerationSurface{
		name: semanticCapabilityGeneration("root", nil),
	}, imports).WithConnectorPackProvenance(".", map[string]ConnectorPackProvenance{name: {
		PackID: "team-bot", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("a", 64), Source: "team-bot/connector.yaml",
	}})
	child := root.WithConnectorPackImports("child", map[string]ConnectorGenerationSurface{
		name: semanticCapabilityGeneration("child", nil),
	}, map[string]ConnectorImportSource{name: MustConnectorImportSource("child/team-bot/connector.yaml")}).
		WithConnectorPackProvenance("child", map[string]ConnectorPackProvenance{name: {
			PackID: "team-bot", Version: "2.0.0", ManifestHash: "sha256:" + strings.Repeat("b", 64), Source: "child/team-bot/connector.yaml",
		}})
	imports[name] = MustConnectorImportSource("foreign")
	for _, cell := range []struct{ flow, generation, version, label string }{
		{".", "root", "1.0.0", "team-bot/connector.yaml"},
		{"child", "child", "2.0.0", "child/team-bot/connector.yaml"},
	} {
		generation, found := child.ConnectorGeneration(cell.flow, name)
		source, hasSource := child.ConnectorImportSource(cell.flow, name)
		identity, hasIdentity := child.ConnectorPackProvenance(cell.flow, name)
		if !found || !hasSource || !hasIdentity || generation.GeneratorVersion() != cell.generation || identity.Version != cell.version || source.URI() != cell.label {
			t.Fatalf("scope metadata conflated a sibling or changed via caller map: %s", cell.flow)
		}
	}
	for _, flow := range []string{"", " child", "child/", "sibling", "foreign"} {
		if _, found := child.ConnectorImportSource(flow, name); found {
			t.Fatalf("metadata repaired or fell back from foreign flow %q", flow)
		}
		if _, found := child.ConnectorGeneration(flow, name); found {
			t.Fatalf("generation borrowed another flow %q", flow)
		}
		if _, found := child.ConnectorPackProvenance(flow, name); found {
			t.Fatalf("identity borrowed another flow %q", flow)
		}
	}
	if _, found := root.ConnectorGeneration("child", name); found {
		t.Fatal("copy-on-write scope addition mutated its predecessor")
	}
	removed := child.WithConnectorPackImports("child", nil, nil)
	if _, found := removed.ConnectorGeneration("child", name); found {
		t.Fatal("removed scope kept its old generation")
	}
	if _, found := removed.ConnectorGeneration(".", name); !found {
		t.Fatal("removing one scope erased its sibling")
	}
	if _, found := child.ConnectorGeneration("child", name); !found {
		t.Fatal("removing one scope mutated a retained predecessor")
	}
	for _, flow := range []string{"", " child", "child/", "./child"} {
		bad := child.WithConnectorPackImports(flow, nil, imports)
		if bad.ConnectorPackImportsApplied() || !child.ConnectorPackImportsApplied() {
			t.Fatalf("invalid scope acquired metadata or changed its predecessor: %q", flow)
		}
	}
	if child.WithoutConnectorPackImports().ConnectorPackImportsApplied() || !child.ConnectorPackImportsApplied() {
		t.Fatal("canonical rebuild did not isolate removal of connector metadata")
	}
	base := Wrap(&contracts.WorkflowContractBundle{})
	generation := triggergeneration.FromCanonicalBytes([]byte("scope-generation"))
	withTrigger := child.WithProviderTriggerEvents(base, generation, nil)
	retained, source, found := withTrigger.WithoutConnectorPackImports().ProviderTriggerEvents()
	if !found || source != base || !retained.Equal(generation) {
		t.Fatal("connector rebuild changed another capability owner's evidence")
	}
}
