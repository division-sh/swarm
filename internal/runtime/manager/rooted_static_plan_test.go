package manager

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1StaticTopologyRequiresExactSourceWithoutAgents(t *testing.T) {
	first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	coordinate, err := agenttopology.NewSourceCoordinate(first.Admission())
	if err != nil {
		t.Fatal(err)
	}
	other, err := agenttopology.NewSourceCoordinate(second.Admission())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifySourceSetDesiredAgents(plan, coordinate, nil); err != nil {
		t.Fatalf("exact empty-agent source was refused: %v", err)
	}
	empty, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct {
		name       string
		plan       agenttopology.SourceSetPlan
		coordinate agenttopology.SourceCoordinate
	}{
		{"same-hash-other-root", plan, other},
		{"missing-root", plan, agenttopology.SourceCoordinate{BundleHash: coordinate.BundleHash}},
		{"absent-source", empty, coordinate},
	} {
		t.Run(cell.name, func(t *testing.T) {
			if err := verifySourceSetDesiredAgents(cell.plan, cell.coordinate, nil); err == nil {
				t.Fatal("zero-agent topology accepted a source absent from the exact rooted plan")
			}
		})
	}
}
