package canonicalrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestFanOutExecutionSourcesLoadExactOverlays(t *testing.T) {
	Prove(t, ArtifactID("internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution"))
	for _, tc := range []struct {
		name string
		copy func(testing.TB) string
	}{
		{"served-reporter", CopyServedFanOutReporter},
		{"mixed-agent", CopyFanOutMixedAgentRoute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.copy(t)
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			repo := RepoRoot(t)
			if _, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, sourceartifactfixture.CapturedRoot(t, artifact), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServedReporterFiniteContractHasRealClosureCarriers(t *testing.T) {
	root := CopyServedFanOutReporter(t)
	repo := RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	if err := runstart.ValidateFinite(semanticview.Wrap(bundle), nil); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []struct {
		flow, node, event string
	}{
		{".", "close-portfolio", "portfolio.closed"},
		{"portfolio", "portfolio-coordinator", "portfolio.close.requested"},
	} {
		graph, ok := bundle.WorkflowStageTopology(stage.flow)
		if !ok || graph.InitialStage != "active" || len(graph.FinalStageIDs()) != 1 || graph.FinalStageIDs()[0] != "done" {
			t.Fatalf("finite fixture lacks an active/end lifecycle: %+v", graph)
		}
		node := identitytest.FlowNode(t, stage.flow, stage.node)
		targets := graph.HandlerTargets(node, stage.event)
		if len(targets) != 1 || targets[0] != "done" {
			t.Fatalf("unreachable decorative final for %s: %v", stage.flow, targets)
		}
	}
}
