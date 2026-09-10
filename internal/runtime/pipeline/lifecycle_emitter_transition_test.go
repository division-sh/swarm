package pipeline

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestSourceLoadedLoopEscapeTransitionAgreement(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	loops := semanticview.WorkflowLoops(source)
	if len(loops) != 1 || loops[0].FlowID != "." || loops[0].ID != "revision" {
		t.Fatalf("compiled loops=%#v", loops)
	}
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("missing compiled stage topology")
	}
	site := runtimecontracts.WorkflowTransitionSite{
		Node:         pipelineSourceNode(t, source, ".", "controller"),
		HandlerEvent: "loop.repeat",
		LoopID:       "revision", LoopOperation: runtimecontracts.LoopOperationRepeat, LoopEscape: true,
	}
	transition, err := graph.AdmitTransition(site, "review", "escaped")
	if err != nil {
		t.Fatalf("admit selected loop escape: %v", err)
	}
	if transition.FlowID() != "." || transition.Edge().Source != "loop.escape" || transition.Edge().Site() != site {
		t.Fatalf("wrong selected escape provenance: %#v", transition)
	}
	site.LoopEscape = false
	if _, err := graph.AdmitTransition(site, "review", "escaped"); err == nil {
		t.Fatal("ordinary repeat admitted as an escape")
	}
	site.LoopEscape = true
	site.LoopID = "foreign-loop"
	if _, err := graph.AdmitTransition(site, "review", "escaped"); err == nil {
		t.Fatal("foreign loop admitted through the same stage pair")
	}
}
