package templatefanin

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	ProducerFlowID    = "operating"
	ProducerOutputPin = "operating.reported"
	ProducerEvent     = "operating.reported"
	ReceiverFlowID    = "portfolio"
	ReceiverInputPin  = "operating.reported"
	ReceiverEvent     = "operating.reported"
	ReceiverNodeID    = "portfolio-collector"
)

func LoadBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInStream), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func LoadSource(t testing.TB) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t))
}
