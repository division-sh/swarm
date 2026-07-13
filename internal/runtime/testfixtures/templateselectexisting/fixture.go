package templateselectexisting

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	PackageName = "template-select-existing"

	ProducerFlowID    = "producer"
	ProducerOutputPin = "account_ready"
	ProducerOutput    = "account.ready"

	TemplateFlowID     = "account"
	TemplateInputPin   = "account_ready"
	TemplateInput      = "account.ready"
	TemplateInstanceBy = "account_id"
)

func LoadBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		root,
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadSource(t testing.TB) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t))
}

func Write(t testing.TB) string {
	t.Helper()
	return canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateSelectExisting)
}
