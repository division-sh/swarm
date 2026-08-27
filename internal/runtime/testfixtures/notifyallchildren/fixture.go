package notifyallchildren

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	OwnerFlowID       = canonicalrouting.NotifyAllChildrenOwnerFlowID
	OwnerOutputPin    = canonicalrouting.NotifyAllChildrenOwnerOutputPin
	OwnerTriggerEvent = canonicalrouting.NotifyAllChildrenOwnerTriggerEvent
	NotifyEvent       = canonicalrouting.NotifyAllChildrenEvent
	ChildFlowID       = canonicalrouting.NotifyAllChildrenChildFlowID
	ChildInputPin     = canonicalrouting.NotifyAllChildrenChildInputPin
)

type Options = canonicalrouting.NotifyAllChildrenOptions

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := ExampleRoot(t)
	if opts != (Options{}) {
		root = WriteVariant(t, opts)
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func ExampleRoot(t testing.TB) string {
	t.Helper()
	return filepath.Join(canonicalrouting.RepoRoot(t), "examples", "routing", "notify-all-children")
}

func WriteVariant(t testing.TB, opts Options) string {
	t.Helper()
	return canonicalrouting.CopyNotifyAllChildren(t, opts)
}
