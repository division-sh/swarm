package singletoncoordinatorpilot

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	FlowID       = "coordinator"
	NodeID       = "coordinator-indexer"
	EntityType   = "coordinator_state"
	InputEvent   = "lead.observed"
	FlowInstance = "coordinator"
)

// Options toggles the singleton coordinator pilot variants used by
// conformance and runtime tests.
type Options struct {
	DynamicBracketTarget bool
	MissingMapKey        bool
	WrongValueShape      bool
	UndeclaredTarget     bool
	UnsupportedOperation bool
	BadListIndex         bool
}

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
	root := Write(t, opts)
	repo := canonicalrouting.RepoRoot(t)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	return canonicalrouting.CopySingletonCoordinatorPilot(t, singletonCoordinatorVariant(t, opts))
}

func singletonCoordinatorVariant(t testing.TB, opts Options) canonicalrouting.SingletonCoordinatorPilotVariant {
	t.Helper()
	variant := canonicalrouting.SingletonCoordinatorPilotDefault
	selected := 0
	candidates := []struct {
		enabled bool
		variant canonicalrouting.SingletonCoordinatorPilotVariant
	}{
		{opts.DynamicBracketTarget, canonicalrouting.SingletonCoordinatorPilotDynamicBracketTarget},
		{opts.MissingMapKey, canonicalrouting.SingletonCoordinatorPilotMissingMapKey},
		{opts.WrongValueShape, canonicalrouting.SingletonCoordinatorPilotWrongValueShape},
		{opts.UndeclaredTarget, canonicalrouting.SingletonCoordinatorPilotUndeclaredTarget},
		{opts.UnsupportedOperation, canonicalrouting.SingletonCoordinatorPilotUnsupportedOperation},
		{opts.BadListIndex, canonicalrouting.SingletonCoordinatorPilotBadListIndex},
	}
	for _, candidate := range candidates {
		if candidate.enabled {
			selected++
			variant = candidate.variant
		}
	}
	if selected > 1 {
		t.Fatal("singleton coordinator pilot accepts exactly one typed variant")
	}
	return variant
}
