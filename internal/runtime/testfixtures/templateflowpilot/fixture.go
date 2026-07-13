package templateflowpilot

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Options applies one deliberate negative mutation to the canonical
// select-or-create artifact.
type Options struct {
	BadConnectMapping            bool
	UnsupportedReceiverSelection bool
	ProducerTarget               bool
	ProducerBroadcast            bool
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t, opts)
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

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := canonicalrouting.CopyTemplateSelectOrCreatePilot(t)
	applyNegativeMutation(t, root, opts)
	addLifecycleOverlay(t, root)
	return root
}

func addLifecycleOverlay(t testing.TB, root string) {
	t.Helper()
	canonicalrouting.ApplyOverlay(t, root, "flows/account/schema.yaml",
		"initial_state: pending\nstates: [pending, done]\nterminal_states: [done]\n")
}

func applyNegativeMutation(t testing.TB, root string, opts Options) {
	// routing-example-census: negative-mutation issue=none owner=examples.routing.template_select_or_create proof=internal/runtime/conformance/template_flow_pilot_conformance_test.go:TestTemplateFlowPilotConformance_FailClosedMatrix
	t.Helper()
	if opts.BadConnectMapping {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateBadConnectMapping)
	}
	if opts.UnsupportedReceiverSelection {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateReceiverSelector)
	}
	if opts.ProducerTarget {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateProducerTarget)
	}
	if opts.ProducerBroadcast {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateProducerBroadcast)
	}
}
