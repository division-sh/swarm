package finalflowinstanceauthoring

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	PackageName = "template-select-or-create"

	ProducerFlowID    = "producer"
	ProducerNodeID    = "producer-node"
	ProducerInputPin  = "account_requested"
	ProducerOutputPin = "account_ready"
	ProducerInput     = "account.requested"
	ProducerOutput    = "account.ready"

	TemplateFlowID       = "account"
	TemplateNodeID       = "account-node"
	TemplateEntityType   = "account_state"
	TemplateInputPin     = "account_ready"
	TemplateInstanceBy   = "account_id"
	TemplatePayloadKey   = "account_id"
	TemplateFlowInstance = TemplateFlowID

	LegacyFlowID = "legacy_static"
)

type Options struct {
	MissingOutputKey            bool
	MissingOutputCarries        bool
	BadConnectMapping           bool
	DuplicateConnectMapping     bool
	UnsupportedReceiverSelector bool
	ProducerTarget              bool
	ProducerBroadcast           bool

	StaticCreateEntity        bool
	StaticSelectEntity        bool
	StaticSelectOrCreate      bool
	StaticMissingAcquisition  bool
	RootDefaultEntityIDSource bool
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
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := canonicalrouting.CopyTemplateSelectOrCreateFinalAuthoring(t)
	if opts.StaticCreateEntity || opts.StaticSelectEntity || opts.StaticSelectOrCreate || opts.StaticMissingAcquisition {
		addLegacyStaticOverlay(t, root, opts)
	}
	if opts.RootDefaultEntityIDSource {
		canonicalrouting.AddRootDefaultEntityIDForNegativeMutation(t, root)
	}
	applyRoutingMutation(t, root, opts)
	addTemplateLifecycleOverlay(t, root)
	return root
}

func addTemplateLifecycleOverlay(t testing.TB, root string) {
	t.Helper()
	canonicalrouting.ApplyOverlay(t, root, "flows/"+TemplateFlowID+"/schema.yaml",
		"initial_state: pending\nstates: [pending, reviewed]\nterminal_states: [reviewed]\n")
}

func applyRoutingMutation(t testing.TB, root string, opts Options) {
	// routing-example-census: negative-mutation issue=none owner=examples.routing.template_select_or_create proof=internal/runtime/conformance/final_flow_instance_authoring_conformance_test.go:TestFinalFlowInstanceAuthoringFixture_FailClosedMatrix
	t.Helper()
	if opts.MissingOutputKey {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateMissingInstanceKey)
	}
	if opts.MissingOutputCarries {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateMissingCarry)
	}
	if opts.BadConnectMapping {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateBadConnectMapping)
	}
	if opts.DuplicateConnectMapping {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateDuplicateConnectMapping)
	}
	if opts.UnsupportedReceiverSelector {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateReceiverSelector)
	}
	if opts.ProducerTarget {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateProducerTarget)
	}
	if opts.ProducerBroadcast {
		canonicalrouting.ApplyTemplateSelectOrCreateNegativeMutation(t, root, canonicalrouting.TemplateSelectOrCreateProducerBroadcast)
	}
}

func addLegacyStaticOverlay(t testing.TB, root string, opts Options) {
	t.Helper()
	mutation := canonicalrouting.RetiredStaticMissingAcquisition
	switch {
	case opts.StaticCreateEntity:
		mutation = canonicalrouting.RetiredStaticCreate
	case opts.StaticSelectEntity:
		mutation = canonicalrouting.RetiredStaticSelect
	case opts.StaticSelectOrCreate:
		mutation = canonicalrouting.RetiredStaticSelectOrCreate
	}
	canonicalrouting.AddRetiredStaticFlowForNegativeMutation(t, root, mutation)
}
