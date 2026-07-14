package templatefanin

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	ProducerFlowID       = "operating"
	ProducerOutputPin    = "operating_reported"
	ProducerEvent        = "operating.reported"
	ReceiverFlowID       = "portfolio"
	ReceiverInputPin     = "operating_reported"
	ReceiverEvent        = "operating.reported"
	ReceiverNodeID       = "portfolio-collector"
	ReceiverFlowInstance = "portfolio"
)

type Options struct {
	MissingDedup             bool
	DedupTuple               bool
	MissingWindow            bool
	BarrierAggregation       bool
	MissingSingleton         bool
	WrongSingleton           bool
	AccumulateDedupMismatch  bool
	AccumulateWindowMismatch bool
	DeliveryMany             bool
	LegacyConnectMap         bool
	EventIDDedup             bool
	NonSingletonReceiver     bool
	MissingReceiverHandler   bool
	MissingAccumulate        bool
	AmbiguousReceiverInput   bool
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
	repoRoot := canonicalrouting.RepoRoot(t)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	id := canonicalrouting.FanInStream
	if opts.BarrierAggregation {
		id = canonicalrouting.FanInBarrier
	}
	root := canonicalrouting.CopyExample(t, id)
	mutations := []struct {
		set      bool
		mutation canonicalrouting.FanInNegativeMutation
	}{
		{opts.MissingDedup, canonicalrouting.FanInMissingDedup},
		{opts.DedupTuple, canonicalrouting.FanInDedupTuple},
		{opts.MissingWindow, canonicalrouting.FanInMissingWindow},
		{opts.MissingSingleton, canonicalrouting.FanInMissingSingleton},
		{opts.WrongSingleton, canonicalrouting.FanInWrongSingleton},
		{opts.AccumulateDedupMismatch, canonicalrouting.FanInAccumulateDedupRedeclaration},
		{opts.AccumulateWindowMismatch, canonicalrouting.FanInAccumulateWindowRedeclaration},
		{opts.DeliveryMany, canonicalrouting.FanInDeliveryMany},
		{opts.LegacyConnectMap, canonicalrouting.FanInLegacyConnectMap},
		{opts.EventIDDedup, canonicalrouting.FanInEventIDDedup},
		{opts.NonSingletonReceiver, canonicalrouting.FanInNonSingletonReceiver},
		{opts.MissingReceiverHandler, canonicalrouting.FanInMissingReceiverHandler},
		{opts.MissingAccumulate, canonicalrouting.FanInMissingRuntimeOwner},
		{opts.AmbiguousReceiverInput, canonicalrouting.FanInAmbiguousReceiverInput},
	}
	for _, item := range mutations {
		if item.set {
			canonicalrouting.ApplyFanInNegativeMutation(t, root, item.mutation)
		}
	}
	return root
}
