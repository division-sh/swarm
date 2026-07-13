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
	ReceiverFlowInstance = "portfolio/default"
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
	return canonicalrouting.CopyTemplateFanIn(t, canonicalrouting.TemplateFanInOptions{
		MissingDedup:             opts.MissingDedup,
		DedupTuple:               opts.DedupTuple,
		MissingWindow:            opts.MissingWindow,
		BarrierAggregation:       opts.BarrierAggregation,
		MissingSingleton:         opts.MissingSingleton,
		WrongSingleton:           opts.WrongSingleton,
		AccumulateDedupMismatch:  opts.AccumulateDedupMismatch,
		AccumulateWindowMismatch: opts.AccumulateWindowMismatch,
		DeliveryMany:             opts.DeliveryMany,
		LegacyConnectMap:         opts.LegacyConnectMap,
		EventIDDedup:             opts.EventIDDedup,
		NonSingletonReceiver:     opts.NonSingletonReceiver,
		MissingReceiverHandler:   opts.MissingReceiverHandler,
		MissingAccumulate:        opts.MissingAccumulate,
		AmbiguousReceiverInput:   opts.AmbiguousReceiverInput,
	})
}
