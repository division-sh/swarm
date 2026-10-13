package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// The external test package supplies the native store composition without an
// import cycle through storetest's runtime-facing helpers.
var InboundTransportNativeIndexFixture func(testing.TB, semanticview.Source, []InboundTarget) runtimepipeline.FlowInstanceIndexReader

func newInboundTestEventBus(t testing.TB, store runtimebus.EventStore, targets ...InboundTarget) (*runtimebus.EventBus, error) {
	return newInboundTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{}, targets...)
}

func inboundTransportSource(t testing.TB, targets []InboundTarget, observe bool) semanticview.Source {
	t.Helper()
	flows := make([]string, 0, len(targets))
	for _, target := range targets {
		flows = append(flows, target.FlowPath)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo,
		canonicalrouting.CopyInboundTransport(t, flows, observe), runtimecontracts.DefaultPlatformSpecFile(repo),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	ingress := make(map[string][]string, len(targets))
	for _, target := range targets {
		schema, found := bundle.FlowSchemas[target.FlowPath]
		if !found {
			t.Fatal("transport fixture omitted its selected declaration")
		}
		for _, pin := range schema.Pins.Inputs.EventPins {
			ingress[target.FlowPath] = append(ingress[target.FlowPath], pin.Event)
		}
	}
	return semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), ingress)
}

// The transport publication owner stays isolated; its instance observations
// come from genuine source-bound construction in the native selected store.
func newInboundTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions, targets ...InboundTarget) (*runtimebus.EventBus, error) {
	t.Helper()
	if len(targets) == 0 {
		targets = []InboundTarget{testInboundTarget("", "")}
	}
	source := opts.ContractBundle
	if source == nil {
		source = inboundTransportSource(t, targets, len(opts.Interceptors) > 0)
	}
	bundle, present := semanticview.Bundle(source)
	if !present || bundle.SourceArtifact == nil {
		return nil, fmt.Errorf("transport fixture requires its admitted source artifact")
	}
	fact := testSourceArtifactFact(t, bundle.SourceArtifact.BundleHash())
	if opts.SourceArtifactFact.Validate() == nil && opts.SourceArtifactFact != fact {
		return nil, fmt.Errorf("transport fixture source fact contradicts its source")
	}
	opts.ContractBundle, opts.SourceArtifactFact = source, fact
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, fact.BundleHash())
	}
	for _, target := range targets {
		if target.BundleHash != "" && target.BundleHash != fact.BundleHash() {
			return nil, fmt.Errorf("transport fixture target source contradicts its source")
		}
	}
	if InboundTransportNativeIndexFixture == nil {
		return nil, fmt.Errorf("native transport fixture composition is not installed")
	}
	opts.Durable.Instances = InboundTransportNativeIndexFixture(t, source, targets)
	return newRuntimeTestEventBusWithOptions(t, store, opts)
}

func inboundTestBusBundleHash(t testing.TB, bus *runtimebus.EventBus) string {
	t.Helper()
	ctx, err := bus.AdmitSourceArtifactFact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fact, present := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !present {
		t.Fatal("transport bus omitted its admitted source fact")
	}
	return fact.BundleHash()
}

func TestInboundTransportFixtureRejectsForeignSourceBeforePublication(t *testing.T) {
	for _, mismatch := range []string{"fact", "target"} {
		t.Run(mismatch, func(t *testing.T) {
			target := testInboundTarget("", "")
			source := inboundTransportSource(t, []InboundTarget{target}, false)
			foreign := testSourceArtifactFact(t, "bundle-v2:sha256:"+strings.Repeat("d", 64))
			opts := runtimebus.EventBusOptions{ContractBundle: source}
			if mismatch == "fact" {
				opts.SourceArtifactFact = foreign
			} else {
				target.BundleHash = foreign.BundleHash()
			}
			store := &capturingInboundEventStore{}
			bus, err := newInboundTestEventBusWithOptions(t, store, opts, target)
			if bus != nil || err == nil || !strings.Contains(err.Error(), "contradicts its source") {
				t.Fatalf("foreign %s: bus=%v err=%v, want exact source refusal", mismatch, bus, err)
			}
			if store.recorded || len(store.events) != 0 || len(store.active) != 0 {
				t.Fatal("foreign source reached the transport publication owner")
			}
		})
	}
}
