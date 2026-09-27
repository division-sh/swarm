package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type producerCensusCounter struct {
	Source
	builds int
}

func (s *producerCensusCounter) WorkflowTimers() []runtimecontracts.WorkflowTimerContract {
	s.builds++
	return s.Source.WorkflowTimers()
}

func TestInputProducerSharesOnlyOperationLocalCensus(t *testing.T) {
	for _, kind := range []runtimecontracts.FlowInputPinSource{runtimecontracts.FlowInputPinSourceNone, runtimecontracts.FlowInputPinSourceHarness} {
		name := "provider"
		if kind == runtimecontracts.FlowInputPinSourceHarness {
			name = "harness"
		}
		t.Run(name, func(t *testing.T) {
			source := &producerCensusCounter{Source: flowInputProducerFixture(t, runtimecontracts.FlowInputEventPin{Event: "work.requested", Source: kind}, nil)}
			if kind == runtimecontracts.FlowInputPinSourceNone {
				source.Source = markedToolOverlaySource{Source: source.Source, capabilities: source.SemanticCapabilities().WithProviderTriggerEvents(source.Source, triggergeneration.FromCanonicalBytes([]byte("producer-work")), nil).WithProviderIngressEvents(map[string][]string{"worker": {"work.requested"}})}
			}
			for calls := 1; calls <= 2; calls++ {
				got := ResolveNonConnectFlowInputProducer(source, "worker", "work.requested")
				want := runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress
				if kind == runtimecontracts.FlowInputPinSourceHarness {
					want = runtimecontracts.FlowInputProducerBoundaryHarnessInjection
				}
				if source.builds != calls || !got.HasEvidenceKind(want) {
					t.Fatalf("builds = %d, want %d; evidence=%#v", source.builds, calls, got.Evidence)
				}
			}
			got := ResolveNonConnectFlowInputProducer(source, "worker", "missing")
			if source.builds != 2 || !got.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
				t.Fatal("invalid input was not rejected before census construction")
			}
		})
	}
}
