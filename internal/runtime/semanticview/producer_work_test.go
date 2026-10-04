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
	for _, tc := range []struct {
		name     string
		provider bool
	}{{name: "provider", provider: true}, {name: "unbound private input"}} {
		t.Run(tc.name, func(t *testing.T) {
			source := &producerCensusCounter{Source: flowInputProducerFixture(t, runtimecontracts.FlowInputEventPin{Event: "work.requested"}, nil)}
			if tc.provider {
				source.Source = markedToolOverlaySource{Source: source.Source, capabilities: source.SemanticCapabilities().WithProviderTriggerEvents(source.Source, triggergeneration.FromCanonicalBytes([]byte("producer-work")), nil).WithProviderIngressEvents(map[string][]string{"worker": {"work.requested"}})}
			}
			for calls := 1; calls <= 2; calls++ {
				got := ResolveNonConnectFlowInputProducer(source, "worker", "work.requested")
				if source.builds != calls || got.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) != tc.provider || got.HasEvidence() != tc.provider {
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
