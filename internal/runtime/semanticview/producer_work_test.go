package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type producerCensusCounter struct {
	Source
	builds int
}

func (s *producerCensusCounter) AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	s.builds++
	return s.Source.AuthoredEventEntries()
}

func TestInputProducerSharesOnlyOperationLocalCensus(t *testing.T) {
	for _, kind := range []runtimecontracts.FlowInputPinSource{runtimecontracts.FlowInputPinSourceExternal, runtimecontracts.FlowInputPinSourceHarness} {
		t.Run(string(kind), func(t *testing.T) {
			source := &producerCensusCounter{Source: flowInputProducerFixture(t, runtimecontracts.FlowInputEventPin{Event: "work.requested", Source: kind}, nil)}
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
