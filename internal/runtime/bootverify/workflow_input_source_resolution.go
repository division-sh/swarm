package bootverify

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (c *checkerContext) resolveDeclaredInputProducerSource(flowID, eventType string) (runtimecontracts.FlowInputProducerResolution, bool) {
	return resolveDeclaredInputProducerSource(c.source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{})
}

func resolveDeclaredInputProducerSource(source semanticview.Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) (runtimecontracts.FlowInputProducerResolution, bool) {
	if source == nil {
		return runtimecontracts.FlowInputProducerResolution{}, false
	}
	association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, eventType)
	endpoint, ok := association.Endpoint()
	if !ok {
		return runtimecontracts.FlowInputProducerResolution{}, false
	}
	return semanticview.ResolveFlowInputProducerWithOptions(source, flowID, endpoint.Event.Authored, opts), true
}

func inputProducerSourceIsExternalNoTarget(resolution runtimecontracts.FlowInputProducerResolution) bool {
	for _, evidence := range resolution.Evidence {
		switch strings.TrimSpace(evidence.Kind) {
		case runtimecontracts.FlowInputProducerBoundaryExternalIngress,
			runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress,
			runtimecontracts.FlowInputProducerBoundaryHarnessInjection,
			runtimecontracts.FlowInputProducerPlatformSource:
			return true
		}
	}
	return false
}

func nonInputEventMetadataProducerSource(entry runtimecontracts.EventCatalogEntry) bool {
	if len(entry.SwarmProducer()) > 0 {
		return true
	}
	source := strings.ToLower(strings.TrimSpace(entry.SwarmSource()))
	return strings.HasPrefix(source, "external") || strings.HasPrefix(source, "platform")
}
