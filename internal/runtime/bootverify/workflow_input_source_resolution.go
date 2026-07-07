package bootverify

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (c *checkerContext) resolveDeclaredInputProducerSource(flowID, eventType string) (runtimecontracts.FlowInputProducerResolution, bool) {
	return resolveDeclaredInputProducerSource(c.source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{
		HarnessInjections: c.opts.HarnessInjections,
	})
}

func resolveDeclaredInputProducerSource(source semanticview.Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) (runtimecontracts.FlowInputProducerResolution, bool) {
	if source == nil {
		return runtimecontracts.FlowInputProducerResolution{}, false
	}
	inputEvent := matchingDeclaredInputEvent(source, flowID, eventType)
	if inputEvent == "" {
		return runtimecontracts.FlowInputProducerResolution{}, false
	}
	return semanticview.ResolveFlowInputProducerWithOptions(source, flowID, inputEvent, opts), true
}

func matchingDeclaredInputEvent(source semanticview.Source, flowID, eventType string) string {
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return ""
	}
	candidates := uniqueNormalizedInputCandidates(
		eventType,
		source.ResolveFlowEventReference(flowID, eventType),
	)
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	candidates = append(candidates, uniqueNormalizedInputCandidates(proof.Authored, proof.Local, proof.Canonical, proof.CatalogKey)...)
	for _, candidate := range candidates {
		if source.FlowHasInputEvent(flowID, candidate) {
			return candidate
		}
	}
	for _, pin := range source.FlowInputEventPins(flowID) {
		for _, candidate := range candidates {
			if inputEventMatches(source, flowID, pin.EventType(), candidate) || inputEventMatches(source, flowID, pin.PinName(), candidate) {
				return pin.EventType()
			}
		}
	}
	return ""
}

func inputEventMatches(source semanticview.Source, flowID, declared, candidate string) bool {
	declared = eventidentity.Normalize(declared)
	candidate = eventidentity.Normalize(candidate)
	if declared == "" || candidate == "" {
		return false
	}
	if declared == candidate {
		return true
	}
	if source == nil {
		return false
	}
	if source.FlowEventMatches(flowID, declared, candidate) || source.FlowEventMatches(flowID, candidate, declared) {
		return true
	}
	return eventidentity.Normalize(source.ResolveFlowEventReference(flowID, declared)) == eventidentity.Normalize(source.ResolveFlowEventReference(flowID, candidate))
}

func uniqueNormalizedInputCandidates(values ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = eventidentity.Normalize(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
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
