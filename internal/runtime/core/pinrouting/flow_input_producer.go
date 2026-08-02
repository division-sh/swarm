package pinrouting

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ResolveFlowInputProducer combines non-connect producer evidence with the
// canonical compiled graph. No semanticview consumer can inspect authored
// connect rows directly.
func ResolveFlowInputProducer(source semanticview.Source, flowID, eventType string) runtimecontracts.FlowInputProducerResolution {
	return ResolveFlowInputProducerWithOptions(source, flowID, eventType, runtimecontracts.FlowInputProducerResolutionOptions{})
}

func ResolveFlowInputProducerWithOptions(source semanticview.Source, flowID, eventType string, opts runtimecontracts.FlowInputProducerResolutionOptions) runtimecontracts.FlowInputProducerResolution {
	out := semanticview.ResolveNonConnectFlowInputProducerWithOptions(source, flowID, eventType, opts)
	flowID = strings.TrimSpace(flowID)
	eventType = eventidentity.Normalize(eventType)
	if source == nil || eventType == "" {
		return out
	}
	connected := false
	for _, plan := range CompileConnectGraph(source).Plans() {
		if plan.ProviderOutputAuthorization != nil || plan.Receiver.IsRoot() != (flowID == "") || strings.TrimSpace(plan.Receiver.flowID) != flowID {
			continue
		}
		if eventidentity.Normalize(plan.Receiver.event) != eventType && eventidentity.Normalize(plan.Receiver.resolvedEvent) != eventType {
			continue
		}
		connected = true
		evidence := runtimecontracts.FlowInputProducerEvidence{
			Kind: runtimecontracts.FlowInputProducerBoundaryParentConnect, FlowID: strings.TrimSpace(plan.Source.flowID),
			EventType: eventType, Pin: strings.TrimSpace(plan.Receiver.pin), Detail: "compiled parent connect",
		}
		if !flowInputProducerEvidenceContains(out.Evidence, evidence) {
			out.Evidence = append(out.Evidence, evidence)
		}
	}
	if connected && flowID == "" && !opts.AllowNonInputEvent {
		filtered := out.Evidence[:0]
		for _, evidence := range out.Evidence {
			if evidence.Kind != runtimecontracts.FlowInputProducerBoundaryExternalIngress {
				filtered = append(filtered, evidence)
			}
		}
		out.Evidence = filtered
	}
	sort.SliceStable(out.Evidence, func(i, j int) bool {
		return flowInputProducerEvidenceKey(out.Evidence[i]) < flowInputProducerEvidenceKey(out.Evidence[j])
	})
	return out
}

func flowInputProducerEvidenceContains(existing []runtimecontracts.FlowInputProducerEvidence, candidate runtimecontracts.FlowInputProducerEvidence) bool {
	for _, evidence := range existing {
		if flowInputProducerEvidenceKey(evidence) == flowInputProducerEvidenceKey(candidate) {
			return true
		}
	}
	return false
}

func flowInputProducerEvidenceKey(e runtimecontracts.FlowInputProducerEvidence) string {
	return strings.Join([]string{e.Kind, e.FlowID, e.EventType, e.Pin, e.Pattern, e.Detail}, "\x00")
}
