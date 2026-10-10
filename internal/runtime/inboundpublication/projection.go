package inboundpublication

import (
	"encoding/json"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

// ProjectOutputEvent is shared by HTTP and native input after the same compiled
// admission owner has projected BODY. Transport adapters do not normalize again.
func ProjectOutputEvent(request Request, ordinal int, output providertriggers.DeliveryEvent, posture executionposture.Posture) (bus.InboundDeliveryEvent, error) {
	source, err := events.NewExternalIngressRoutingSource(request.FlowPath, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		return bus.InboundDeliveryEvent{}, err
	}
	id, err := DeterministicEventID(request.PublicationID, ordinal)
	if err != nil {
		return bus.InboundDeliveryEvent{}, err
	}
	payload, err := json.Marshal(output.Payload)
	if err != nil {
		return bus.InboundDeliveryEvent{}, err
	}
	event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: events.EventFacts{
		ID: id, Type: output.Name, Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "inbound-gateway"},
		Payload: payload, RoutingSource: source, CreatedAt: request.OriginalReceivedAt, ExecutionMode: posture.RootMode(),
	}, RunID: request.ResolvedRunID})
	return bus.InboundDeliveryEvent{Event: event, Kind: provideroutput.Kind(output.Kind), Authorization: output.Authorization}, err
}

func ProjectEvidence(request Request, eventIDs, eventNames []string, posture executionposture.Posture) (events.Event, error) {
	payload, err := BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		return events.Event{}, err
	}
	return events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: events.EventFacts{
		ID: request.MarkerEventID, Type: events.EventTypePlatformInboundRecord,
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"}, Payload: payload,
		CreatedAt: request.OriginalReceivedAt, ExecutionMode: posture.RootMode(),
	}, RunID: request.ResolvedRunID})
}
