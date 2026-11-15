// Package eventfixture prepares immutable event facts for integration proofs.
// Persistence belongs to the original selected native owners.
package eventfixture

import (
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func eventFacts(
	eventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (events.EventFacts, error) {
	routingSource := events.NoRoutingSource()
	source := envelope.Source.Normalized()
	if !source.Empty() {
		var err error
		if source.FlowInstance != "" {
			routingSource, err = events.NewConcreteTemplateInstanceRoutingSource(source)
		} else {
			routingSource, err = events.NewExternalIngressRoutingSource(source.FlowID, events.RoutingSourceAuthorityProviderAdmissionPlan)
		}
		if err != nil {
			return events.EventFacts{}, err
		}
	}
	return events.EventFacts{
		ID: eventID, Type: eventType,
		Producer: events.ProducerClaim{Type: producer.Type(), ID: producer.ID()},
		Payload:  payload, Envelope: envelope, RoutingSource: routingSource,
		CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}, nil
}

// BindPayload attaches explicit schema-less fixture admission evidence. It is
// shared by store fixtures that intentionally bypass runtime publication.
func BindPayload(event events.Event) (events.Event, error) {
	flowID := event.RoutingSource().Route().FlowID
	if event.RoutingSource().Kind() == events.RoutingSourceRoot {
		flowID = "."
	}
	input := events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:0000000000000000000000000000000000000000000000000000000000000000",
		FlowID:     flowID, EventKey: string(event.Type()),
		SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		SchemaClass:  events.PayloadSchemaSchemaLess,
	}
	producer := events.EventPayloadProducer(event, flowID)
	var binding events.PayloadSchemaBinding
	var err error
	if producer.IsPlatform() {
		input.FlowID, input.SchemaClass = "", events.PayloadSchemaPlatform
		reference := ""
		if event.Type() == events.EventTypePlatformRuntimeLog {
			reference = events.RuntimeLogSchemaReference
		}
		binding, err = events.NewPlatformPayloadSchemaBinding(input, producer, reference)
	} else {
		binding, err = events.NewPayloadSchemaBinding(input)
	}
	if err != nil {
		return event.Clone(), err
	}
	admission, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		return event.Clone(), err
	}
	return events.ApplyPayloadAdmission(event, admission)
}

func fixtureSettlement(event events.Event) (events.RouteSettlement, error) {
	switch event.Type() {
	case events.EventTypePlatformRuntimeLog:
		return events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformInboundRecord:
		return events.NewNoDeliverySettlement(events.EventWriteInboundEvidenceDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformAgentDirective:
		return events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	default:
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			return events.RouteSettlement{}, err
		}
		return events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	}
}
