package events

import (
	"fmt"
	"strings"
)

const RuntimeLogSchemaReference = "platform-spec.yaml#platform_tables.diagnostics_encoding"

// PayloadProducer separates authored flow coordinates from platform identities.
// Neither the absence of a flow nor a schema-class label selects a variant.
type PayloadProducer struct {
	kind     string
	flowID   string
	producer ProducerIdentity
	class    EventAdmissionClass
	event    EventType
}

func AuthoredPayloadProducer(flowID string) PayloadProducer {
	return PayloadProducer{kind: "flow", flowID: flowID}
}

func PlatformPayloadProducer(event Event) PayloadProducer {
	return PayloadProducer{kind: "platform", producer: event.Producer(), class: event.AdmissionClass(), event: event.Type()}
}

func (p PayloadProducer) FlowID() string   { return p.flowID }
func (p PayloadProducer) IsPlatform() bool { return p.kind == "platform" }
func (p PayloadProducer) IsAuthored() bool { return p.kind == "flow" }

func (p PayloadProducer) ValidateForEvent(event Event) error {
	switch p.kind {
	case "flow":
		if p.producer != (ProducerIdentity{}) || p.class != "" || p.event != "" {
			return fmt.Errorf("authored payload producer contains platform authority")
		}
		if platformPayloadIdentity(event.AdmissionClass(), event.Type(), event.Producer()) {
			return fmt.Errorf("platform event requires explicit platform payload producer")
		}
		if authoredTimerPayloadIdentity(event.AdmissionClass(), event.Type(), event.Producer()) {
			source := event.RoutingSource()
			if !(source.Kind() == RoutingSourceRoot && p.flowID == ".") &&
				!(source.Kind() == RoutingSourceFlowOwnedControl && p.flowID != "" && p.flowID != "." && p.flowID == source.Route().FlowID) {
				return fmt.Errorf("workflow timer payload requires its exact authored flow routing source")
			}
			return ValidateEventContract(event)
		}
	case "platform":
		if p.flowID != "" || p.class != event.AdmissionClass() || p.event != event.Type() || !p.producer.Equal(event.Producer()) {
			return fmt.Errorf("platform payload producer disagrees with event identity")
		}
		if !platformPayloadIdentity(p.class, p.event, p.producer) || p.producer.Type() != EventProducerPlatform {
			return fmt.Errorf("platform payload producer requires an admitted platform event class")
		}
		if err := ValidateEventContract(event); err != nil {
			return err
		}
	default:
		return fmt.Errorf("explicit payload producer is required")
	}
	return nil
}

// EventPayloadProducer consumes the event's explicit class, never an empty route.
func EventPayloadProducer(event Event, authoredFlowID string) PayloadProducer {
	if platformPayloadIdentity(event.AdmissionClass(), event.Type(), event.Producer()) {
		return PlatformPayloadProducer(event)
	}
	return AuthoredPayloadProducer(authoredFlowID)
}

func platformPayloadIdentity(class EventAdmissionClass, event EventType, producer ProducerIdentity) bool {
	return (class == EventAdmissionRuntimeControl && !authoredTimerPayloadIdentity(class, event, producer)) || class == EventAdmissionRuntimeDiagnostic || class == EventAdmissionDiagnosticDirect ||
		(class == EventAdmissionReplay && event == "event.replayed" && producer.Type() == EventProducerPlatform)
}

func authoredTimerPayloadIdentity(class EventAdmissionClass, event EventType, producer ProducerIdentity) bool {
	// The reserved platform namespace remains catalog-owned, including stage timers.
	return class == EventAdmissionRuntimeControl && producer.Type() == EventProducerPlatform && producer.ID() == "runtime.workflow_timer" &&
		event != "" && !strings.HasPrefix(string(event), "platform.")
}
