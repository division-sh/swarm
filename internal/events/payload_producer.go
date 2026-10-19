package events

import "fmt"

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
		if platformPayloadClass(event.AdmissionClass()) {
			return fmt.Errorf("platform event requires explicit platform payload producer")
		}
	case "platform":
		if p.flowID != "" || p.class != event.AdmissionClass() || p.event != event.Type() || !p.producer.Equal(event.Producer()) {
			return fmt.Errorf("platform payload producer disagrees with event identity")
		}
		if !platformPayloadClass(p.class) || p.producer.Type() != EventProducerPlatform {
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
	if platformPayloadClass(event.AdmissionClass()) {
		return PlatformPayloadProducer(event)
	}
	return AuthoredPayloadProducer(authoredFlowID)
}

func platformPayloadClass(class EventAdmissionClass) bool {
	return class == EventAdmissionRuntimeControl || class == EventAdmissionRuntimeDiagnostic || class == EventAdmissionDiagnosticDirect
}
