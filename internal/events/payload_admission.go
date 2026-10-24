package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

var payloadSchemaDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type PayloadSchemaClass string

const (
	PayloadSchemaAuthored   PayloadSchemaClass = "authored"
	PayloadSchemaImported   PayloadSchemaClass = "imported"
	PayloadSchemaGenerated  PayloadSchemaClass = "generated"
	PayloadSchemaPattern    PayloadSchemaClass = "pattern"
	PayloadSchemaPlatform   PayloadSchemaClass = "platform"
	PayloadSchemaSchemaLess PayloadSchemaClass = "schema_less"
)

// PayloadSchemaBinding is the immutable schema evidence used to admit an
// event payload. Its fields are private so durable consumers cannot assemble
// partial or ambient-current-schema evidence.
type PayloadSchemaBinding struct {
	bundleHash   string
	flowID       string
	eventKey     string
	schemaDigest string
	schemaClass  PayloadSchemaClass
	producer     PayloadProducer
	reference    string
}

type PayloadSchemaBindingInput struct {
	BundleHash   string
	FlowID       string
	EventKey     string
	SchemaDigest string
	SchemaClass  PayloadSchemaClass
}

func NewPayloadSchemaBinding(input PayloadSchemaBindingInput) (PayloadSchemaBinding, error) {
	binding := PayloadSchemaBinding{
		bundleHash: strings.TrimSpace(input.BundleHash),
		flowID:     strings.TrimSpace(input.FlowID), eventKey: strings.TrimSpace(input.EventKey),
		schemaDigest: strings.TrimSpace(input.SchemaDigest), schemaClass: PayloadSchemaClass(strings.TrimSpace(string(input.SchemaClass))),
		producer: AuthoredPayloadProducer(input.FlowID),
	}
	if err := binding.Validate(); err != nil {
		return PayloadSchemaBinding{}, err
	}
	return binding, nil
}

func NewPlatformPayloadSchemaBinding(input PayloadSchemaBindingInput, producer PayloadProducer, reference string) (PayloadSchemaBinding, error) {
	if input.FlowID != "" {
		return PayloadSchemaBinding{}, fmt.Errorf("platform payload binding cannot carry authored flow authority")
	}
	binding, err := NewPayloadSchemaBinding(input)
	if err != nil {
		return PayloadSchemaBinding{}, err
	}
	binding.producer, binding.reference = producer, reference
	if !producer.IsPlatform() {
		return PayloadSchemaBinding{}, fmt.Errorf("platform payload binding requires explicit producer")
	}
	if err := binding.Validate(); err != nil {
		return PayloadSchemaBinding{}, err
	}
	return binding, nil
}

// RestoreEventPayloadSchemaBinding uses the complete durable producer/class
// tuple. An empty flow column or platform schema label alone selects nothing.
func RestoreEventPayloadSchemaBinding(input PayloadSchemaBindingInput, class EventAdmissionClass, eventType EventType, producer ProducerIdentity) (PayloadSchemaBinding, error) {
	p := PayloadProducer{kind: "platform", producer: producer, class: class, event: eventType}
	if joinOccurrenceEventType(eventType) && !joinOccurrencePayloadIdentity(p) {
		return PayloadSchemaBinding{}, fmt.Errorf("join occurrence payload contradicts its persisted producer or class")
	}
	if eventType == "platform.stage_timer" && !stageTimerPayloadIdentity(p) {
		return PayloadSchemaBinding{}, fmt.Errorf("stage timer payload contradicts its persisted producer or class")
	}
	binding, err := RestorePayloadSchemaBinding(input)
	if err != nil {
		return PayloadSchemaBinding{}, err
	}
	if input.SchemaClass == PayloadSchemaPlatform && platformPayloadIdentity(class, eventType, producer) {
		reference := ""
		if eventType == EventTypePlatformRuntimeLog {
			reference = RuntimeLogSchemaReference
		} else if eventType == "platform.dead_letter" {
			reference = DeadLetterSchemaReference
		} else if joinOccurrencePayloadIdentity(p) {
			reference = JoinOccurrencePayloadReference
		} else if stageTimerPayloadIdentity(p) {
			reference = StageTimerPayloadReference
		}
		return NewPlatformPayloadSchemaBinding(input, p, reference)
	}
	if platformPayloadIdentity(class, eventType, producer) || eventType == EventTypePlatformRuntimeLog {
		return PayloadSchemaBinding{}, fmt.Errorf("platform payload requires persisted producer authority")
	}
	return binding, nil
}

func RestorePayloadSchemaBinding(input PayloadSchemaBindingInput) (PayloadSchemaBinding, error) {
	for name, value := range map[string]string{
		"bundle_hash": input.BundleHash, "flow_id": input.FlowID,
		"event_key": input.EventKey, "schema_digest": input.SchemaDigest, "schema_class": string(input.SchemaClass),
	} {
		if value != strings.TrimSpace(value) {
			return PayloadSchemaBinding{}, fmt.Errorf("payload schema binding %s is not canonical", name)
		}
	}
	return NewPayloadSchemaBinding(input)
}

func (b PayloadSchemaBinding) Validate() error {
	if err := runtimebundleidentity.ValidateCanonicalHash(b.bundleHash); err != nil {
		return fmt.Errorf("payload schema bundle: %w", err)
	}
	if b.eventKey == "" {
		return fmt.Errorf("payload schema event_key is required")
	}
	if !payloadSchemaDigestPattern.MatchString(b.schemaDigest) {
		return fmt.Errorf("payload schema digest must be sha256:<64 lowercase hex>")
	}
	switch b.schemaClass {
	case PayloadSchemaAuthored, PayloadSchemaImported, PayloadSchemaGenerated, PayloadSchemaPattern, PayloadSchemaPlatform, PayloadSchemaSchemaLess:
	default:
		return fmt.Errorf("payload schema class %q is invalid", b.schemaClass)
	}
	if b.producer.IsPlatform() {
		p := b.producer
		if b.flowID != "" || b.schemaClass != PayloadSchemaPlatform || b.eventKey != string(p.event) || !platformPayloadIdentity(p.class, p.event, p.producer) || p.producer.Type() != EventProducerPlatform {
			return fmt.Errorf("platform payload binding disagrees with producer, class or event")
		}
		if err := p.producer.Validate(); err != nil {
			return err
		}
		return b.validatePlatformReference()
	} else if !b.producer.IsAuthored() || b.producer.FlowID() != b.flowID || b.reference != "" {
		return fmt.Errorf("payload binding requires an explicit producer variant")
	}
	return nil
}

func (b PayloadSchemaBinding) validatePlatformReference() error {
	p := b.producer
	if p.event == EventTypePlatformRuntimeLog {
		if p.class != EventAdmissionDiagnosticDirect || p.producer.ID() != "runtime" || b.reference != RuntimeLogSchemaReference {
			return fmt.Errorf("runtime-log payload requires exact platform/runtime reference authority")
		}
	} else if p.event == "platform.dead_letter" {
		if p.class != EventAdmissionRuntimeDiagnostic || b.reference != DeadLetterSchemaReference {
			return fmt.Errorf("dead-letter payload requires exact catalog reference authority")
		}
	} else if joinOccurrencePayloadIdentity(p) {
		if b.reference != JoinOccurrencePayloadReference {
			return fmt.Errorf("join occurrence payload requires typed declaration authority")
		}
	} else if stageTimerPayloadIdentity(p) {
		if b.reference != StageTimerPayloadReference {
			return fmt.Errorf("stage timer payload requires typed declaration authority")
		}
	} else if b.reference != "" {
		return fmt.Errorf("platform payload subtype reference is unsupported")
	}
	return nil
}

func (b PayloadSchemaBinding) BundleHash() string              { return b.bundleHash }
func (b PayloadSchemaBinding) FlowID() string                  { return b.flowID }
func (b PayloadSchemaBinding) EventKey() string                { return b.eventKey }
func (b PayloadSchemaBinding) SchemaDigest() string            { return b.schemaDigest }
func (b PayloadSchemaBinding) SchemaClass() PayloadSchemaClass { return b.schemaClass }
func (b PayloadSchemaBinding) Reference() string               { return b.reference }
func (b PayloadSchemaBinding) Producer() PayloadProducer       { return b.producer }

func (b PayloadSchemaBinding) Equal(other PayloadSchemaBinding) bool {
	return b.bundleHash == other.bundleHash && b.flowID == other.flowID &&
		b.eventKey == other.eventKey && b.schemaDigest == other.schemaDigest && b.schemaClass == other.schemaClass &&
		b.producer == other.producer && b.reference == other.reference
}

type PayloadAdmission struct {
	payload []byte
	binding PayloadSchemaBinding
}

func NewPayloadAdmission(payload []byte, binding PayloadSchemaBinding) (PayloadAdmission, error) {
	if !json.Valid(payload) {
		return PayloadAdmission{}, fmt.Errorf("admitted event payload must be valid JSON")
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return PayloadAdmission{}, fmt.Errorf("admitted event payload must be a JSON object")
	}
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		return PayloadAdmission{}, fmt.Errorf("admitted event payload must be a JSON object")
	}
	// Nested null belongs to the bound schema (notably JSON fields); this
	// structural admission can reject only top-level null unconditionally.
	for _, value := range object {
		if value == nil {
			return PayloadAdmission{}, fmt.Errorf("admitted event payload cannot contain top-level null")
		}
	}
	if err := binding.Validate(); err != nil {
		return PayloadAdmission{}, err
	}
	return PayloadAdmission{payload: bytes.Clone(payload), binding: binding}, nil
}

func (a PayloadAdmission) Payload() []byte               { return bytes.Clone(a.payload) }
func (a PayloadAdmission) Binding() PayloadSchemaBinding { return a.binding }
func (a PayloadAdmission) Valid() bool                   { return json.Valid(a.payload) && a.binding.Validate() == nil }

func joinOccurrencePayloadIdentity(producer PayloadProducer) bool {
	return producer.IsPlatform() && (producer.class == EventAdmissionRuntimeControl || producer.class == EventAdmissionSelectedForkReplay) &&
		producer.producer.Type() == EventProducerPlatform && producer.producer.ID() == "runtime.generic_schedule" &&
		joinOccurrenceEventType(producer.event)
}

func joinOccurrenceEventType(eventType EventType) bool {
	return eventType == "platform.join_complete" || eventType == "platform.join_timeout"
}

func stageTimerPayloadIdentity(producer PayloadProducer) bool {
	return producer.IsPlatform() && producer.class == EventAdmissionRuntimeControl &&
		producer.producer.Type() == EventProducerPlatform && producer.producer.ID() == "runtime.workflow_timer" && producer.event == "platform.stage_timer"
}

// JoinOccurrencePayloadDigest binds the occurrence's exact durable identity and
// bytes. Declaration authority is supplied only by the workflow join resolver.
func JoinOccurrencePayloadDigest(event Event) (string, error) {
	if !joinOccurrencePayloadIdentity(PlatformPayloadProducer(event)) {
		return "", fmt.Errorf("join payload identity requires its platform occurrence producer")
	}
	return occurrencePayloadDigest(event, JoinOccurrencePayloadReference)
}

func StageTimerPayloadDigest(event Event) (string, error) {
	if !stageTimerPayloadIdentity(PlatformPayloadProducer(event)) {
		return "", fmt.Errorf("stage timer payload requires its platform timer producer")
	}
	return occurrencePayloadDigest(event, StageTimerPayloadReference)
}

func occurrencePayloadDigest(event Event, reference string) (string, error) {
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return "", err
	}
	source := event.RoutingSource()
	identity := map[string]any{
		"reference": reference, "run_id": event.RunID(), "task_id": event.TaskID(),
		"event":        string(event.Type()),
		"routing_kind": string(source.Kind()), "routing_flow": source.Route().FlowID,
		"routing_instance": source.Route().FlowInstance, "routing_entity": source.Route().EntityID,
		"payload": payload,
	}
	if reference == StageTimerPayloadReference {
		identity["event_id"] = event.ID()
		identity["entity_id"] = event.EntityID()
		identity["flow_instance"] = event.FlowInstance()
	}
	raw, err := canonicaljson.Bytes(identity)
	if err != nil {
		return "", err
	}
	return canonicaljson.HashBytes(raw), nil
}

// ApplyPayloadAdmission replaces the pre-admission payload exactly once.
// Reapplication is accepted only when both bytes and binding are identical.
func ApplyPayloadAdmission(event Event, admission PayloadAdmission) (Event, error) {
	if !admission.Valid() {
		return Event{}, fmt.Errorf("payload admission is invalid")
	}
	if err := admission.binding.producer.ValidateForEvent(event); err != nil {
		return Event{}, err
	}
	if joinOccurrenceEventType(event.Type()) && admission.binding.reference != JoinOccurrencePayloadReference {
		return Event{}, fmt.Errorf("join occurrence payload requires typed declaration admission")
	}
	if event.Type() == "platform.stage_timer" && admission.binding.reference != StageTimerPayloadReference {
		return Event{}, fmt.Errorf("stage timer payload requires typed declaration admission")
	}
	if admission.binding.reference == JoinOccurrencePayloadReference {
		digest, err := JoinOccurrencePayloadDigest(event)
		if err != nil || digest != admission.binding.schemaDigest || !bytes.Equal(event.Payload(), admission.Payload()) {
			return Event{}, fmt.Errorf("join payload binding contradicts its occurrence identity")
		}
	}
	if admission.binding.reference == StageTimerPayloadReference {
		digest, err := StageTimerPayloadDigest(event)
		if err != nil || digest != admission.binding.schemaDigest || !bytes.Equal(event.Payload(), admission.Payload()) {
			return Event{}, fmt.Errorf("stage timer payload binding contradicts its occurrence identity")
		}
	}
	if current, ok := event.PayloadAdmission(); ok {
		if !bytes.Equal(current.Payload(), admission.Payload()) || !current.Binding().Equal(admission.Binding()) {
			return Event{}, fmt.Errorf("event payload admission cannot be replaced")
		}
		return event.Clone(), nil
	}
	admitted := event.Clone()
	admitted.payload = clonePayload(admission.payload)
	binding := admission.binding
	admitted.payloadSchema = &binding
	return admitted, nil
}

// ApplyAdmittedPayload binds payload evidence without re-running event
// admission or reconstructing any already-admitted event fact.
func ApplyAdmittedPayload(event AdmittedEvent, admission PayloadAdmission) (AdmittedEvent, error) {
	bound, err := ApplyPayloadAdmission(event.event, admission)
	if err != nil {
		return AdmittedEvent{}, err
	}
	return newAdmittedEvent(bound, event.runDisposition), nil
}

func (e Event) PayloadAdmission() (PayloadAdmission, bool) {
	if e.payloadSchema == nil {
		return PayloadAdmission{}, false
	}
	admission, err := NewPayloadAdmission(e.payload, *e.payloadSchema)
	return admission, err == nil
}
