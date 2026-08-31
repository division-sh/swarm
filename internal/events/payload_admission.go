package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

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
	bundleSource string
	flowID       string
	eventKey     string
	schemaDigest string
	schemaClass  PayloadSchemaClass
}

type PayloadSchemaBindingInput struct {
	BundleHash   string
	BundleSource string
	FlowID       string
	EventKey     string
	SchemaDigest string
	SchemaClass  PayloadSchemaClass
}

func NewPayloadSchemaBinding(input PayloadSchemaBindingInput) (PayloadSchemaBinding, error) {
	binding := PayloadSchemaBinding{
		bundleHash: strings.TrimSpace(input.BundleHash), bundleSource: strings.TrimSpace(input.BundleSource),
		flowID: strings.TrimSpace(input.FlowID), eventKey: strings.TrimSpace(input.EventKey),
		schemaDigest: strings.TrimSpace(input.SchemaDigest), schemaClass: PayloadSchemaClass(strings.TrimSpace(string(input.SchemaClass))),
	}
	if err := binding.Validate(); err != nil {
		return PayloadSchemaBinding{}, err
	}
	return binding, nil
}

func RestorePayloadSchemaBinding(input PayloadSchemaBindingInput) (PayloadSchemaBinding, error) {
	for name, value := range map[string]string{
		"bundle_hash": input.BundleHash, "bundle_source": input.BundleSource, "flow_id": input.FlowID,
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
	if b.bundleSource != "persisted" && b.bundleSource != "ephemeral" {
		return fmt.Errorf("payload schema bundle_source must be persisted or ephemeral")
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
	return nil
}

func (b PayloadSchemaBinding) BundleHash() string              { return b.bundleHash }
func (b PayloadSchemaBinding) BundleSource() string            { return b.bundleSource }
func (b PayloadSchemaBinding) FlowID() string                  { return b.flowID }
func (b PayloadSchemaBinding) EventKey() string                { return b.eventKey }
func (b PayloadSchemaBinding) SchemaDigest() string            { return b.schemaDigest }
func (b PayloadSchemaBinding) SchemaClass() PayloadSchemaClass { return b.schemaClass }

func (b PayloadSchemaBinding) Equal(other PayloadSchemaBinding) bool {
	return b.bundleHash == other.bundleHash && b.bundleSource == other.bundleSource && b.flowID == other.flowID &&
		b.eventKey == other.eventKey && b.schemaDigest == other.schemaDigest && b.schemaClass == other.schemaClass
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
	if payloadValueContainsNull(object) {
		return PayloadAdmission{}, fmt.Errorf("admitted event payload cannot contain null")
	}
	if err := binding.Validate(); err != nil {
		return PayloadAdmission{}, err
	}
	return PayloadAdmission{payload: bytes.Clone(payload), binding: binding}, nil
}

func payloadValueContainsNull(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case map[string]any:
		for _, item := range typed {
			if payloadValueContainsNull(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if payloadValueContainsNull(item) {
				return true
			}
		}
	}
	return false
}

func (a PayloadAdmission) Payload() []byte               { return bytes.Clone(a.payload) }
func (a PayloadAdmission) Binding() PayloadSchemaBinding { return a.binding }
func (a PayloadAdmission) Valid() bool                   { return json.Valid(a.payload) && a.binding.Validate() == nil }

// ApplyPayloadAdmission replaces the pre-admission payload exactly once.
// Reapplication is accepted only when both bytes and binding are identical.
func ApplyPayloadAdmission(event Event, admission PayloadAdmission) (Event, error) {
	if !admission.Valid() {
		return Event{}, fmt.Errorf("payload admission is invalid")
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
