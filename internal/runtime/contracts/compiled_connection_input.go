package contracts

import (
	"errors"
	"fmt"
	"sort"
)

var ErrConnectionProjectionCollision = errors.New("connection projection conflicts with producer")
var errConnectionProducerUnbound = errors.New("connect producer event schema is unavailable")

// CompiledConnectionInput owns one edge's admitted ordinary instance policy
// and schema projection. Shared pins never select creation or key sources.
type CompiledConnectionInput struct {
	value *compiledConnectionInputValue
}

type compiledConnectionInputValue struct {
	mode           FlowInputResolutionMode
	evidence       FlowInputInstanceSourceTypeEvidence
	receiverSchema CompiledEventSchema
	receiverFlow   string
	receiverEvent  string
}

type CompiledConnectionInputs struct {
	value *compiledConnectionInputsValue
}

type compiledConnectionInputsValue struct {
	inputs map[FlowConnect]CompiledConnectionInput
	errors map[FlowConnect]error
}

type ConnectionInputPinProvider interface {
	FlowInputEventPin(flowID, event string) (CompiledFlowInputPin, bool)
}

func (p CompiledConnectionInput) Mode() FlowInputResolutionMode {
	if p.value == nil {
		return FlowInputResolutionModeNone
	}
	return p.value.mode
}

func (p CompiledConnectionInput) SourceEvidence() FlowInputInstanceSourceTypeEvidence {
	if p.value == nil {
		return FlowInputInstanceSourceTypeEvidence{}
	}
	evidence := p.value.evidence
	evidence.SourceType.Catalog = cloneTypeCatalogDocument(evidence.SourceType.Catalog)
	evidence.ReceiverType.Catalog = cloneTypeCatalogDocument(evidence.ReceiverType.Catalog)
	return evidence
}

func (p CompiledConnectionInput) ReceiverEventSchema() (CompiledEventSchema, bool) {
	if p.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.receiverSchema, p.value.receiverSchema.value != nil
}

func (b *WorkflowContractBundle) ConnectionInput(connect FlowConnect) (CompiledConnectionInput, bool, error) {
	if b == nil {
		return CompiledConnectionInput{}, false, fmt.Errorf("compiled connection input is unavailable")
	}
	return b.connectionInputs.Input(connect)
}

func (b *WorkflowContractBundle) ConnectionInputs() CompiledConnectionInputs {
	if b == nil {
		return CompiledConnectionInputs{}
	}
	return b.connectionInputs
}

func (p CompiledConnectionInputs) Input(connect FlowConnect) (CompiledConnectionInput, bool, error) {
	if p.value == nil {
		return CompiledConnectionInput{}, false, nil
	}
	connect = connect.normalized()
	if err := p.value.errors[connect]; err != nil {
		return CompiledConnectionInput{}, false, err
	}
	input, ok := p.value.inputs[connect]
	return input, ok, nil
}

// Schema imports may still await composition; every other bound policy failure
// must reject composition before the source becomes executable.
func (p CompiledConnectionInputs) ValidateBindings() error {
	if p.value == nil {
		return nil
	}
	var messages []string
	for connect, err := range p.value.errors {
		if !errors.Is(err, errConnectionProducerUnbound) {
			messages = append(messages, fmt.Sprintf("%s: %v", connect.AuthoredLocation(), err))
		}
	}
	sort.Strings(messages)
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("compiled connection input binding failed: %v", messages)
}

// CompileConnectionInputs freezes the admitted source after any provider schema
// binding. Binding changes schema evidence, never the authored edge policy.
func CompileConnectionInputs(bundle *WorkflowContractBundle, pins ConnectionInputPinProvider) CompiledConnectionInputs {
	value := &compiledConnectionInputsValue{inputs: make(map[FlowConnect]CompiledConnectionInput), errors: make(map[FlowConnect]error)}
	if bundle == nil || pins == nil {
		return CompiledConnectionInputs{value: value}
	}
	for _, connect := range bundle.Semantics.CompositionConnects {
		connect = connect.normalized()
		if connect.Resolution == FlowInputResolutionModeNone && connect.KeyFrom == "" {
			continue
		}
		input, err := compileConnectionInput(bundle, pins, connect)
		if err != nil {
			value.errors[connect] = err
			continue
		}
		value.inputs[connect] = input
	}
	return CompiledConnectionInputs{value: value}
}

func compileConnectionInput(bundle *WorkflowContractBundle, pins ConnectionInputPinProvider, connect FlowConnect) (CompiledConnectionInput, error) {
	if !ordinaryInstanceResolution(connect.Resolution) {
		return CompiledConnectionInput{}, fmt.Errorf("connect.resolution must be create, select or select-or-create")
	}
	receiverFlow := connectEndpointFlowID(connect.OwnerFlowPath, connect.To)
	receiverEvent := connect.Event
	if connect.Rename != "" {
		receiverEvent = connect.Rename
	}
	receiverEvent = packageEndpointLocalEvent(bundle, receiverFlow, receiverEvent, true)
	pin, ok := pins.FlowInputEventPin(receiverFlow, receiverEvent)
	if !ok {
		return CompiledConnectionInput{}, fmt.Errorf("connect receiver input pin is unavailable")
	}
	if !pin.Resolution().Empty() {
		return CompiledConnectionInput{}, fmt.Errorf("connect.resolution cannot override a fan-in or reply input policy")
	}
	if connect.Resolution == FlowInputResolutionModeSelect && len(pin.Initialization().Bindings()) != 0 {
		return CompiledConnectionInput{}, fmt.Errorf("input initialize requires a creating connection, not resolution: select")
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlow)
	if err != nil {
		return CompiledConnectionInput{}, err
	}
	producer, ok := pin.ProducerEventSchema()
	if !ok {
		return CompiledConnectionInput{}, errConnectionProducerUnbound
	}
	evidence, err := bundle.ResolveFlowInputInstanceSourceType(nil, receiverFlow, connect, pin, instance)
	if err != nil {
		return CompiledConnectionInput{}, err
	}
	receiver, err := connectionReceiverEventSchema(pin.EventType(), producer, evidence)
	if err != nil {
		return CompiledConnectionInput{}, err
	}
	return CompiledConnectionInput{value: &compiledConnectionInputValue{
		mode: connect.Resolution, evidence: evidence, receiverSchema: receiver,
		receiverFlow: receiverFlow, receiverEvent: receiverEvent,
	}}, nil
}

func connectionReceiverEventSchema(event string, producer CompiledEventSchema, evidence FlowInputInstanceSourceTypeEvidence) (CompiledEventSchema, error) {
	if !evidence.Source.RequiresDeliveryProjection() {
		return producer, nil
	}
	field := evidence.Field.Path()
	for _, declared := range producer.Fields() {
		if declared.Name() == field {
			return CompiledEventSchema{}, fmt.Errorf("%w: producer event %s field %s conflicts with connection projection %s", ErrConnectionProjectionCollision, event, field, evidence.Source.Path)
		}
	}
	fieldSchema, _ := eventSchemaForTypeRef(evidence.SourceType.Type, evidence.SourceType.Catalog, map[string]struct{}{})
	return producer.withRequiredField(field, evidence.SourceType.Type, fieldSchema)
}

func (p CompiledConnectionInputs) ReceiverEventSchema(flowID, eventType string) (CompiledEventSchema, bool, error) {
	if p.value == nil {
		return CompiledEventSchema{}, false, nil
	}
	var schema CompiledEventSchema
	for _, input := range p.value.inputs {
		if input.value.receiverFlow != flowID || input.value.receiverEvent != eventType {
			continue
		}
		candidate := input.value.receiverSchema
		if schema.value != nil && schema.AcceptanceSchemaDigest() != candidate.AcceptanceSchemaDigest() {
			return CompiledEventSchema{}, false, fmt.Errorf("receiver %s event %s has conflicting connection acceptance schemas", flowID, eventType)
		}
		schema = candidate
	}
	return schema, schema.value != nil, nil
}
