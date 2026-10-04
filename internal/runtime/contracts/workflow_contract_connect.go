package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func (p FlowInputPins) EventTypes() []string {
	out := make([]string, 0, len(p.EventPins))
	for _, pin := range p.EventPins {
		out = append(out, pin.EventType())
	}
	return out
}

func (p FlowOutputPins) EventTypes() []string {
	out := make([]string, 0, len(p.EventPins))
	for _, pin := range p.EventPins {
		out = append(out, pin.EventType())
	}
	return out
}

type compiledFlowInputPinValue struct {
	event               string
	context             FlowPinCompilationContext
	producerEventSchema CompiledEventSchema
	receiverEventSchema CompiledEventSchema
	initialization      ReceiverInitialization
	provenance          CompiledFlowPinProvenance
	digest              string
}

// FlowPinCompilationContext supplies the already-admitted scope and event
// schema facts required to compile a pin independently of any connect edge.
type FlowPinCompilationContext struct {
	FlowID        string
	FlowPath      string
	SourceFile    string
	EventSchema   CompiledEventSchema
	Configuration ReceiverConfiguration
}

// CompiledFlowPinProvenance is diagnostic source evidence. It is immutable
// and deliberately separate from the semantic digest.
type CompiledFlowPinProvenance struct {
	FlowID       string
	FlowPath     string
	SourceFile   string
	SourceLine   int
	SourceColumn int
}

// CompiledFlowInputPin is the immutable semantic owner for one admitted input
// pin. Authored DTOs never cross this boundary into runtime consumers.
type CompiledFlowInputPin struct{ value *compiledFlowInputPinValue }

func CompileFlowInputPin(context FlowPinCompilationContext, pin FlowInputEventPin) (CompiledFlowInputPin, error) {
	if err := validateFlowPinCompilationContext(context); err != nil {
		return CompiledFlowInputPin{}, err
	}
	if err := validateAuthoredFlowInputPin(pin); err != nil {
		return CompiledFlowInputPin{}, err
	}
	provenance := compiledFlowPinProvenance(context, pin.sourceLine, pin.sourceCol)
	initialization, err := CompileReceiverInitialization(context.Configuration, pin.Initialize, context.EventSchema)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s: %w", pin.Event, err)
	}
	digest, err := compiledFlowPinDigest("input", context, pin.Event, context.EventSchema, context.EventSchema, initialization)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compile input pin %s digest: %w", pin.Event, err)
	}
	storedContext := context
	storedContext.EventSchema = CompiledEventSchema{}
	return CompiledFlowInputPin{value: &compiledFlowInputPinValue{
		event: pin.Event,
		context: storedContext, producerEventSchema: context.EventSchema, receiverEventSchema: context.EventSchema,
		provenance: provenance, digest: digest, initialization: initialization,
	}}, nil
}

func validateAuthoredFlowInputPin(pin FlowInputEventPin) error {
	event := pin.Event
	if event == "" || event != strings.TrimSpace(event) || !eventidentity.IsValidName(event) || strings.ContainsAny(event, "/*") {
		return fmt.Errorf("input pin event %q is not an exact local canonical event identity", event)
	}
	return nil
}

func (p CompiledFlowInputPin) Empty() bool { return p.value == nil }

// QualifiedEventName projects only the scope and local event admitted with this
// pin; it never consults the mutable source tree or the producer's event name.
func (p CompiledFlowInputPin) QualifiedEventName() string {
	if p.value == nil {
		return ""
	}
	return compiledPinEventName(p.value.context.FlowPath, p.value.event)
}

func compiledPinEventName(flowPath, event string) string {
	if flowPath == "." {
		flowPath = ""
	}
	return eventidentity.ExternalizeForFlow(flowPath, []string{event}, event)
}

func (p CompiledFlowInputPin) EventType() string {
	if p.value == nil {
		return ""
	}
	return p.value.event
}
func (p CompiledFlowInputPin) FlowID() string {
	if p.value == nil {
		return ""
	}
	return p.value.context.FlowID
}

func (p CompiledFlowInputPin) FlowPath() string {
	if p.value == nil {
		return ""
	}
	return p.value.context.FlowPath
}

// ProducerEventSchema returns the shared interface's producer declaration.
func (p CompiledFlowInputPin) ProducerEventSchema() (CompiledEventSchema, bool) {
	if p.value == nil || p.value.producerEventSchema.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.producerEventSchema, true
}

// ReceiverEventSchema returns the shared interface schema. Ordinary intrinsic
// projection belongs to CompiledConnectionInput, not the shared pin.
func (p CompiledFlowInputPin) ReceiverEventSchema() (CompiledEventSchema, bool) {
	if p.value == nil || p.value.receiverEventSchema.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.receiverEventSchema, true
}

func (p CompiledFlowInputPin) Provenance() CompiledFlowPinProvenance {
	if p.value == nil {
		return CompiledFlowPinProvenance{}
	}
	return p.value.provenance
}

func (p CompiledFlowInputPin) Digest() string {
	if p.value == nil {
		return ""
	}
	return p.value.digest
}

// BindImportedEventSchema completes a schema-less compiled ingress pin from
// one admitted provider-owned schema. Existing schema evidence cannot be
// replaced, and the returned pin remains an immutable value.
func (p CompiledFlowInputPin) BindImportedEventSchema(schema CompiledEventSchema) (CompiledFlowInputPin, error) {
	if p.value == nil || schema.value == nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compiled input pin and imported event schema are required")
	}
	if p.value.producerEventSchema.value != nil || p.value.receiverEventSchema.value != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s already owns event schema evidence", p.value.event)
	}
	if schema.Classification() != CompiledEventSchemaImported || schema.EventName() != p.value.event {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s cannot bind imported event schema %s (%s)", p.value.event, schema.EventName(), schema.Classification())
	}
	value := *p.value
	value.producerEventSchema = schema
	value.receiverEventSchema = schema
	var err error
	value.initialization, err = CompileReceiverInitialization(value.initialization.configuration, value.initialization.Bindings(), schema)
	if err != nil {
		return CompiledFlowInputPin{}, err
	}
	value.digest, err = compiledFlowPinDigest("input", value.context, value.event, schema, schema, value.initialization)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compile imported input pin %s digest: %w", value.event, err)
	}
	return CompiledFlowInputPin{value: &value}, nil
}

type compiledFlowOutputPinValue struct {
	event      string
	context    FlowPinCompilationContext
	provenance CompiledFlowPinProvenance
	digest     string
}

// CompiledFlowOutputPin is the immutable semantic owner for one admitted
// output pin.
type CompiledFlowOutputPin struct{ value *compiledFlowOutputPinValue }

func CompileFlowOutputPin(context FlowPinCompilationContext, pin FlowOutputEventPin) (CompiledFlowOutputPin, error) {
	if err := validateFlowPinCompilationContext(context); err != nil {
		return CompiledFlowOutputPin{}, err
	}
	if err := validateAuthoredFlowOutputPin(pin); err != nil {
		return CompiledFlowOutputPin{}, err
	}
	provenance := compiledFlowPinProvenance(context, pin.sourceLine, pin.sourceCol)
	digest, err := compiledFlowPinDigest("output", context, pin.Event, context.EventSchema, CompiledEventSchema{}, ReceiverInitialization{})
	if err != nil {
		return CompiledFlowOutputPin{}, fmt.Errorf("compile output pin %s digest: %w", pin.Event, err)
	}
	return CompiledFlowOutputPin{value: &compiledFlowOutputPinValue{
		event: pin.Event, context: context, provenance: provenance, digest: digest,
	}}, nil
}

func validateAuthoredFlowOutputPin(pin FlowOutputEventPin) error {
	event := pin.Event
	if event == "" || event != strings.TrimSpace(event) || !eventidentity.IsValidName(event) || strings.ContainsAny(event, "/*") {
		return fmt.Errorf("output pin event %q is not an exact local canonical event identity", event)
	}
	return nil
}

func (p CompiledFlowOutputPin) Empty() bool { return p.value == nil }
func (p CompiledFlowOutputPin) EventType() string {
	if p.value == nil {
		return ""
	}
	return p.value.event
}
func (p CompiledFlowOutputPin) FlowID() string {
	if p.value == nil {
		return ""
	}
	return p.value.context.FlowID
}

func (p CompiledFlowOutputPin) FlowPath() string {
	if p.value == nil {
		return ""
	}
	return p.value.context.FlowPath
}

func (p CompiledFlowOutputPin) EventSchema() (CompiledEventSchema, bool) {
	if p.value == nil || p.value.context.EventSchema.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.context.EventSchema, true
}

// BindImportedEventSchema completes an output's schema evidence without
// changing the authored pin or granting producer authority to a schema import.
func (p CompiledFlowOutputPin) BindImportedEventSchema(schema CompiledEventSchema) (CompiledFlowOutputPin, error) {
	if p.value == nil || schema.value == nil {
		return CompiledFlowOutputPin{}, fmt.Errorf("compiled output pin and imported event schema are required")
	}
	if p.value.context.EventSchema.value != nil {
		return CompiledFlowOutputPin{}, fmt.Errorf("output pin %s already owns event schema evidence", p.value.event)
	}
	if schema.Classification() != CompiledEventSchemaImported || schema.EventName() != p.value.event {
		return CompiledFlowOutputPin{}, fmt.Errorf("output pin %s cannot bind imported event schema %s (%s)", p.value.event, schema.EventName(), schema.Classification())
	}
	value := *p.value
	value.context.EventSchema = schema
	var err error
	value.digest, err = compiledFlowPinDigest("output", value.context, value.event, schema, CompiledEventSchema{}, ReceiverInitialization{})
	if err != nil {
		return CompiledFlowOutputPin{}, fmt.Errorf("compile imported output pin %s digest: %w", value.event, err)
	}
	return CompiledFlowOutputPin{value: &value}, nil
}

func (p CompiledFlowOutputPin) Provenance() CompiledFlowPinProvenance {
	if p.value == nil {
		return CompiledFlowPinProvenance{}
	}
	return p.value.provenance
}

func (p CompiledFlowOutputPin) Digest() string {
	if p.value == nil {
		return ""
	}
	return p.value.digest
}

func validateFlowPinCompilationContext(context FlowPinCompilationContext) error {
	for label, value := range map[string]string{
		"flow id": context.FlowID, "flow path": context.FlowPath, "source file": context.SourceFile,
	} {
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("pin compilation %s %q must be exact", label, value)
		}
	}
	if strings.HasPrefix(context.FlowPath, "/") || strings.HasSuffix(context.FlowPath, "/") {
		return fmt.Errorf("pin compilation flow path %q must be root-relative", context.FlowPath)
	}
	return nil
}

func compiledFlowPinProvenance(context FlowPinCompilationContext, line, column int) CompiledFlowPinProvenance {
	return CompiledFlowPinProvenance{
		FlowID: context.FlowID, FlowPath: context.FlowPath, SourceFile: context.SourceFile,
		SourceLine: line, SourceColumn: column,
	}
}

func compiledFlowPinDigest(direction string, context FlowPinCompilationContext, event string, producerSchema, receiverSchema CompiledEventSchema, initialization ReceiverInitialization) (string, error) {
	key, hasKey := producerSchema.BusinessKey()
	var initialize any
	if direction == "input" {
		initialize = initialization.semanticEvidence()
	}
	return canonicaljson.Hash(struct {
		Direction            string                 `json:"direction"`
		FlowPath             string                 `json:"flow_path"`
		Event                string                 `json:"event"`
		EventSchemaName      string                 `json:"event_schema_name,omitempty"`
		EventSchemaDigest    string                 `json:"event_schema_digest,omitempty"`
		BusinessKeyField     string                 `json:"business_key_field,omitempty"`
		BusinessKeyType      string                 `json:"business_key_type,omitempty"`
		HasEventBusinessKey  bool                   `json:"has_event_business_key"`
		ReceiverSchemaDigest string                 `json:"receiver_schema_digest,omitempty"`
		Initialization       any                    `json:"initialization,omitempty"`
	}{
		Direction: direction, FlowPath: context.FlowPath, Event: event,
		EventSchemaName: producerSchema.EventName(),
		EventSchemaDigest: producerSchema.AcceptanceSchemaDigest(),
		BusinessKeyField:  key.Field, BusinessKeyType: key.SemanticType, HasEventBusinessKey: hasKey,
		ReceiverSchemaDigest: receiverSchema.AcceptanceSchemaDigest(),
		Initialization:       initialize,
	})
}

func (p FlowInputEventPin) EventType() string {
	return p.Event
}

func (p FlowOutputEventPin) EventType() string {
	return p.Event
}

func (c FlowConnect) WithOwnerFlowPath(flowPath string) FlowConnect {
	out := c.normalized()
	out.OwnerFlowPath = strings.TrimSpace(flowPath)
	return out
}

func (c FlowConnect) WithOwnerSource(flowPath, sourceFile string) FlowConnect {
	out := c.WithOwnerFlowPath(flowPath)
	out.SourceFile = strings.TrimSpace(sourceFile)
	return out
}

func (c FlowConnect) AuthoredLocation() string {
	file := strings.TrimSpace(c.SourceFile)
	if file == "" || c.SourceLine <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", file, c.SourceLine)
}

func (c FlowConnect) normalized() FlowConnect {
	return FlowConnect{
		OwnerFlowPath: strings.TrimSpace(c.OwnerFlowPath),
		SourceFile:    strings.TrimSpace(c.SourceFile),
		SourceLine:    c.SourceLine,
		Event:         c.Event,
		From:          c.From,
		To:            c.To,
		Rename:        c.Rename,
		Resolution:    c.Resolution,
		KeyFrom:       c.KeyFrom,
		RepliesTo:     c.RepliesTo,
		CorrelationKey: c.CorrelationKey,
	}
}

func compileFlowInputPins(bundle *WorkflowContractBundle, flowID, flowPath, sourceFile string, in []FlowInputEventPin) ([]CompiledFlowInputPin, error) {
	if bundle != nil {
		if _, err := bundle.ReceiverConfigurationForFlow(flowID); err != nil {
			return nil, err
		}
	}
	out := make([]CompiledFlowInputPin, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, authored := range in {
		if _, duplicate := seen[authored.Event]; duplicate {
			return nil, fmt.Errorf("input pin event %q is declared more than once", authored.Event)
		}
		seen[authored.Event] = struct{}{}
		context, err := flowInputPinCompilationContext(bundle, flowID, flowPath, sourceFile, authored.Event)
		if err != nil {
			return nil, err
		}
		pin, err := CompileFlowInputPin(context, authored)
		if err != nil {
			return nil, err
		}
		out = append(out, pin)
	}
	return out, nil
}

func flowInputPinCompilationContext(bundle *WorkflowContractBundle, flowID, flowPath, sourceFile, event string) (FlowPinCompilationContext, error) {
	context := FlowPinCompilationContext{FlowID: flowID, FlowPath: flowPath, SourceFile: sourceFile}
	if bundle == nil {
		return context, nil
	}
	configuration, err := bundle.ReceiverConfigurationForFlow(flowID)
	if err != nil {
		return FlowPinCompilationContext{}, err
	}
	context.Configuration = configuration
	producerFlowID, producerEvent := flowID, event
	if row, found, ambiguous := connectedEventSchemaOwnershipRow(bundle, flowID, event); ambiguous {
		return FlowPinCompilationContext{}, fmt.Errorf("input pin %s has ambiguous connected producer ownership", event)
	} else if found {
		producerFlowID, producerEvent = row.producerFlowID, row.producerName
	}
	compiled, ok, err := bundle.ResolveCompiledFlowEventSchema(producerFlowID, producerEvent)
	if err != nil {
		return FlowPinCompilationContext{}, err
	}
	if ok {
		context.EventSchema = compiled
	}
	return context, nil
}

func compileFlowOutputPins(bundle *WorkflowContractBundle, flowID, flowPath, sourceFile string, in []FlowOutputEventPin) ([]CompiledFlowOutputPin, error) {
	out := make([]CompiledFlowOutputPin, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, authored := range in {
		if _, duplicate := seen[authored.Event]; duplicate {
			return nil, fmt.Errorf("output pin event %q is declared more than once", authored.Event)
		}
		seen[authored.Event] = struct{}{}
		context, err := flowPinCompilationContext(bundle, flowID, flowPath, sourceFile, authored.Event)
		if err != nil {
			return nil, err
		}
		pin, err := CompileFlowOutputPin(context, authored)
		if err != nil {
			return nil, err
		}
		out = append(out, pin)
	}
	return out, nil
}

func flowPinCompilationContext(bundle *WorkflowContractBundle, flowID, flowPath, sourceFile, event string) (FlowPinCompilationContext, error) {
	context := FlowPinCompilationContext{FlowID: flowID, FlowPath: flowPath, SourceFile: sourceFile}
	if bundle == nil {
		return context, nil
	}
	compiled, ok, err := bundle.ResolveCompiledFlowEventSchema(flowID, event)
	if err != nil {
		return FlowPinCompilationContext{}, err
	}
	if ok {
		context.EventSchema = compiled
	}
	return context, nil
}

func cloneCompiledFlowInputPins(in []CompiledFlowInputPin) []CompiledFlowInputPin {
	return append([]CompiledFlowInputPin(nil), in...)
}

func cloneCompiledFlowOutputPins(in []CompiledFlowOutputPin) []CompiledFlowOutputPin {
	return append([]CompiledFlowOutputPin(nil), in...)
}

func cloneFlowConnects(in []FlowConnect) []FlowConnect {
	out := make([]FlowConnect, 0, len(in))
	for _, connect := range in {
		normalized := connect.normalized()
		out = append(out, normalized)
	}
	return out
}
