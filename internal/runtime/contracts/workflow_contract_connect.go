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
	source              FlowInputPinSource
	resolution          FlowInputPinResolution
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
	resolution := pin.Resolution.clone()
	if err := validateCompiledFlowInputResolution(resolution); err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s resolution: %w", pin.Event, err)
	}
	provenance := compiledFlowPinProvenance(context, pin.sourceLine, pin.sourceCol)
	initialization, err := CompileReceiverInitialization(context.Configuration, pin.Initialize, context.EventSchema)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s: %w", pin.Event, err)
	}
	digest, err := compiledFlowPinDigest("input", context, pin.Event, FlowInputPinSourceCode(pin.Source), "", resolution, context.EventSchema, context.EventSchema, initialization)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compile input pin %s digest: %w", pin.Event, err)
	}
	storedContext := context
	storedContext.EventSchema = CompiledEventSchema{}
	return CompiledFlowInputPin{value: &compiledFlowInputPinValue{
		event: pin.Event, source: pin.Source, resolution: resolution,
		context: storedContext, producerEventSchema: context.EventSchema, receiverEventSchema: context.EventSchema,
		provenance: provenance, digest: digest, initialization: initialization,
	}}, nil
}

func validateAuthoredFlowInputPin(pin FlowInputEventPin) error {
	if len(pin.Initialize) > 0 && !pin.Resolution.Empty() {
		return fmt.Errorf("input initialize requires an ordinary creating connection, not a reply or fan-out pin policy")
	}
	event := pin.Event
	if event == "" || event != strings.TrimSpace(event) || !eventidentity.IsValidName(event) || strings.ContainsAny(event, "/*") {
		return fmt.Errorf("input pin event %q is not an exact local canonical event identity", event)
	}
	if !pin.Source.Valid() {
		return fmt.Errorf("input pin %s has invalid source %q", event, FlowInputPinSourceCode(pin.Source))
	}
	return nil
}

func validateCompiledFlowInputResolution(resolution FlowInputPinResolution) error {
	if resolution.Empty() {
		return nil
	}
	if !resolution.Mode.Valid() {
		return fmt.Errorf("mode is required")
	}
	for label, value := range map[string]string{
		"replies_to": resolution.RepliesTo, "correlation_key": resolution.CorrelationKey,
	} {
		if value != "" && value != strings.TrimSpace(value) {
			return fmt.Errorf("%s must be an exact value", label)
		}
	}
	if resolution.RepliesTo != "" && (!eventidentity.IsValidName(resolution.RepliesTo) || strings.ContainsAny(resolution.RepliesTo, "/*")) {
		return fmt.Errorf("replies_to %q must be an exact local event identity", resolution.RepliesTo)
	}
	switch resolution.Mode {
	case FlowInputResolutionModeCreate, FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate:
		return fmt.Errorf("input-pin resolution mode %s is not supported for ordinary instance selection", FlowInputResolutionModeCode(resolution.Mode))
	case FlowInputResolutionModeFanOut:
		if resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
			return fmt.Errorf("mode fan-out may only declare mode")
		}
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
func (p CompiledFlowInputPin) Source() FlowInputPinSource {
	if p.value == nil {
		return FlowInputPinSourceNone
	}
	return p.value.source
}
func (p CompiledFlowInputPin) Resolution() FlowInputPinResolution {
	if p.value == nil {
		return FlowInputPinResolution{}
	}
	return p.value.resolution.clone()
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
	value.digest, err = compiledFlowPinDigest("input", value.context, value.event, FlowInputPinSourceCode(value.source), "", value.resolution, schema, schema, value.initialization)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compile imported input pin %s digest: %w", value.event, err)
	}
	return CompiledFlowInputPin{value: &value}, nil
}

type compiledFlowOutputPinValue struct {
	event      string
	sink       FlowOutputSink
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
	digest, err := compiledFlowPinDigest("output", context, pin.Event, "", FlowOutputSinkCode(pin.Sink), FlowInputPinResolution{}, context.EventSchema, CompiledEventSchema{}, ReceiverInitialization{})
	if err != nil {
		return CompiledFlowOutputPin{}, fmt.Errorf("compile output pin %s digest: %w", pin.Event, err)
	}
	return CompiledFlowOutputPin{value: &compiledFlowOutputPinValue{
		event: pin.Event, sink: pin.Sink, context: context, provenance: provenance, digest: digest,
	}}, nil
}

func validateAuthoredFlowOutputPin(pin FlowOutputEventPin) error {
	event := pin.Event
	if event == "" || event != strings.TrimSpace(event) || !eventidentity.IsValidName(event) || strings.ContainsAny(event, "/*") {
		return fmt.Errorf("output pin event %q is not an exact local canonical event identity", event)
	}
	if !pin.Sink.Valid() {
		return fmt.Errorf("output pin %s has invalid sink %q", event, FlowOutputSinkCode(pin.Sink))
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
func (p CompiledFlowOutputPin) Sink() FlowOutputSink {
	if p.value == nil {
		return FlowOutputSinkNone
	}
	return p.value.sink
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
	value.digest, err = compiledFlowPinDigest("output", value.context, value.event, "", FlowOutputSinkCode(value.sink), FlowInputPinResolution{}, schema, CompiledEventSchema{}, ReceiverInitialization{})
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

func compiledFlowPinDigest(direction string, context FlowPinCompilationContext, event, source, sink string, resolution FlowInputPinResolution, producerSchema, receiverSchema CompiledEventSchema, initialization ReceiverInitialization) (string, error) {
	key, hasKey := producerSchema.BusinessKey()
	var initialize any
	if direction == "input" {
		initialize = initialization.semanticEvidence()
	}
	return canonicaljson.Hash(struct {
		Direction            string                 `json:"direction"`
		FlowPath             string                 `json:"flow_path"`
		Event                string                 `json:"event"`
		Source               string                 `json:"source,omitempty"`
		Sink                 string                 `json:"sink,omitempty"`
		Resolution           FlowInputPinResolution `json:"resolution"`
		EventSchemaName      string                 `json:"event_schema_name,omitempty"`
		EventSchemaDigest    string                 `json:"event_schema_digest,omitempty"`
		BusinessKeyField     string                 `json:"business_key_field,omitempty"`
		BusinessKeyType      string                 `json:"business_key_type,omitempty"`
		HasEventBusinessKey  bool                   `json:"has_event_business_key"`
		ReceiverSchemaDigest string                 `json:"receiver_schema_digest,omitempty"`
		Initialization       any                    `json:"initialization,omitempty"`
	}{
		Direction: direction, FlowPath: context.FlowPath, Event: event, Source: source, Sink: sink,
		Resolution: resolution, EventSchemaName: producerSchema.EventName(),
		EventSchemaDigest: producerSchema.AcceptanceSchemaDigest(),
		BusinessKeyField:  key.Field, BusinessKeyType: key.SemanticType, HasEventBusinessKey: hasKey,
		ReceiverSchemaDigest: receiverSchema.AcceptanceSchemaDigest(),
		Initialization:       initialize,
	})
}

func (p FlowInputEventPin) EventType() string {
	return p.Event
}

func (r FlowInputPinResolution) Empty() bool {
	r = r.normalized()
	return r.Mode == FlowInputResolutionModeNone &&
		r.RepliesTo == "" &&
		r.CorrelationKey == ""
}

func (r FlowInputPinResolution) normalized() FlowInputPinResolution {
	return FlowInputPinResolution{
		Mode:           r.Mode,
		RepliesTo:      r.RepliesTo,
		CorrelationKey: r.CorrelationKey,
	}
}

func (r FlowInputPinResolution) clone() FlowInputPinResolution {
	return r
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
