package contracts

import (
	"fmt"
	"sort"
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
	projection          CompiledFlowInputProjection
	provenance          CompiledFlowPinProvenance
	digest              string
}

type compiledFlowInputProjectionValue struct {
	field        string
	source       FlowInputInstanceSource
	sourceType   string
	receiverType string
}

// CompiledFlowInputProjection is the immutable receiver-owned field injected
// during delivery for an intrinsic template-instance source.
type CompiledFlowInputProjection struct {
	value *compiledFlowInputProjectionValue
}

type CompiledFlowInputProjectionReadback struct {
	Field        string
	SourceKind   string
	SourcePath   string
	SourceType   string
	ReceiverType string
}

func (p CompiledFlowInputProjection) Empty() bool { return p.value == nil }
func (p CompiledFlowInputProjection) Readback() CompiledFlowInputProjectionReadback {
	if p.value == nil {
		return CompiledFlowInputProjectionReadback{}
	}
	return CompiledFlowInputProjectionReadback{
		Field: p.value.field, SourceKind: string(p.value.source.Kind), SourcePath: p.value.source.Path,
		SourceType: p.value.sourceType, ReceiverType: p.value.receiverType,
	}
}

// FlowPinCompilationContext supplies the already-admitted scope and event
// schema facts required to compile a pin independently of any connect edge.
type FlowPinCompilationContext struct {
	FlowID      string
	FlowPath    string
	SourceFile  string
	EventSchema CompiledEventSchema
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
	sort.Strings(resolution.DedupBy)
	if err := validateCompiledFlowInputResolution(resolution); err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s resolution: %w", pin.Event, err)
	}
	provenance := compiledFlowPinProvenance(context, pin.sourceLine, pin.sourceCol)
	digest, err := compiledFlowPinDigest("input", context, pin.Event, FlowInputPinSourceCode(pin.Source), "", resolution, context.EventSchema, context.EventSchema, CompiledFlowInputProjection{})
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("compile input pin %s digest: %w", pin.Event, err)
	}
	storedContext := context
	storedContext.EventSchema = CompiledEventSchema{}
	return CompiledFlowInputPin{value: &compiledFlowInputPinValue{
		event: pin.Event, source: pin.Source, resolution: resolution,
		context: storedContext, producerEventSchema: context.EventSchema, receiverEventSchema: context.EventSchema,
		provenance: provenance, digest: digest,
	}}, nil
}

func validateAuthoredFlowInputPin(pin FlowInputEventPin) error {
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
		"from": resolution.From, "aggregation": resolution.Aggregation, "window": resolution.Window,
		"singleton": resolution.Singleton, "replies_to": resolution.RepliesTo, "correlation_key": resolution.CorrelationKey,
	} {
		if value != "" && value != strings.TrimSpace(value) {
			return fmt.Errorf("%s must be an exact value", label)
		}
	}
	for index, field := range resolution.DedupBy {
		if field == "" || field != strings.TrimSpace(field) {
			return fmt.Errorf("dedup_by field %q must be an exact non-empty scalar", field)
		}
		if index > 0 && resolution.DedupBy[index-1] == field {
			return fmt.Errorf("dedup_by field %q is declared more than once", field)
		}
	}
	if resolution.Aggregation != "" && resolution.Aggregation != "stream" && resolution.Aggregation != "barrier" {
		return fmt.Errorf("aggregation must be stream or barrier")
	}
	if resolution.RepliesTo != "" && (!eventidentity.IsValidName(resolution.RepliesTo) || strings.ContainsAny(resolution.RepliesTo, "/*")) {
		return fmt.Errorf("replies_to %q must be an exact local event identity", resolution.RepliesTo)
	}
	switch resolution.Mode {
	case FlowInputResolutionModeCreate, FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate:
		if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
			return fmt.Errorf("mode %s may only declare mode and from", FlowInputResolutionModeCode(resolution.Mode))
		}
	case FlowInputResolutionModeFanIn:
		if resolution.From != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
			return fmt.Errorf("mode fan-in may only declare mode, aggregation, window, dedup_by, and singleton")
		}
	case FlowInputResolutionModeFanOut:
		if resolution.From != "" || resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
			return fmt.Errorf("mode fan-out may only declare mode")
		}
	case FlowInputResolutionModeReply:
		if resolution.From != "" || resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
			return fmt.Errorf("mode reply may only declare mode, replies_to, and correlation_key")
		}
	}
	return nil
}

func (p CompiledFlowInputPin) Empty() bool { return p.value == nil }
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

// ProducerEventSchema returns the event declaration before receiver-owned
// projection fields are added.
func (p CompiledFlowInputPin) ProducerEventSchema() (CompiledEventSchema, bool) {
	if p.value == nil || p.value.producerEventSchema.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.producerEventSchema, true
}

// ReceiverEventSchema returns the acceptance schema after receiver-owned
// projection fields are added.
func (p CompiledFlowInputPin) ReceiverEventSchema() (CompiledEventSchema, bool) {
	if p.value == nil || p.value.receiverEventSchema.value == nil {
		return CompiledEventSchema{}, false
	}
	return p.value.receiverEventSchema, true
}

// ProducerProjectionConflict reports whether a receiver-owned projection
// would overwrite a field declared by the producer.
func (p CompiledFlowInputPin) ProducerProjectionConflict() error {
	if p.value == nil || p.value.producerEventSchema.value == nil || p.value.projection.Empty() {
		return nil
	}
	return validateFlowInputProjectionAgainstProducerSchema(p.value.event, p.value.producerEventSchema, p.value.projection)
}

func (p CompiledFlowInputPin) Projection() (CompiledFlowInputProjectionReadback, bool) {
	if p.value == nil || p.value.projection.Empty() {
		return CompiledFlowInputProjectionReadback{}, false
	}
	return p.value.projection.Readback(), true
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
	if err := validateFlowInputProjectionAgainstProducerSchema(p.value.event, schema, p.value.projection); err != nil {
		return CompiledFlowInputPin{}, err
	}
	receiverSchema, err := deriveFlowInputReceiverEventSchema(schema, p.value.projection)
	if err != nil {
		return CompiledFlowInputPin{}, fmt.Errorf("input pin %s imported receiver projection: %w", p.value.event, err)
	}
	value := *p.value
	value.producerEventSchema = schema
	value.receiverEventSchema = receiverSchema
	value.digest, err = compiledFlowPinDigest("input", value.context, value.event, FlowInputPinSourceCode(value.source), "", value.resolution, schema, receiverSchema, value.projection)
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
	digest, err := compiledFlowPinDigest("output", context, pin.Event, "", FlowOutputSinkCode(pin.Sink), FlowInputPinResolution{}, context.EventSchema, CompiledEventSchema{}, CompiledFlowInputProjection{})
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

func compiledFlowPinDigest(direction string, context FlowPinCompilationContext, event, source, sink string, resolution FlowInputPinResolution, producerSchema, receiverSchema CompiledEventSchema, projection CompiledFlowInputProjection) (string, error) {
	key, hasKey := producerSchema.BusinessKey()
	projectionReadback := projection.Readback()
	return canonicaljson.Hash(struct {
		Direction            string                              `json:"direction"`
		FlowPath             string                              `json:"flow_path"`
		Event                string                              `json:"event"`
		Source               string                              `json:"source,omitempty"`
		Sink                 string                              `json:"sink,omitempty"`
		Resolution           FlowInputPinResolution              `json:"resolution"`
		EventSchemaName      string                              `json:"event_schema_name,omitempty"`
		EventSchemaDigest    string                              `json:"event_schema_digest,omitempty"`
		BusinessKeyField     string                              `json:"business_key_field,omitempty"`
		BusinessKeyType      string                              `json:"business_key_type,omitempty"`
		HasEventBusinessKey  bool                                `json:"has_event_business_key"`
		ReceiverSchemaDigest string                              `json:"receiver_schema_digest,omitempty"`
		Projection           CompiledFlowInputProjectionReadback `json:"projection,omitempty"`
	}{
		Direction: direction, FlowPath: context.FlowPath, Event: event, Source: source, Sink: sink,
		Resolution: resolution, EventSchemaName: producerSchema.EventName(),
		EventSchemaDigest: producerSchema.AcceptanceSchemaDigest(),
		BusinessKeyField:  key.Field, BusinessKeyType: key.SemanticType, HasEventBusinessKey: hasKey,
		ReceiverSchemaDigest: receiverSchema.AcceptanceSchemaDigest(), Projection: projectionReadback,
	})
}

// CompiledFlowEntityPermissions is a canonical immutable field set. The YAML
// order cannot affect semantic identity and returned fields are defensive
// copies.
type CompiledFlowEntityPermissions struct{ fields []string }

func CompileFlowEntityPermissions(fields []string) (CompiledFlowEntityPermissions, error) {
	out := append([]string(nil), fields...)
	for _, field := range out {
		if field == "" || field != strings.TrimSpace(field) {
			return CompiledFlowEntityPermissions{}, fmt.Errorf("entity permission field %q must be an exact non-empty scalar", field)
		}
	}
	sort.Strings(out)
	for index := 1; index < len(out); index++ {
		if out[index-1] == out[index] {
			return CompiledFlowEntityPermissions{}, fmt.Errorf("entity permission field %q is declared more than once", out[index])
		}
	}
	return CompiledFlowEntityPermissions{fields: out}, nil
}

func (p CompiledFlowEntityPermissions) Fields() []string {
	return append([]string(nil), p.fields...)
}

func (p FlowInputEventPin) EventType() string {
	return p.Event
}

func (r FlowInputPinResolution) Empty() bool {
	r = r.normalized()
	return r.Mode == FlowInputResolutionModeNone &&
		r.From == "" &&
		r.Aggregation == "" &&
		r.Window == "" &&
		len(r.DedupBy) == 0 &&
		r.Singleton == "" &&
		r.RepliesTo == "" &&
		r.CorrelationKey == ""
}

func (r FlowInputPinResolution) normalized() FlowInputPinResolution {
	return FlowInputPinResolution{
		Mode:           r.Mode,
		From:           r.From,
		Aggregation:    r.Aggregation,
		Window:         r.Window,
		DedupBy:        append([]string(nil), r.DedupBy...),
		Singleton:      r.Singleton,
		RepliesTo:      r.RepliesTo,
		CorrelationKey: r.CorrelationKey,
	}
}

func (r FlowInputPinResolution) clone() FlowInputPinResolution {
	out := r
	out.DedupBy = append([]string(nil), r.DedupBy...)
	return out
}

func (p FlowOutputEventPin) EventType() string {
	return p.Event
}

func (c FlowPackageConnect) WithPackageKey(packageKey string) FlowPackageConnect {
	out := c.normalized()
	out.PackageKey = strings.TrimSpace(packageKey)
	return out
}

func (c FlowPackageConnect) WithPackageSource(packageKey, sourceFile string) FlowPackageConnect {
	out := c.WithPackageKey(packageKey)
	out.SourceFile = strings.TrimSpace(sourceFile)
	return out
}

func (c FlowPackageConnect) AuthoredLocation() string {
	file := strings.TrimSpace(c.SourceFile)
	if file == "" || c.SourceLine <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", file, c.SourceLine)
}

func (c FlowPackageConnect) normalized() FlowPackageConnect {
	return FlowPackageConnect{
		PackageKey: strings.TrimSpace(c.PackageKey),
		SourceFile: strings.TrimSpace(c.SourceFile),
		SourceLine: c.SourceLine,
		Event:      c.Event,
		From:       c.From,
		To:         c.To,
		Rename:     c.Rename,
	}
}

func compileFlowInputPins(bundle *WorkflowContractBundle, flowID, flowPath, sourceFile string, in []FlowInputEventPin) ([]CompiledFlowInputPin, error) {
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
		pin, err = compileIntrinsicFlowInputProjection(bundle, flowID, pin)
		if err != nil {
			return nil, fmt.Errorf("compile input pin %s receiver projection: %w", authored.Event, err)
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

func compileIntrinsicFlowInputProjection(bundle *WorkflowContractBundle, flowID string, pin CompiledFlowInputPin) (CompiledFlowInputPin, error) {
	if bundle == nil || pin.Empty() {
		return pin, nil
	}
	from := strings.TrimSpace(pin.Resolution().From)
	if from != FlowInputInstanceSourceGeneratedUUIDPath && from != FlowInputInstanceSourceEventIDPath {
		return pin, nil
	}
	instance, err := bundle.ResolveFlowTemplateInstance(flowID)
	if err != nil {
		return CompiledFlowInputPin{}, err
	}
	evidence, err := bundle.resolveFlowInputInstanceSourceType(bundle, flowID, pin, instance, false)
	if err != nil {
		return CompiledFlowInputPin{}, err
	}
	projection := CompiledFlowInputProjection{value: &compiledFlowInputProjectionValue{
		field: evidence.Field.Path(), source: evidence.Source,
		sourceType: strings.TrimSpace(evidence.SourceType.Type), receiverType: strings.TrimSpace(evidence.ReceiverType.Type),
	}}
	value := *pin.value
	value.projection = projection
	producerSchema, hasProducerSchema := pin.ProducerEventSchema()
	if hasProducerSchema {
		value.receiverEventSchema, err = deriveFlowInputReceiverEventSchema(producerSchema, projection)
		if err != nil {
			return CompiledFlowInputPin{}, err
		}
	}
	value.digest, err = compiledFlowPinDigest("input", value.context, value.event, FlowInputPinSourceCode(value.source), "", value.resolution, producerSchema, value.receiverEventSchema, projection)
	if err != nil {
		return CompiledFlowInputPin{}, err
	}
	return CompiledFlowInputPin{value: &value}, nil
}

func validateFlowInputProjectionAgainstProducerSchema(event string, schema CompiledEventSchema, projection CompiledFlowInputProjection) error {
	if schema.value == nil || projection.Empty() {
		return nil
	}
	field := projection.Readback().Field
	for _, declared := range schema.Fields() {
		if declared.Name() == field {
			return fmt.Errorf("producer event %s field %s conflicts with receiver-owned resolution projection %s", event, field, projection.Readback().SourcePath)
		}
	}
	return nil
}

func deriveFlowInputReceiverEventSchema(producerSchema CompiledEventSchema, projection CompiledFlowInputProjection) (CompiledEventSchema, error) {
	if producerSchema.value == nil || projection.Empty() {
		return producerSchema, nil
	}
	readback := projection.Readback()
	fieldSchema, _ := eventSchemaForTypeRef(readback.SourceType, TypeCatalogDocument{}, map[string]struct{}{})
	return producerSchema.withRequiredField(readback.Field, fieldSchema)
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

func cloneFlowPackageConnects(in []FlowPackageConnect) []FlowPackageConnect {
	out := make([]FlowPackageConnect, 0, len(in))
	for _, connect := range in {
		normalized := connect.normalized()
		out = append(out, normalized)
	}
	return out
}
