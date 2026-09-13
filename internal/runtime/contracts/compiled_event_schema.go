package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
)

// CompiledEventSchemaClassification distinguishes authored declarations from
// event shapes that cannot own importable business data.
type CompiledEventSchemaClassification string

const (
	CompiledEventSchemaAuthored  CompiledEventSchemaClassification = "authored"
	CompiledEventSchemaImported  CompiledEventSchemaClassification = "imported"
	CompiledEventSchemaGenerated CompiledEventSchemaClassification = "generated"
	CompiledEventSchemaPattern   CompiledEventSchemaClassification = "pattern"
	CompiledEventSchemaPlatform  CompiledEventSchemaClassification = "platform"
)

// CompiledEventSchemaProvider exposes admitted event semantics without
// exposing the YAML carrier or requiring consumers to resolve event identity.
type CompiledEventSchemaProvider interface {
	CompiledEventSchemas() ([]CompiledEventSchema, error)
}

// FlowEventStructuralTypeProvider exposes one effective, presence-aware event
// schema without exposing its JSON-schema readback projection.
type FlowEventStructuralTypeProvider interface {
	ResolveFlowEventStructuralType(flowID, eventType string) (ResolvedCatalogType, bool)
}

type compiledEventFieldValue struct {
	name       string
	schema     map[string]any
	isOptional bool
}

// CompiledEventField is one immutable field in an admitted event schema.
type CompiledEventField struct {
	value *compiledEventFieldValue
}

func (f CompiledEventField) Name() string {
	if f.value == nil {
		return ""
	}
	return f.value.name
}

func (f CompiledEventField) SemanticSchema() map[string]any {
	if f.value == nil {
		return nil
	}
	return cloneEventSchemaMap(f.value.schema)
}

func (f CompiledEventField) IsOptional() bool {
	return f.value != nil && f.value.isOptional
}

// CompiledEventBusinessKey is present only after admission proves that the
// field is required and has bool, number, or string value semantics.
type CompiledEventBusinessKey struct {
	Field        string
	SemanticType string
}

// CompiledEventSchemaSource is diagnostic provenance. It is deliberately not
// part of the declaration coordinate or acceptance-schema digest.
type CompiledEventSchemaSource struct {
	FlowPath string
	Layer    string
	File     string
}

type compiledEventSchemaValue struct {
	flowPath               string
	eventName              string
	classification         CompiledEventSchemaClassification
	fields                 []CompiledEventField
	businessKey            CompiledEventBusinessKey
	hasBusinessKey         bool
	acceptanceSchema       map[string]any
	canonicalSchema        []byte
	acceptanceSchemaDigest string
	source                 CompiledEventSchemaSource
	structuralType         ResolvedCatalogType
	readback               EventSchema
	declaration            EventCatalogEntry
}

// CompiledEventSchema is the immutable, admitted event-schema boundary used
// by downstream compilers. Authored maps and source paths never become
// identity inputs.
type CompiledEventSchema struct {
	value *compiledEventSchemaValue
}

func (s CompiledEventSchema) FlowPath() string {
	if s.value == nil {
		return ""
	}
	return s.value.flowPath
}

func (s CompiledEventSchema) EventName() string {
	if s.value == nil {
		return ""
	}
	return s.value.eventName
}

func (s CompiledEventSchema) Classification() CompiledEventSchemaClassification {
	if s.value == nil {
		return ""
	}
	return s.value.classification
}

func (s CompiledEventSchema) Importable() bool {
	return s.Classification() == CompiledEventSchemaAuthored || s.Classification() == CompiledEventSchemaImported
}

func (s CompiledEventSchema) Fields() []CompiledEventField {
	if s.value == nil {
		return nil
	}
	return append([]CompiledEventField(nil), s.value.fields...)
}

func (s CompiledEventSchema) BusinessKey() (CompiledEventBusinessKey, bool) {
	if s.value == nil || !s.value.hasBusinessKey {
		return CompiledEventBusinessKey{}, false
	}
	return s.value.businessKey, true
}

func (s CompiledEventSchema) AcceptanceSchema() map[string]any {
	if s.value == nil {
		return nil
	}
	return cloneEventSchemaMap(s.value.acceptanceSchema)
}

// EventSchema is the immutable owner's tooling/readback projection, including
// diagnostic descriptions and citations that do not belong in the digest.
func (s CompiledEventSchema) EventSchema() EventSchema {
	if s.value == nil {
		return EventSchema{}
	}
	out := s.value.readback
	out.Schema = cloneEventSchemaMap(out.Schema)
	out.CitationFields = make(map[string]CriteriaCitation, len(s.value.readback.CitationFields))
	for name, citation := range s.value.readback.CitationFields {
		citation.AllowedClasses = append([]string(nil), citation.AllowedClasses...)
		out.CitationFields[name] = citation
	}
	return out
}

func (s CompiledEventSchema) CanonicalAcceptanceSchema() []byte {
	if s.value == nil {
		return nil
	}
	return append([]byte(nil), s.value.canonicalSchema...)
}

func (s CompiledEventSchema) AcceptanceSchemaDigest() string {
	if s.value == nil {
		return ""
	}
	return s.value.acceptanceSchemaDigest
}

func (s CompiledEventSchema) Source() CompiledEventSchemaSource {
	if s.value == nil {
		return CompiledEventSchemaSource{}
	}
	return s.value.source
}

// StructuralType returns the recursive, presence-aware payload root admitted
// with this exact schema. It never exposes the owner's mutable backing value.
func (s CompiledEventSchema) StructuralType() (ResolvedCatalogType, bool) {
	if s.value == nil || s.value.structuralType.Kind == "" {
		return ResolvedCatalogType{}, false
	}
	return s.value.structuralType.Clone(), true
}

func (s CompiledEventSchema) StructuralField(name string) (ResolvedCatalogField, bool) {
	structural, ok := s.StructuralType()
	if !ok {
		return ResolvedCatalogField{}, false
	}
	return structural.Field(name)
}

func (s CompiledEventSchema) RequiredFieldNames() []string {
	structural, ok := s.StructuralType()
	if !ok {
		return nil
	}
	names := make([]string, 0, len(structural.Fields))
	for _, field := range structural.Fields {
		if name := strings.TrimSpace(field.Name); name != "" && !field.IsOptional {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (s CompiledEventSchema) withRequiredField(name, typeRef string, fieldSchema map[string]any) (CompiledEventSchema, error) {
	name = strings.TrimSpace(name)
	if s.value == nil {
		return CompiledEventSchema{}, fmt.Errorf("receiver event schema is unavailable")
	}
	if name == "" || strings.Contains(name, ".") {
		return CompiledEventSchema{}, fmt.Errorf("receiver projection field %q must be one exact top-level field", name)
	}
	acceptanceSchema := s.AcceptanceSchema()
	properties, _ := acceptanceSchema["properties"].(map[string]any)
	if properties == nil {
		properties = map[string]any{}
		acceptanceSchema["properties"] = properties
	}
	if _, exists := properties[name]; exists {
		// The compiled projection remains explicit so edge validation can report
		// the producer/receiver ownership collision through the supported verify
		// surface instead of failing before a report can be produced.
		return s, nil
	}
	properties[name] = cloneEventSchemaMap(fieldSchema)
	required := compiledEventRequiredFields(acceptanceSchema)
	required[name] = struct{}{}
	requiredNames := make([]string, 0, len(required))
	for field := range required {
		if field != "" {
			requiredNames = append(requiredNames, field)
		}
	}
	sort.Strings(requiredNames)
	acceptanceSchema["required"] = requiredNames
	canonicalSchema, err := canonicaljson.Bytes(acceptanceSchema)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("canonical receiver acceptance schema: %w", err)
	}
	fields := append([]CompiledEventField(nil), s.value.fields...)
	fields = append(fields, CompiledEventField{value: &compiledEventFieldValue{
		name: name, schema: cloneEventSchemaMap(fieldSchema),
	}})
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name() < fields[j].Name() })
	value := *s.value
	value.fields = fields
	value.acceptanceSchema = cloneEventSchemaMap(acceptanceSchema)
	value.readback = s.EventSchema()
	readbackProperties, _ := value.readback.Schema["properties"].(map[string]any)
	if readbackProperties == nil {
		readbackProperties = map[string]any{}
		value.readback.Schema["properties"] = readbackProperties
	}
	readbackProperties[name] = cloneEventSchemaMap(fieldSchema)
	value.readback.Schema["required"] = append([]string(nil), requiredNames...)
	value.canonicalSchema = append([]byte(nil), canonicalSchema...)
	value.acceptanceSchemaDigest = canonicaljson.HashBytes(canonicalSchema)
	structuralType := s.value.structuralType.Clone()
	fieldType, err := ResolveJSONSchemaStructuralType(fieldSchema, "event."+value.acceptanceSchemaDigest+"."+name)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("receiver structural schema: %w", err)
	}
	structuralType.Fields = append(structuralType.Fields, ResolvedCatalogField{
		Name:    name,
		TypeRef: strings.TrimSpace(typeRef),
		Type:    fieldType,
	})
	sort.Slice(structuralType.Fields, func(i, j int) bool { return structuralType.Fields[i].Name < structuralType.Fields[j].Name })
	value.structuralType = structuralType
	value.declaration = cloneEventCatalogEntry(s.value.declaration)
	value.declaration.Payload.Properties[name] = EventFieldSpec{Type: typeRef}
	value.declaration.Payload.Required = normalizeStrings(append(value.declaration.Payload.Required, name))
	return CompiledEventSchema{value: &value}, nil
}

var _ CompiledEventSchemaProvider = (*WorkflowContractBundle)(nil)

type compiledFlowEventSchemas struct {
	path        string
	template    bool
	descendants []string
	bindings    map[string]CompiledEventSchema
}

// ResolveCompiledFlowEventSchema returns the immutable producer declaration
// for one exact flow-local event. It never rebuilds evidence from reader-side
// authored maps.
func (b *WorkflowContractBundle) ResolveCompiledFlowEventSchema(flowID, eventType string) (CompiledEventSchema, bool, error) {
	if b == nil {
		return CompiledEventSchema{}, false, nil
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		flowID = "."
	}
	bindings, exists := b.compiledEventSchemas[flowID]
	if !exists {
		return CompiledEventSchema{}, false, nil
	}
	name := eventidentity.Normalize(eventType)
	compiled, ok := bindings.bindings[name]
	if !ok && bindings.template && strings.HasPrefix(name, bindings.path+"/") {
		remainder := strings.TrimPrefix(name, bindings.path+"/")
		for _, descendant := range bindings.descendants {
			if remainder == descendant || strings.HasPrefix(remainder, descendant+"/") {
				return CompiledEventSchema{}, false, nil
			}
		}
		// Concrete template spellings are readback projections of this
		// declaration, never another declaration or receiver authority.
		if strings.Contains(remainder, "/") {
			compiled, ok = bindings.bindings[eventidentity.LeafName(remainder)]
		}
	}
	return compiled, ok, nil
}

// compileEventSchemaBindings admits producer declarations before pins capture
// them. Later lookups may resolve a name within this exact scope, but cannot
// recompile a declaration from a mutable catalog or choose a foreign owner.
func (b *WorkflowContractBundle) compileEventSchemaBindings() error {
	b.compiledEventSchemas = nil
	bindings := make(map[string]compiledFlowEventSchemas)
	views := b.FlowViews()
	for _, view := range views {
		scope := compiledFlowEventSchemas{path: view.Path, template: view.Schema.Mode == "template", bindings: make(map[string]CompiledEventSchema)}
		for _, descendant := range views {
			if strings.HasPrefix(descendant.Path, view.Path+"/") {
				scope.descendants = append(scope.descendants, strings.TrimPrefix(descendant.Path, view.Path+"/"))
			}
		}
		bindings[view.Paths.FlowPath] = scope
	}
	records := b.canonicalCurrentEventDeclarationRecords()
	generated := b.generatedActivityDeclarationRecords()
	authoredCount := len(records)
	records = append(records, generated...)
	for i, record := range records {
		compiled, ok, err := b.compileCurrentEventDeclaration(record.flowPath, record.layer, record.sourceFile,
			record.localName, record.qualifiedName, record.entry, record.types)
		if eventidentity.IsCanonicalPattern(record.localName) {
			compiled, err = newCompiledEventSchema(record.flowPath, record.qualifiedName, record.entry, record.types, "",
				CompiledEventSchemaSource{FlowPath: record.flowPath, Layer: record.layer, File: record.sourceFile})
			ok = err == nil
			if ok {
				compiled.value.classification = CompiledEventSchemaPattern
			}
		}
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if i >= authoredCount {
			compiled.value.classification = CompiledEventSchemaGenerated
		}
		flow := compiled.FlowPath()
		scope, exists := bindings[flow]
		if !exists {
			return fmt.Errorf("compiled event %s:%s has no admitted flow", flow, record.qualifiedName)
		}
		for _, key := range uniqueNormalizedEventSchemaKeys(record.localName, record.qualifiedName) {
			if previous, exists := scope.bindings[key]; exists {
				return fmt.Errorf("compiled event %s:%s has multiple declaration owners (%s and %s)", flow, key, previous.Source().File, compiled.Source().File)
			}
			scope.bindings[key] = compiled
		}
	}
	b.compiledEventSchemas = bindings
	return nil
}

// ResolveEffectiveCompiledFlowEventSchema returns the exact receiver schema
// when a flow input pin owns one, otherwise the producer declaration. It does
// not rebuild structural evidence from the JSON-schema readback projection.
func (b *WorkflowContractBundle) ResolveEffectiveCompiledFlowEventSchema(flowID, eventType string) (CompiledEventSchema, bool, error) {
	if b == nil {
		return CompiledEventSchema{}, false, nil
	}
	if _, ok := b.exactFlowEventDeclarationView(flowID); !ok {
		return CompiledEventSchema{}, false, nil
	}
	if pin, ok := b.flowInputEventPinForResolvedEvent(flowID, eventType); ok {
		if schema, owned := pin.ReceiverEventSchema(); owned {
			return schema, true, nil
		}
		if schema, owned := pin.ProducerEventSchema(); owned {
			return schema, true, nil
		}
	}
	return b.ResolveCompiledFlowEventSchema(flowID, eventType)
}

type currentEventDeclarationRecord struct {
	flowPath      string
	layer         string
	sourceFile    string
	localName     string
	qualifiedName string
	entry         EventCatalogEntry
	types         TypeCatalogDocument
}

// CompiledEventSchemas enumerates exact importable authored declarations in
// canonical coordinate order. Pattern declarations, generated events, and
// noncanonical package restatements do not become resource identities.
func (b *WorkflowContractBundle) CompiledEventSchemas() ([]CompiledEventSchema, error) {
	if b == nil {
		return nil, nil
	}
	if b.compiledEventSchemas == nil {
		return nil, fmt.Errorf("event schema bindings have not been admitted")
	}
	var out []CompiledEventSchema
	for _, scope := range b.compiledEventSchemas {
		for key, compiled := range scope.bindings {
			if key == compiled.EventName() && compiled.Importable() {
				out = append(out, compiled)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowPath() != out[j].FlowPath() {
			return out[i].FlowPath() < out[j].FlowPath()
		}
		return out[i].EventName() < out[j].EventName()
	})
	for index := 1; index < len(out); index++ {
		if out[index-1].FlowPath() == out[index].FlowPath() && out[index-1].EventName() == out[index].EventName() {
			return nil, fmt.Errorf(
				"compiled event declaration %s:%s has multiple admitted authored owners (%s and %s)",
				out[index].FlowPath(),
				out[index].EventName(),
				out[index-1].Source().File,
				out[index].Source().File,
			)
		}
	}
	return out, nil
}

func (b *WorkflowContractBundle) currentEventDeclarationRecords() []currentEventDeclarationRecord {
	records := make([]currentEventDeclarationRecord, 0, len(b.FlowTree.ByID))
	for _, view := range b.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.FlowPath)
		for _, localName := range sortedContractKeys(view.Events) {
			records = append(records, currentEventDeclarationRecord{
				flowPath:      flowID,
				layer:         "flow",
				sourceFile:    view.Paths.EventsFile,
				localName:     localName,
				qualifiedName: resolvedEventSchemaKey(b, flowID, localName),
				entry:         view.Events[localName],
				types:         b.ResolvedTypeCatalogForFlow(flowID),
			})
		}
	}
	return records
}

// canonicalCurrentEventDeclarationRecords is the sole declaration-list owner
// for non-behavioral compiled projections. Connected producer ownership is
// admitted separately; declarations retain their exact compiled flow coordinate.
func (b *WorkflowContractBundle) canonicalCurrentEventDeclarationRecords() []currentEventDeclarationRecord {
	return b.currentEventDeclarationRecords()
}

func (b *WorkflowContractBundle) compileCurrentEventDeclaration(
	flowPath, layer, sourceFile, localName, qualifiedName string,
	entry EventCatalogEntry,
	types TypeCatalogDocument,
) (CompiledEventSchema, bool, error) {
	if eventidentity.IsCanonicalPattern(localName) {
		return CompiledEventSchema{}, false, nil
	}
	if !eventidentity.IsCanonicalName(localName) {
		return CompiledEventSchema{}, false, fmt.Errorf("compiled event declaration %q is not an exact canonical event identity", localName)
	}
	if !eventidentity.IsCanonicalName(qualifiedName) {
		return CompiledEventSchema{}, false, fmt.Errorf("compiled event declaration %q resolves to noncanonical identity %q", localName, qualifiedName)
	}
	admittedFlow, err := runtimeidentity.AdmitFlowIdentity(flowPath)
	if err != nil {
		return CompiledEventSchema{}, false, fmt.Errorf("compiled event %q has invalid flow owner %q: %w", localName, flowPath, err)
	}
	compiled, err := newCompiledEventSchema(
		admittedFlow.String(),
		qualifiedName,
		entry,
		types,
		entry.BusinessKeyField,
		CompiledEventSchemaSource{FlowPath: admittedFlow.String(), Layer: layer, File: strings.TrimSpace(sourceFile)},
	)
	if err != nil {
		return CompiledEventSchema{}, false, fmt.Errorf("compile event %s:%s: %w", admittedFlow.String(), qualifiedName, err)
	}
	return compiled, true, nil
}

// CompileImportedEventSchema admits an externally owned event declaration
// once so late-bound provider catalogs can supply immutable pin evidence
// without exposing a reader-side EventCatalogEntry fallback.
func CompileImportedEventSchema(flowPath, eventName string, entry EventCatalogEntry, source CompiledEventSchemaSource) (CompiledEventSchema, error) {
	if !eventidentity.IsCanonicalName(eventName) {
		return CompiledEventSchema{}, fmt.Errorf("imported event declaration %q is not an exact canonical event identity", eventName)
	}
	admittedFlow, err := runtimeidentity.AdmitFlowIdentity(flowPath)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("imported event %q has invalid flow owner %q: %w", eventName, flowPath, err)
	}
	compiled, err := newCompiledEventSchema(admittedFlow.String(), eventName, entry, TypeCatalogDocument{}, entry.BusinessKeyField, source)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("compile imported event %s:%s: %w", admittedFlow.String(), eventName, err)
	}
	value := *compiled.value
	value.classification = CompiledEventSchemaImported
	return CompiledEventSchema{value: &value}, nil
}

func newCompiledEventSchema(
	flowPath, eventName string,
	entry EventCatalogEntry,
	types TypeCatalogDocument,
	businessKeyField string,
	source CompiledEventSchemaSource,
) (CompiledEventSchema, error) {
	readback := eventSchemaFromCatalogEntry(eventName, entry, types)
	acceptanceSchema := runtimeeventschema.CanonicalAcceptanceSchema(readback.Schema)
	canonicalSchema, err := canonicaljson.Bytes(acceptanceSchema)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("canonical acceptance schema: %w", err)
	}
	required := compiledEventRequiredFields(acceptanceSchema)
	properties, _ := acceptanceSchema["properties"].(map[string]any)
	fieldNames := make([]string, 0, len(properties))
	for name := range properties {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	fields := make([]CompiledEventField, 0, len(fieldNames))
	for _, name := range fieldNames {
		fieldSchema, ok := properties[name].(map[string]any)
		if !ok {
			return CompiledEventSchema{}, fmt.Errorf("field %q semantic schema is %T, want object", name, properties[name])
		}
		_, isRequired := required[name]
		fields = append(fields, CompiledEventField{value: &compiledEventFieldValue{
			name:       name,
			schema:     cloneEventSchemaMap(fieldSchema),
			isOptional: !isRequired,
		}})
	}
	value := &compiledEventSchemaValue{
		flowPath:               flowPath,
		eventName:              eventName,
		classification:         CompiledEventSchemaAuthored,
		fields:                 fields,
		acceptanceSchema:       cloneEventSchemaMap(acceptanceSchema),
		canonicalSchema:        append([]byte(nil), canonicalSchema...),
		acceptanceSchemaDigest: canonicaljson.HashBytes(canonicalSchema),
		source:                 source,
		readback:               readback,
		declaration:            cloneEventCatalogEntry(entry),
	}
	structuralType, err := ResolveJSONSchemaStructuralType(acceptanceSchema, "event."+value.acceptanceSchemaDigest)
	if err != nil {
		return CompiledEventSchema{}, fmt.Errorf("compiled structural schema: %w", err)
	}
	for index := range structuralType.Fields {
		field := &structuralType.Fields[index]
		declaration, ok := entry.Payload.Properties[field.Name]
		if !ok {
			continue
		}
		field.TypeRef = strings.TrimSpace(declaration.Type)
		field.Refinements = declaration.Refinements
	}
	value.structuralType = structuralType
	if strings.TrimSpace(businessKeyField) != "" {
		key, err := compileEventBusinessKey(strings.TrimSpace(businessKeyField), fields)
		if err != nil {
			return CompiledEventSchema{}, err
		}
		value.businessKey = key
		value.hasBusinessKey = true
	}
	return CompiledEventSchema{value: value}, nil
}

func compiledEventRequiredFields(schema map[string]any) map[string]struct{} {
	out := map[string]struct{}{}
	switch values := schema["required"].(type) {
	case []string:
		for _, value := range values {
			out[strings.TrimSpace(value)] = struct{}{}
		}
	case []any:
		for _, value := range values {
			if name, ok := value.(string); ok {
				out[strings.TrimSpace(name)] = struct{}{}
			}
		}
	}
	return out
}

func compileEventBusinessKey(fieldName string, fields []CompiledEventField) (CompiledEventBusinessKey, error) {
	for _, field := range fields {
		if field.Name() != fieldName {
			continue
		}
		if field.IsOptional() {
			return CompiledEventBusinessKey{}, fmt.Errorf("business key field %q must be required", fieldName)
		}
		typeName, _ := field.SemanticSchema()["type"].(string)
		semanticType := typeName
		if typeName == "integer" {
			semanticType = "number"
		}
		switch semanticType {
		case "boolean", "number", "string":
			return CompiledEventBusinessKey{Field: fieldName, SemanticType: semanticType}, nil
		default:
			return CompiledEventBusinessKey{}, fmt.Errorf("business key field %q must have boolean, number, or string semantics, got %q", fieldName, typeName)
		}
	}
	return CompiledEventBusinessKey{}, fmt.Errorf("business key field %q is not declared", fieldName)
}
