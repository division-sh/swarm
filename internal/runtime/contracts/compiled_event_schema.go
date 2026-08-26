package contracts

import (
	"fmt"
	"path/filepath"
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
	CompiledEventSchemaGenerated CompiledEventSchemaClassification = "generated"
	CompiledEventSchemaPattern   CompiledEventSchemaClassification = "pattern"
)

// CompiledEventSchemaProvider exposes admitted event semantics without
// exposing the YAML carrier or requiring consumers to resolve event identity.
type CompiledEventSchemaProvider interface {
	CompiledEventSchemas() ([]CompiledEventSchema, error)
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
	FlowID string
	Layer  string
	File   string
}

type compiledEventSchemaValue struct {
	packageKey             string
	eventName              string
	classification         CompiledEventSchemaClassification
	fields                 []CompiledEventField
	businessKey            CompiledEventBusinessKey
	hasBusinessKey         bool
	acceptanceSchema       map[string]any
	canonicalSchema        []byte
	acceptanceSchemaDigest string
	source                 CompiledEventSchemaSource
}

// CompiledEventSchema is the immutable, admitted event-schema boundary used
// by downstream compilers. Authored maps and source paths never become
// identity inputs.
type CompiledEventSchema struct {
	value *compiledEventSchemaValue
}

func (s CompiledEventSchema) PackageKey() string {
	if s.value == nil {
		return ""
	}
	return s.value.packageKey
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
	return s.Classification() == CompiledEventSchemaAuthored
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

var _ CompiledEventSchemaProvider = (*WorkflowContractBundle)(nil)

type currentEventDeclarationRecord struct {
	packageKey    string
	flowID        string
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
	var out []CompiledEventSchema
	for _, record := range b.canonicalCurrentEventDeclarationRecords() {
		compiled, ok, err := b.compileCurrentEventDeclaration(
			record.packageKey,
			record.flowID,
			record.layer,
			record.sourceFile,
			record.localName,
			record.qualifiedName,
			record.entry,
			record.types,
		)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, compiled)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PackageKey() != out[j].PackageKey() {
			return out[i].PackageKey() < out[j].PackageKey()
		}
		return out[i].EventName() < out[j].EventName()
	})
	for index := 1; index < len(out); index++ {
		if out[index-1].PackageKey() == out[index].PackageKey() && out[index-1].EventName() == out[index].EventName() {
			return nil, fmt.Errorf(
				"compiled event declaration %s:%s has multiple admitted authored owners (%s and %s)",
				out[index].PackageKey(),
				out[index].EventName(),
				out[index-1].Source().File,
				out[index].Source().File,
			)
		}
	}
	return out, nil
}

func (b *WorkflowContractBundle) currentEventDeclarationRecords() []currentEventDeclarationRecord {
	flowDeclarations := map[string]struct{}{}
	flowRecords := make([]currentEventDeclarationRecord, 0, len(b.FlowTree.ByID))
	for _, view := range b.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		for _, localName := range sortedContractKeys(view.Events) {
			if key := currentEventDeclarationRecordKey(view.Paths.EventsFile, localName); key != "" {
				flowDeclarations[key] = struct{}{}
			}
			flowRecords = append(flowRecords, currentEventDeclarationRecord{
				packageKey:    view.Paths.PackageKey,
				flowID:        flowID,
				layer:         "flow",
				sourceFile:    view.Paths.EventsFile,
				localName:     localName,
				qualifiedName: resolvedEventSchemaKey(b, flowID, localName),
				entry:         view.Events[localName],
				types:         b.ResolvedTypeCatalogForFlow(flowID),
			})
		}
	}

	records := make([]currentEventDeclarationRecord, 0, len(b.ProjectViews())+len(flowRecords))
	for _, view := range b.ProjectViews() {
		flowID := strings.TrimSpace(view.Paths.OwningFlowID)
		types := b.RootTypeCatalog()
		if flowID != "" {
			types = b.ResolvedTypeCatalogForFlow(flowID)
		}
		for _, localName := range sortedContractKeys(view.Events) {
			if key := currentEventDeclarationRecordKey(view.Paths.ProjectEventsFile, localName); key != "" {
				if _, representedByFlow := flowDeclarations[key]; representedByFlow {
					continue
				}
			}
			qualifiedName := localName
			if flowID != "" {
				qualifiedName = resolvedOwnedEventSchemaKey(b, flowID, localName)
			}
			records = append(records, currentEventDeclarationRecord{
				packageKey:    view.Paths.Key,
				flowID:        flowID,
				layer:         "project",
				sourceFile:    view.Paths.ProjectEventsFile,
				localName:     localName,
				qualifiedName: qualifiedName,
				entry:         view.Events[localName],
				types:         types,
			})
		}
	}
	records = append(records, flowRecords...)
	return records
}

// canonicalCurrentEventDeclarationRecords is the sole declaration-list owner
// for non-behavioral compiled projections. Connected producer ownership is
// admitted separately; this projection only collapses a package-qualified
// project view when the same coordinate has one exact flow declaration.
func (b *WorkflowContractBundle) canonicalCurrentEventDeclarationRecords() []currentEventDeclarationRecord {
	records := b.currentEventDeclarationRecords()
	byCoordinate := make(map[string][]currentEventDeclarationRecord, len(records))
	coordinates := make([]string, 0, len(records))
	for _, record := range records {
		coordinate := strings.TrimSpace(record.packageKey) + "\x00" + eventidentity.Normalize(record.qualifiedName)
		if _, exists := byCoordinate[coordinate]; !exists {
			coordinates = append(coordinates, coordinate)
		}
		byCoordinate[coordinate] = append(byCoordinate[coordinate], record)
	}

	out := make([]currentEventDeclarationRecord, 0, len(records))
	for _, coordinate := range coordinates {
		candidates := byCoordinate[coordinate]
		if len(candidates) == 1 {
			out = append(out, candidates[0])
			continue
		}
		flowOwners := make([]currentEventDeclarationRecord, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.layer == "flow" {
				flowOwners = append(flowOwners, candidate)
			}
		}
		if len(flowOwners) == 1 {
			out = append(out, flowOwners[0])
			continue
		}
		// Preserve ambiguity so compiled admission fails closed rather than
		// selecting an owner by source or map order.
		out = append(out, candidates...)
	}
	return out
}

func resolvedOwnedEventSchemaKey(bundle *WorkflowContractBundle, flowID, eventName string) string {
	resolved := resolvedEventSchemaKey(bundle, flowID, eventName)
	if bundle == nil || strings.TrimSpace(flowID) == "" || resolved != eventidentity.Normalize(eventName) {
		return resolved
	}
	// Ownership discovery proves this project declaration is local to the flow,
	// even though it is not repeated in the flow view's local event map.
	return eventidentity.ExternalizeForFlow(bundle.FlowPath(flowID), []string{resolved}, resolved)
}

func currentEventDeclarationRecordKey(sourceFile, localName string) string {
	sourceFile = strings.TrimSpace(sourceFile)
	if sourceFile == "" {
		return ""
	}
	return filepath.Clean(sourceFile) + "\x00" + strings.TrimSpace(localName)
}

func (b *WorkflowContractBundle) compileCurrentEventDeclaration(
	packageKey, flowID, layer, sourceFile, localName, qualifiedName string,
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
	admittedPackage, err := runtimeidentity.ParsePackageKey(packageKey)
	if err != nil {
		return CompiledEventSchema{}, false, fmt.Errorf("compiled event %q has invalid package owner %q: %w", localName, packageKey, err)
	}
	compiled, err := newCompiledEventSchema(
		admittedPackage.String(),
		qualifiedName,
		entry,
		types,
		entry.BusinessKeyField,
		CompiledEventSchemaSource{FlowID: strings.TrimSpace(flowID), Layer: layer, File: strings.TrimSpace(sourceFile)},
	)
	if err != nil {
		return CompiledEventSchema{}, false, fmt.Errorf("compile event %s:%s: %w", admittedPackage.String(), qualifiedName, err)
	}
	return compiled, true, nil
}

func newCompiledEventSchema(
	packageKey, eventName string,
	entry EventCatalogEntry,
	types TypeCatalogDocument,
	businessKeyField string,
	source CompiledEventSchemaSource,
) (CompiledEventSchema, error) {
	schema := eventSchemaFromCatalogEntry(eventName, entry, types).Schema
	acceptanceSchema := runtimeeventschema.CanonicalAcceptanceSchema(schema)
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
		packageKey:             packageKey,
		eventName:              eventName,
		classification:         CompiledEventSchemaAuthored,
		fields:                 fields,
		acceptanceSchema:       cloneEventSchemaMap(acceptanceSchema),
		canonicalSchema:        append([]byte(nil), canonicalSchema...),
		acceptanceSchemaDigest: canonicaljson.HashBytes(canonicalSchema),
		source:                 source,
	}
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
