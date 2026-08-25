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

// CompiledEventSchemas enumerates exact importable authored declarations in
// canonical coordinate order. Pattern declarations, generated events, and
// noncanonical package restatements do not become resource identities.
func (b *WorkflowContractBundle) CompiledEventSchemas() ([]CompiledEventSchema, error) {
	if b == nil {
		return nil, nil
	}
	var out []CompiledEventSchema
	for _, view := range b.ProjectViews() {
		for _, localName := range sortedContractKeys(view.Events) {
			compiled, ok, err := b.compileCurrentEventDeclaration(
				view.Paths.Key,
				"",
				"project",
				view.Paths.ProjectEventsFile,
				localName,
				localName,
				view.Events[localName],
				b.RootTypeCatalog(),
			)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, compiled)
			}
		}
	}
	for _, view := range b.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		for _, localName := range sortedContractKeys(view.Events) {
			compiled, ok, err := b.compileCurrentEventDeclaration(
				view.Paths.PackageKey,
				flowID,
				"flow",
				view.Paths.EventsFile,
				localName,
				resolvedEventSchemaKey(b, flowID, localName),
				view.Events[localName],
				b.ResolvedTypeCatalogForFlow(flowID),
			)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, compiled)
			}
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

func (b *WorkflowContractBundle) compileCurrentEventDeclaration(
	packageKey, flowID, layer, sourceFile, localName, qualifiedName string,
	entry EventCatalogEntry,
	types TypeCatalogDocument,
) (CompiledEventSchema, bool, error) {
	if strings.Contains(localName, "*") {
		return CompiledEventSchema{}, false, nil
	}
	if eventidentity.Normalize(localName) != localName {
		return CompiledEventSchema{}, false, nil
	}
	qualifiedName = eventidentity.Normalize(qualifiedName)
	if !eventidentity.IsValidName(qualifiedName) {
		return CompiledEventSchema{}, false, nil
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
		"",
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
