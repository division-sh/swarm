package contracts

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const eventRequiredByDefaultRule = "event_payload_field_required_by_default/v1"

var retiredEventCatalogMetadataFields = map[string]struct{}{
	"swarm":                {},
	"emitter":              {},
	"emitter_type":         {},
	"producer":             {},
	"_producer":            {},
	"alternate_emitters":   {},
	"consumer":             {},
	"_consumer":            {},
	"consumer_type":        {},
	"_consumer_type":       {},
	"_source":              {},
	"_status":              {},
	"_note":                {},
	"intercepted":          {},
	"passthrough":          {},
	"runtime_handling":     {},
	"owning_node":          {},
	"delivery_channel":     {},
	"author_summary_field": {},
}

func loadOptionalEventCatalog(path string) (map[string]EventCatalogEntry, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationEvents)
	if err != nil || !present {
		return map[string]EventCatalogEntry{}, err
	}
	entries, err := admitEventCatalogDocument(admission.document)
	if err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return entries, admission.RequireLive(len(entries))
}

func loadOptionalEventCatalogFromSource(artifact *sourceartifact.AdmittedSourceArtifact, label string) (map[string]EventCatalogEntry, error) {
	admission, present, err := admitOptionalDeclarationSource(artifact, label, optionalDeclarationEvents)
	if err != nil || !present {
		return map[string]EventCatalogEntry{}, err
	}
	entries, err := admitEventCatalogDocument(admission.document)
	if err != nil {
		return nil, wrapLoaderDiagnosticFile(err, label)
	}
	return entries, admission.RequireLive(len(entries))
}

func admitEventCatalogDocument(document yamlsource.Document) (map[string]EventCatalogEntry, error) {
	root := document.Root()
	switch root.Presence() {
	case yamlsource.PresenceEmptyMapping:
		return map[string]EventCatalogEntry{}, nil
	case yamlsource.PresenceMapping:
	case yamlsource.PresenceMissing:
		return nil, fmt.Errorf("events.yaml source is missing")
	default:
		return nil, fmt.Errorf("events.yaml document at %s is %s, want mapping", root.Location(), root.Presence())
	}
	fields, err := uniqueYAMLMappingFields(root, "events.yaml event declaration")
	if err != nil {
		return nil, err
	}
	out := make(map[string]EventCatalogEntry, len(fields))
	admitted := make(map[string]yamlsource.Location, len(fields))
	for _, declaration := range fields {
		name := declaration.Name
		if !eventidentity.IsCanonicalDeclaration(name) {
			return nil, fmt.Errorf("event declaration name %q at %s must be an exact canonical event identity or supported wildcard pattern", name, declaration.KeyLocation)
		}
		if previous, exists := admitted[name]; exists {
			return nil, fmt.Errorf("events.yaml has colliding admitted event declaration %q at %s and %s", name, previous, declaration.KeyLocation)
		}
		admitted[name] = declaration.KeyLocation
		entry, err := admitEventCatalogEntry(name, declaration)
		if err != nil {
			return nil, fmt.Errorf("event %q: %w", name, err)
		}
		out[name] = entry
	}
	return out, nil
}

func admitEventCatalogEntry(name string, declaration yamlsource.MappingField) (EventCatalogEntry, error) {
	value := declaration.Value
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return EventCatalogEntry{}, fmt.Errorf("declaration at %s is %s, want mapping", value.Location(), value.Presence())
	}
	fields, err := value.Mapping()
	if err != nil {
		return EventCatalogEntry{}, err
	}
	for _, field := range fields {
		if _, retired := retiredEventCatalogMetadataFields[field.Name]; retired {
			return EventCatalogEntry{}, fmt.Errorf("RETIRED: events.yaml metadata field %s at %s is unsupported; remove it; inputs, outputs, connections and executable declarations own event roles, and event APIs expose full payloads", field.Name, field.KeyLocation)
		}
		if field.Name == "required" {
			return EventCatalogEntry{}, fmt.Errorf("RETIRED: events.yaml field required at %s is no longer supported; fields are required by default and optional fields use one trailing ? on their type", field.KeyLocation)
		}
		if field.Name == "payload" && eventPayloadValueIsRetiredNestedBlock(field.Value) {
			return EventCatalogEntry{}, fmt.Errorf("RETIRED: nested events.yaml payload blocks are no longer supported; move payload fields to the event top level")
		}
	}
	fields, err = uniqueYAMLMappingFields(value, "event "+name)
	if err != nil {
		return EventCatalogEntry{}, err
	}
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, field := range fields {
		byName[field.Name] = field
		if err := field.Value.ValidateUniqueMappings(); err != nil {
			return EventCatalogEntry{}, err
		}
	}

	entry := EventCatalogEntry{
		Payload:             EventPayloadSpec{Properties: map[string]EventFieldSpec{}},
		admissionProvenance: map[string]EffectiveValueProvenance{},
	}
	entry.admissionProvenance["declaration"] = EffectiveValueProvenance{
		Origin:         EffectiveValueOriginAuthored,
		SourceFile:     declaration.IntroductionLocation().File,
		SourceLine:     declaration.IntroductionLocation().Line,
		SourceColumn:   declaration.IntroductionLocation().Column,
		SourcePresence: value.Presence().String(),
	}

	if field, ok := byName["key"]; ok {
		key, err := requiredLiteralString(field.Value, "event key")
		if err != nil {
			return EventCatalogEntry{}, err
		}
		if strings.TrimSpace(key) != key {
			return EventCatalogEntry{}, fmt.Errorf("event key at %s must not have surrounding whitespace", field.Value.Location())
		}
		entry.BusinessKeyField = key
		entry.admissionProvenance["business_key"] = authoredEventProvenance(field.Value)
	}

	payloadFieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Name == "key" || field.Name == "description" {
			continue
		}
		payloadField, optional, typeSource, err := admitEventPayloadField(field.Value)
		if err != nil {
			return EventCatalogEntry{}, fmt.Errorf("payload field %q at %s: %w", field.Name, field.Value.Location(), err)
		}
		if !optional {
			entry.Payload.Required = append(entry.Payload.Required, field.Name)
		}
		entry.Payload.Properties[field.Name] = payloadField
		payloadFieldNames = append(payloadFieldNames, field.Name)
		typePath := "fields." + field.Name + ".type"
		entry.admissionProvenance[typePath] = authoredEventProvenance(typeSource)
		optionalPath := "fields." + field.Name + ".is_optional"
		if optional {
			entry.admissionProvenance[optionalPath] = authoredEventProvenance(typeSource)
		} else {
			location := typeSource.IntroductionLocation()
			entry.admissionProvenance[optionalPath] = EffectiveValueProvenance{
				Origin:       EffectiveValueOriginDerived,
				RuleID:       eventRequiredByDefaultRule,
				InputPaths:   []string{typePath},
				SourceFile:   location.File,
				SourceLine:   location.Line,
				SourceColumn: location.Column,
			}
		}
		if err := populateEventPayloadFieldAdmissionProvenance(&entry, field.Name, field.Value); err != nil {
			return EventCatalogEntry{}, err
		}
	}
	sort.Strings(entry.Payload.Required)
	sort.Strings(payloadFieldNames)
	entry.admissionProvenance["payload.required"] = EffectiveValueProvenance{
		Origin:     EffectiveValueOriginDerived,
		RuleID:     eventRequiredByDefaultRule,
		InputPaths: eventFieldTypePaths(payloadFieldNames),
	}

	if entry.BusinessKeyField != "" {
		field, ok := entry.Payload.Properties[entry.BusinessKeyField]
		if !ok {
			return EventCatalogEntry{}, fmt.Errorf("event key %q is not a declared payload field", entry.BusinessKeyField)
		}
		if !slices.Contains(entry.Payload.Required, entry.BusinessKeyField) {
			return EventCatalogEntry{}, fmt.Errorf("event key field %q must be required", entry.BusinessKeyField)
		}
		if strings.TrimSpace(field.Type) == "" {
			return EventCatalogEntry{}, fmt.Errorf("event key field %q has no admitted type", entry.BusinessKeyField)
		}
	}
	return entry, nil
}

func populateEventPayloadFieldAdmissionProvenance(entry *EventCatalogEntry, fieldName string, value yamlsource.Value) error {
	if entry == nil || value.Presence() != yamlsource.PresenceMapping {
		return nil
	}
	fields, err := uniqueYAMLMappingFields(value, "event payload field provenance")
	if err != nil {
		return err
	}
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, field := range fields {
		byName[field.Name] = field
	}
	prefix := "fields." + strings.TrimSpace(fieldName) + "."
	for _, name := range []string{"description", "pattern", "equal_to"} {
		if field, ok := byName[name]; ok {
			path := name
			if name != "description" {
				path = "refinements." + name
			}
			entry.admissionProvenance[prefix+path] = authoredEventProvenance(field.Value)
		}
	}
	for _, nested := range []struct {
		prefix string
		field  yamlsource.MappingField
		names  []string
	}{
		{prefix: prefix + "refinements.length.", field: byName["length"], names: []string{"min", "max"}},
		{prefix: prefix + "refinements.range.", field: byName["range"], names: []string{"min", "max"}},
		{prefix: prefix + "citation.", field: byName["citation"], names: []string{"criteria", "allowed_classes"}},
	} {
		if err := populateEventNestedAdmissionProvenance(entry, nested.prefix, nested.field, nested.names); err != nil {
			return err
		}
	}
	return nil
}

func populateEventNestedAdmissionProvenance(entry *EventCatalogEntry, prefix string, parent yamlsource.MappingField, names []string) error {
	if entry == nil || parent.Value.Presence() != yamlsource.PresenceMapping {
		return nil
	}
	fields, err := uniqueYAMLMappingFields(parent.Value, "event nested provenance")
	if err != nil {
		return err
	}
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	for _, field := range fields {
		if _, ok := allowed[field.Name]; ok {
			entry.admissionProvenance[prefix+field.Name] = authoredEventProvenance(field.Value)
		}
	}
	return nil
}

func admitEventPayloadField(value yamlsource.Value) (EventFieldSpec, bool, yamlsource.Value, error) {
	const context = "event payload field"
	switch value.Presence() {
	case yamlsource.PresenceScalar:
		rawType, err := requiredLiteralString(value, context+" type")
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		typeName, optional, err := admitEventFieldTypeMarker(rawType, context)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		if err := validateWave1TypeRef(typeName, context); err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		return EventFieldSpec{Type: typeName}, optional, value, nil
	case yamlsource.PresenceSequence:
		values, err := value.Sequence()
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		if len(values) != 1 {
			return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s list shorthand requires exactly one element type", context)
		}
		element, err := requiredLiteralString(values[0], context+" element type")
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		typeName := "[" + strings.TrimSpace(element) + "]"
		if err := rejectEventTypeOptionalMarker(typeName, context); err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		if err := validateWave1TypeRef(typeName, context); err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		return EventFieldSpec{Type: typeName}, false, values[0], nil
	case yamlsource.PresenceMapping:
	case yamlsource.PresenceEmptyMapping:
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	default:
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	}

	fields, err := uniqueYAMLMappingFields(value, context)
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, field := range fields {
		if _, ok := eventPayloadFieldMappingKeys[field.Name]; !ok {
			switch field.Name {
			case "properties", "fields", "shape":
				return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("RETIRED: %s inline object declarations are retired; declare a named type in types.yaml", context)
			default:
				return EventFieldSpec{}, false, yamlsource.Value{}, NewUndefinedFieldDiagnostic(context, field.Name, eventPayloadFieldMappingKeys)
			}
		}
		byName[field.Name] = field
	}
	typeField, hasType := byName["type"]
	if !hasType {
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	}
	rawType, err := requiredLiteralString(typeField.Value, context+" type")
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	typeName, optional, err := admitEventFieldTypeMarker(rawType, context)
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	if strings.EqualFold(strings.TrimSpace(typeName), "list") {
		element, err := requiredLiteralString(eventFieldValue(byName, "of"), context+" list element type")
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("RETIRED: %s list declarations require an of: element type", context)
		}
		typeName = "[" + strings.TrimSpace(element) + "]"
	} else if _, hasOf := byName["of"]; hasOf {
		return EventFieldSpec{}, false, yamlsource.Value{}, NewUndefinedFieldDiagnostic(context, "of", eventPayloadFieldMappingKeys)
	}
	if err := rejectEventTypeOptionalMarker(typeName, context); err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	if err := validateWave1TypeRef(typeName, context); err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	out := EventFieldSpec{Type: typeName}
	if field, ok := byName["description"]; ok {
		out.Description, err = optionalScalarString(field.Value, context+" description")
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
	}
	if field, ok := byName["pattern"]; ok {
		out.Refinements.Pattern, err = admitEventPattern(field.Value)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s pattern: %w", context, err)
		}
	}
	if field, ok := byName["length"]; ok {
		out.Refinements.Length, err = admitEventLengthRefinement(field.Value)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s length: %w", context, err)
		}
	}
	if field, ok := byName["range"]; ok {
		out.Refinements.Range, err = admitEventRangeRefinement(field.Value)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s range: %w", context, err)
		}
	}
	if field, ok := byName["equal_to"]; ok {
		out.Refinements.EqualTo, err = requiredLiteralString(field.Value, context+" equal_to field")
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
	}
	if field, ok := byName["citation"]; ok {
		out.Citation, err = admitEventCitation(field.Value)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
	}
	return out, optional, typeField.Value, nil
}

func admitEventFieldTypeMarker(raw, context string) (string, bool, error) {
	optional := strings.HasSuffix(raw, "?")
	typeName := raw
	if optional {
		typeName = strings.TrimSuffix(typeName, "?")
		if typeName == "" || strings.TrimSpace(typeName) != typeName {
			return "", false, fmt.Errorf("%s optional marker must immediately follow its type", context)
		}
	}
	if err := rejectEventTypeOptionalMarker(typeName, context); err != nil {
		return "", false, err
	}
	return strings.TrimSpace(typeName), optional, nil
}

func rejectEventTypeOptionalMarker(typeName, context string) error {
	if strings.Contains(typeName, "?") {
		return fmt.Errorf("%s optional type must use exactly one trailing ?", context)
	}
	return nil
}

var eventPayloadFieldMappingKeys = map[string]struct{}{
	"type": {}, "of": {}, "description": {}, "pattern": {}, "length": {}, "range": {}, "equal_to": {}, "citation": {},
}

func admitEventPattern(value yamlsource.Value) (string, error) {
	pattern, err := requiredLiteralString(value, "pattern")
	if err != nil {
		return "", err
	}
	pattern = strings.TrimSpace(pattern)
	if _, err := regexp.Compile(pattern); err != nil {
		return "", fmt.Errorf("must compile as a regular expression: %w", err)
	}
	return pattern, nil
}

func admitEventLengthRefinement(value yamlsource.Value) (SchemaLengthRefinement, error) {
	fields, err := uniqueYAMLMappingFields(value, "length")
	if err != nil {
		return SchemaLengthRefinement{}, fmt.Errorf("must be a mapping with min and/or max: %w", err)
	}
	var out SchemaLengthRefinement
	for _, field := range fields {
		var number int
		if field.Name != "min" && field.Name != "max" {
			return SchemaLengthRefinement{}, NewUndefinedFieldDiagnostic("length", field.Name, schemaLengthRefinementFieldOptions)
		}
		if err := field.Value.Project(&number); err != nil {
			return SchemaLengthRefinement{}, fmt.Errorf("%s: %w", field.Name, err)
		}
		if field.Name == "min" {
			out.Min = &number
		} else {
			out.Max = &number
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaLengthRefinement{}, fmt.Errorf("must declare min and/or max")
	}
	if out.Min != nil && *out.Min < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("min must be >= 0")
	}
	if out.Max != nil && *out.Max < 0 {
		return SchemaLengthRefinement{}, fmt.Errorf("max must be >= 0")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaLengthRefinement{}, fmt.Errorf("min must be <= max")
	}
	return out, nil
}

func admitEventRangeRefinement(value yamlsource.Value) (SchemaRangeRefinement, error) {
	fields, err := uniqueYAMLMappingFields(value, "range")
	if err != nil {
		return SchemaRangeRefinement{}, fmt.Errorf("must be a mapping with min and/or max: %w", err)
	}
	var out SchemaRangeRefinement
	for _, field := range fields {
		var number float64
		if field.Name != "min" && field.Name != "max" {
			return SchemaRangeRefinement{}, NewUndefinedFieldDiagnostic("range", field.Name, schemaRangeRefinementFieldOptions)
		}
		if err := field.Value.Project(&number); err != nil {
			return SchemaRangeRefinement{}, fmt.Errorf("%s: %w", field.Name, err)
		}
		if field.Name == "min" {
			out.Min = &number
		} else {
			out.Max = &number
		}
	}
	if out.Min == nil && out.Max == nil {
		return SchemaRangeRefinement{}, fmt.Errorf("must declare min and/or max")
	}
	if out.Min != nil && out.Max != nil && *out.Min > *out.Max {
		return SchemaRangeRefinement{}, fmt.Errorf("min must be <= max")
	}
	return out, nil
}

func admitEventCitation(value yamlsource.Value) (CriteriaCitation, error) {
	fields, err := uniqueYAMLMappingFields(value, "event citation")
	if err != nil {
		return CriteriaCitation{}, err
	}
	var out CriteriaCitation
	for _, field := range fields {
		switch field.Name {
		case "criteria":
			out.Criteria, err = optionalScalarString(field.Value, "citation.criteria")
		case "allowed_classes":
			out.AllowedClasses, err = optionalStrictStringSequence(field.Value, "citation.allowed_classes")
		default:
			return CriteriaCitation{}, fmt.Errorf("event citation field %q is not supported", field.Name)
		}
		if err != nil {
			return CriteriaCitation{}, err
		}
	}
	out.AllowedClasses = normalizeStrings(out.AllowedClasses)
	return out, nil
}

func eventPayloadValueIsRetiredNestedBlock(value yamlsource.Value) bool {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return false
	}
	fields, err := value.Mapping()
	if err != nil {
		return false
	}
	hasType := false
	for _, field := range fields {
		if field.Name == "type" {
			hasType = true
			break
		}
	}
	for _, field := range fields {
		switch field.Name {
		case "properties", "fields", "shape", "required":
			return true
		case "type":
		default:
			if _, supported := eventPayloadFieldMappingKeys[field.Name]; field.Name != "" && !supported {
				return !hasType
			}
		}
	}
	return false
}

func uniqueYAMLMappingFields(value yamlsource.Value, context string) ([]yamlsource.MappingField, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	seen := map[string]yamlsource.Location{}
	for _, field := range fields {
		if previous, ok := seen[field.Name]; ok {
			return nil, fmt.Errorf("%s has duplicate effective field %q at %s and %s", context, field.Name, previous, field.KeyLocation)
		}
		seen[field.Name] = field.KeyLocation
	}
	return fields, nil
}

func eventFieldValue(fields map[string]yamlsource.MappingField, name string) yamlsource.Value {
	if field, ok := fields[name]; ok {
		return field.Value
	}
	return yamlsource.MissingDocument("").Root()
}

func requiredLiteralString(value yamlsource.Value, context string) (string, error) {
	if value.Presence() != yamlsource.PresenceScalar {
		return "", fmt.Errorf("%s at %s is %s, want non-empty scalar", context, value.Location(), value.Presence())
	}
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	if scalar.Value == "" {
		return "", fmt.Errorf("%s at %s must not be empty", context, scalar.Location)
	}
	return scalar.Value, nil
}

func optionalScalarString(value yamlsource.Value, context string) (string, error) {
	switch value.Presence() {
	case yamlsource.PresenceMissing, yamlsource.PresenceNull:
		return "", nil
	case yamlsource.PresenceEmptyScalar, yamlsource.PresenceScalar:
		scalar, err := value.Scalar()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(scalar.Value), nil
	default:
		return "", fmt.Errorf("%s at %s is %s, want scalar", context, value.Location(), value.Presence())
	}
}

func optionalStrictStringSequence(value yamlsource.Value, context string) ([]string, error) {
	switch value.Presence() {
	case yamlsource.PresenceMissing, yamlsource.PresenceNull, yamlsource.PresenceEmptySequence:
		return nil, nil
	case yamlsource.PresenceSequence:
		var values []string
		if err := value.Project(&values); err != nil {
			return nil, fmt.Errorf("%s at %s: %w", context, value.Location(), err)
		}
		return normalizeStrings(values), nil
	default:
		return nil, fmt.Errorf("%s at %s is %s, want sequence", context, value.Location(), value.Presence())
	}
}

func authoredEventProvenance(value yamlsource.Value) EffectiveValueProvenance {
	location := value.IntroductionLocation()
	return EffectiveValueProvenance{
		Origin:         EffectiveValueOriginAuthored,
		SourceFile:     location.File,
		SourceLine:     location.Line,
		SourceColumn:   location.Column,
		SourcePresence: value.Presence().String(),
	}
}

func eventFieldTypePaths(fields []string) []string {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, "fields."+field+".type")
	}
	return out
}
