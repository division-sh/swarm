package contracts

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const eventRequiredByDefaultRule = "event_payload_field_required_by_default/v1"

// Reserved names cannot be reinterpreted as business payload fields.
var reservedEventCatalogFieldNames = map[string]struct{}{
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
	bare, err := admitEventDeclarationShape(name, value)
	if err != nil {
		return EventCatalogEntry{}, err
	}
	fields, byName, err := admitEventCatalogFields(name, value, bare)
	if err != nil {
		return EventCatalogEntry{}, err
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
		entry.admissionProvenance["business_key"] = authoredSourceProvenance(field.Value)
	}
	if err := populateEventCatalogPayload(&entry, fields); err != nil {
		return EventCatalogEntry{}, err
	}
	if !bare && len(entry.Payload.Properties) == 0 {
		return EventCatalogEntry{}, payloadlessEventDeclarationError(name, "mapping")
	}
	if err := validateEventCatalogBusinessKey(entry); err != nil {
		return EventCatalogEntry{}, err
	}
	return entry, nil
}

func admitEventCatalogFields(name string, value yamlsource.Value, bare bool) ([]yamlsource.MappingField, map[string]yamlsource.MappingField, error) {
	var fields []yamlsource.MappingField
	if !bare {
		var err error
		fields, err = value.Mapping()
		if err != nil {
			return nil, nil, err
		}
	}
	for _, field := range fields {
		if _, reserved := reservedEventCatalogFieldNames[field.Name]; reserved {
			return nil, nil, fmt.Errorf("event field name %q at %s is reserved and cannot declare business payload", field.Name, field.IntroductionLocation())
		}
		if field.Name == "required" {
			return nil, nil, fmt.Errorf("event field name %q at %s is reserved for required-by-default field semantics", field.Name, field.IntroductionLocation())
		}
		if field.Name == "payload" && eventPayloadValueIsRetiredNestedBlock(field.Value) {
			return nil, nil, fmt.Errorf("event payload at %s must be a field type declaration, not a nested payload block", field.IntroductionLocation())
		}
	}
	if !bare {
		var err error
		fields, err = uniqueYAMLMappingFields(value, "event "+name)
		if err != nil {
			return nil, nil, err
		}
	}
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, field := range fields {
		byName[field.Name] = field
		if err := field.Value.ValidateUniqueMappings(); err != nil {
			return nil, nil, err
		}
	}
	return fields, byName, nil
}

func populateEventCatalogPayload(entry *EventCatalogEntry, fields []yamlsource.MappingField) error {
	payloadFieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Name == "key" {
			continue
		}
		payloadField, optional, typeSource, err := admitEventPayloadField(field.Value)
		if err != nil {
			return fmt.Errorf("payload field %q at %s: %w", field.Name, field.Value.Location(), err)
		}
		if !optional {
			entry.Payload.Required = append(entry.Payload.Required, field.Name)
		}
		entry.Payload.Properties[field.Name] = payloadField
		payloadFieldNames = append(payloadFieldNames, field.Name)
		typePath := "fields." + field.Name + ".type"
		entry.admissionProvenance[typePath] = authoredSourceProvenance(typeSource)
		optionalPath := "fields." + field.Name + ".is_optional"
		if optional {
			entry.admissionProvenance[optionalPath] = authoredSourceProvenance(typeSource)
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
	}
	sort.Strings(entry.Payload.Required)
	sort.Strings(payloadFieldNames)
	entry.admissionProvenance["payload.required"] = EffectiveValueProvenance{
		Origin:     EffectiveValueOriginDerived,
		RuleID:     eventRequiredByDefaultRule,
		InputPaths: eventFieldTypePaths(payloadFieldNames),
	}

	return nil
}

func validateEventCatalogBusinessKey(entry EventCatalogEntry) error {
	if entry.BusinessKeyField == "" {
		return nil
	}
	field, ok := entry.Payload.Properties[entry.BusinessKeyField]
	if !ok {
		return fmt.Errorf("event key %q is not a declared payload field", entry.BusinessKeyField)
	}
	if !slices.Contains(entry.Payload.Required, entry.BusinessKeyField) {
		return fmt.Errorf("event key field %q must be required", entry.BusinessKeyField)
	}
	if strings.TrimSpace(field.Type) == "" {
		return fmt.Errorf("event key field %q has no admitted type", entry.BusinessKeyField)
	}
	return nil
}

func payloadlessEventDeclarationError(name, variant string) error {
	return fmt.Errorf("an event with no fields is declared bare \u2014 write `%s:` (remove the `%s`).", name, variant)
}

func admitEventDeclarationShape(name string, value yamlsource.Value) (bool, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull:
		scalar, err := value.Scalar()
		if err != nil {
			return false, err
		}
		variant := scalar.Value
		switch {
		case scalar.Alias != "":
			variant = "*" + scalar.Alias
		case scalar.Anchor != "":
			variant = "&" + scalar.Anchor
		case scalar.Style&yamlsource.TaggedStyle != 0:
			variant = "!!null"
		}
		if variant != "" {
			return false, payloadlessEventDeclarationError(name, variant)
		}
		return true, nil
	case yamlsource.PresenceEmptyMapping:
		return false, payloadlessEventDeclarationError(name, "{}")
	case yamlsource.PresenceEmptyScalar:
		variant := `""`
		if scalar, err := value.Scalar(); err == nil && scalar.Style&yamlsource.SingleQuotedStyle != 0 {
			variant = "''"
		}
		return false, payloadlessEventDeclarationError(name, variant)
	case yamlsource.PresenceMapping:
		return false, nil
	default:
		return false, fmt.Errorf("declaration at %s is %s, want mapping", value.Location(), value.Presence())
	}
}

func admitEventPayloadField(value yamlsource.Value) (EventFieldSpec, bool, yamlsource.Value, error) {
	const context = "event payload field"
	switch value.Presence() {
	case yamlsource.PresenceScalar:
		rawType, err := listDeclarationScalar(value, context, false)
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
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		typeName, err := listDeclarationSequence(value, context)
		if err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		if err := rejectEventTypeOptionalMarker(typeName, context); err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		if err := validateWave1TypeRef(typeName, context); err != nil {
			return EventFieldSpec{}, false, yamlsource.Value{}, err
		}
		values, _ := value.Sequence()
		return EventFieldSpec{Type: typeName}, false, values[0], nil
	case yamlsource.PresenceMapping:
	case yamlsource.PresenceEmptyMapping:
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	default:
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	}
	return admitEventPayloadMapping(value)
}

func admitEventPayloadMapping(value yamlsource.Value) (EventFieldSpec, bool, yamlsource.Value, error) {
	const context = "event payload field"
	fields, err := uniqueYAMLMappingFields(value, context)
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	if err := rejectLongListField(fields, context); err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	byName := make(map[string]yamlsource.MappingField, len(fields))
	for _, field := range fields {
		if _, ok := eventPayloadFieldMappingKeys[field.Name]; !ok {
			return EventFieldSpec{}, false, yamlsource.Value{}, NewUndefinedFieldDiagnostic(context, field.Name, eventPayloadFieldMappingKeys, field)
		}
		byName[field.Name] = field
	}
	typeField, hasType := byName["type"]
	if !hasType {
		return EventFieldSpec{}, false, yamlsource.Value{}, fmt.Errorf("%s type is required", context)
	}
	rawType, err := listDeclarationScalar(typeField.Value, context, true)
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	typeName, optional, err := admitEventFieldTypeMarker(rawType, context)
	if err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	if err := rejectEventTypeOptionalMarker(typeName, context); err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	if err := validateWave1TypeRef(typeName, context); err != nil {
		return EventFieldSpec{}, false, yamlsource.Value{}, err
	}
	return EventFieldSpec{Type: typeName}, optional, typeField.Value, nil
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
	"type": {},
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

func authoredSourceProvenance(value yamlsource.Value) EffectiveValueProvenance {
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
