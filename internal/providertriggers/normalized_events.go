package providertriggers

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

type OutputKind string

const (
	OutputKindRaw        OutputKind = "raw"
	OutputKindNormalized OutputKind = "normalized"
)

type NormalizedEventManifest struct {
	Event              string                                    `yaml:"event"`
	Fields             map[string]NormalizedEventFieldProjection `yaml:"fields"`
	When               NormalizedEventWhen                       `yaml:"when,omitempty"`
	AuthorSummaryField string                                    `yaml:"author_summary_field,omitempty"`
	AuthorSubject      AuthorSubjectManifest                     `yaml:"author_subject,omitempty"`
}

// NormalizedEventFieldProjection owns the exact value admitted after its
// optional provider conversion. Schema is the source for normalization,
// compiler assignability, event catalogs, and capability projection.
type NormalizedEventFieldProjection struct {
	From     string                           `yaml:"from" json:"from"`
	Schema   runtimecontracts.ToolInputSchema `yaml:"schema" json:"schema"`
	Optional bool                             `yaml:"optional,omitempty" json:"optional,omitempty"`
	Convert  string                           `yaml:"convert,omitempty" json:"convert,omitempty"`
}

func (p NormalizedEventFieldProjection) normalized() NormalizedEventFieldProjection {
	p.From = strings.TrimSpace(p.From)
	p.Schema = cloneNormalizedEventSchema(p.Schema)
	p.Convert = strings.ToLower(strings.TrimSpace(p.Convert))
	return p
}

type AuthorSubjectManifest struct {
	Type  string `yaml:"type"`
	Field string `yaml:"field"`
}

type NormalizedEventWhen struct {
	Exists []string          `yaml:"exists,omitempty"`
	Absent []string          `yaml:"absent,omitempty"`
	Equals map[string]string `yaml:"equals,omitempty"`
}

type OutputManifest struct {
	Kind               OutputKind
	EventName          EventNameManifest
	Event              string
	Fields             map[string]NormalizedEventFieldProjection
	When               NormalizedEventWhen
	AuthorSummaryField string
	AuthorSubject      AuthorSubjectManifest
}

type NormalizationError struct {
	Event string
	Path  string
	Cause string
}

func (e NormalizationError) Error() string {
	return fmt.Sprintf("normalized event %q field path %q failed: %s", e.Event, e.Path, e.Cause)
}

var normalizedFieldNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (m Manifest) OutputManifest() []OutputManifest {
	out := []OutputManifest{{Kind: OutputKindRaw, EventName: m.EventName}}
	for _, item := range m.NormalizedEvents {
		fields := make(map[string]NormalizedEventFieldProjection, len(item.Fields))
		for name, field := range item.Fields {
			fields[strings.TrimSpace(name)] = field.normalized()
		}
		out = append(out, OutputManifest{
			Kind: OutputKindNormalized, Event: strings.TrimSpace(item.Event),
			Fields: fields, When: item.When.normalized(fields), AuthorSummaryField: strings.TrimSpace(item.AuthorSummaryField), AuthorSubject: item.AuthorSubject.normalized(),
		})
	}
	return out
}

func (m Manifest) validateNormalizedEvents() error {
	provider := NormalizeProviderName(m.Provider)
	seen := map[string]struct{}{}
	branches := make([]OutputManifest, 0, len(m.NormalizedEvents))
	for index, item := range m.NormalizedEvents {
		eventName := strings.TrimSpace(item.Event)
		if eventName == "" {
			return fmt.Errorf("%s normalized_events[%d].event is required", provider, index)
		}
		if !strings.HasPrefix(eventName, "inbound."+provider+".") {
			return fmt.Errorf("%s normalized event %q must use inbound.%s.* namespace", provider, eventName, provider)
		}
		if !eventidentity.IsValidName(eventName) {
			return fmt.Errorf("%s normalized event %q is not a valid canonical event name", provider, eventName)
		}
		if m.EventName.Accepts(eventName) {
			return fmt.Errorf("%s normalized event %q collides with the raw event-name policy", provider, eventName)
		}
		if _, exists := seen[eventName]; exists {
			return fmt.Errorf("%s normalized event %q duplicates another declared output", provider, eventName)
		}
		seen[eventName] = struct{}{}
		if len(item.Fields) == 0 {
			return fmt.Errorf("%s normalized event %q requires fields", provider, eventName)
		}
		summaryField := strings.TrimSpace(item.AuthorSummaryField)
		if summaryField != "" {
			field, ok := item.Fields[summaryField]
			if !ok {
				return fmt.Errorf("%s normalized event %q author_summary_field %q is not a declared field", provider, eventName, summaryField)
			}
			field = field.normalized()
			if field.Schema.Type != "string" {
				return fmt.Errorf("%s normalized event %q author_summary_field %q must project text", provider, eventName, summaryField)
			}
		}
		authorSubject := item.AuthorSubject.normalized()
		if (authorSubject.Type == "") != (authorSubject.Field == "") {
			return fmt.Errorf("%s normalized event %q author_subject requires type and field together", provider, eventName)
		}
		if authorSubject.Type != "" {
			if authorSubject.Type != "chat" {
				return fmt.Errorf("%s normalized event %q author_subject type %q is not registered", provider, eventName, authorSubject.Type)
			}
			field, ok := item.Fields[authorSubject.Field]
			if !ok {
				return fmt.Errorf("%s normalized event %q author_subject field %q is not declared", provider, eventName, authorSubject.Field)
			}
			field = field.normalized()
			if field.Schema.Type != "string" {
				return fmt.Errorf("%s normalized event %q author_subject field %q must project text", provider, eventName, authorSubject.Field)
			}
		}
		fields := make(map[string]NormalizedEventFieldProjection, len(item.Fields))
		for declaredName, field := range item.Fields {
			name := strings.TrimSpace(declaredName)
			if declaredName != name {
				return fmt.Errorf("%s normalized event %q field name %q is not canonical; remove surrounding whitespace", provider, eventName, declaredName)
			}
			if _, duplicate := fields[name]; duplicate {
				return fmt.Errorf("%s normalized event %q field name %q duplicates another field after normalization", provider, eventName, declaredName)
			}
			field = field.normalized()
			if !normalizedFieldNamePattern.MatchString(name) {
				return fmt.Errorf("%s normalized event %q has invalid field name %q", provider, eventName, name)
			}
			if _, err := runtimepaths.ParseStrictRelative(field.From); err != nil {
				return fmt.Errorf("%s normalized event %q field %q: %w", provider, eventName, name, err)
			}
			if err := validateNormalizedEventFieldSchema(provider, eventName, name, field.Schema); err != nil {
				return err
			}
			switch field.Convert {
			case "":
			case runtimecontracts.FieldProjectionConvertNumberToText:
				if field.Schema.Type != "string" {
					return fmt.Errorf("%s normalized event %q field %q conversion number_to_text requires a string output schema", provider, eventName, name)
				}
			default:
				return fmt.Errorf("%s normalized event %q field %q has unsupported conversion %q; use number_to_text or remove convert", provider, eventName, name, field.Convert)
			}
			fields[name] = field
		}
		when, err := validateNormalizedWhen(provider, eventName, item.When, fields)
		if err != nil {
			return err
		}
		branches = append(branches, OutputManifest{Kind: OutputKindNormalized, Event: eventName, Fields: fields, When: when})
	}
	for i := 0; i < len(branches); i++ {
		for j := i + 1; j < len(branches); j++ {
			if normalizedBranchesExclusive(branches[i].When, branches[j].When) {
				continue
			}
			return fmt.Errorf("%s normalized events %q and %q can match the same payload; add when.absent to one branch so their exists/absent predicates are mutually exclusive", provider, branches[i].Event, branches[j].Event)
		}
	}
	return nil
}

func validateNormalizedWhen(provider, eventName string, declared NormalizedEventWhen, fields map[string]NormalizedEventFieldProjection) (NormalizedEventWhen, error) {
	when := declared.normalized(fields)
	for _, path := range append(append([]string{}, when.Exists...), when.Absent...) {
		if _, err := runtimepaths.ParseStrictRelative(path); err != nil {
			return NormalizedEventWhen{}, fmt.Errorf("%s normalized event %q when path: %w", provider, eventName, err)
		}
	}
	for path, value := range when.Equals {
		if _, err := runtimepaths.ParseStrictRelative(path); err != nil {
			return NormalizedEventWhen{}, fmt.Errorf("%s normalized event %q when.equals path: %w", provider, eventName, err)
		}
		if value == "" {
			return NormalizedEventWhen{}, fmt.Errorf("%s normalized event %q when.equals[%q] must be non-empty", provider, eventName, path)
		}
	}
	for _, exists := range when.Exists {
		for _, absent := range when.Absent {
			if pathIsSameOrDescendant(exists, absent) {
				return NormalizedEventWhen{}, fmt.Errorf("%s normalized event %q requires path %q while declaring ancestor %q absent", provider, eventName, exists, absent)
			}
		}
	}
	return when, nil
}

func (w NormalizedEventWhen) normalized(fields map[string]NormalizedEventFieldProjection) NormalizedEventWhen {
	exists := append([]string{}, w.Exists...)
	for _, field := range fields {
		if !field.Optional {
			exists = append(exists, field.From)
		}
	}
	equals := make(map[string]string, len(w.Equals))
	for path, value := range w.Equals {
		path = strings.TrimSpace(path)
		value = strings.TrimSpace(value)
		if path != "" {
			equals[path] = value
		}
	}
	if len(equals) == 0 {
		equals = nil
	}
	return NormalizedEventWhen{Exists: normalizedPaths(exists), Absent: normalizedPaths(w.Absent), Equals: equals}
}

func normalizedPaths(in []string) []string {
	seen := map[string]struct{}{}
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item != "" {
			seen[item] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for item := range seen {
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func normalizedBranchesExclusive(left, right NormalizedEventWhen) bool {
	for _, exists := range left.Exists {
		for _, absent := range right.Absent {
			if pathIsSameOrDescendant(exists, absent) {
				return true
			}
		}
	}
	for _, exists := range right.Exists {
		for _, absent := range left.Absent {
			if pathIsSameOrDescendant(exists, absent) {
				return true
			}
		}
	}
	return false
}

func validateNormalizedEventFieldSchema(provider, eventName, fieldName string, schema runtimecontracts.ToolInputSchema) error {
	subject := fmt.Sprintf("%s normalized event %q field %q schema", provider, eventName, fieldName)
	if err := runtimecontracts.ValidateToolInputSchema(schema); err != nil {
		return fmt.Errorf("%s: %w", subject, err)
	}
	return nil
}

func pathIsSameOrDescendant(candidate, ancestor string) bool {
	candidate = strings.TrimSpace(candidate)
	ancestor = strings.TrimSpace(ancestor)
	return candidate == ancestor || strings.HasPrefix(candidate, ancestor+".")
}

func (m Manifest) normalizedDeliveryEvents(payload any) ([]DeliveryEvent, error) {
	var matched []OutputManifest
	for _, output := range m.OutputManifest() {
		if output.Kind != OutputKindNormalized || !normalizedWhenMatches(payload, output.When) {
			continue
		}
		matched = append(matched, output)
	}
	if len(matched) > 1 {
		names := make([]string, 0, len(matched))
		for _, output := range matched {
			names = append(names, output.Event)
		}
		sort.Strings(names)
		return nil, badRequest("verified provider trigger normalized-event plan matched multiple branches: " + strings.Join(names, ", "))
	}
	if len(matched) == 0 {
		return nil, nil
	}
	output := matched[0]
	normalized := make(map[string]any, len(output.Fields))
	fieldNames := make([]string, 0, len(output.Fields))
	for name := range output.Fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	for _, name := range fieldNames {
		field := output.Fields[name]
		value, ok := valueFromRelativePath(payload, field.From)
		if !ok {
			if field.Optional {
				continue
			}
			return nil, NormalizationError{Event: output.Event, Path: field.From, Cause: "required value is missing"}
		}
		converted, err := normalizeProjectedValue(value, field)
		if err != nil {
			return nil, NormalizationError{Event: output.Event, Path: field.From, Cause: err.Error()}
		}
		normalized[name] = converted
	}
	result := DeliveryEvent{Name: events.EventType(output.Event), Kind: OutputKindNormalized, Payload: normalized}
	if output.AuthorSummaryField != "" {
		if value, ok := normalized[output.AuthorSummaryField].(string); ok {
			result.AuthorSummary = value
		}
	}
	if output.AuthorSubject.Type != "" {
		if value, ok := normalized[output.AuthorSubject.Field].(string); ok {
			result.AuthorSubjectType = output.AuthorSubject.Type
			result.AuthorSubjectID = value
		}
	}
	return []DeliveryEvent{result}, nil
}

func (m AuthorSubjectManifest) normalized() AuthorSubjectManifest {
	m.Type = strings.TrimSpace(m.Type)
	m.Field = strings.TrimSpace(m.Field)
	return m
}

func normalizedWhenMatches(payload any, when NormalizedEventWhen) bool {
	for _, path := range when.Exists {
		if _, ok := valueFromRelativePath(payload, path); !ok {
			return false
		}
	}
	for _, path := range when.Absent {
		if _, ok := valueFromRelativePath(payload, path); ok {
			return false
		}
	}
	for path, want := range when.Equals {
		got, ok := valueFromRelativePath(payload, path)
		if !ok {
			return false
		}
		text, ok := got.(string)
		if !ok || text != want {
			return false
		}
	}
	return true
}

func valueFromRelativePath(value any, path string) (any, bool) {
	parsed, err := runtimepaths.ParseStrictRelative(path)
	if err != nil {
		return nil, false
	}
	current := value
	for _, segment := range parsed.Segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok || current == nil {
			return nil, false
		}
	}
	return current, true
}

func normalizeProjectedValue(value any, field NormalizedEventFieldProjection) (any, error) {
	var normalized any
	var err error
	if field.Convert == runtimecontracts.FieldProjectionConvertNumberToText {
		normalized, err = exactNumberText(value)
	} else {
		normalized = value
	}
	if err != nil {
		return nil, err
	}
	if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(field.Schema), normalized); err != nil {
		return nil, fmt.Errorf("projected value violates its declared output schema: %w", err)
	}
	return normalized, nil
}

func exactNumberText(value any) (string, error) {
	switch typed := value.(type) {
	case json.Number:
		text := typed.String()
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return "", fmt.Errorf("number_to_text requires an integer JSON number, got %q", text)
		}
		return text, nil
	case int:
		return strconv.Itoa(typed), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || math.Abs(typed) > 1<<53 {
			return "", fmt.Errorf("number_to_text requires an exact integer number")
		}
		return strconv.FormatInt(int64(typed), 10), nil
	default:
		return "", fmt.Errorf("number_to_text requires a numeric value, got %T", value)
	}
}

func (m Manifest) eventCatalogEntries() map[string]runtimecontracts.EventCatalogEntry {
	out := map[string]runtimecontracts.EventCatalogEntry{}
	if literal := strings.TrimSpace(m.EventName.Literal); literal != "" {
		out[literal] = RawEventCatalogEntry()
	}
	for _, normalized := range m.NormalizedEvents {
		entry := runtimecontracts.EventCatalogEntry{
			Source: "provider_trigger_pack_normalized", Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"},
			Payload:            runtimecontracts.EventPayloadSpec{Type: "object", Properties: map[string]runtimecontracts.EventFieldSpec{}},
			AuthorSummaryField: strings.TrimSpace(normalized.AuthorSummaryField),
		}
		for name, projection := range normalized.Fields {
			projection = projection.normalized()
			name = strings.TrimSpace(name)
			entry.Payload.Properties[name] = normalizedEventFieldSpec(projection.Schema)
			if !projection.Optional {
				entry.Required = append(entry.Required, strings.TrimSpace(name))
			}
		}
		sort.Strings(entry.Required)
		entry.Payload.Required = append([]string{}, entry.Required...)
		out[strings.TrimSpace(normalized.Event)] = entry
	}
	return out
}

func normalizedEventFieldSpec(schema runtimecontracts.ToolInputSchema) runtimecontracts.EventFieldSpec {
	exact := runtimecontracts.CloneToolInputSchema(schema)
	typeName := strings.ToLower(strings.TrimSpace(schema.Type))
	if typeName == "string" {
		typeName = "text"
	}
	return runtimecontracts.EventFieldSpec{
		Type:        typeName,
		ExactSchema: &exact,
		Refinements: runtimecontracts.SchemaRefinements{
			Pattern: schema.Pattern,
			Length:  runtimecontracts.SchemaLengthRefinement{Min: cloneNormalizedInt(schema.MinLength), Max: cloneNormalizedInt(schema.MaxLength)},
			Range:   runtimecontracts.SchemaRangeRefinement{Min: cloneNormalizedFloat(schema.Minimum), Max: cloneNormalizedFloat(schema.Maximum)},
		},
	}
}

func cloneNormalizedEventSchema(in runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return runtimecontracts.CloneToolInputSchema(in)
}

func cloneNormalizedInt(in *int) *int {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneNormalizedFloat(in *float64) *float64 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func RawEventCatalogEntry() runtimecontracts.EventCatalogEntry {
	properties := map[string]runtimecontracts.EventFieldSpec{
		"entity_id":            {Type: "text"},
		"provider":             {Type: "text"},
		"event_type":           {Type: "text"},
		"provider_event_type":  {Type: "text"},
		"provider_event_id":    {Type: "text"},
		"provider_delivery_id": {Type: "text"},
		"payload":              {Type: "json"},
		"headers":              {Type: "json"},
		"received_at":          {Type: "text"},
	}
	required := make([]string, 0, len(properties))
	for name := range properties {
		required = append(required, name)
	}
	sort.Strings(required)
	return runtimecontracts.EventCatalogEntry{
		Source: "provider_trigger_pack_raw", Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"},
		Payload:  runtimecontracts.EventPayloadSpec{Type: "object", Properties: properties, Required: append([]string{}, required...)},
		Required: required,
	}
}
