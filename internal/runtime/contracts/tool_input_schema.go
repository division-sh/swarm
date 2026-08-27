package contracts

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

const MaxToolInputSchemaDepth = 64

func validateToolSchemaValue(path string, schema ToolInputSchema, value semanticvalue.Value, checkEnum bool) error {
	if enum, declared := schema.EnumValues(); checkEnum && declared {
		matched := false
		for _, candidate := range enum {
			if value.Equal(candidate) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not one of the declared enum values", path)
		}
	}
	switch schema.Kind() {
	case ToolSchemaString:
		text, ok := value.String()
		if !ok {
			return fmt.Errorf("%s must be string", path)
		}
		switch schema.Format() {
		case "":
		case "uuid":
			if _, err := uuid.Parse(strings.TrimSpace(text)); err != nil {
				return fmt.Errorf("%s must be uuid", path)
			}
		case "date-time":
			if _, err := time.Parse(time.RFC3339, strings.TrimSpace(text)); err != nil {
				return fmt.Errorf("%s must be RFC3339 date-time", path)
			}
		default:
			panic("admitted tool schema contains unsupported string format")
		}
		if pattern := schema.Pattern(); pattern != "" && !regexp.MustCompile(pattern).MatchString(text) {
			return fmt.Errorf("%s does not match pattern %q", path, pattern)
		}
		length := utf8.RuneCountInString(text)
		if minimum, ok := schema.MinLength(); ok && length < minimum {
			return fmt.Errorf("%s length must be >= %d", path, minimum)
		}
		if maximum, ok := schema.MaxLength(); ok && length > maximum {
			return fmt.Errorf("%s length must be <= %d", path, maximum)
		}
	case ToolSchemaNumber, ToolSchemaInteger:
		number, ok := value.Number()
		if !ok || (schema.Kind() == ToolSchemaInteger && math.Trunc(number) != number) {
			return fmt.Errorf("%s must be %s", path, schema.Kind())
		}
		if minimum, ok := schema.Minimum(); ok && number < minimum {
			return fmt.Errorf("%s must be >= %v", path, minimum)
		}
		if maximum, ok := schema.Maximum(); ok && number > maximum {
			return fmt.Errorf("%s must be <= %v", path, maximum)
		}
	case ToolSchemaBoolean:
		if _, ok := value.Bool(); !ok {
			return fmt.Errorf("%s must be boolean", path)
		}
	case ToolSchemaNull:
		if value.Kind() != semanticvalue.KindNull {
			return fmt.Errorf("%s must be null", path)
		}
	case ToolSchemaArray:
		if value.Kind() != semanticvalue.KindArray {
			return fmt.Errorf("%s must be array", path)
		}
		if minimum, ok := schema.MinItems(); ok && value.Len() < minimum {
			return fmt.Errorf("%s length must be >= %d", path, minimum)
		}
		if maximum, ok := schema.MaxItems(); ok && value.Len() > maximum {
			return fmt.Errorf("%s length must be <= %d", path, maximum)
		}
		items, _ := schema.ItemsSchema()
		for index := 0; index < value.Len(); index++ {
			item, _ := value.At(index)
			if err := validateToolSchemaValue(fmt.Sprintf("%s[%d]", path, index), items, item, true); err != nil {
				return err
			}
		}
	case ToolSchemaObject:
		members, ok := value.ObjectMap()
		if !ok {
			return fmt.Errorf("%s must be object", path)
		}
		for _, name := range schema.RequiredProperties() {
			if _, exists := members[name]; !exists {
				return fmt.Errorf("%s.%s is required", path, name)
			}
		}
		for name, member := range members {
			property, known := schema.Property(name)
			if known {
				if err := validateToolSchemaValue(path+"."+name, property, member, true); err != nil {
					return err
				}
				continue
			}
			if additional, ok := schema.AdditionalPropertiesSchema(); ok {
				if err := validateToolSchemaValue(path+"."+name, additional, member, true); err != nil {
					return err
				}
				continue
			}
			if allowed, declared := schema.AdditionalPropertiesAllowed(); declared && !allowed {
				return fmt.Errorf("%s.%s is not allowed", path, name)
			}
		}
		for _, name := range schema.PropertyNames() {
			property, _ := schema.Property(name)
			target := property.EqualTo()
			if target == "" {
				continue
			}
			member, exists := members[name]
			if !exists {
				continue
			}
			other, exists := members[target]
			if !exists {
				return fmt.Errorf("%s.%s must equal %s.%s, but target is missing", path, name, path, target)
			}
			if !member.Equal(other) {
				return fmt.Errorf("%s.%s must equal %s.%s", path, name, path, target)
			}
		}
	}
	return nil
}

// cloneEventCatalogEntry copies authored event-catalog syntax. Admitted exact
// schemas are immutable values; only the surrounding YAML/topology carrier
// requires readback isolation.
func cloneEventCatalogEntry(in EventCatalogEntry) EventCatalogEntry {
	out := in
	out.Swarm.Producer = append([]string(nil), in.Swarm.Producer...)
	out.Swarm.Consumer = append([]string(nil), in.Swarm.Consumer...)
	out.Producer = append([]string(nil), in.Producer...)
	out.AlternateEmitters = append([]string(nil), in.AlternateEmitters...)
	out.Consumer = append([]string(nil), in.Consumer...)
	out.ConsumerType = append([]string(nil), in.ConsumerType...)
	out.Payload.Required = append([]string(nil), in.Payload.Required...)
	out.admissionProvenance = make(map[string]EffectiveValueProvenance, len(in.admissionProvenance))
	for path, provenance := range in.admissionProvenance {
		out.admissionProvenance[path] = cloneEffectiveValueProvenance(provenance)
	}
	out.Payload.Properties = make(map[string]EventFieldSpec, len(in.Payload.Properties))
	for name, field := range in.Payload.Properties {
		field.Citation.AllowedClasses = append([]string(nil), field.Citation.AllowedClasses...)
		field.Refinements.Length.Min = cloneToolSchemaInt(field.Refinements.Length.Min)
		field.Refinements.Length.Max = cloneToolSchemaInt(field.Refinements.Length.Max)
		field.Refinements.Range.Min = cloneToolSchemaFloat(field.Refinements.Range.Min)
		field.Refinements.Range.Max = cloneToolSchemaFloat(field.Refinements.Range.Max)
		if field.ExactSchema != nil {
			schema := *field.ExactSchema
			field.ExactSchema = &schema
		}
		out.Payload.Properties[name] = field
	}
	return out
}

func cloneEventCatalogEntries(in map[string]EventCatalogEntry) map[string]EventCatalogEntry {
	out := make(map[string]EventCatalogEntry, len(in))
	for name, entry := range in {
		out[name] = cloneEventCatalogEntry(entry)
	}
	return out
}

func cloneToolSchemaInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneToolSchemaFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
