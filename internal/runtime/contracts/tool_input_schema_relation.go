package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// ValidateAssignableTo proves that every value admitted by the source is valid
// for the target. It intentionally implements the platform's finite schema
// subset rather than general JSON-Schema implication.
func (source ToolInputSchema) ValidateAssignableTo(subject string, target ToolInputSchema) error {
	if source.IsZero() || target.IsZero() {
		return fmt.Errorf("%s has no source or target schema", subject)
	}
	return validateToolSchemaSubset(subject, source, target)
}

func validateToolSchemaSubset(subject string, source, target ToolInputSchema) error {
	sourceType := string(source.Kind())
	targetType := string(target.Kind())
	if target.EqualTo() != "" && source.EqualTo() != target.EqualTo() {
		return fmt.Errorf("%s source equality target %q is not provably assignable to target equality %q", subject, source.EqualTo(), target.EqualTo())
	}

	finite, err := validateFiniteToolSchemaSourceEnum(subject, source, target)
	if err != nil {
		return err
	}
	if finite {
		return nil
	}
	if _, declared := target.EnumValues(); declared {
		return fmt.Errorf("%s target enum is narrower than an unbounded source", subject)
	}
	if target.Kind() == ToolSchemaAny {
		return nil
	}
	if sourceType != targetType && !(sourceType == "integer" && targetType == "number") {
		return fmt.Errorf("%s has incompatible types %s and %s", subject, sourceType, targetType)
	}

	switch sourceType {
	case "string":
		if err := validateToolSchemaIntBoundsSubset(subject+" string length", admittedToolSchemaInt(source.MinLength), admittedToolSchemaInt(source.MaxLength), admittedToolSchemaInt(target.MinLength), admittedToolSchemaInt(target.MaxLength)); err != nil {
			return err
		}
		if target.Pattern() != "" && source.Pattern() != target.Pattern() {
			return fmt.Errorf("%s source pattern %q is not provably assignable to target pattern %q", subject, source.Pattern(), target.Pattern())
		}
		if target.Format() != "" && source.Format() != target.Format() {
			return fmt.Errorf("%s source format %q is not provably assignable to target format %q", subject, source.Format(), target.Format())
		}
	case "integer", "number":
		if err := validateToolSchemaFloatBoundsSubset(subject+" numeric range", admittedToolSchemaFloat(source.Minimum), admittedToolSchemaFloat(source.Maximum), admittedToolSchemaFloat(target.Minimum), admittedToolSchemaFloat(target.Maximum)); err != nil {
			return err
		}
	case "array":
		if err := validateToolSchemaIntBoundsSubset(subject+" array length", admittedToolSchemaInt(source.MinItems), admittedToolSchemaInt(source.MaxItems), admittedToolSchemaInt(target.MinItems), admittedToolSchemaInt(target.MaxItems)); err != nil {
			return err
		}
		targetItems, targetHasItems := target.ItemsSchema()
		if targetHasItems {
			sourceItems, sourceHasItems := source.ItemsSchema()
			if !sourceHasItems {
				return fmt.Errorf("%s source array items are unconstrained while target items are constrained", subject)
			}
			if err := validateToolSchemaSubset(subject+"[]", sourceItems, targetItems); err != nil {
				return err
			}
		}
	case "object":
		return validateToolSchemaObjectSubset(subject, source, target)
	case "boolean", "null":
	default:
		return fmt.Errorf("%s has unsupported schema type %q", subject, sourceType)
	}
	return nil
}

func validateFiniteToolSchemaSourceEnum(subject string, source, target ToolInputSchema) (bool, error) {
	sourceValues, sourceDeclared := source.EnumValues()
	if !sourceDeclared {
		return false, nil
	}
	targetValues, targetDeclared := target.EnumValues()
	targetEnums, err := toolSchemaEnumSet(targetValues)
	if err != nil {
		return false, fmt.Errorf("%s target enum: %w", subject, err)
	}
	sourceConstraint := source.WithoutEnum()
	targetConstraint := target.WithoutEnum()
	for _, value := range sourceValues {
		raw, err := canonicaljson.Bytes(value.Interface())
		if err != nil {
			return false, fmt.Errorf("%s source enum: %w", subject, err)
		}
		key := string(raw)
		if err := sourceConstraint.Validate(value.Interface()); err != nil {
			return false, fmt.Errorf("%s source enum value is outside its declared schema: %w", subject, err)
		}
		if targetDeclared {
			if _, ok := targetEnums[key]; !ok {
				return false, fmt.Errorf("%s source enum value %s is absent from target enum", subject, key)
			}
		}
		if err := targetConstraint.Validate(value.Interface()); err != nil {
			return false, fmt.Errorf("%s source enum value is outside target schema: %w", subject, err)
		}
	}
	return true, nil
}

func toolSchemaEnumSet(values []semanticvalue.Value) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		raw, err := canonicaljson.Bytes(value.Interface())
		if err != nil {
			return nil, err
		}
		out[string(raw)] = struct{}{}
	}
	return out, nil
}

func admittedToolSchemaInt(accessor func() (int, bool)) *int {
	value, ok := accessor()
	if !ok {
		return nil
	}
	return &value
}

func admittedToolSchemaFloat(accessor func() (float64, bool)) *float64 {
	value, ok := accessor()
	if !ok {
		return nil
	}
	return &value
}

func validateToolSchemaIntBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax *int) error {
	if targetMin != nil && (sourceMin == nil || *sourceMin < *targetMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %d", subject, *targetMin)
	}
	if targetMax != nil && (sourceMax == nil || *sourceMax > *targetMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %d", subject, *targetMax)
	}
	return nil
}

func validateToolSchemaFloatBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax *float64) error {
	if targetMin != nil && (sourceMin == nil || *sourceMin < *targetMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %v", subject, *targetMin)
	}
	if targetMax != nil && (sourceMax == nil || *sourceMax > *targetMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %v", subject, *targetMax)
	}
	return nil
}

func validateToolSchemaObjectSubset(subject string, source, target ToolInputSchema) error {
	sourceRequired := toolSchemaStringSet(source.RequiredProperties())
	for _, name := range target.RequiredProperties() {
		if _, ok := sourceRequired[name]; !ok {
			return fmt.Errorf("%s target requires property %q that source does not require", subject, name)
		}
	}

	targetAdditional := admittedToolSchemaAdditionalProperties(target)
	sourceAdditional := admittedToolSchemaAdditionalProperties(source)
	sourceProperties := toolSchemaStringSet(source.PropertyNames())
	for _, name := range source.PropertyNames() {
		sourceProperty, _ := source.Property(name)
		if targetProperty, ok := target.Property(name); ok {
			if err := validateToolSchemaSubset(subject+"."+name, sourceProperty, targetProperty); err != nil {
				return err
			}
			continue
		}
		if !targetAdditional.allowed {
			return fmt.Errorf("%s source property %q is forbidden by target", subject, name)
		}
		if targetAdditional.schema != nil {
			if err := validateToolSchemaSubset(subject+"."+name, sourceProperty, *targetAdditional.schema); err != nil {
				return err
			}
		}
	}

	if sourceAdditional.allowed {
		for _, name := range target.PropertyNames() {
			if _, declaredBySource := sourceProperties[name]; declaredBySource {
				continue
			}
			targetProperty, _ := target.Property(name)
			if sourceAdditional.schema == nil {
				if targetProperty.Kind() != ToolSchemaAny {
					return fmt.Errorf("%s source admits unconstrained additional property %q while target constrains it", subject, name)
				}
				continue
			}
			if err := validateToolSchemaSubset(subject+"."+name, *sourceAdditional.schema, targetProperty); err != nil {
				return err
			}
		}
	}
	if !sourceAdditional.allowed {
		return nil
	}
	if !targetAdditional.allowed {
		return fmt.Errorf("%s source admits additional properties while target is closed", subject)
	}
	if targetAdditional.schema == nil {
		return nil
	}
	if sourceAdditional.schema == nil {
		return fmt.Errorf("%s source admits unconstrained additional properties while target constrains them", subject)
	}
	return validateToolSchemaSubset(subject+".*", *sourceAdditional.schema, *targetAdditional.schema)
}

type admittedToolSchemaAdditionalPropertyConstraint struct {
	allowed bool
	schema  *ToolInputSchema
}

func admittedToolSchemaAdditionalProperties(schema ToolInputSchema) admittedToolSchemaAdditionalPropertyConstraint {
	if allowed, declared := schema.AdditionalPropertiesAllowed(); declared {
		return admittedToolSchemaAdditionalPropertyConstraint{allowed: allowed}
	}
	if additional, ok := schema.AdditionalPropertiesSchema(); ok {
		return admittedToolSchemaAdditionalPropertyConstraint{allowed: true, schema: &additional}
	}
	return admittedToolSchemaAdditionalPropertyConstraint{allowed: true}
}

func toolSchemaStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}
