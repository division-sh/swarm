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
	return validateToolSchemaSubset(subject, source, target, false)
}

func validateToolSchemaSubset(subject string, source, target ToolInputSchema, declaredProperties bool) error {
	_, sourceBranches := source.OneOfSchemas()
	_, targetBranches := target.OneOfSchemas()
	if sourceBranches || targetBranches {
		// General exclusive-union implication is outside this finite proof owner.
		_, targetEnum := target.EnumValues()
		if toolSchemaRelationEqual(source, target, declaredProperties) || (target.Kind() == ToolSchemaAny && !targetBranches && !targetEnum && target.EqualTo() == "") {
			return nil
		}
		return fmt.Errorf("%s oneOf schemas are not provably assignable unless identical", subject)
	}
	if err := validateToolSchemaEqualitySubset(subject, source, target, declaredProperties); err != nil {
		return err
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
	return validateToolSchemaKindSubset(subject, source, target)
}

func validateToolSchemaKindSubset(subject string, source, target ToolInputSchema) error {
	sourceType := string(source.Kind())
	targetType := string(target.Kind())
	if sourceType != targetType && !(sourceType == "integer" && targetType == "number") {
		return fmt.Errorf("%s has incompatible types %s and %s", subject, sourceType, targetType)
	}

	switch sourceType {
	case "string":
		return validateToolSchemaStringSubset(subject, source, target)
	case "integer", "number":
		return validateToolSchemaFloatBoundsSubset(subject+" numeric range", source.Minimum, source.Maximum, target.Minimum, target.Maximum)
	case "array":
		if err := validateToolSchemaIntBoundsSubset(subject+" array length", source.MinItems, source.MaxItems, target.MinItems, target.MaxItems); err != nil {
			return err
		}
		targetItems, targetHasItems := target.ItemsSchema()
		if targetHasItems {
			sourceItems, sourceHasItems := source.ItemsSchema()
			if !sourceHasItems {
				return fmt.Errorf("%s source array items are unconstrained while target items are constrained", subject)
			}
			return validateToolSchemaSubset(subject+"[]", sourceItems, targetItems, false)
		}
	case "object":
		return validateToolSchemaObjectSubset(subject, source, target)
	case "boolean", "null":
	default:
		return fmt.Errorf("%s has unsupported schema type %q", subject, sourceType)
	}
	return nil
}

func validateToolSchemaEqualitySubset(subject string, source, target ToolInputSchema, declaredProperties bool) error {
	if target.EqualTo() == "" {
		return nil
	}
	if source.EqualTo() != target.EqualTo() {
		return fmt.Errorf("%s source equality target %q is not provably assignable to target equality %q", subject, source.EqualTo(), target.EqualTo())
	}
	if !declaredProperties {
		return fmt.Errorf("%s target equality %q requires its enclosing object relation", subject, target.EqualTo())
	}
	return nil
}

func toolSchemaRelationEqual(source, target ToolInputSchema, declaredProperties bool) bool {
	if !declaredProperties {
		return source.Equal(target)
	}
	// Sibling equality is valid on declared properties, not standalone roots.
	for _, schema := range []ToolInputSchema{source, target} {
		if err := validateAdmittedToolInputSchemaActive("$", schema, 0, true, map[*toolInputSchemaValue]struct{}{}); err != nil {
			return false
		}
	}
	left, err := canonicaljson.Hash(source.Projection())
	if err != nil {
		return false
	}
	right, err := canonicaljson.Hash(target.Projection())
	return err == nil && left == right
}

func validateToolSchemaStringSubset(subject string, source, target ToolInputSchema) error {
	if err := validateToolSchemaIntBoundsSubset(subject+" string length", source.MinLength, source.MaxLength, target.MinLength, target.MaxLength); err != nil {
		return err
	}
	if target.Pattern() != "" && source.Pattern() != target.Pattern() {
		return fmt.Errorf("%s source pattern %q is not provably assignable to target pattern %q", subject, source.Pattern(), target.Pattern())
	}
	if target.Format() != "" && source.Format() != target.Format() {
		return fmt.Errorf("%s source format %q is not provably assignable to target format %q", subject, source.Format(), target.Format())
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
		if _, ok := targetEnums[key]; targetDeclared && !ok {
			return false, fmt.Errorf("%s source enum value %s is absent from target enum", subject, key)
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

func validateToolSchemaIntBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax func() (int, bool)) error {
	sMin, sHasMin := sourceMin()
	sMax, sHasMax := sourceMax()
	tMin, tHasMin := targetMin()
	tMax, tHasMax := targetMax()
	if tHasMin && (!sHasMin || sMin < tMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %d", subject, tMin)
	}
	if tHasMax && (!sHasMax || sMax > tMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %d", subject, tMax)
	}
	return nil
}

func validateToolSchemaFloatBoundsSubset(subject string, sourceMin, sourceMax, targetMin, targetMax func() (float64, bool)) error {
	sMin, sHasMin := sourceMin()
	sMax, sHasMax := sourceMax()
	tMin, tHasMin := targetMin()
	tMax, tHasMax := targetMax()
	if tHasMin && (!sHasMin || sMin < tMin) {
		return fmt.Errorf("%s source minimum is broader than target minimum %v", subject, tMin)
	}
	if tHasMax && (!sHasMax || sMax > tMax) {
		return fmt.Errorf("%s source maximum is broader than target maximum %v", subject, tMax)
	}
	return nil
}

func validateToolSchemaObjectSubset(subject string, source, target ToolInputSchema) error {
	if err := validateToolSchemaPropertyNamesSubset(subject, source, target); err != nil {
		return err
	}
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
			if err := validateToolSchemaSubset(subject+"."+name, sourceProperty, targetProperty, true); err != nil {
				return err
			}
			continue
		}
		if !targetAdditional.allowed {
			return fmt.Errorf("%s source property %q is forbidden by target", subject, name)
		}
		if targetAdditional.schema != nil {
			if err := validateToolSchemaSubset(subject+"."+name, sourceProperty, *targetAdditional.schema, false); err != nil {
				return err
			}
		}
	}

	if sourceAdditional.allowed {
		if err := validateToolSchemaOpenSourceMembersSubset(subject, sourceProperties, sourceAdditional, target); err != nil {
			return err
		}
	}
	if !sourceAdditional.allowed || (targetAdditional.allowed && targetAdditional.schema == nil) {
		return nil
	}
	if !targetAdditional.allowed {
		return fmt.Errorf("%s source admits additional properties while target is closed", subject)
	}
	if sourceAdditional.schema == nil {
		return fmt.Errorf("%s source admits unconstrained additional properties while target constrains them", subject)
	}
	return validateToolSchemaSubset(subject+".*", *sourceAdditional.schema, *targetAdditional.schema, false)
}

func validateToolSchemaOpenSourceMembersSubset(subject string, sourceProperties map[string]struct{}, sourceAdditional admittedToolSchemaAdditionalPropertyConstraint, target ToolInputSchema) error {
	for _, name := range target.PropertyNames() {
		if _, declaredBySource := sourceProperties[name]; declaredBySource {
			continue
		}
		targetProperty, _ := target.Property(name)
		sourceProperty := sourceAdditional.schema
		if sourceProperty == nil {
			if targetProperty.Kind() != ToolSchemaAny {
				return fmt.Errorf("%s source admits unconstrained additional property %q while target constrains it", subject, name)
			}
			unrestricted := MustToolInputSchema(ToolSchemaAny)
			sourceProperty = &unrestricted
		}
		if err := validateToolSchemaSubset(subject+"."+name, *sourceProperty, targetProperty, false); err != nil {
			return err
		}
	}
	return nil
}

func validateToolSchemaPropertyNamesSubset(subject string, source, target ToolInputSchema) error {
	targetNames, constrained := target.PropertyNamesSchema()
	if !constrained {
		return nil
	}
	if sourceNames, sourceConstrained := source.PropertyNamesSchema(); sourceConstrained {
		return validateToolSchemaSubset(subject+" propertyNames", sourceNames, targetNames, false)
	}
	if admittedToolSchemaAdditionalProperties(source).allowed {
		return fmt.Errorf("%s source admits unconstrained property names while target constrains them", subject)
	}
	for _, name := range source.PropertyNames() {
		if err := targetNames.Validate(name); err != nil {
			return fmt.Errorf("%s source property name %q is outside target propertyNames: %w", subject, name, err)
		}
	}
	return nil
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
