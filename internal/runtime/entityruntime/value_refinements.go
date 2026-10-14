package entityruntime

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

func validateValueRefinements(contract Contract, fieldName, typeRef string, refinements runtimecontracts.SchemaRefinements, value any) (any, error) {
	if refinements.Empty() {
		return value, nil
	}
	if err := validatePatternRefinement(fieldName, refinements.Pattern, value); err != nil {
		return nil, err
	}
	if !refinements.Length.Empty() {
		if err := validateLengthRefinement(fieldName, refinements, value); err != nil {
			return nil, err
		}
	}
	if !refinements.Range.Empty() {
		if err := validateRangeRefinement(contract, fieldName, typeRef, refinements, value); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func validatePatternRefinement(fieldName, pattern string, value any) error {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return fieldTypeError(fieldName, "must be string for pattern refinement")
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return fieldTypeError(fieldName, "has invalid pattern refinement")
	}
	if !compiled.MatchString(text) {
		return fieldTypeError(fieldName, "must match pattern")
	}
	return nil
}

func validateLengthRefinement(fieldName string, refinements runtimecontracts.SchemaRefinements, value any) error {
	length, ok := refinementLength(value)
	if !ok {
		return fieldTypeError(fieldName, "must be string or list for length refinement")
	}
	if min := refinements.Length.Min; min != nil && length < *min {
		return fieldTypeError(fieldName, fmt.Sprintf("length must be >= %d", *min))
	}
	if max := refinements.Length.Max; max != nil && length > *max {
		return fieldTypeError(fieldName, fmt.Sprintf("length must be <= %d", *max))
	}
	return nil
}

func validateRangeRefinement(contract Contract, fieldName, typeRef string, refinements runtimecontracts.SchemaRefinements, value any) error {
	if !isIntegerType(contract, typeRef) && !isNumericType(contract, typeRef) {
		return fieldTypeError(fieldName, "must be numeric for range refinement")
	}
	number, ok := runtimesharedjson.AsFloat64(value)
	if !ok {
		return fieldTypeError(fieldName, "must be numeric for range refinement")
	}
	if min := refinements.Range.Min; min != nil && number < *min {
		return fieldTypeError(fieldName, fmt.Sprintf("must be >= %v", *min))
	}
	if max := refinements.Range.Max; max != nil && number > *max {
		return fieldTypeError(fieldName, fmt.Sprintf("must be <= %v", *max))
	}
	return nil
}

func refinementLength(value any) (int, bool) {
	switch typed := value.(type) {
	case string:
		return utf8.RuneCountInString(typed), true
	default:
		items, ok := listValues(value)
		if !ok {
			return 0, false
		}
		return len(items), true
	}
}

func validateEntityFieldEqualities(context string, fields map[string]runtimecontracts.EntityFieldDecl, values map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	for name, field := range fields {
		name = strings.TrimSpace(name)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if name == "" || target == "" {
			continue
		}
		_, leftPresent := values[name]
		_, rightPresent := values[target]
		if !leftPresent && !rightPresent {
			continue
		}
		if err := validateEqualityValue(context, name, target, values); err != nil {
			return err
		}
	}
	return nil
}

func validateStructuralFieldEqualities(context string, fields []runtimecontracts.ResolvedCatalogField, values map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if name == "" || target == "" {
			continue
		}
		_, leftPresent := values[name]
		_, rightPresent := values[target]
		targetField, targetDeclared := resolvedStructuralField(fields, target)
		if !leftPresent && !rightPresent && field.IsOptional && targetDeclared && targetField.IsOptional {
			continue
		}
		if err := validateEqualityValue(context, name, target, values); err != nil {
			return err
		}
	}
	return nil
}

func validateEqualityValue(context, name, target string, values map[string]any) error {
	left, ok := values[name]
	if !ok {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target)+" but source is missing")
	}
	right, ok := values[target]
	if !ok {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target)+" but target is missing")
	}
	if !reflect.DeepEqual(left, right) {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target))
	}
	return nil
}

func resolvedStructuralField(fields []runtimecontracts.ResolvedCatalogField, name string) (runtimecontracts.ResolvedCatalogField, bool) {
	name = strings.TrimSpace(name)
	for _, field := range fields {
		if field.Name == name {
			return field, true
		}
	}
	return runtimecontracts.ResolvedCatalogField{}, false
}
