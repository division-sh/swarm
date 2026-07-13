package contracts

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// ValidateStandaloneWave1TypeReference rejects references that require a
// separate type catalog. Provider trigger manifests do not own such a catalog.
func ValidateStandaloneWave1TypeReference(raw, context string) error {
	if err := ValidateWave1TypeReference(raw, context); err != nil {
		return err
	}
	if _, err := (CatalogTypeReference{Type: strings.TrimSpace(raw)}).Resolve(); err != nil {
		return fmt.Errorf("%s type %q is not standalone-resolvable: %w", context, strings.TrimSpace(raw), err)
	}
	return nil
}

func ValidateStandaloneWave1Value(typeRef string, value any) error {
	resolved, err := (CatalogTypeReference{Type: strings.TrimSpace(typeRef)}).Resolve()
	if err != nil {
		return err
	}
	return validateStandaloneWave1Value(resolved, value)
}

func validateStandaloneWave1Value(expected ResolvedCatalogType, value any) error {
	switch expected.Kind {
	case CatalogTypeDynamic:
		return nil
	case CatalogTypeText:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("value type %T is incompatible with text", value)
		}
		return nil
	case CatalogTypeInteger:
		if !standaloneInteger(value) {
			return fmt.Errorf("value type %T is incompatible with integer", value)
		}
		return nil
	case CatalogTypeNumber:
		if !standaloneNumber(value) {
			return fmt.Errorf("value type %T is incompatible with numeric", value)
		}
		return nil
	case CatalogTypeBoolean:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("value type %T is incompatible with boolean", value)
		}
		return nil
	case CatalogTypeList:
		items, ok := standaloneList(value)
		if !ok || expected.Element == nil {
			return fmt.Errorf("value type %T is incompatible with list", value)
		}
		for index, item := range items {
			if err := validateStandaloneWave1Value(*expected.Element, item); err != nil {
				return fmt.Errorf("list item %d: %w", index, err)
			}
		}
		return nil
	case CatalogTypeMap:
		object, ok := value.(map[string]any)
		if !ok || expected.Key == nil || expected.Value == nil {
			return fmt.Errorf("value type %T is incompatible with map", value)
		}
		for key, item := range object {
			if err := validateStandaloneWave1Value(*expected.Key, key); err != nil {
				return fmt.Errorf("map key %q: %w", key, err)
			}
			if err := validateStandaloneWave1Value(*expected.Value, item); err != nil {
				return fmt.Errorf("map value at %q: %w", key, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("standalone projection cannot validate type kind %q", expected.Kind)
	}
}

func standaloneInteger(value any) bool {
	switch typed := value.(type) {
	case json.Number:
		_, err := typed.Int64()
		return err == nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32:
		value64 := float64(typed)
		return !math.IsNaN(value64) && !math.IsInf(value64, 0) && typed == float32(int64(typed))
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0) && math.Trunc(typed) == typed && math.Abs(typed) <= 1<<53
	default:
		return false
	}
}

func standaloneNumber(value any) bool {
	switch typed := value.(type) {
	case json.Number:
		_, err := typed.Float64()
		return err == nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32:
		value64 := float64(typed)
		return !math.IsNaN(value64) && !math.IsInf(value64, 0)
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0)
	default:
		return false
	}
}

func standaloneList(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	default:
		return nil, false
	}
}
