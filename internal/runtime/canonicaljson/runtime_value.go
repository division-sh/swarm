package canonicaljson

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"unicode/utf8"
)

// CloneRuntimeValue validates and isolates an already projected JSON carrier.
// Unlike FromGo it does not accept DTOs or erase int/double execution kinds.
func CloneRuntimeValue(value any) (any, error) {
	return cloneRuntimeValue(value, map[visit]struct{}{})
}

func cloneRuntimeValue(value any, seen map[visit]struct{}) (any, error) {
	switch typed := value.(type) {
	case nil, bool:
		return typed, nil
	case string:
		if !utf8.ValidString(typed) {
			return nil, admissionErrorf("JSON string is not valid UTF-8")
		}
		return typed, nil
	case map[string]any:
		if typed == nil {
			return nil, nil
		}
		key := visit{typeID: reflect.TypeOf(typed), ptr: reflect.ValueOf(typed).Pointer()}
		if _, exists := seen[key]; exists {
			return nil, admissionErrorf("JSON value contains a cycle")
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		out := make(map[string]any, len(typed))
		keys := make([]string, 0, len(typed))
		for name := range typed {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		for _, name := range keys {
			item := typed[name]
			if !utf8.ValidString(name) {
				return nil, admissionErrorf("JSON object key is not valid UTF-8")
			}
			cloned, err := cloneRuntimeValue(item, seen)
			if err != nil {
				return nil, fmt.Errorf("JSON member %q: %w", name, err)
			}
			out[name] = cloned
		}
		return out, nil
	case []any:
		if typed == nil {
			return nil, nil
		}
		key := visit{typeID: reflect.TypeOf(typed), ptr: reflect.ValueOf(typed).Pointer()}
		if _, exists := seen[key]; exists {
			return nil, admissionErrorf("JSON value contains a cycle")
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		out := make([]any, len(typed))
		for index, item := range typed {
			cloned, err := cloneRuntimeValue(item, seen)
			if err != nil {
				return nil, fmt.Errorf("JSON index %d: %w", index, err)
			}
			out[index] = cloned
		}
		return out, nil
	default:
		return NormalizeRuntimeNumber(value)
	}
}

// NormalizeRuntimeNumber admits the shared safe numeric domain without treating
// a numeric declaration as an instruction to manufacture a double.
func NormalizeRuntimeNumber(value any) (any, error) {
	number, err := NormalizeNumber(value)
	if err != nil {
		return nil, err
	}
	if lexical, ok := value.(json.Number); ok {
		if integer, err := lexical.Int64(); err == nil {
			return integer, nil
		}
		return number, nil
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(number), nil
	case reflect.Float32, reflect.Float64:
		return number, nil
	default:
		return nil, admissionErrorf("runtime JSON requires a projected number, got %T", value)
	}
}
