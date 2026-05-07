package contracts

import (
	"fmt"
	"regexp"
	"strings"
)

var nodeStateNumericTypePattern = regexp.MustCompile(`(?i)^numeric\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
var nodeStateNamedTypePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]*$`)

// NormalizeNodeStateFieldType enforces the supported canonical vocabulary for
// node-local accumulator/state fields. Unlike the Wave 1 entity/event contract
// surface, node state still allows jsonb and float-like primitives, but it does
// not allow descriptive prose or inline modifiers to masquerade as a type.
func NormalizeNodeStateFieldType(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("state_schema field type is required")
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		base := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
		if base == "" {
			return "", fmt.Errorf("state_schema list type requires an element type")
		}
		if !nodeStateNamedTypePattern.MatchString(base) {
			return "", fmt.Errorf("unsupported state_schema named list type %q; use [NamedType] for declared types.yaml references", raw)
		}
		return "[" + base + "]", nil
	}
	if strings.HasPrefix(raw, "list<") && strings.HasSuffix(raw, ">") {
		base := strings.TrimSpace(raw[len("list<") : len(raw)-1])
		if base == "" {
			return "", fmt.Errorf("state_schema list type requires an element type")
		}
		if !nodeStateNamedTypePattern.MatchString(base) {
			return "", fmt.Errorf("unsupported state_schema named list type %q; use list<NamedType> for declared types.yaml references", raw)
		}
		return "list<" + base + ">", nil
	}
	if strings.HasSuffix(raw, "[]") {
		base := strings.TrimSpace(strings.TrimSuffix(raw, "[]"))
		if base == "" {
			return "", fmt.Errorf("state_schema list type requires an element type")
		}
		normalizedBase, err := NormalizeNodeStateFieldType(base)
		if err != nil {
			return "", err
		}
		if named, ok := NodeStateNamedTypeName(normalizedBase); ok {
			return "[" + named + "]", nil
		}
		return normalizedBase + "[]", nil
	}
	if matches := nodeStateNumericTypePattern.FindStringSubmatch(raw); len(matches) == 3 {
		return fmt.Sprintf("numeric(%s,%s)", matches[1], matches[2]), nil
	}

	normalized := strings.ToLower(raw)
	switch normalized {
	case "text", "string", "integer", "int", "bigint", "float", "double", "real", "numeric", "boolean", "bool", "jsonb", "json", "timestamp", "timestamptz", "uuid":
		switch normalized {
		case "bool":
			return "boolean", nil
		default:
			return normalized, nil
		}
	}

	if strings.ContainsAny(raw, " \t\r\n") {
		return "", fmt.Errorf("state_schema field type %q is not canonical; descriptive notes and inline modifiers must not appear in type declarations", raw)
	}
	if nodeStateNamedTypePattern.MatchString(raw) {
		return raw, nil
	}
	return "", fmt.Errorf("unsupported state_schema field type %q; use a canonical scalar, numeric(p,s), [NamedType], or [] form", raw)
}

func NodeStateNamedTypeName(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		raw = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
	}
	if strings.HasPrefix(raw, "list<") && strings.HasSuffix(raw, ">") {
		raw = strings.TrimSpace(raw[len("list<") : len(raw)-1])
	}
	if !nodeStateNamedTypePattern.MatchString(raw) {
		return "", false
	}
	return raw, true
}

func IsNodeStateJSONBType(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json", "jsonb":
		return true
	default:
		return false
	}
}
