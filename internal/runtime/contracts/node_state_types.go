package contracts

import (
	"fmt"
	"regexp"
	"strings"
)

var nodeStateNumericTypePattern = regexp.MustCompile(`(?i)^numeric\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)

// NormalizeNodeStateFieldType enforces the supported canonical vocabulary for
// node-local accumulator/state fields. Unlike the Wave 1 entity/event contract
// surface, node state still allows jsonb and float-like primitives, but it does
// not allow descriptive prose or inline modifiers to masquerade as a type.
func NormalizeNodeStateFieldType(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("state_schema field type is required")
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
		return normalizedBase + "[]", nil
	}
	if matches := nodeStateNumericTypePattern.FindStringSubmatch(raw); len(matches) == 3 {
		return fmt.Sprintf("numeric(%s,%s)", matches[1], matches[2]), nil
	}

	normalized := strings.ToLower(raw)
	switch normalized {
	case "text", "string", "integer", "int", "bigint", "float", "double", "real", "numeric", "boolean", "bool", "jsonb", "json", "timestamp", "timestamptz", "uuid":
		return normalized, nil
	}

	if strings.ContainsAny(raw, " \t\r\n") {
		return "", fmt.Errorf("state_schema field type %q is not canonical; descriptive notes and inline modifiers must not appear in type declarations", raw)
	}
	return "", fmt.Errorf("unsupported state_schema field type %q; use a canonical scalar, numeric(p,s), or [] form", raw)
}
