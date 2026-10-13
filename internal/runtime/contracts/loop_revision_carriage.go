package contracts

import "strings"

// CarriesLoopRevision recognizes declaration meaning, not an expression to run
// again against historical data. Verification and fork admission share this law.
func CarriesLoopRevision(value ExpressionValue) bool {
	switch value.Kind {
	case ExpressionKindRef:
		return strings.TrimSpace(value.Ref) == "loop.revision_id"
	case ExpressionKindCEL:
		return strings.TrimSpace(value.CEL) == "loop.revision_id"
	default:
		return false
	}
}

// RequiresLoopRevision consumes the effective admitted schema, including
// imported/ExactSchema fields. Structural string lowering erases enum/format
// distinctions, so it cannot establish this domain on its own.
func RequiresLoopRevision(schema EventSchema, field string) bool {
	properties, _ := schema.Schema["properties"].(map[string]any)
	property, ok := properties[field].(map[string]any)
	if !ok || property["type"] != "string" {
		return false
	}
	for _, domain := range []string{"enum", "format", "oneOf"} {
		if _, present := property[domain]; present {
			return false
		}
	}
	_, required := compiledEventRequiredFields(schema.Schema)[field]
	return required
}
