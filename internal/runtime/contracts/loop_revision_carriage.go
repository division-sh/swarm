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

func RequiresLoopRevision(payload EventPayloadSpec, field string) bool {
	property, ok := payload.Properties[field]
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(property.Type)) {
	case "text", "string":
	default:
		return false
	}
	for _, required := range payload.Required {
		if required == field {
			return true
		}
	}
	return false
}
