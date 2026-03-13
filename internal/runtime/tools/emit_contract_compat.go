package tools

import (
	"strings"

	"empireai/internal/events"
)

// NormalizeScanModeCompat preserves the small generic normalization surface
// still referenced by agents without reintroducing Empire-specific scan rules.
func NormalizeScanModeCompat(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", "default":
		return ""
	case "scan", "discovery":
		return "scan"
	default:
		return mode
	}
}

// NormalizeScanPriorityCompat keeps a minimal generic priority vocabulary.
func NormalizeScanPriorityCompat(raw string) string {
	priority := strings.ToLower(strings.TrimSpace(raw))
	switch priority {
	case "", "default", "medium":
		return "normal"
	case "med":
		return "normal"
	case "low", "normal", "high", "critical":
		return priority
	default:
		return priority
	}
}

// EnforceRequiredEmitContract is intentionally neutral now that product-specific
// emit guardrails have been deleted from the platform runtime.
func EnforceRequiredEmitContract(_ string, _ events.Event, _ []events.Event) error {
	return nil
}

// RequiredEmitToolContractText no longer injects product-specific tool
// requirements into the generic platform agent prompt.
func RequiredEmitToolContractText(_ string, _ events.Event) string {
	return ""
}

// EmitContractRemediationPrompt is a no-op after deleting product-specific emit
// contract guardrails from the platform runtime.
func EmitContractRemediationPrompt(_ string, _ events.Event, _ error) (string, bool) {
	return "", false
}
