package runtime

import (
	"encoding/json"
	"strings"
)

func parsePayloadMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return map[string]any{}
	}
	return payload
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func expectedScoringDimensions(rubric string) []string {
	switch strings.ToLower(strings.TrimSpace(rubric)) {
	case "", "universal":
		return []string{
			"build_complexity",
			"automation_completeness",
			"icp_crispness",
			"distribution_leverage",
			"time_to_value",
			"operational_drag",
			"pain_severity",
			"competition_gap",
			"monetization_clarity",
			"retention_architecture",
			"expansion_potential",
		}
	default:
		return nil
	}
}
