package pipeline

import "strings"

func NormalizeScanMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return strings.ToLower(strings.TrimSpace(raw))
	case "local_underserved":
		return "local_services"
	case "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	default:
		return ""
	}
}

func NormalizeScanPriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "normal", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}
