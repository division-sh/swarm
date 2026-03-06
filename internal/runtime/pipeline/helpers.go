package pipeline

import "strings"

func NormalizeScanMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return strings.ToLower(strings.TrimSpace(raw))
	case "local_underserved":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func NormalizeScanPriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "normal", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}
