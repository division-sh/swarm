package scanmode

import "strings"

func NormalizeMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	mode = strings.ReplaceAll(mode, "-", "_")
	mode = strings.Join(strings.Fields(mode), "_")
	switch mode {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return mode
	case "local_underserved", "local", "local_service", "services":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas_trend_scan", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func NormalizePriority(raw string) string {
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

func DefaultMode() string {
	return "saas_gap"
}

func ExpectedScannerCount(mode string) int {
	if NormalizeMode(mode) == "corpus" {
		return 1
	}
	return 3
}
