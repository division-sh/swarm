package tools

import (
	"fmt"
	"log"
	"strings"

	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

func canonicalRuntimeRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func runtimeWarn(component string, format string, args ...any) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" {
		return
	}
	log.Printf("runtime.warn component=%s message=%s", component, msg)
}

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func normalizeScanMode(raw string) string {
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

func normalizeScanPriority(raw string) string {
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

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		var n int
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	default:
		return 0
	}
}
