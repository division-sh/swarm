package tools

import (
	"fmt"
	"log"
	"strings"

	runtimescanmode "empireai/internal/runtime/scanmode"
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
	return runtimescanmode.NormalizeMode(raw)
}

func normalizeScanPriority(raw string) string {
	return runtimescanmode.NormalizePriority(raw)
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
