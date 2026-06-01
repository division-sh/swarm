package tools

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

func canonicalRuntimeRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func processWarn(component string, format string, args ...any) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" {
		return
	}
	diaglog.ProcessLog("warn", component, msg)
}

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
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
