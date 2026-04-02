package runtime

import (
	"fmt"
	"strings"
	"sync"

	"swarm/internal/runtime/diaglog"
)

var processWarnOnceSeen sync.Map

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

func processWarnOnce(key, component string, format string, args ...any) {
	key = strings.TrimSpace(key)
	if key == "" {
		processWarn(component, format, args...)
		return
	}
	if _, loaded := processWarnOnceSeen.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	processWarn(component, format, args...)
}

func summarizeLogList(values []string, max int) string {
	if len(values) == 0 {
		return ""
	}
	if max <= 0 {
		max = 10
	}
	if len(values) <= max {
		return strings.Join(values, ", ")
	}
	return strings.Join(values[:max], ", ") + fmt.Sprintf(" (+%d more)", len(values)-max)
}

func snippetForLog(raw string, max int) string {
	raw = strings.TrimSpace(raw)
	if max <= 0 {
		max = 180
	}
	if len(raw) <= max {
		return raw
	}
	return raw[:max] + "..."
}
