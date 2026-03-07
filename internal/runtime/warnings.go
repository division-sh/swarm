package runtime

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

var runtimeWarnOnceSeen sync.Map

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

func runtimeWarnOnce(key, component string, format string, args ...any) {
	key = strings.TrimSpace(key)
	if key == "" {
		runtimeWarn(component, format, args...)
		return
	}
	if _, loaded := runtimeWarnOnceSeen.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	runtimeWarn(component, format, args...)
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

func RuntimeWarnForTest(component string, format string, args ...any) {
	runtimeWarn(component, format, args...)
}
