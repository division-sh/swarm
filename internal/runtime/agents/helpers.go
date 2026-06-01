package agents

import (
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
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
