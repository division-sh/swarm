package runtime

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

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
