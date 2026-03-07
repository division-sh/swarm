package agents

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
