package agents

import (
	"fmt"
	"log"
	"strings"
	"sync"
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
	log.Printf("runtime.warn component=%s message=%s", component, msg)
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
