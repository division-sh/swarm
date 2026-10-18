//go:build linux || darwin

package serveapp

import (
	"io"
	"log"
	"strings"
	"sync"
	"testing"
)

var originalCauseLeafLogs sync.Map

type originalCauseLeafLog struct {
	mu       sync.Mutex
	t        *testing.T
	prior    io.Writer
	ceremony string
	stage    string
}

func beginOriginalCauseLeafLog(t *testing.T) *originalCauseLeafLog {
	t.Helper()
	trace := &originalCauseLeafLog{t: t, prior: log.Writer(), ceremony: "initial_connect", stage: "start"}
	t.Setenv("SWARM_TEST_2614_ORIGINAL_CAUSE", "1")
	originalCauseLeafLogs.Store(t, trace)
	log.SetOutput(trace)
	t.Cleanup(func() {
		log.SetOutput(trace.prior)
		originalCauseLeafLogs.Delete(t)
	})
	return trace
}

func (trace *originalCauseLeafLog) phase(ceremony, stage string) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if ceremony != "" {
		trace.ceremony = ceremony
	}
	trace.stage = stage
}

func (trace *originalCauseLeafLog) Write(p []byte) (int, error) {
	if _, metadata, found := strings.Cut(string(p), "ONBOARDING_ORIGINAL_CAUSE "); found {
		trace.mu.Lock()
		ceremony, stage := trace.ceremony, trace.stage
		trace.mu.Unlock()
		trace.t.Logf("ORIGINAL_CAUSE leaf=%s ceremony=%s stage=%s metadata=%s", trace.t.Name(), ceremony, stage, strings.TrimSpace(metadata))
		return len(p), nil
	}
	return trace.prior.Write(p)
}
