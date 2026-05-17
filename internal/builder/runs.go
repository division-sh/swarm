package builder

import (
	"sync"

	runtimepkg "swarm/internal/runtime"
)

type runHub struct {
	runtimeProvider func() *runtimepkg.Runtime
	pauseRuntime    func() error
	resumeRuntime   func() error
	runDebug        RunDebugReader

	mu       sync.RWMutex
	sessions map[string]*runSession
	attached *runtimepkg.Runtime
}

type runSession struct {
	runID              string
	runtime            *runtimepkg.Runtime
	entityIDs          map[string]struct{}
	breakpoints        map[string]struct{}
	trippedBreakpoints map[string]struct{}
	pendingHuman       *pendingHumanDecision
	pendingStep        *pendingNodeAction
	subs               map[string]func(RunEventEnvelope)
	controlEvents      []RunEventEnvelope
	terminal           bool
	paused             bool
	debug              runDebugStreamState
}

type runDebugStreamState struct {
	startedKey    string
	terminalKey   string
	eventIDs      map[string]struct{}
	runtimeLogIDs map[string]struct{}
}

type pendingHumanDecision struct {
	nodeID          string
	instanceID      string
	requestingAgent string
}

type pendingNodeAction struct {
	kind       string
	nodeID     string
	instanceID string
}

type RunEventEnvelope = map[string]any
