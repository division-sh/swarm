package builder

import (
	"context"
	"sync"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
)

type runHub struct {
	runtimeAcquirer RuntimeAcquirer
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
	waitForQuiescence  func(context.Context) error
	entityIDs          map[string]struct{}
	breakpoints        map[string]struct{}
	trippedBreakpoints map[string]struct{}
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

type pendingNodeAction struct {
	kind       string
	nodeID     string
	instanceID string
}

type RunEventEnvelope = map[string]any
