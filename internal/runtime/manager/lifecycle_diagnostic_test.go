package manager

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"sync/atomic"
	"testing"
)

type diagnosticProjectionProbe struct {
	calls atomic.Int32
	err   error
}

func (p *diagnosticProjectionProbe) ListPendingAgentLifecycleDiagnostics(context.Context, int) ([]diaglog.LifecycleDiagnostic, error) {
	p.calls.Add(1)
	return nil, p.err
}

func TestLifecycleDiagnosticProjectionRefusesMissingLogger(t *testing.T) {
	probe := &diagnosticProjectionProbe{}
	am := &AgentManager{lifecycle: &agentLifecycleCoordinator{}, roles: PersistenceRoles{LifecycleDiagnostics: probe}}
	if err := am.projectLifecycleDiagnostics(context.Background()); err == nil {
		t.Fatal("missing logger was accepted")
	}
	if probe.calls.Load() != 0 {
		t.Fatal("read durable work without a settlement owner")
	}
}

func TestLifecycleDiagnosticFailureDoesNotChangeCompletedRetirement(t *testing.T) {
	failure := errors.New("injected durable diagnostic failure")
	probe := &diagnosticProjectionProbe{err: failure}
	am := &AgentManager{bus: newProjectionTestBus(), lifecycle: &agentLifecycleCoordinator{}, roles: PersistenceRoles{LifecycleDiagnostics: probe}}
	if err := am.projectLifecycleDiagnostics(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("original projection failure lost: %v", err)
	}
	if err := am.completeTerminalRetirements(context.Background(), nil); err != nil {
		t.Fatalf("auxiliary projection poisoned completed retirement: %v", err)
	}
	if probe.calls.Load() != 1 {
		t.Fatal("retirement completion still owns auxiliary diagnostic failure")
	}
	am.lifecycle.recordLifecycleDiagnosticFailure(failure)
	if !errors.Is(am.lifecycle.terminalErr, failure) {
		t.Fatal("shutdown lost auxiliary diagnostic cause")
	}
}
