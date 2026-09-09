package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type diagnosticProjectionProbe struct {
	calls atomic.Int32
	err   error
}

func (p *diagnosticProjectionProbe) ProjectAgentLifecycleDiagnostics(context.Context, int) (int, error) {
	p.calls.Add(1)
	return 0, p.err
}

func TestLifecycleDiagnosticProjectionUsesPersistenceWithoutOptionalBus(t *testing.T) {
	probe := &diagnosticProjectionProbe{}
	am := &AgentManager{lifecycle: &agentLifecycleCoordinator{}, roles: PersistenceRoles{LifecycleDiagnostics: probe}}
	if err := am.projectLifecycleDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.calls.Load() != 1 {
		t.Fatal("nil optional EventBus bypassed durable projection")
	}
}

func TestLifecycleDiagnosticFailureDoesNotChangeCompletedRetirement(t *testing.T) {
	failure := errors.New("injected durable diagnostic failure")
	probe := &diagnosticProjectionProbe{err: failure}
	am := &AgentManager{lifecycle: &agentLifecycleCoordinator{}, roles: PersistenceRoles{LifecycleDiagnostics: probe}}
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
