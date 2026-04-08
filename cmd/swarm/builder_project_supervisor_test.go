package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/runtime/semanticview"
)

func TestRuntimeProjectSupervisorReplaceCurrentRuntime_ClearsReadinessBeforeShutdown(t *testing.T) {
	oldRT := &runtimepkg.Runtime{}
	newRT := &runtimepkg.Runtime{}
	var ready atomic.Bool
	ready.Store(true)

	supervisor := &runtimeProjectSupervisor{
		ready:         &ready,
		currentRoot:   "/tmp/old-project",
		currentBundle: &runtimecontracts.WorkflowContractBundle{},
		currentSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:     oldRT,
	}

	shutdownCalled := false
	startCalled := false
	supervisor.shutdownRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		shutdownCalled = true
		if rt != oldRT {
			t.Fatalf("shutdown runtime = %p, want old runtime %p", rt, oldRT)
		}
		if got := supervisor.CurrentRuntime(); got != nil {
			t.Fatalf("CurrentRuntime during shutdown = %p, want nil", got)
		}
		if got := supervisor.CurrentProject(); got.Loaded {
			t.Fatalf("CurrentProject.Loaded during shutdown = true, want false")
		}
		if ready.Load() {
			t.Fatal("ready flag remained true during shutdown")
		}
		return nil
	}
	supervisor.startRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		startCalled = true
		if rt != newRT {
			t.Fatalf("start runtime = %p, want new runtime %p", rt, newRT)
		}
		if got := supervisor.CurrentRuntime(); got != nil {
			t.Fatalf("CurrentRuntime before attach = %p, want nil", got)
		}
		if ready.Load() {
			t.Fatal("ready flag became true before new runtime attached")
		}
		return nil
	}

	status, err := supervisor.replaceCurrentRuntime(
		context.Background(),
		"/tmp/new-project",
		semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		&runtimecontracts.WorkflowContractBundle{},
		newRT,
	)
	if err != nil {
		t.Fatalf("replaceCurrentRuntime: %v", err)
	}
	if !shutdownCalled {
		t.Fatal("expected shutdown to be called")
	}
	if !startCalled {
		t.Fatal("expected start to be called")
	}
	if !ready.Load() {
		t.Fatal("ready flag = false after attach, want true")
	}
	if got := supervisor.CurrentRuntime(); got != newRT {
		t.Fatalf("CurrentRuntime after attach = %p, want new runtime %p", got, newRT)
	}
	if !status.Loaded {
		t.Fatal("status.Loaded = false, want true")
	}
	if status.ProjectDir != "/tmp/new-project" {
		t.Fatalf("status.ProjectDir = %q, want /tmp/new-project", status.ProjectDir)
	}
}

type builderControlTestAgent struct{ id string }

func (a builderControlTestAgent) ID() string                      { return a.id }
func (builderControlTestAgent) Type() string                      { return "stub" }
func (builderControlTestAgent) Subscriptions() []events.EventType { return nil }
func (builderControlTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (builderControlTestAgent) BoardStep(context.Context, string) (string, error) { return "ok", nil }

func TestDashboardDynamicAgentControl_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	agent := builderControlTestAgent{id: "agent-1"}
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	supervisor := &runtimeProjectSupervisor{
		currentRT: &runtimepkg.Runtime{Manager: manager},
	}
	control := dashboardDynamicAgentControl{supervisor: supervisor}

	if err := control.RestartAgent(agent.id); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("RestartAgent err = %v, want runtime shutting down", err)
	}
	if err := control.ReplayAgentBacklog(context.Background(), agent.id); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("ReplayAgentBacklog err = %v, want runtime shutting down", err)
	}
	if _, err := control.ChatWithAgent(context.Background(), agent.id, "run corpus", false); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("ChatWithAgent err = %v, want runtime shutting down", err)
	}
}
