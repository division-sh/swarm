package main

import (
	"context"
	"sync/atomic"
	"testing"

	runtimepkg "swarm/internal/runtime"
	runtimecontracts "swarm/internal/runtime/contracts"
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
