package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type selectedRecoveryFailureProbe struct {
	selectedForkProcessOwner
	failure         error
	bindings        int
	recoveries      int
	environmentSeen bool
}

func (p *selectedRecoveryFailureProbe) BindSelectedProcess(ctx context.Context, process *worklifetime.Process, capability startupownership.ProcessCapability) error {
	p.bindings++
	return p.selectedForkProcessOwner.BindSelectedProcess(ctx, process, capability)
}

func (p *selectedRecoveryFailureProbe) RecoverSelectedForkContexts(_ context.Context, _ effects.RecoveryRequest, environment runforkexecution.SelectedForkRecoveryEnvironment) ([]runfork.SelectedForkRecoveryResult, error) {
	p.recoveries++
	if p.bindings != 1 {
		return nil, errors.New("selected recovery preceded process binding")
	}
	p.environmentSeen = environment.SourceLoader != nil && environment.AgentRuntime.ProcessCapability != nil
	if !p.environmentSeen {
		return nil, errors.New("selected recovery lacks source loader or process-bound runtime")
	}
	return nil, p.failure
}

func TestSelectedRecoveryFailurePreventsServeReadiness(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	failure := errors.New("selected recovery probe failure")
	var probes []*selectedRecoveryFailureProbe
	prior := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := prior(owner, req)
		if err == nil && caps.SelectedForkProcess != nil {
			probe := &selectedRecoveryFailureProbe{selectedForkProcessOwner: caps.SelectedForkProcess, failure: failure}
			probes = append(probes, probe)
			caps.SelectedForkProcess = probe
		}
		return caps, err
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = prior })
	var out lockedBuffer
	code := runFrom(context.Background(), repoRootForTest(), cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "recovery.db")),
		SourceRoot: writeServedEventPublishFollowUpFixture(t), APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true,
		TestLLMRuntime: servedNoopLLMRuntime{}, Output: &out,
	})
	var probe *selectedRecoveryFailureProbe
	for _, candidate := range probes {
		if candidate.recoveries != 0 {
			if probe != nil {
				t.Fatal("serve attempted recovery through multiple process owners")
			}
			probe = candidate
		}
	}
	if probe == nil {
		t.Fatalf("serve did not attempt selected recovery\n%s", out.String())
	}
	if probe.bindings != 1 || probe.recoveries != 1 || !probe.environmentSeen {
		t.Fatalf("selected recovery composition binds=%d recoveries=%d environment=%t\n%s", probe.bindings, probe.recoveries, probe.environmentSeen, out.String())
	}
	if code == 0 || !strings.Contains(out.String(), failure.Error()) || strings.Contains(out.String(), "[22/22] ready") {
		t.Fatalf("selected recovery failure reached readiness: code=%d\n%s", code, out.String())
	}
}
