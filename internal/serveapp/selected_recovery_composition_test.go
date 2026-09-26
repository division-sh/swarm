package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
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
	var probe *selectedRecoveryFailureProbe
	prior := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := prior(owner, req)
		if err == nil && caps.SelectedForkProcess != nil {
			probe = &selectedRecoveryFailureProbe{selectedForkProcessOwner: caps.SelectedForkProcess, failure: failure}
			caps.SelectedForkProcess = probe
		}
		return caps, err
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = prior })
	var out lockedBuffer
	code := runFrom(context.Background(), repoRootForTest(), cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "recovery.db")),
		SourceRoot: writeServedEventPublishFollowUpFixture(t), PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true,
		TestLLMRuntime: servedNoopLLMRuntime{}, Output: &out,
	})
	if probe == nil {
		t.Fatalf("serve did not construct selected recovery capability\n%s", out.String())
	}
	if probe.bindings != 1 || probe.recoveries != 1 || !probe.environmentSeen {
		t.Fatalf("selected recovery composition binds=%d recoveries=%d environment=%t\n%s", probe.bindings, probe.recoveries, probe.environmentSeen, out.String())
	}
	if code == 0 || !strings.Contains(out.String(), failure.Error()) || strings.Contains(out.String(), "[22/22] ready") {
		t.Fatalf("selected recovery failure reached readiness: code=%d\n%s", code, out.String())
	}
}

func TestSelectedResetReconcilesSuccessorBeforeExecutionBuildBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			captured := captureSelectedResetSupervisor(t)
			rt := startServedControlProofRuntime(t, backend)
			supervisor := <-captured
			predecessor := supervisor.selected
			original := supervisor.resetBuildExecution
			checked := false
			supervisor.operationMu.Lock()
			supervisor.resetBuildExecution = func(candidate serveRuntimeBundleContext) (map[string]apiv1.MethodHandler, error) {
				if supervisor.selected == nil || supervisor.selected == predecessor {
					return nil, errors.New("reset execution build adopted predecessor selected owner")
				}
				if supervisor.selectedRecoveryEnvironment.SourceLoader == nil || supervisor.selectedRecoveryEnvironment.AgentRuntime.ProcessCapability == nil {
					return nil, errors.New("reset successor lost selected recovery environment")
				}
				_, err := supervisor.selected.RecoverSelectedForkContexts(context.Background(), effects.NewRecoveryRequest(time.Now().UTC(), candidate.runtime.ExecutionPosture), supervisor.selectedRecoveryEnvironment)
				if err == nil || !strings.Contains(err.Error(), "fresh bound process composition") {
					return nil, errors.New("reset execution build preceded selected successor recovery")
				}
				checked = true
				return original(candidate)
			}
			supervisor.operationMu.Unlock()
			response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", map[string]any{
				"include_source_artifacts": false, "idempotency_key": uuid.NewString(),
			})
			if response.Error != nil {
				t.Fatalf("reset failed: %+v", response.Error)
			}
			supervisor.operationMu.Lock()
			supervisor.resetBuildExecution = original
			if !checked || supervisor.selected == predecessor || !supervisor.resetConverged {
				t.Error("reset did not publish a recovered selected successor")
			}
			supervisor.operationMu.Unlock()
		})
	}
}
