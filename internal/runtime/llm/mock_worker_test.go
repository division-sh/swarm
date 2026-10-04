package llm

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

type mockHostWorkspace struct{ target *workspace.Target }

// This protocol fixture is not workspace-owner or Docker isolation proof.
func (w mockHostWorkspace) ResolveForkChatWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func (w mockHostWorkspace) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func (w mockHostWorkspace) ResolveWorkspaceForCapabilityAdmission(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func mockHostRuntimeOptions(t *testing.T) MockRuntimeOptions {
	t.Helper()
	return MockRuntimeOptions{Workspaces: mockHostWorkspace{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}}}
}

func mockHostCompletionExecutor(t *testing.T) mockCompletionExecutor {
	t.Helper()
	target := &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}
	return func(ctx context.Context, module pythonmodule.Request) (pythonmodule.Result, error) {
		result, err := workspace.RunWorker(ctx, target, "", worker.Request{Mode: "model", Module: &module})
		if err != nil {
			return pythonmodule.Result{}, err
		}
		return *result.Module, nil
	}
}

func TestMockWorkerFailureDistinguishesNoDispatchObservedFailureAndLostResponse(t *testing.T) {
	want := errors.New("worker failure")
	for _, test := range []struct {
		name       string
		failure    *workspace.WorkerExecutionError
		state      effects.State
		invocation completionProviderInvocation
	}{
		{"never_started", &workspace.WorkerExecutionError{Err: want}, effects.StateTerminalFailure, completionProviderInvocationNotStarted},
		{"identity_refusal", &workspace.WorkerExecutionError{Started: true, Observed: true, Err: want}, effects.StateTerminalFailure, completionProviderInvocationNotStarted},
		{"observed_model_failure", &workspace.WorkerExecutionError{Started: true, Observed: true, ModelStarted: true, Err: want}, effects.StateTerminalFailure, completionProviderInvocationStarted},
		{"lost_launched_response", &workspace.WorkerExecutionError{Started: true, Err: want}, effects.StateOutcomeUncertain, completionProviderInvocationStarted},
		{"observed_but_cleanup_unproven", &workspace.WorkerExecutionError{Started: true, Observed: true, RemoteCleanupUnproven: true, Err: want}, effects.StateOutcomeUncertain, completionProviderInvocationStarted},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx := llmTestWorkContext(t, managedEffectHarnessContext(t, harness, t.Name()))
			ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeMock)
			actor := models.AgentConfig{ID: "effect-test-agent", ExecutionMode: effects.ExecutionModeMock}
			_, _, _, dispatch, err := executeMockCompletionWithExecutor(ctx, actor, nil, []byte(`{"round":1}`), selection.ResolvedModel{ConcreteModel: "test-model"}, false, managedProviderCallForEffectTest(t, ctx), func(context.Context, pythonmodule.Request) (pythonmodule.Result, error) {
				return pythonmodule.Result{}, test.failure
			})
			if !errors.Is(err, want) || dispatch == nil || dispatch.state != test.state || dispatch.invocation != test.invocation {
				t.Fatalf("worker outcome = %+v err=%v", dispatch, err)
			}
			settleEffectTestCompletionFailure(t, context.WithoutCancel(ctx), dispatch, err, test.state)
		})
	}
}
