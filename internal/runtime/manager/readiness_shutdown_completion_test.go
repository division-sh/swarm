package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestReadinessShutdownCompletionKeepsStrictAdmissionAndIndependentFailures(t *testing.T) {
	for _, mode := range []string{"after_readiness", "during_verification", "already_emitted", "cleanup_failure", "live_fenced"} {
		t.Run(mode, func(t *testing.T) {
			shutdown := mode != "after_readiness" && mode != "live_fenced"
			autoEmit := ""
			if mode == "already_emitted" {
				autoEmit = "task.started"
			}
			instances, agents := &flowActivationTestInstanceStore{}, &flowActivationTestStore{}
			bus := &flowActivationTestBus{}
			bundle := testFlowBundle(t, autoEmit)
			am, ctx := newFlowActivationManagerForSource(t, bus, instances, semanticview.Wrap(bundle), agents)
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := am.lifecycle.waitForWork(ctx); err != nil {
				t.Fatal(err)
			}
			before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || before.Phase != runtimepipeline.FlowAttachmentReady {
				t.Fatalf("initial attachment: found=%t err=%v row=%+v", found, err, before)
			}
			if mode == "already_emitted" && (before.CreationEventEmittedAt.IsZero() || len(bus.published) != 1) {
				t.Fatal("control did not commit the creation occurrence")
			}
			records, err := agents.LoadAgents(ctx)
			if err != nil || len(records) != 1 {
				t.Fatalf("initial agents: %v %v", records, err)
			}
			rec := records[0]
			if _, started, err := am.lifecycle.beginRun(managedExecutionTestContext(t, ctx), AgentRunModeStandard, am.workOwner); err != nil || !started {
				t.Fatalf("begin manager run: started=%t err=%v", started, err)
			}
			am.lifecycle.mu.Lock()
			am.lifecycle.watcherExpected = false
			am.lifecycle.mu.Unlock()
			oldCtx, token, done, err := replaceCoordinatorLoop(am.lifecycle, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := am.lifecycle.acquireExecutionIdentity(ctx, rec.Config.Identity, "readiness dependent caller", true)
			if err != nil {
				t.Fatal(err)
			}
			defer accepted.Release()
			revisedBundle := testFlowBundleWithTwoAgents(t, autoEmit)
			revisedCtx, err := setFlowActivationManagerSemanticSource(am, ctx, semanticview.Wrap(revisedBundle))
			if err != nil {
				t.Fatal(err)
			}
			revisedCtx = runtimecorrelation.WithRunID(revisedCtx, req.TriggerEvent.RunID())
			revisedCtx = worklifetime.WithOccurrence(revisedCtx, am.workOwner)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			am.testBeforeDynamicFlowAgentReadiness = func() {
				once.Do(func() { close(entered); <-release })
			}
			if err := am.ReconcileDynamicFlowRuntimeReadinessPlansForRun(revisedCtx, time.Now().UTC()); !errors.Is(err, errDynamicFlowRuntimeReadinessRetiring) {
				t.Fatalf("dependent caller did not return captured predecessor retirement: %v", err)
			}
			<-oldCtx.Done()
			am.dynamicFlowReadinessMu.Lock()
			attempt := am.dynamicFlowReadinessAttempts[dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.InstancePath}]
			am.dynamicFlowReadinessMu.Unlock()
			if attempt == nil {
				t.Fatal("accepted attempt was discarded before predecessor join")
			}
			select {
			case <-attempt.retiring:
			default:
				t.Fatal("caller return was not captured")
			}
			accepted.Release()
			if err := releaseCoordinatorLoop(am.lifecycle, token, done); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("verification did not reach its boundary")
			}
			fault := errors.New("independent durable abandonment failure")
			if mode == "cleanup_failure" {
				var calls atomic.Int32
				instances.retireAttempt = func() error {
					if calls.Add(1) == 1 {
						return fault
					}
					return nil
				}
			}
			shutdownDone := make(chan error, 1)
			if shutdown {
				go func() { shutdownDone <- am.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second}) }()
				waitForManagerShuttingDown(t, am)
			} else if mode == "live_fenced" {
				am.lifecycle.mu.Lock()
				am.lifecycle.cells[rec.Config.Identity].execution.fenced = true
				am.lifecycle.mu.Unlock()
			}
			close(release)
			select {
			case <-attempt.done:
			case <-time.After(5 * time.Second):
				t.Fatal("accepted readiness attempt did not join")
			}
			var shutdownErr error
			if shutdown {
				select {
				case shutdownErr = <-shutdownDone:
				case <-time.After(5 * time.Second):
					t.Fatal("shutdown did not join accepted readiness")
				}
			} else {
				shutdownErr = am.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second})
			}
			switch mode {
			case "cleanup_failure":
				if !errors.Is(attempt.err, fault) || !errors.Is(shutdownErr, fault) || isReadinessShutdownRefusal(attempt.err) {
					t.Fatalf("independent failure lost: attempt=%v shutdown=%v", attempt.err, shutdownErr)
				}
			case "live_fenced":
				if attempt.err == nil || shutdownErr == nil || isReadinessShutdownRefusal(attempt.err) {
					t.Fatalf("live corruption was treated as shutdown: attempt=%v shutdown=%v", attempt.err, shutdownErr)
				}
			default:
				if shutdownErr != nil {
					t.Fatalf("lawful shutdown refusal became terminal completion: %v", shutdownErr)
				}
				if shutdown && !isReadinessShutdownRefusal(attempt.err) {
					t.Fatalf("strict executable refusal disappeared: %v", attempt.err)
				}
				if !shutdown && attempt.err != nil {
					t.Fatalf("ordinary readiness failed: %v", attempt.err)
				}
			}
			after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || !after.CreationEventEmittedAt.Equal(before.CreationEventEmittedAt) {
				t.Fatalf("shutdown changed creation evidence: found=%t err=%v before=%+v after=%+v", found, err, before, after)
			}
			if mode == "cleanup_failure" || mode == "live_fenced" {
				return
			}
			restartBus := &flowActivationTestBus{}
			restarted, restartCtx := newFlowActivationManagerForSource(t, restartBus, instances, semanticview.Wrap(revisedBundle), agents)
			restartReq := testActivationRequest(revisedBundle, "review", "inst-1", "ent-1", "review/inst-1")
			if created, err := restarted.EnsureFlowInstance(restartCtx, restartReq); err != nil || created {
				t.Fatalf("restart did not reuse desired instance: created=%t err=%v", created, err)
			}
			if !flowActivationAttemptReadyForTest(restarted, restartCtx, restartReq) || len(restartBus.published) != 0 {
				t.Fatal("restart missed readiness or repeated the creation occurrence")
			}
			if err := restarted.Shutdown(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReadinessShutdownRefusalRequiresEveryExactOwnerBranch(t *testing.T) {
	t.Run("joined native cancellation disposition", proveReadinessShutdownCancellationDisposition)
	identity := runtimeagentidentitytest.RootRuntime(t, "agent-lifecycle-test", "lifecycle-test")
	shutdown := executableReadinessError(identity, runtimeLifecycleShuttingDown, AgentLifecycleState{}, "manager lifecycle does not admit executable readiness")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"exact", shutdown, true},
		{"wrapped", fmt.Errorf("verify attachment: %w", shutdown), true},
		{"joined exact", errors.Join(shutdown, shutdown), true},
		{"independent storage", errors.Join(shutdown, errors.New("storage unavailable")), false},
		{"independent cancellation", errors.Join(shutdown, context.Canceled), false},
		{"live refusal", executableReadinessError(identity, runtimeLifecycleRunning, AgentLifecycleState{}, "fenced execution"), false},
		{"stopped refusal", executableReadinessError(identity, runtimeLifecycleStopped, AgentLifecycleState{}, "not prepared"), false},
		{"resetting refusal", executableReadinessError(identity, runtimeLifecycleResetting, AgentLifecycleState{}, "resetting"), false},
		{"foreign component", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_not_ready", "foreign", "verify_executable_readiness", map[string]any{"manager_phase": "shutting_down"}), false},
		{"foreign operation", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_not_ready", "agent-lifecycle", "foreign", map[string]any{"manager_phase": "shutting_down"}), false},
		{"foreign code", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "foreign", "agent-lifecycle", "verify_executable_readiness", map[string]any{"manager_phase": "shutting_down"}), false},
		{"foreign class", runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_execution_not_ready", "agent-lifecycle", "verify_executable_readiness", map[string]any{"manager_phase": "shutting_down"}), false},
		{"missing phase", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_not_ready", "agent-lifecycle", "verify_executable_readiness", nil), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isReadinessShutdownRefusal(tc.err); got != tc.want {
				t.Fatalf("classification=%t, want %t: %v", got, tc.want, tc.err)
			}
		})
	}
}

func proveReadinessShutdownCancellationDisposition(t *testing.T) {
	key := dynamicFlowRuntimeReadinessKey{runID: "run", instancePath: "review/child"}
	attempt := &dynamicFlowRuntimeReadinessAttempt{attemptOrdinal: 3, planHash: "exact-plan"}
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before_attempt=%t", before), func(t *testing.T) {
			result, cancellation := (runtimepipeline.FlowAttachmentAdvanceResult{}).WithCleanCancellation(context.Canceled, before)
			if got, ok := result.CleanCancellation(cancellation); !ok || got != before || !errors.Is(cancellation, context.Canceled) {
				t.Fatalf("lost native cancellation distinction: before=%t ok=%t err=%v", got, ok, cancellation)
			}
			other, otherErr := (runtimepipeline.FlowAttachmentAdvanceResult{}).WithCleanCancellation(context.Canceled, before)
			acknowledged := result
			acknowledged.Acknowledged = true
			for _, hostile := range []struct {
				name   string
				result runtimepipeline.FlowAttachmentAdvanceResult
				err    error
			}{
				{"raw caller cancellation", result, context.Canceled},
				{"independent cancellation", result, errors.Join(cancellation, context.Canceled)},
				{"storage", result, errors.Join(cancellation, errors.New("storage"))},
				{"another result", other, cancellation},
				{"another error", result, otherErr},
				{"acknowledged", acknowledged, cancellation},
				{"missing outcome", runtimepipeline.FlowAttachmentAdvanceResult{}, cancellation},
			} {
				t.Run(hostile.name, func(t *testing.T) {
					if _, ok := hostile.result.CleanCancellation(hostile.err); ok {
						t.Fatal("foreign or independently failed outcome inherited clean cancellation")
					}
				})
			}
			cancelled := &readinessShutdownCancellation{cause: cancellation, key: key, ordinal: attempt.attemptOrdinal, planHash: attempt.planHash}
			refusal := executableReadinessError(lifecycleTestPersistedAgent(t).Config.Identity, runtimeLifecycleShuttingDown, AgentLifecycleState{}, "shutdown")
			for _, tc := range []struct {
				name string
				err  error
				want bool
			}{
				{"exact", cancelled, true},
				{"wrapped", fmt.Errorf("attachment: %w", cancelled), true},
				{"both closed shutdown outcomes", errors.Join(cancelled, refusal), true},
				{"lease failure", errors.Join(cancelled, errors.New("lease join")), false},
				{"independent cancellation", errors.Join(cancelled, context.Canceled), false},
				{"grace", errors.Join(cancelled, context.DeadlineExceeded), false},
				{"classified domain failure", runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "storage", "storage", "cleanup", nil, cancelled), false},
				{"classified failure wrapping canonical refusal", runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "storage", "storage", "cleanup", nil, refusal), false},
				{"wrong run", &readinessShutdownCancellation{cause: cancellation, key: dynamicFlowRuntimeReadinessKey{runID: "foreign", instancePath: key.instancePath}, ordinal: attempt.attemptOrdinal, planHash: attempt.planHash}, false},
				{"wrong instance", &readinessShutdownCancellation{cause: cancellation, key: dynamicFlowRuntimeReadinessKey{runID: key.runID, instancePath: "review/foreign"}, ordinal: attempt.attemptOrdinal, planHash: attempt.planHash}, false},
				{"wrong ordinal", &readinessShutdownCancellation{cause: cancellation, key: key, ordinal: 4, planHash: attempt.planHash}, false},
				{"wrong plan", &readinessShutdownCancellation{cause: cancellation, key: key, ordinal: attempt.attemptOrdinal, planHash: "foreign"}, false},
				{"missing native cause", &readinessShutdownCancellation{key: key, ordinal: attempt.attemptOrdinal, planHash: attempt.planHash}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if got := isReadinessShutdownCompletion(tc.err, key, attempt); got != tc.want {
						t.Fatalf("completion disposition=%t want=%t err=%v", got, tc.want, tc.err)
					}
				})
			}
			for _, phase := range []runtimeLifecyclePhase{runtimeLifecycleRunning, runtimeLifecycleResetting, runtimeLifecycleShuttingDown} {
				for _, cause := range []error{context.Canceled, worklifetime.ErrRetired, context.DeadlineExceeded} {
					ctx, cancel := context.WithCancelCause(context.Background())
					cancel(cause)
					am := &AgentManager{lifecycle: &agentLifecycleCoordinator{phase: phase}}
					if am.cleanReadinessShutdownCancellation(ctx, result, cancellation) {
						t.Fatalf("current phase=%s promoted foreign retirement cause=%v", phase, cause)
					}
				}
			}
		})
	}
}
