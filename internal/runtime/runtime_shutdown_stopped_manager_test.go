package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type stoppedManagerContinuationStore struct {
	deliverylifecycle.Store
	failure error
}

func (s stoppedManagerContinuationStore) ScanDeliveryContinuations(context.Context, deliverylifecycle.ExecutionAuthority, deliverylifecycle.ContinuationCursor, int) (deliverylifecycle.ContinuationPage, error) {
	return deliverylifecycle.ContinuationPage{}, s.failure
}

type stoppedManagerCompletionStore struct {
	runtimeTestCandidateOwner
	entered chan struct{}
	failure error
}

func (s stoppedManagerCompletionStore) ExecuteCompletionCandidate(context.Context, runlifecycle.Candidate, runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error) {
	close(s.entered)
	return runlifecycle.CompletionResult{}, errors.Join(runlifecycle.ErrCompletionAuthority, s.failure)
}

func VerifyRuntimeShutdownAfterStoppedManagerJoinsDependenciesForTest(t *testing.T, open RuntimeLogNativeOpenerForTest) {
	store, fixture := newRuntimeShutdownDeliveryStore(t, open)
	ctx := testAuthorActivityContext(fixture.Context)
	bus, err := newRuntimeTestEventBus(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	activateRuntimeShutdownDelivery(t, store, bus, fixture.Context)
	owner := runtimeTestEventBusRuntimeOccurrence(t, bus)
	rt := &Runtime{Bus: bus, workOccurrence: owner}
	t.Cleanup(func() { _ = rt.Shutdown() })
	lifecycle, roles := runtimeTestEphemeralManagerRows(t)
	busRoles := runtimeTestManagerBusRoles(bus)
	busRoles.LifecycleState, busRoles.LifecycleOutcome = roles.LifecycleState, roles.LifecycleOutcome
	rt.Manager = manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
		BaseContext: ctx, MutationContext: fixture.MutationContext,
		ExecutionPosture: executionposture.Live, WorkOwner: owner,
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed, ShutdownDependencies: rt,
		LifecycleStore: lifecycle, DeliveryStore: store, PersistenceRoles: busRoles,
		ReceiverExecution: eventreceiver.NormalExecution(),
	}, &runtimeShutdownManagerStore{})
	if err := rt.Manager.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	continuationFailure := errors.New("retained continuation scan failure")
	coordinator, err := deliverycontinuation.New(stoppedManagerContinuationStore{Store: store, failure: continuationFailure}, store.authority, owner, &shutdownBlockedDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt.deliveryContinuations = coordinator
	if err := coordinator.Start(ctx); !errors.Is(err, continuationFailure) {
		t.Fatalf("continuation failure was not retained: %v", err)
	}
	completionFailure := errors.New("retained completion settlement failure")
	completion := stoppedManagerCompletionStore{entered: make(chan struct{}), failure: completionFailure}
	executor, err := runlifecycle.NewExecutor(completion, runlifecycle.CandidateScope{BundleHash: owner.Identity().BundleHash}, runlifecycle.FinalCatalog{}, owner, runlifecycle.ExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rt.runLifecycleExecutor = executor
	accepted, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = accepted.Done() })
	if err := executor.Start(accepted.Context()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(accepted.Context(), runlifecycle.Candidate{
		RunID: "stopped-manager-run", BundleHash: owner.Identity().BundleHash,
		Revision: 1, DueAt: runlifecycle.CanonicalTimestamp(time.Now().Add(-time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	awaitShutdownFanOut(t, completion.entered, "completion settlement")
	_ = accepted.Done()
	// This is a completed manager transition, not a never-started manager.
	if err := rt.Manager.Shutdown(); err != nil {
		t.Fatalf("manager-only shutdown while runtime admission is open: %v", err)
	}
	if rt.Manager.IsRunning() || rt.shutdownAdmissionClosed() {
		t.Fatal("control did not retain an open runtime with an already-stopped manager")
	}
	for _, caller := range []string{"first", "later"} {
		result := rt.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second})
		for _, failure := range []error{continuationFailure, completionFailure} {
			if !errors.Is(result, failure) || strings.Count(result.Error(), failure.Error()) != 1 {
				t.Errorf("%s runtime shutdown lost or duplicated dependency failure %q: %v", caller, failure, result)
			}
		}
		if owner.ActiveCount() != 0 {
			t.Errorf("%s runtime shutdown left %d active leases", caller, owner.ActiveCount())
		}
	}
}
