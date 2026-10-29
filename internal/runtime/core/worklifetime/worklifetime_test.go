package worklifetime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/google/uuid"
)

func TestRuntimeOccurrenceFenceRetireAndProcessJoin(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	lease, err := runtime.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin runtime work: %v", err)
	}
	if err := runtime.Fence(); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if _, err := runtime.Begin(context.Background()); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("begin after fence = %v, want ErrAdmissionFenced", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait with active lease = %v, want deadline", err)
	}
	if err := runtime.Reopen(); err == nil {
		t.Fatal("reopen with active finite work succeeded")
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("settle lease: %v", err)
	}
	if err := runtime.Reopen(); err != nil {
		t.Fatalf("reopen reversible fence: %v", err)
	}
	second, err := runtime.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin after reopen: %v", err)
	}
	runtime.Retire()
	select {
	case <-second.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("retirement did not cancel accepted work")
	}
	if err := runtime.Reopen(); !errors.Is(err, ErrRetired) {
		t.Fatalf("reopen retired occurrence = %v, want ErrRetired", err)
	}
	if err := second.Done(); err != nil {
		t.Fatalf("settle cancelled lease: %v", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire and wait: %v", err)
	}
	receipt, err := process.Join(context.Background())
	if err != nil {
		t.Fatalf("process join: %v", err)
	}
	if err := process.ValidateJoinReceipt(receipt); err != nil {
		t.Fatalf("validate process receipt: %v", err)
	}
}

func TestRuntimeOccurrenceFencedDescendantRequiresLiveSameOwnerLease(t *testing.T) {
	process := NewProcess()
	owner, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "descendant-owner", BundleHash: "bundle-a"})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "foreign-owner", BundleHash: "bundle-b"})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := owner.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	foreignLease, err := foreign.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Fence(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Begin(parent.Context()); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("ordinary admission from accepted context = %v", err)
	}
	if _, err := owner.BeginAcceptedDescendant(context.Background()); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("unrelated context admitted descendant: %v", err)
	}
	if _, err := owner.BeginAcceptedDescendant(foreignLease.Context()); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("foreign lease admitted descendant: %v", err)
	}
	child, err := owner.BeginAcceptedDescendant(context.WithoutCancel(parent.Context()))
	if err != nil {
		t.Fatalf("accepted child: %v", err)
	}
	if err := parent.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.BeginAcceptedDescendant(parent.Context()); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("settled ancestor admitted descendant: %v", err)
	}
	if err := child.Done(); err != nil {
		t.Fatal(err)
	}
	if err := foreignLease.Done(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner.Retire()
	if _, err := owner.BeginAcceptedDescendant(child.Context()); !errors.Is(err, ErrRetired) {
		t.Fatalf("retired owner admitted descendant: %v", err)
	}
	foreign.Retire()
	if _, err := owner.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFencedRuntimeRouteCarriesOnlyLiveAcceptedWork(t *testing.T) {
	process := NewProcess()
	owner, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "route-owner", BundleHash: "bundle-a"})
	if err != nil {
		t.Fatal(err)
	}
	route, err := owner.NewRoute(context.Background(), RouteIdentity{
		RuntimeEpoch: 1,
		Agent:        agentidentitytest.RootRuntime(t, "agent-1", "route-owner"),
		Generation:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := owner.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("message.received"), eventtest.Producer(events.EventProducerPlatform, "test"), "",
		[]byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := owner.Fence(); err != nil {
		t.Fatal(err)
	}
	if _, err := route.NewEventDelivery(context.Background(), event); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("unrelated delivery after fence = %v", err)
	}
	carried := CarryAcceptedLease(WithOccurrence(context.Background(), owner), WithProcess(accepted.Context(), NewProcess()))
	if original, found := ProcessFromContext(carried); !found || original != process {
		t.Fatal("accepted handoff did not retain its lease's original process")
	}
	delivery, err := route.NewEventDelivery(carried, event)
	if err != nil {
		t.Fatalf("accepted route delivery after fence: %v", err)
	}
	if err := delivery.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := accepted.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := route.NewEventDelivery(carried, event); !errors.Is(err, ErrAdmissionFenced) {
		t.Fatalf("settled accepted lease reopened route admission: %v", err)
	}
	if err := route.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeAndManagerLeasesRetainExactProcessOwner(t *testing.T) {
	t.Run("shutdown cancellation retains exact owner", proveManagerShutdownCancellationOwner)
	process := NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), RuntimeIdentity{
		RuntimeInstanceID: "runtime-process-owner",
		BundleHash:        "bundle-process-owner",
	})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	runtimeLease, err := runtimeOwner.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin runtime work: %v", err)
	}
	if got, ok := ProcessFromContext(runtimeLease.Context()); !ok || got != process {
		t.Fatalf("runtime process owner = %p/%t, want exact owner %p", got, ok, process)
	}
	if err := runtimeLease.Done(); err != nil {
		t.Fatalf("settle runtime work: %v", err)
	}

	manager, err := NewManagerRunOccurrence(context.Background(), runtimeOwner, ManagerRunIdentity{Generation: 1})
	if err != nil {
		t.Fatalf("new manager run occurrence: %v", err)
	}
	managerLease, err := manager.Begin(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin manager work: %v", err)
	}
	if got, ok := ProcessFromContext(managerLease.Context()); !ok || got != process {
		t.Fatalf("manager process owner = %p/%t, want exact owner %p", got, ok, process)
	}
	if err := managerLease.Done(); err != nil {
		t.Fatalf("settle manager work: %v", err)
	}
	if err := manager.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire manager run: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime: %v", err)
	}
	process.Retire()
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

// Adds cancellation-origin reach: a later shutdown cannot relabel reset,
// caller cancellation, or loss of the original runtime/selected companion.
func proveManagerShutdownCancellationOwner(t *testing.T) {
	for _, mode := range []string{"shutdown", "reset_retire", "companion_lost", "parent_lost", "caller_cancel"} {
		t.Run(mode, func(t *testing.T) {
			process := NewProcess()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			runtimeOwner, err := process.NewRuntime(ctx, RuntimeIdentity{RuntimeInstanceID: "shutdown-owner-" + mode, BundleHash: "exact-bundle"})
			if err != nil {
				t.Fatal(err)
			}
			manager, err := NewManagerRunOccurrence(ctx, runtimeOwner, ManagerRunIdentity{Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := NewManagerRunOccurrence(ctx, runtimeOwner, ManagerRunIdentity{Generation: 2})
			if err != nil {
				t.Fatal(err)
			}
			var companion *SelectedForkOccurrence
			var owner Occurrence
			if mode == "companion_lost" {
				companion, err = process.NewSelectedFork(ctx, SelectedForkIdentity{ExecutionID: "shutdown-companion", RunID: uuid.NewString(), Generation: 1})
				if err != nil {
					t.Fatal(err)
				}
				owner = companion
			}
			caller, cancelCaller := context.WithCancel(ctx)
			defer cancelCaller()
			lease, err := manager.Begin(caller, owner)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "shutdown":
				manager.RetireForShutdown()
			case "reset_retire":
				manager.Retire()
			case "companion_lost":
				companion.Retire()
			case "parent_lost":
				runtimeOwner.Retire()
			case "caller_cancel":
				cancelCaller()
			}
			select {
			case <-lease.Context().Done():
			case <-ctx.Done():
				t.Fatal("original cancellation did not reach admitted lease")
			}
			manager.RetireForShutdown()
			if got := manager.CancelledByShutdown(lease.Context()); got != (mode == "shutdown") {
				t.Fatalf("mode=%s shutdown origin=%t cause=%v", mode, got, context.Cause(lease.Context()))
			}
			if foreign.CancelledByShutdown(lease.Context()) || manager.CancelledByShutdown(context.Background()) {
				t.Fatal("foreign or unadmitted context inherited shutdown origin")
			}
			if mode != "caller_cancel" && !errors.Is(context.Cause(lease.Context()), ErrRetired) {
				t.Fatal("retirement lost its original public cause")
			}
			if mode == "shutdown" {
				t.Run("legacy branch traversal retains closed origin", func(t *testing.T) {
					if errors.Unwrap(context.Cause(lease.Context())) != nil {
						t.Error("branch traversal strips the owning Manager identity before comparing its cancellation cause")
					}
				})
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			if err := manager.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := foreign.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if companion != nil {
				if err := companion.RetireAndWait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := runtimeOwner.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			process.Retire()
			if _, err := process.Join(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLeaseSettlesExactlyOnce(t *testing.T) {
	process := NewProcess()
	lease, err := process.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("first Done: %v", err)
	}
	if err := lease.Done(); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("second Done = %v, want ErrAlreadySettled", err)
	}
}

func TestManagerRunQuiescenceExcludesStandingWorkButRetirementJoinsIt(t *testing.T) {
	process := NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	manager, err := NewManagerRunOccurrence(context.Background(), runtimeOwner, ManagerRunIdentity{Generation: 1})
	if err != nil {
		t.Fatalf("new manager run occurrence: %v", err)
	}
	standing, err := manager.BeginStanding(context.Background())
	if err != nil {
		t.Fatalf("begin standing work: %v", err)
	}
	transient, err := manager.Begin(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transient work: %v", err)
	}
	if err := runtimeOwner.Fence(); err != nil {
		t.Fatalf("fence runtime with standing manager generation: %v", err)
	}
	if err := runtimeOwner.Reopen(); err != nil {
		t.Fatalf("reopen runtime with only standing child ownership: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWait()
	if err := manager.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("quiescence with transient work = %v, want deadline", err)
	}
	if err := transient.Done(); err != nil {
		t.Fatalf("settle transient work: %v", err)
	}
	if err := manager.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("quiescence with only standing work: %v", err)
	}

	retired := make(chan error, 1)
	go func() { retired <- manager.RetireAndWait(context.Background()) }()
	select {
	case err := <-retired:
		t.Fatalf("retirement completed before standing work settled: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := standing.Done(); err != nil {
		t.Fatalf("settle standing work: %v", err)
	}
	if err := <-retired; err != nil {
		t.Fatalf("retire manager run occurrence: %v", err)
	}
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
	process.Retire()
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestChildOccurrenceTransitivelyBlocksProcessJoin(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	process.Retire()
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := process.Join(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("process join before runtime retirement = %v, want deadline", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("runtime retirement: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("process join after runtime retirement: %v", err)
	}
}

func TestChildOccurrencesIgnoreConstructionCancellationAndFollowOwnerRetirement(t *testing.T) {
	process := NewProcess()
	constructionCtx, cancelConstruction := context.WithCancel(context.Background())
	runtime, err := process.NewRuntime(constructionCtx, RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	runtimeRoute, err := runtime.NewRoute(constructionCtx, RouteIdentity{
		RuntimeEpoch: 1,
		Agent:        agentidentitytest.RootRuntime(t, "runtime-agent", "worklifetime-test"),
		Generation:   1,
	})
	if err != nil {
		t.Fatalf("new runtime route: %v", err)
	}
	siblingFork, err := process.NewSelectedFork(constructionCtx, SelectedForkIdentity{ExecutionID: "sibling-fork", RunID: "run-2", Generation: 1})
	if err != nil {
		t.Fatalf("new sibling selected fork: %v", err)
	}
	processFork, err := process.NewSelectedFork(constructionCtx, SelectedForkIdentity{ExecutionID: "process-fork", RunID: "run-3", Generation: 1})
	if err != nil {
		t.Fatalf("new process selected fork: %v", err)
	}
	forkRoute, err := siblingFork.NewRoute(constructionCtx, RouteIdentity{
		RuntimeEpoch: 1,
		Agent:        agentidentitytest.RootRuntime(t, "fork-agent", "worklifetime-test"),
		Generation:   1,
	})
	if err != nil {
		t.Fatalf("new selected-fork route: %v", err)
	}

	cancelConstruction()
	starters := []struct {
		name  string
		begin func(context.Context) (*Lease, error)
	}{
		{name: "runtime", begin: runtime.Begin},
		{name: "runtime route", begin: runtimeRoute.Begin},
		{name: "sibling selected fork", begin: siblingFork.Begin},
		{name: "process selected fork", begin: processFork.Begin},
		{name: "selected-fork route", begin: forkRoute.Begin},
	}
	leases := make([]*Lease, 0, len(starters))
	for _, starter := range starters {
		lease, beginErr := starter.begin(context.Background())
		if beginErr != nil {
			t.Fatalf("begin %s after construction cancellation: %v", starter.name, beginErr)
		}
		select {
		case <-lease.Context().Done():
			t.Fatalf("%s inherited cancelled construction context", starter.name)
		default:
		}
		leases = append(leases, lease)
	}

	process.Retire()
	for i, lease := range leases {
		select {
		case <-lease.Context().Done():
		case <-time.After(time.Second):
			t.Fatalf("%s was not cancelled by owner retirement", starters[i].name)
		}
		if err := lease.Done(); err != nil {
			t.Fatalf("settle %s: %v", starters[i].name, err)
		}
	}
	if _, err := runtime.Begin(context.Background()); !errors.Is(err, ErrRetired) {
		t.Fatalf("runtime admission after process retirement = %v, want ErrRetired", err)
	}
	retirements := []struct {
		name   string
		retire func(context.Context) error
	}{
		{name: "runtime route", retire: runtimeRoute.RetireAndWait},
		{name: "selected-fork route", retire: forkRoute.RetireAndWait},
		{name: "sibling fork", retire: siblingFork.RetireAndWait},
		{name: "process fork", retire: processFork.RetireAndWait},
	}
	for _, retirement := range retirements {
		if err := retirement.retire(context.Background()); err != nil {
			t.Fatalf("retire %s: %v", retirement.name, err)
		}
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("join process: %v", err)
	}
}

func TestBeginRetireRaceRejectsLateAdmissionAndJoinsAcceptedWork(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}

	const contenders = 64
	start := make(chan struct{})
	results := make(chan struct {
		lease *Lease
		err   error
	}, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for range contenders {
		go func() {
			ready.Done()
			<-start
			lease, beginErr := runtime.Begin(context.Background())
			results <- struct {
				lease *Lease
				err   error
			}{lease: lease, err: beginErr}
		}()
	}
	ready.Wait()
	close(start)
	runtime.Retire()

	for range contenders {
		result := <-results
		if result.err != nil {
			if !errors.Is(result.err, ErrRetired) {
				t.Fatalf("racing Begin = %v, want accepted or ErrRetired", result.err)
			}
			continue
		}
		select {
		case <-result.lease.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("accepted racing work was not cancelled by retirement")
		}
		if err := result.lease.Done(); err != nil {
			t.Fatalf("settle accepted racing work: %v", err)
		}
	}
	if _, err := runtime.Begin(context.Background()); !errors.Is(err, ErrRetired) {
		t.Fatalf("Begin after retirement = %v, want ErrRetired", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire and wait: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("process join: %v", err)
	}
}

func TestBufferedRouteDeliveryBlocksRuntimeUntilExactlyOnceCompletion(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	route, err := runtime.NewRoute(context.Background(), RouteIdentity{
		RuntimeEpoch: 1,
		Agent:        agentidentitytest.RootRuntime(t, "agent-1", "worklifetime-test"),
		Generation:   1,
	})
	if err != nil {
		t.Fatalf("new route occurrence: %v", err)
	}
	event := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("message.received"), eventtest.Producer(events.EventProducerPlatform, "test"), "",
		[]byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	delivery, err := route.NewEventDelivery(context.Background(), event)
	if err != nil {
		t.Fatalf("new route delivery: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runtime wait while route item is buffered = %v, want deadline", err)
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete route delivery: %v", err)
	}
	if err := delivery.Complete(); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("second route delivery completion = %v, want ErrAlreadySettled", err)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatalf("runtime wait after delivery completion: %v", err)
	}
	if err := route.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("route retirement: %v", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("runtime retirement: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("process join: %v", err)
	}
}

type scriptedDeliveryContinuation struct {
	attempts   int
	resolution DeliveryContinuationResolution
	err        error
}

func (*scriptedDeliveryContinuation) DeliveryID() string { return "delivery-1" }
func (c *scriptedDeliveryContinuation) Resolve(context.Context, DeliveryContinuationIntent) (DeliveryContinuationResolution, error) {
	c.attempts++
	return c.resolution, c.err
}

func TestEventDeliveryCompletionReportsResolutionFailureWithoutPolling(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	event := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("message.received"), eventtest.Producer(events.EventProducerPlatform, "test"), "",
		[]byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	delivery, err := runtime.NewRoutedEventDelivery(context.Background(), event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("worker")})
	if err != nil {
		t.Fatalf("new routed delivery: %v", err)
	}
	continuation := &scriptedDeliveryContinuation{err: errors.New("injected continuation resolution failure")}
	if err := delivery.AttachContinuation(continuation); err != nil {
		t.Fatalf("attach continuation: %v", err)
	}
	if err := delivery.Complete(); err == nil {
		t.Fatal("completion hid continuation resolution failure")
	}
	if continuation.attempts != 1 {
		t.Fatalf("continuation resolution attempts = %d, want exactly 1", continuation.attempts)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runtime wait after failed continuation return = %v, want deadline", err)
	}
	continuation.err = nil
	continuation.resolution = DeliveryContinuationReturned
	if err := delivery.Complete(); err != nil {
		t.Fatalf("explicit owner repair delivery completion: %v", err)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatalf("runtime wait after successful continuation return: %v", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("runtime retirement: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("process join: %v", err)
	}
}

func TestEventDeliveryCarrierGuardReturnsTypedResolutionExactlyOnce(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-1", BundleHash: "bundle-1"})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	event := eventtest.PersistedProjectionForProducer(
		uuid.NewString(), events.EventType("message.received"), eventtest.Producer(events.EventProducerPlatform, "test"), "",
		[]byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	delivery, err := runtime.NewRoutedEventDelivery(context.Background(), event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("worker")})
	if err != nil {
		t.Fatalf("new routed delivery: %v", err)
	}
	continuation := &scriptedDeliveryContinuation{resolution: DeliveryContinuationReturned}
	if err := delivery.AttachContinuation(continuation); err != nil {
		t.Fatalf("attach continuation: %v", err)
	}
	guard, err := NewEventDeliveryCarrierGuard(delivery)
	if err != nil {
		t.Fatalf("construct carrier guard: %v", err)
	}
	resolution, err := guard.Complete(nil)
	if err != nil {
		t.Fatalf("complete guarded delivery: %v", err)
	}
	if resolution != DeliveryContinuationReturned || continuation.attempts != 1 {
		t.Fatalf("continuation resolution/attempts = %d/%d, want returned/1", resolution, continuation.attempts)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatalf("runtime wait after guarded completion: %v", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("runtime retirement: %v", err)
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatalf("process join: %v", err)
	}
}

func TestDeliveryContinuationGuardReturnsTerminalFenceWithoutPolling(t *testing.T) {
	continuation := &scriptedDeliveryContinuation{resolution: DeliveryContinuationTerminal}
	guard, err := NewDeliveryContinuationGuard(context.Background(), continuation)
	if err != nil {
		t.Fatalf("construct continuation guard: %v", err)
	}
	resolution, err := guard.Consume(nil)
	if err != nil {
		t.Fatalf("resolve terminal carrier: %v", err)
	}
	if resolution != DeliveryContinuationTerminal || continuation.attempts != 1 {
		t.Fatalf("terminal resolution/attempts = %d/%d, want terminal/1", resolution, continuation.attempts)
	}
}

func TestDeliveryContinuationGuardReportsInvariantFailureOnce(t *testing.T) {
	continuation := &scriptedDeliveryContinuation{err: errors.New("injected invariant failure")}
	guard, err := NewDeliveryContinuationGuard(context.Background(), continuation)
	if err != nil {
		t.Fatalf("construct continuation guard: %v", err)
	}
	reported := 0
	if _, err := guard.Consume(func(error) { reported++ }); err == nil {
		t.Fatal("guard hid continuation invariant failure")
	}
	if continuation.attempts != 1 || reported != 1 {
		t.Fatalf("continuation attempts/reports = %d/%d, want 1/1", continuation.attempts, reported)
	}
}
