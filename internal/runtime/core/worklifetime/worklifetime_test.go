package worklifetime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
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
	standing, err := runtime.NewStanding(constructionCtx, StandingIdentity{ServiceID: "service-1", RunID: "run-1", Generation: 1})
	if err != nil {
		t.Fatalf("new standing occurrence: %v", err)
	}
	runtimeRoute, err := runtime.NewRoute(constructionCtx, RouteIdentity{RuntimeEpoch: 1, AgentID: "runtime-agent", Generation: 1})
	if err != nil {
		t.Fatalf("new runtime route: %v", err)
	}
	runtimeFork, err := runtime.NewSelectedFork(constructionCtx, SelectedForkIdentity{ExecutionID: "runtime-fork", RunID: "run-2", Generation: 1})
	if err != nil {
		t.Fatalf("new runtime selected fork: %v", err)
	}
	processFork, err := process.NewSelectedFork(constructionCtx, SelectedForkIdentity{ExecutionID: "process-fork", RunID: "run-3", Generation: 1})
	if err != nil {
		t.Fatalf("new process selected fork: %v", err)
	}
	forkRoute, err := runtimeFork.NewRoute(constructionCtx, RouteIdentity{RuntimeEpoch: 1, AgentID: "fork-agent", Generation: 1})
	if err != nil {
		t.Fatalf("new selected-fork route: %v", err)
	}

	cancelConstruction()
	starters := []struct {
		name  string
		begin func(context.Context) (*Lease, error)
	}{
		{name: "runtime", begin: runtime.Begin},
		{name: "standing", begin: standing.Begin},
		{name: "runtime route", begin: runtimeRoute.Begin},
		{name: "runtime selected fork", begin: runtimeFork.Begin},
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
		{name: "standing", retire: standing.RetireAndWait},
		{name: "runtime fork", retire: runtimeFork.RetireAndWait},
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

func TestRuntimeRouteDeliveryComposesStandingOwnerUntilDescendantCompletion(t *testing.T) {
	process := NewProcess()
	runtime, err := process.NewRuntime(context.Background(), RuntimeIdentity{RuntimeInstanceID: "runtime-standing", BundleHash: "bundle-standing"})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	standing, err := runtime.NewStanding(context.Background(), StandingIdentity{ServiceID: "telegram", RunID: uuid.NewString(), Generation: 1})
	if err != nil {
		t.Fatalf("new standing occurrence: %v", err)
	}
	route, err := runtime.NewRoute(context.Background(), RouteIdentity{RuntimeEpoch: 1, AgentID: "normalizer", Generation: 1})
	if err != nil {
		t.Fatalf("new runtime route: %v", err)
	}
	evt := eventtest.RuntimeControl(uuid.NewString(), events.EventType("telegram.received"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	delivery, err := route.NewEventDelivery(WithOccurrence(context.Background(), standing), evt)
	if err != nil {
		t.Fatalf("new standing-owned route delivery: %v", err)
	}
	if owner, ok := OccurrenceFromContext(delivery.Context()); !ok || owner != standing {
		t.Fatalf("delivery context owner = %T, want exact standing occurrence", owner)
	}
	if err := standing.Fence(); err != nil {
		t.Fatalf("fence standing occurrence: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := standing.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("standing wait before descendant completion = %v, want deadline", err)
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete descendant delivery: %v", err)
	}
	if err := standing.Wait(context.Background()); err != nil {
		t.Fatalf("standing wait after descendant completion: %v", err)
	}
	if err := route.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire route: %v", err)
	}
	if err := standing.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire standing: %v", err)
	}
	if _, err := runtime.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime: %v", err)
	}
	process.Retire()
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
	route, err := runtime.NewRoute(context.Background(), RouteIdentity{RuntimeEpoch: 1, AgentID: "agent-1", Generation: 1})
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
