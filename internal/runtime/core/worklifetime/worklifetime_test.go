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
