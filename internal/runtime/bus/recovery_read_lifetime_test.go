package bus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type recoveryReadProbe struct {
	phase   string
	entered chan context.Context
	release chan struct{}
	failure error
	reads   atomic.Int32
}

func (p *recoveryReadProbe) read(ctx context.Context, phase string) error {
	p.reads.Add(1)
	if phase != p.phase {
		return nil
	}
	p.entered <- ctx
	<-p.release
	return p.failure
}

func (p *recoveryReadProbe) QueueableIngressPaused(ctx context.Context) (bool, error) {
	return false, p.read(ctx, "ingress")
}

func (p *recoveryReadProbe) QueueableRunDispatchBlocked(ctx context.Context, _ string) (bool, error) {
	return p.phase == "run_parked", p.read(ctx, "run_blocked")
}

func (p *recoveryReadProbe) QueueableRunDispatchParked(ctx context.Context, _ string) (bool, error) {
	return false, p.read(ctx, "run_parked")
}

type recoveryReadScan struct {
	runtimedelivery.Store
	item runtimedelivery.ContinuationItem
}

func (s recoveryReadScan) ScanDeliveryContinuations(context.Context, runtimedelivery.ExecutionAuthority, runtimedelivery.ContinuationCursor, int) (runtimedelivery.ContinuationPage, error) {
	return runtimedelivery.ContinuationPage{Items: []runtimedelivery.ContinuationItem{s.item}, Exhausted: true}, nil
}

func TestContinuationOriginReadDrainsBeforeRetirement(t *testing.T) {
	for _, phase := range []string{"ingress", "run_blocked", "run_parked"} {
		for _, stop := range []string{"retire", "parent_cancel"} {
			for _, failure := range []string{"none", "independent", "joined"} {
				t.Run(fmt.Sprintf("%s/%s/%s", phase, stop, failure), func(t *testing.T) {
					process := worklifetime.NewProcess()
					owner := newReceiverProjectionRuntimeOwner(t, process, "recovery-read")
					eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{WorkOwner: owner})
					if err != nil {
						t.Fatal(err)
					}
					probe := &recoveryReadProbe{phase: phase, entered: make(chan context.Context, 1), release: make(chan struct{})}
					independent := errors.New("independent origin read failure")
					if failure == "independent" {
						probe.failure = independent
					}
					if failure == "joined" {
						probe.failure = errors.Join(context.Canceled, independent)
					}
					eb.SetRuntimeIngressDispatchGate(probe)
					eb.SetRunDispatchGate(probe)
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "recovery.read", "test", "", []byte(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
					route := nodeOnlyDeliveryPlan(t, event, "reader-node").DeliveryRoutes()[0]
					id, err := runtimedelivery.DeliveryID(event.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					authority, err := eb.DeliveryAuthority()
					if err != nil {
						t.Fatal(err)
					}
					scan := recoveryReadScan{item: runtimedelivery.ContinuationItem{DeliveryID: id, Event: event, Disposition: runtimedelivery.ClaimReclaimable, Snapshot: runtimedelivery.Snapshot{DeliveryID: id, RunID: event.RunID(), Route: route}}}
					c, err := deliverycontinuation.New(scan, authority, owner, eb, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := eb.SetDeliveryContinuationOwner(c); err != nil {
						t.Fatal(err)
					}
					var dispatched atomic.Int32
					eb.SetInterceptors(electionNodeInterceptor{resolve: func(context.Context, events.Event, events.DeliveryRoute) error {
						dispatched.Add(1)
						return errors.New("post-stop business dispatch")
					}})
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if err := c.Start(ctx); err != nil {
						t.Fatal(err)
					}
					var readCtx context.Context
					select {
					case readCtx = <-probe.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("read not entered")
					}
					retired := make(chan error, 1)
					if stop == "retire" {
						waitCtx, stopWait := context.WithCancel(context.Background())
						stopWait()
						_ = c.Retire(waitCtx)
						go func() { retired <- c.Retire(context.Background()) }()
					} else {
						cancel()
						go func() { retired <- c.Retire(context.Background()) }()
					}
					select {
					case err := <-retired:
						t.Errorf("worker completed before read: %v", err)
					default:
					}
					if owner.ActiveCount() == 0 {
						t.Error("read lost joined worker")
					}
					if readCtx.Done() != nil {
						t.Error("admitted metadata read still receives retirement cancellation")
					}
					close(probe.release)
					select {
					case err := <-retired:
						if (failure == "none" && err != nil) || (failure != "none" && !errors.Is(err, independent)) {
							t.Errorf("retirement result: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("read did not drain")
					}
					err = c.Retire(context.Background())
					if (failure == "none" && err != nil) || (failure != "none" && !errors.Is(err, independent)) {
						t.Errorf("retirement lost error accounting: %v", err)
					}
					if dispatched.Load() != 0 || owner.ActiveCount() != 0 {
						t.Errorf("post-stop dispatch=%d active=%d", dispatched.Load(), owner.ActiveCount())
					}
					wantReads := map[string]int32{"ingress": 1, "run_blocked": 2, "run_parked": 3}[phase]
					if reads := probe.reads.Load(); reads != wantReads {
						t.Errorf("metadata reads=%d, want %d with no successor read after stop", reads, wantReads)
					}
					if _, err := c.Acquire(id); err == nil {
						t.Error("retired coordinator accepted new carrier")
					}
					if _, err := owner.RetireAndWait(context.Background()); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
		}
	}
}
