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
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type recoveryReadProbe struct {
	phase   string
	entered chan context.Context
	release chan struct{}
	failure error
	origin  runlifecycle.RunOrigin
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

func (p *recoveryReadProbe) LoadRunOrigin(ctx context.Context, _ string) (runlifecycle.RunOrigin, error) {
	return p.origin, p.read(ctx, "origin")
}

func (p *recoveryReadProbe) StandingRunRestartDisposition(ctx context.Context, _ string) (runtimepipeline.StandingRestartDisposition, error) {
	if err := p.read(ctx, "standing"); err != nil {
		return runtimepipeline.StandingRestartDisposition{}, err
	}
	return runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{})
}

type recoveryReadScan struct {
	runtimedelivery.Store
	item runtimedelivery.ContinuationItem
}

func (s recoveryReadScan) ScanDeliveryContinuations(context.Context, runtimedelivery.ExecutionAuthority, runtimedelivery.ContinuationCursor, int) (runtimedelivery.ContinuationPage, error) {
	return runtimedelivery.ContinuationPage{Items: []runtimedelivery.ContinuationItem{s.item}, Exhausted: true}, nil
}

func TestContinuationOriginReadDrainsBeforeRetirement(t *testing.T) {
	for _, phase := range []string{"origin", "standing"} {
		for _, stop := range []string{"retire", "parent_cancel"} {
			for _, failure := range []string{"none", "independent", "joined"} {
				t.Run(fmt.Sprintf("%s/%s/%s", phase, stop, failure), func(t *testing.T) {
					process := worklifetime.NewProcess()
					owner := newReceiverProjectionRuntimeOwner(t, process, "recovery-read")
					eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{WorkOwner: owner})
					if err != nil {
						t.Fatal(err)
					}
					probe := &recoveryReadProbe{phase: phase, entered: make(chan context.Context, 1), release: make(chan struct{}), origin: runlifecycle.ScenarioSetupRunOrigin()}
					if phase == "standing" {
						probe.origin, err = runlifecycle.StandingGenerationRunOrigin(uuid.NewString(), 1)
						if err != nil {
							t.Fatal(err)
						}
					}
					independent := errors.New("independent origin read failure")
					if failure == "independent" {
						probe.failure = independent
					}
					if failure == "joined" {
						probe.failure = errors.Join(context.Canceled, independent)
					}
					eb.durable.RunOrigins, eb.durable.StandingRestarts = probe, probe
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
					// The scan's standing classifier is separate from the dispatch's
					// blocked origin/standing read; both are real coordinator handoffs.
					c, err := deliverycontinuation.New(scan, &recoveryOriginStore{}, authority, owner, eb, nil)
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
					started := make(chan error, 1)
					go func() { started <- c.Start(ctx) }()
					var readCtx context.Context
					select {
					case readCtx = <-probe.entered:
					case err := <-started:
						t.Fatalf("did not reach read: %v", err)
					case <-time.After(5 * time.Second):
						t.Fatal("read not entered")
					}
					if stop == "retire" {
						waitCtx, stopWait := context.WithCancel(context.Background())
						stopWait()
						_ = c.Retire(waitCtx)
					} else {
						cancel()
					}
					select {
					case err := <-started:
						t.Errorf("worker completed before read: %v", err)
					default:
					}
					if owner.ActiveCount() == 0 {
						t.Error("read lost joined worker")
					}
					if readCtx.Done() != nil {
						t.Error("admitted origin read still receives retirement cancellation")
					}
					close(probe.release)
					select {
					case err := <-started:
						if err == nil || (failure != "none" && !errors.Is(err, independent)) {
							t.Errorf("start result: %v", err)
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

func TestRecoveryMetadataReadAdmissionAndIsolation(t *testing.T) {
	for _, cut := range []string{"live", "canceled_before", "retired_before", "fenced_before"} {
		t.Run(cut, func(t *testing.T) {
			process := worklifetime.NewProcess()
			owner := newReceiverProjectionRuntimeOwner(t, process, "read-admission")
			defer func() {
				_, _ = owner.RetireAndWait(context.Background())
				process.Retire()
				_, _ = process.Join(context.Background())
			}()
			probe := &recoveryOriginStore{origin: runlifecycle.ScenarioSetupRunOrigin()}
			eb := &EventBus{workOwner: owner, durable: DurableDependencies{RunOrigins: probe, StandingRestarts: probe}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch cut {
			case "canceled_before":
				cancel()
			case "retired_before":
				owner.Retire()
			case "fenced_before":
				if err := owner.Fence(); err != nil {
					t.Fatal(err)
				}
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "read.admission", "test", "", []byte(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			bound, lease, err := eb.bindClaimedRunWork(ctx, event)
			if cut == "live" {
				if err != nil || lease != nil || bound != ctx || probe.loads != 1 {
					t.Fatalf("live binding: lease=%v reads=%d err=%v", lease, probe.loads, err)
				}
				cancel()
				if bound.Err() != context.Canceled {
					t.Error("business context lost cancellation")
				}
			} else if err == nil || lease != nil || probe.loads != 0 {
				t.Fatalf("stopped read admission: lease=%v reads=%d err=%v", lease, probe.loads, err)
			}
			if owner.ActiveCount() != 0 {
				t.Fatal("metadata read leaked lease")
			}
		})
	}
}
