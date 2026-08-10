package builder

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

const builderTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type testRuntimeAcquirer struct {
	runtime *runtimepkg.Runtime
	owner   *worklifetime.RuntimeOccurrence
	process *worklifetime.Process
}

type testRuntimeUse struct {
	runtime *runtimepkg.Runtime
	lease   *worklifetime.Lease
	ctx     context.Context
}

func newTestRuntimeAcquirer(t testing.TB, rt *runtimepkg.Runtime) RuntimeAcquirer {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        builderTestBundleHash,
	})
	if err != nil {
		t.Fatalf("new builder test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire builder test runtime occurrence: %v", err)
			return
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join builder test process: %v", err)
		}
	})
	return &testRuntimeAcquirer{runtime: rt, owner: owner, process: process}
}

func newTestOwnedEventBus(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimepkg.Runtime, *testRuntimeAcquirer) {
	t.Helper()
	acquirer := newTestRuntimeAcquirer(t, nil).(*testRuntimeAcquirer)
	opts.WorkOwner = acquirer.owner
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.BundleSourceFact.Validate() != nil {
		fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(builderTestBundleHash)
		if err != nil {
			t.Fatalf("construct builder test bundle source fact: %v", err)
		}
		opts.BundleSourceFact = fact
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, err := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
			acquirer.owner.Identity().RuntimeInstanceID,
			1,
		)
		if err != nil {
			t.Fatalf("construct builder test delivery authority: %v", err)
		}
		opts.DeliveryAuthority = authority
	}
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(store, opts)
	if err != nil {
		t.Fatalf("new owned builder event bus: %v", err)
	}
	if err := bus.SetDeliveryContinuationOwner(&builderTestDeliveryOwner{}); err != nil {
		t.Fatalf("set builder test delivery continuation owner: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus, ExecutionPosture: opts.ExecutionPosture}
	acquirer.runtime = rt
	return rt, acquirer
}

func recordBuilderTestDeliveryReceipt(request runtimebus.CommitPublishRequest) error {
	_ = request
	return nil
}

type builderTestDeliveryOwner struct{}

func (*builderTestDeliveryOwner) AcceptCommitted([]runtimedelivery.DurableHandoffProof) error {
	return nil
}

func (*builderTestDeliveryOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	if strings.TrimSpace(deliveryID) == "" {
		return nil, errors.New("builder test delivery id is required")
	}
	return &builderTestDeliveryContinuation{deliveryID: deliveryID}, nil
}

func (*builderTestDeliveryOwner) Retain(runtimedelivery.Snapshot) error { return nil }
func (*builderTestDeliveryOwner) Release(string) error                  { return nil }
func (*builderTestDeliveryOwner) OwnsPersistedRecovery() bool           { return false }
func (*builderTestDeliveryOwner) Signal()                               {}

type builderTestDeliveryContinuation struct {
	mu         sync.Mutex
	deliveryID string
	settled    bool
}

func (c *builderTestDeliveryContinuation) DeliveryID() string { return c.deliveryID }

func (c *builderTestDeliveryContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if intent != worklifetime.DeliveryContinuationReturn && intent != worklifetime.DeliveryContinuationConsume {
		return 0, errors.New("builder test delivery continuation intent is invalid")
	}
	if err := c.settle(); err != nil {
		return 0, err
	}
	if intent == worklifetime.DeliveryContinuationReturn {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	return worklifetime.DeliveryContinuationConsumed, nil
}

func (c *builderTestDeliveryContinuation) settle() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return errors.New("builder test delivery continuation is already settled")
	}
	c.settled = true
	return nil
}

func (a *testRuntimeAcquirer) AcquireCurrentRuntime(ctx context.Context) (RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *testRuntimeAcquirer) AcquireRunRuntime(ctx context.Context, _ string) (RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *testRuntimeAcquirer) acquire(ctx context.Context) (RuntimeUse, error) {
	ctx = worklifetime.WithProcess(ctx, a.process)
	ctx = worklifetime.WithOccurrence(ctx, a.owner)
	lease, err := a.owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	workCtx := worklifetime.WithProcess(lease.Context(), a.process)
	workCtx = worklifetime.WithOccurrence(workCtx, a.owner)
	return &testRuntimeUse{runtime: a.runtime, lease: lease, ctx: workCtx}, nil
}

func (u *testRuntimeUse) Runtime() *runtimepkg.Runtime { return u.runtime }
func (u *testRuntimeUse) WorkContext() context.Context { return u.ctx }
func (u *testRuntimeUse) Done() error                  { return u.lease.Done() }
