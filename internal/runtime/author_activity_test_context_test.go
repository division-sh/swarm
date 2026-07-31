package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"
const runtimeTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type runtimeTestWorkFixture struct {
	process *worklifetime.Process
}

var runtimeTestWorkFixtures sync.Map
var runtimeTestEventBusOwners sync.Map

type runtimeTestCandidateOwner struct{}

type runtimeTestCandidateRegistration struct{}

func (runtimeTestCandidateOwner) ListCompletionCandidates(
	context.Context,
	runtimerunlifecycle.CandidateScope,
	runtimerunlifecycle.CandidateCursor,
	int,
) (runtimerunlifecycle.CandidatePage, error) {
	return runtimerunlifecycle.CandidatePage{Exhausted: true}, nil
}

func (runtimeTestCandidateOwner) ExecuteCompletionCandidate(
	context.Context,
	runtimerunlifecycle.Candidate,
	runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	return runtimerunlifecycle.CompletionResult{}, errors.New("unexpected runtime test completion candidate")
}

func (runtimeTestCandidateOwner) RegisterCompletionCandidateSink(
	context.Context,
	runtimerunlifecycle.CandidateScope,
	runtimerunlifecycle.CandidateSink,
) (runtimerunlifecycle.CandidateRegistration, error) {
	return runtimeTestCandidateRegistration{}, nil
}

func (runtimeTestCandidateRegistration) Release() {}

func runtimeTestProcessWorkOwner(t testing.TB) *worklifetime.Process {
	t.Helper()
	if existing, ok := runtimeTestWorkFixtures.Load(t); ok {
		return existing.(*runtimeTestWorkFixture).process
	}
	fixture := &runtimeTestWorkFixture{process: worklifetime.NewProcess()}
	actual, loaded := runtimeTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*runtimeTestWorkFixture).process
	}
	t.Cleanup(func() {
		defer runtimeTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join runtime test process owner: %v", err)
		}
	})
	return fixture.process
}

func runtimeTestOccurrence(t testing.TB, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner, err := runtimeTestProcessWorkOwner(t).NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        strings.TrimSpace(bundleHash),
	})
	if err != nil {
		t.Fatalf("create runtime test occurrence: %v", err)
	}
	t.Cleanup(func() {
		owner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("retire runtime test occurrence: %v", err)
		}
	})
	return owner
}

func newRuntimeTestEventBus(t testing.TB, store runtimebus.EventStore) (*runtimebus.EventBus, error) {
	t.Helper()
	return newRuntimeTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{})
}

func newRuntimeTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = testBundleSourceFact(t, runtimeTestBundleHash)
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, runtimeTestBundleHash)
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
			opts.RuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
		}
		opts.DeliveryAuthority = authority
	}
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	var bus *runtimebus.EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = runtimebus.NewEphemeralEventBusWithOptions(store, opts)
	} else {
		bus, err = runtimebus.NewEventBusWithOptions(store, opts)
	}
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(opts.PipelineObligations == nil),
	); err != nil {
		return nil, err
	}
	runtimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { runtimeTestEventBusOwners.Delete(bus) })
	return bus, nil
}

func runtimeTestEventBusWorkOwner(t testing.TB, bus *runtimebus.EventBus) worklifetime.Occurrence {
	t.Helper()
	owner, ok := runtimeTestEventBusOwners.Load(bus)
	if !ok {
		t.Fatal("runtime test event bus has no registered work owner")
	}
	return owner.(worklifetime.Occurrence)
}

func runtimeTestEventBusRuntimeOccurrence(t testing.TB, bus *runtimebus.EventBus) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner := runtimeTestEventBusWorkOwner(t, bus)
	runtimeOwner, ok := owner.(*worklifetime.RuntimeOccurrence)
	if !ok {
		t.Fatalf("runtime test event bus owner is %T, want runtime occurrence", owner)
	}
	return runtimeOwner
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, runtimeTestBundleHash)
}

func testAuthorActivityContextForBundle(ctx context.Context, bundleHash string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	if err != nil {
		panic(err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		bundleHash,
	))
}

func newScopedTestRuntime(t testing.TB, ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	t.Helper()
	if strings.TrimSpace(deps.Options.RuntimeInstanceID) == "" {
		deps.Options.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if deps.Options.BundleSourceFact.Validate() != nil {
		deps.Options.BundleSourceFact = testBundleSourceFact(t, runtimeTestBundleHash)
	}
	if deps.Options.ProcessWorkOwner == nil {
		deps.Options.ProcessWorkOwner = runtimeTestProcessWorkOwner(t)
	}
	if deps.Stores.SQLDB != nil && deps.Stores.RunLifecycleCandidates == nil {
		if candidates, ok := deps.Stores.EventStore.(runtimerunlifecycle.CandidateOwner); ok {
			deps.Stores.RunLifecycleCandidates = candidates
		} else {
			deps.Stores.RunLifecycleCandidates = runtimeTestCandidateOwner{}
		}
	}
	if deps.Stores.PipelineObligations == nil {
		if provider, ok := deps.Stores.EventStore.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			deps.Stores.PipelineObligations = provider.PipelineObligations()
		}
	}
	runtime, err := NewRuntime(ctx, deps)
	if err == nil {
		if runtime.Bus.DeliveryContinuationOwner() == nil {
			if ownerErr := runtime.Bus.SetDeliveryContinuationOwner(
				runtimebustest.NewDeliveryContinuationOwner(true),
			); ownerErr != nil {
				return nil, ownerErr
			}
		}
		t.Cleanup(func() {
			if shutdownErr := runtime.Shutdown(); shutdownErr != nil {
				t.Errorf("shutdown runtime test fixture: %v", shutdownErr)
			}
		})
	}
	return runtime, err
}

func testBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct test bundle source fact: %v", err)
	}
	return fact
}

func testPersistedBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct persisted test bundle source fact: %v", err)
	}
	return fact
}
