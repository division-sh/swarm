package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	pipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Both publishers must finish their missing-owner plans before either can commit.
// Waiting for two finalizers is too late: the second lookup may reuse the first commit.
type concurrentPlanPreparationBarrier struct {
	*testFlowInstanceActivationOwner
	planner  pipeline.FlowInstanceActivationPlanner
	eventIDs [2]string
	mu       sync.Mutex
	seen     [2]bool
	first    *pipeline.FlowInstanceActivationPlan
	done     chan struct{}
	prepared chan pipeline.FlowInstanceActivationPlan
	ended    bool
	failure  error
}

func newConcurrentPlanPreparationBarrier(owner *testFlowInstanceActivationOwner, eventIDs [2]string) *concurrentPlanPreparationBarrier {
	return &concurrentPlanPreparationBarrier{
		testFlowInstanceActivationOwner: owner, planner: owner, eventIDs: eventIDs,
		done: make(chan struct{}), prepared: make(chan pipeline.FlowInstanceActivationPlan, 2),
	}
}

func (o *concurrentPlanPreparationBarrier) PrepareFlowInstanceActivation(ctx context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.FlowInstanceActivationPlan{}, o.abort(err)
	}
	plan, err := o.planner.PrepareFlowInstanceActivation(ctx, request)
	if err == nil {
		err = plan.Validate()
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = o.arrive(plan)
	}
	if err != nil {
		return pipeline.FlowInstanceActivationPlan{}, o.abort(err)
	}
	select {
	case <-ctx.Done():
		o.abort(ctx.Err())
	case <-o.done:
	}
	o.mu.Lock()
	err = errors.Join(o.failure, ctx.Err())
	o.mu.Unlock()
	if err != nil {
		return pipeline.FlowInstanceActivationPlan{}, err
	}
	return plan, nil
}

func (o *concurrentPlanPreparationBarrier) arrive(plan pipeline.FlowInstanceActivationPlan) (err error) {
	o.mu.Lock()
	defer func() {
		if err != nil && !o.ended {
			o.failure, o.ended = err, true
			close(o.done)
		}
		o.mu.Unlock()
	}()
	if o.ended {
		return errors.Join(o.failure, errors.New("preparation rendezvous has ended"))
	}
	participant := slices.Index(o.eventIDs[:], plan.CreatingInput.EventID)
	if o.eventIDs[0] == "" || o.eventIDs[1] == "" || o.eventIDs[0] == o.eventIDs[1] || participant < 0 {
		return errors.New("preparation rendezvous requires two exact distinct publishers")
	}
	if o.seen[participant] {
		return errors.New("duplicate publisher cannot complete preparation rendezvous")
	}
	if o.first != nil && !sameConcurrentPlanScope(*o.first, plan) {
		return errors.New("preparation rendezvous crosses source/run/receiver scope")
	}
	if o.first == nil {
		o.first = &plan
	}
	o.seen[participant] = true
	o.prepared <- plan
	if o.seen[0] && o.seen[1] {
		o.ended = true
		close(o.done)
	}
	return nil
}

func sameConcurrentPlanScope(a, b pipeline.FlowInstanceActivationPlan) bool {
	return a.Identity == b.Identity && a.Readiness.RunID == b.Readiness.RunID &&
		a.Readiness.BundleHash == b.Readiness.BundleHash && a.Readiness.WorkflowVersion == b.Readiness.WorkflowVersion &&
		a.Instance.WorkflowVersion == b.Instance.WorkflowVersion &&
		a.Instance.InstanceKey == b.Instance.InstanceKey && a.CreatingInput.Input == b.CreatingInput.Input
}

func (o *concurrentPlanPreparationBarrier) abort(cause error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.ended {
		o.failure, o.ended = cause, true
		close(o.done)
	}
	return errors.Join(cause, o.failure)
}

type concurrentPublicationFixture struct {
	source semanticview.Source
	store  *connectRoutePlanConcurrentLifecycleStore
	owner  *testFlowInstanceActivationOwner
	events [2]events.Event
}

func newConcurrentPublicationFixture(t *testing.T) concurrentPublicationFixture {
	t.Helper()
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelectOrCreate)
	store := &connectRoutePlanConcurrentLifecycleStore{connectRoutePlanLifecycleStore: &connectRoutePlanLifecycleStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}}
	f := concurrentPublicationFixture{source: source, store: store, owner: newTestFlowInstanceActivationOwner(store.Activate)}
	for i := range f.events {
		f.events[i] = connectRoutePlanStaticProducerEvent(uuid.NewString(), "producer/account.ready", "", "", []byte(`{"account_id":"acct-race"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	}
	return f
}

func (f concurrentPublicationFixture) bus(t *testing.T, owner pipeline.FlowInstanceActivationPlanner) *EventBus {
	t.Helper()
	eb, err := newScopedTestEventBus(f.store, EventBusOptions{
		ContractBundle: f.source, Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
		TemplateInstancePlanner: owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	installConnectionSourceConstruction(t, eb, f.source, "producer")
	return eb
}

func (f concurrentPublicationFixture) barrier() *concurrentPlanPreparationBarrier {
	return newConcurrentPlanPreparationBarrier(f.owner, [2]string{f.events[0].ID(), f.events[1].ID()})
}

func awaitConcurrentPlan(t *testing.T, ctx context.Context, prepared <-chan pipeline.FlowInstanceActivationPlan) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	select {
	case plan := <-prepared:
		return plan
	case <-ctx.Done():
		t.Fatal("publisher did not reach the observed preparation phase:", ctx.Err())
		return pipeline.FlowInstanceActivationPlan{}
	}
}

func TestConcurrentPlanPreparationBarrierHoldsFirstPlanBeforeCommit(t *testing.T) {
	f := newConcurrentPublicationFixture(t)
	barrier := f.barrier()
	eb := f.bus(t, barrier)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var joined sync.WaitGroup
	defer func() { cancel(); joined.Wait() }()
	first := make(chan error, 1)
	joined.Go(func() { first <- eb.Publish(ctx, f.events[0]) })
	firstPlan := awaitConcurrentPlan(t, ctx, barrier.prepared)
	barrier.mu.Lock()
	ended, seen := barrier.ended, barrier.seen
	barrier.mu.Unlock()
	if ended || seen != [2]bool{true, false} || len(f.store.events) != 0 {
		t.Fatalf("first plan did not remain pre-commit: ended=%t seen=%v events=%d", ended, seen, len(f.store.events))
	}
	if err := eb.Publish(ctx, f.events[1]); err != nil {
		t.Fatal("second publisher:", err)
	}
	if err := <-first; err != nil {
		t.Fatal("first publisher:", err)
	}
	secondPlan := awaitConcurrentPlan(t, ctx, barrier.prepared)
	if !sameConcurrentPlanScope(firstPlan, secondPlan) || firstPlan.CreatingInput.EventID != f.events[0].ID() || secondPlan.CreatingInput.EventID != f.events[1].ID() {
		t.Fatal("rendezvous did not acknowledge both exact valid plans")
	}
	if len(f.store.events) != 2 || f.store.ActivationCount() != 1 {
		t.Fatal("two prepared publications did not converge on one activation")
	}
}

func TestConcurrentPlanPreparationBarrierAbortsAndJoinsPublishers(t *testing.T) {
	for _, scenario := range []string{"cancel_waiter", "preparation_failure", "duplicate_publisher", "foreign_publisher", "foreign_run", "foreign_source", "foreign_workflow", "foreign_receiver", "foreign_key", "foreign_input"} {
		t.Run(scenario, func(t *testing.T) {
			f := newConcurrentPublicationFixture(t)
			barrier := f.barrier()
			eb := f.bus(t, barrier)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			firstCtx, cancelFirst := context.WithCancel(ctx)
			var joined sync.WaitGroup
			defer func() { cancelFirst(); cancel(); joined.Wait() }()
			first := make(chan error, 1)
			joined.Go(func() { first <- eb.Publish(firstCtx, f.events[0]) })
			plan := awaitConcurrentPlan(t, ctx, barrier.prepared)
			if len(f.store.events) != 0 {
				t.Fatal("a publisher committed before the second plan")
			}
			cause := errors.New("second preparation failed")
			second := f.events[1]
			var firstErr error
			switch scenario {
			case "cancel_waiter":
				cancelFirst()
				firstErr = <-first
				if firstErr == nil {
					t.Fatal("canceled preparation waiter succeeded")
				}
				cause = context.Canceled
			case "preparation_failure":
				barrier.planner = pipeline.FlowInstanceActivationPlannerFunc(func(context.Context, pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
					return pipeline.FlowInstanceActivationPlan{}, cause
				})
			case "duplicate_publisher":
				barrier.planner = pipeline.FlowInstanceActivationPlannerFunc(func(next context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
					nextPlan, err := f.owner.PrepareFlowInstanceActivation(next, request)
					nextPlan.CreatingInput.EventID = plan.CreatingInput.EventID
					return nextPlan, err
				})
			case "foreign_publisher":
				barrier.eventIDs[1] = uuid.NewString()
			default:
				barrier.planner = pipeline.FlowInstanceActivationPlannerFunc(func(next context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
					nextPlan, err := f.owner.PrepareFlowInstanceActivation(next, request)
					if err == nil {
						mutateConcurrentPlanScope(scenario, &nextPlan)
					}
					return nextPlan, err
				})
			}
			secondErr := eb.Publish(ctx, second)
			if firstErr == nil {
				firstErr = <-first
			}
			if firstErr == nil || secondErr == nil {
				t.Fatalf("incomplete rendezvous succeeded: first=%v second=%v plan=%+v", firstErr, secondErr, plan.Identity)
			}
			if errors.Is(firstErr, context.DeadlineExceeded) || errors.Is(secondErr, context.DeadlineExceeded) {
				t.Fatalf("rendezvous waited for its safety deadline: first=%v second=%v", firstErr, secondErr)
			}
			barrier.mu.Lock()
			failure := barrier.failure
			barrier.mu.Unlock()
			if failure == nil || !errors.Is(firstErr, failure) || !errors.Is(secondErr, failure) {
				t.Fatalf("publishers did not consume the same failed rendezvous: first=%v second=%v cause=%v", firstErr, secondErr, failure)
			}
			if (scenario == "cancel_waiter" || scenario == "preparation_failure") && (!errors.Is(firstErr, cause) || !errors.Is(secondErr, cause)) {
				t.Fatalf("lost original failure: first=%v second=%v cause=%v", firstErr, secondErr, cause)
			}
			if err := eb.Publish(ctx, f.events[0]); err == nil || !errors.Is(err, failure) {
				t.Fatalf("late publisher escaped failed rendezvous: %v", err)
			}
			if len(f.store.events) != 0 || f.store.ActivationCount() != 0 || len(f.store.routes) != 0 {
				t.Fatal("failed rendezvous committed publication or activation")
			}
			for _, observed := range f.store.instanceObservations {
				if observed.Identity().TemplateID == "account" {
					t.Fatal("failed rendezvous published receiver construction")
				}
			}
		})
	}
}

func mutateConcurrentPlanScope(scenario string, plan *pipeline.FlowInstanceActivationPlan) {
	switch scenario {
	case "foreign_run":
		plan.Readiness.RunID = uuid.NewString()
	case "foreign_source":
		plan.Readiness.BundleHash = "bundle-v2:sha256:" + fmt.Sprintf("%064x", 1)
	case "foreign_workflow":
		plan.Readiness.WorkflowVersion = "other-workflow"
		plan.Instance.WorkflowVersion = "other-workflow"
	case "foreign_receiver":
		plan.Identity.EntityID = uuid.NewString()
	case "foreign_key":
		plan.Instance.InstanceKey = "other-key"
	case "foreign_input":
		plan.CreatingInput.Input = "other.ready"
	}
}

type heldCommittedActivationOwner struct {
	*testFlowInstanceActivationOwner
	arrivals chan pipeline.CommittedFlowInstanceActivation
	release  chan struct{}
}

func (o *heldCommittedActivationOwner) FinalizeCommittedFlowInstanceActivation(ctx context.Context, committed pipeline.CommittedFlowInstanceActivation) error {
	o.arrivals <- committed
	select {
	case <-o.release:
		return o.testFlowInstanceActivationOwner.FinalizeCommittedFlowInstanceActivation(ctx, committed)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestConcurrentPlanFinalizerArrivalIsNotAPreparationRendezvous(t *testing.T) {
	f := newConcurrentPublicationFixture(t)
	owner := &heldCommittedActivationOwner{testFlowInstanceActivationOwner: f.owner, arrivals: make(chan pipeline.CommittedFlowInstanceActivation, 2), release: make(chan struct{})}
	eb := f.bus(t, owner)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var joined sync.WaitGroup
	release := sync.OnceFunc(func() { close(owner.release) })
	defer func() { cancel(); release(); joined.Wait() }()
	first := make(chan error, 1)
	joined.Go(func() { first <- eb.Publish(ctx, f.events[0]) })
	var committed pipeline.CommittedFlowInstanceActivation
	select {
	case committed = <-owner.arrivals:
	case <-ctx.Done():
		t.Fatal("first commit did not reach finalization:", ctx.Err())
	}
	if !committed.Acknowledged || committed.Plan.CreatingInput.EventID != f.events[0].ID() {
		t.Fatal("first finalization lacks exact acknowledged construction")
	}
	if err := eb.Publish(ctx, f.events[1]); err != nil {
		t.Fatal("existing-receiver publication did not finish independently:", err)
	}
	select {
	case extra := <-owner.arrivals:
		t.Fatalf("existing receiver fabricated a second activation: %+v", extra)
	default:
	}
	select {
	case err := <-first:
		t.Fatalf("first finalization escaped its explicit hold: %v", err)
	default:
	}
	requireOrderedSameKeyPublicationEvidence(t, ctx, f, committed)
	release()
	if err := <-first; err != nil {
		t.Fatal("first finalization:", err)
	}
	if f.store.ActivationCount() != 1 {
		t.Fatal("expected exactly one activation after releasing its owner")
	}
	t.Log("controlled_counterexample: committed receiver reused by second publisher; finalizer arrivals=1, not 2; historical scheduler not reconstructed")
}

func requireOrderedSameKeyPublicationEvidence(t *testing.T, ctx context.Context, f concurrentPublicationFixture, committed pipeline.CommittedFlowInstanceActivation) {
	t.Helper()
	identity := committed.Plan.Identity
	evidence, err := f.store.LoadFlowConstructionPublication(ctx, testRunScopedFlowRouteForRun(busInternalTestRunID, identity.Route()), identity.EntityID)
	if err != nil || evidence.Identity != identity || evidence.InstanceKey != "acct-race" || evidence.CreatingInput != committed.Plan.CreatingInput || !reflect.DeepEqual(evidence.Fields, committed.Plan.Instance.Fields) {
		t.Fatalf("lost exact original construction/receipt: %+v err=%v", evidence, err)
	}
	var observed []pipeline.FlowInstanceObservation
	for _, candidate := range f.store.instanceObservations {
		if candidate.Identity().TemplateID == "account" {
			observed = append(observed, candidate)
		}
	}
	if len(observed) != 1 || observed[0].Identity() != identity || observed[0].InstanceKey() != "acct-race" {
		t.Fatal("ordered publications did not retain exactly one constructed receiver")
	}
	for i, event := range f.events {
		routes, err := f.store.ListEventDeliveryRoutes(ctx, event.ID())
		if err != nil || len(routes) != 1 {
			t.Fatalf("publication %d routes=%+v err=%v", i, routes, err)
		}
		route := routes[0]
		if route.Recipient != events.MustNodeDeliveryRecipient(testFlowNode(t, "account", "account-node")) || route.Target.Route() != (events.RouteIdentity{FlowID: "account", FlowInstance: identity.InstancePath, EntityID: identity.EntityID}) {
			t.Fatalf("publication %d changed recipient/target: %+v", i, route)
		}
		if (i == 0 && !route.Target.MaterializingEntity()) || (i == 1 && !route.Target.ExistingEntity()) {
			t.Fatalf("ordered publication %d changed target disposition: %+v", i, route)
		}
		wantInitialization := events.ReceiverInitialization{}
		if i == 0 {
			wantInitialization, err = events.AdmitFlowReceiverInitialization(event, route.Target, flowReceiverRouteFixture(t, identity.Route()))
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(route.Initialization, wantInitialization) {
			t.Fatalf("ordered publication %d changed initialization evidence: %+v", i, route)
		}
	}
	if len(f.store.events) != 2 || f.store.ActivationCount() != 0 {
		t.Fatal("expected two committed publications before the single held activation")
	}
}
