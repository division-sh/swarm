package bus

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

type sourceMutationProbeOwner struct {
	runtimepipelineobligation.Store

	mu               sync.Mutex
	claimIssuer      *runtimepipelineobligation.ClaimIssuer
	scanIssuer       *runtimepipelineobligation.ScanIssuer
	claims           map[string]runtimepipelineobligation.Claim
	opened           chan struct{}
	openOnce         sync.Once
	claimPublication int
	claimEvent       int
	openScan         int
	claimBatch       int
	closeScan        int
	release          int
	settle           int
	decision         int
	retainScan       bool
	claimFact        runtimecorrelation.BundleSourceFact
	openFact         runtimecorrelation.BundleSourceFact
	batchFact        runtimecorrelation.BundleSourceFact
	closeFact        runtimecorrelation.BundleSourceFact
	releaseFact      runtimecorrelation.BundleSourceFact
	settleFact       runtimecorrelation.BundleSourceFact
	decisionFact     runtimecorrelation.BundleSourceFact
}

type sourceMutationProbeCounts struct {
	claimPublication int
	claimEvent       int
	openScan         int
	claimBatch       int
	closeScan        int
	release          int
	settle           int
	decision         int
}

func newSourceMutationProbeOwner() *sourceMutationProbeOwner {
	return &sourceMutationProbeOwner{
		claimIssuer: runtimepipelineobligation.NewClaimIssuer(),
		scanIssuer:  runtimepipelineobligation.NewScanIssuer(),
		claims:      map[string]runtimepipelineobligation.Claim{},
		opened:      make(chan struct{}),
	}
}

func (o *sourceMutationProbeOwner) ClaimPublication(ctx context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimPublication++
	o.claimFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	claim, err := o.claimIssuer.Issue(eventID, runtimepipelineobligation.PurposePublication)
	if err == nil {
		o.claims[eventID] = claim
	}
	return claim, err
}

func (o *sourceMutationProbeOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimEvent++
	claim, err := o.claimIssuer.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	o.claims[eventID] = claim
	return runtimepipelineobligation.ClaimedWork{Claim: claim}, nil
}

func (o *sourceMutationProbeOwner) OpenScan(ctx context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.openScan++
	o.openFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	o.openOnce.Do(func() { close(o.opened) })
	return o.scanIssuer.Issue()
}

func (o *sourceMutationProbeOwner) ClaimBatch(ctx context.Context, _ runtimepipelineobligation.Scan, _ int) (runtimepipelineobligation.ScanBatch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimBatch++
	o.batchFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	if o.retainScan {
		return runtimepipelineobligation.ScanBatch{Examined: 1}, nil
	}
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (o *sourceMutationProbeOwner) CloseScan(ctx context.Context, scan runtimepipelineobligation.Scan) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closeScan++
	o.closeFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	_, err := o.scanIssuer.Token(scan)
	return err
}

func (o *sourceMutationProbeOwner) MarkDecisionProcessed(ctx context.Context, _ runtimepipelineobligation.Claim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.decision++
	o.decisionFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	return nil
}

func (o *sourceMutationProbeOwner) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.settle++
	o.settleFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	delete(o.claims, claim.EventID())
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
}

func (o *sourceMutationProbeOwner) Release(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.release++
	o.releaseFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	delete(o.claims, claim.EventID())
	return nil
}

func (*sourceMutationProbeOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func (*sourceMutationProbeOwner) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}, nil
}

func (*sourceMutationProbeOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

func (o *sourceMutationProbeOwner) counts() sourceMutationProbeCounts {
	o.mu.Lock()
	defer o.mu.Unlock()
	return sourceMutationProbeCounts{
		claimPublication: o.claimPublication,
		claimEvent:       o.claimEvent,
		openScan:         o.openScan,
		claimBatch:       o.claimBatch,
		closeScan:        o.closeScan,
		release:          o.release,
		settle:           o.settle,
		decision:         o.decision,
	}
}

func (o *sourceMutationProbeOwner) owns(eventID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.claims[eventID]
	return ok
}

func (o *sourceMutationProbeOwner) sourceFact(action string) runtimecorrelation.BundleSourceFact {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch action {
	case "claim":
		return o.claimFact
	case "open":
		return o.openFact
	case "batch":
		return o.batchFact
	case "close":
		return o.closeFact
	case "release":
		return o.releaseFact
	case "settle":
		return o.settleFact
	case "decision":
		return o.decisionFact
	default:
		return runtimecorrelation.BundleSourceFact{}
	}
}

type sourceBoundaryProbeStore struct {
	InMemoryEventStore
	runtimedelivery.Store

	upsertCalls int
	deleteCalls int
	bindCalls   int
	routes      map[string]FlowInstanceRouteRecord
	upsertFact  runtimecorrelation.BundleSourceFact
	deleteFact  runtimecorrelation.BundleSourceFact
	bindFact    runtimecorrelation.BundleSourceFact
}

func (s *sourceBoundaryProbeStore) UpsertFlowInstanceRoute(ctx context.Context, route FlowInstanceRouteRecord) error {
	s.upsertCalls++
	s.upsertFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	if s.routes == nil {
		s.routes = map[string]FlowInstanceRouteRecord{}
	}
	s.routes[route.Identity.InstancePath+"\x00"+route.EventPattern] = route
	return nil
}

func (s *sourceBoundaryProbeStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	s.deleteCalls++
	s.deleteFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	for key, route := range s.routes {
		if route.Identity == identity {
			delete(s.routes, key)
		}
	}
	return nil
}

func (s *sourceBoundaryProbeStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []FlowInstanceRouteRecord,
) error {
	if len(routes) == 0 {
		return s.DeleteFlowInstanceRoute(ctx, identity)
	}
	for _, route := range routes {
		if err := s.UpsertFlowInstanceRoute(ctx, route); err != nil {
			return err
		}
	}
	return nil
}

func (s *sourceBoundaryProbeStore) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []FlowInstanceRouteRecordSet,
) error {
	for _, set := range sets {
		if err := s.ReplaceFlowInstanceRouteRecords(ctx, set.Identity, set.Routes); err != nil {
			return err
		}
	}
	return nil
}

func (*sourceBoundaryProbeStore) ListActiveFlowInstanceDescriptors(context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}

func (*sourceBoundaryProbeStore) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (s *sourceBoundaryProbeStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	seen := map[runtimeflowidentity.Route]struct{}{}
	for _, route := range s.routes {
		seen[route.Identity] = struct{}{}
	}
	out := make([]runtimeflowidentity.Route, 0, len(seen))
	for route := range seen {
		out = append(out, route)
	}
	return out, nil
}

func (s *sourceBoundaryProbeStore) BindAgentSession(ctx context.Context, _ runtimedelivery.Claim, _ string) (runtimedelivery.Snapshot, error) {
	s.bindCalls++
	s.bindFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	return runtimedelivery.Snapshot{}, nil
}

func sourceMutationFact(t testing.TB, marker string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat(marker, 64))
	if err != nil {
		t.Fatalf("construct source fact: %v", err)
	}
	return fact
}

func sourceMutationRouteSource() semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Path:   "work",
		Paths:  runtimecontracts.FlowContractPaths{ID: "work", Flow: "work"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "template"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {},
			"task.requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				ID:           "worker-{instance_id}",
				Produces:     []string{"task.completed"},
				SubscribesTo: []string{"task.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"work": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"work": {Mode: "template"}},
	})
}

func newSourceMutationProbeBus(
	t testing.TB,
	fact runtimecorrelation.BundleSourceFact,
	owner *sourceMutationProbeOwner,
) *EventBus {
	return newSourceMutationProbeBusWithStore(t, InMemoryEventStore{}, fact, owner)
}

func newSourceMutationProbeBusWithStore(
	t testing.TB,
	store EventStore,
	fact runtimecorrelation.BundleSourceFact,
	owner *sourceMutationProbeOwner,
	sources ...semanticview.Source,
) *EventBus {
	t.Helper()
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        "source-mutation-probe",
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = runtimeOwner.RetireAndWait(ctx)
		process.Retire()
		_, _ = process.Join(ctx)
	})
	options := EventBusOptions{
		BundleSourceFact:    fact,
		RuntimeInstanceID:   uuid.NewString(),
		PipelineObligations: owner,
		Durable:             ExactDurableTestDependencies(store),
		WorkOwner:           runtimeOwner,
		ReceiverExecution:   eventreceiver.NormalExecution(),
		DeliveryAuthority:   mustSourceMutationDeliveryAuthority(t, fact),
		ExecutionPosture:    executionposture.Live,
	}
	if len(sources) > 0 {
		options.ContractBundle = sources[0]
	}
	bus, err := NewEventBusWithOptions(store, options)
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	if err := bus.SetDeliveryContinuationOwner(permissiveTestDeliveryOwner{}); err != nil {
		t.Fatalf("install delivery continuation owner: %v", err)
	}
	return bus
}

func mustSourceMutationDeliveryAuthority(t testing.TB, fact runtimecorrelation.BundleSourceFact) runtimedelivery.ExecutionAuthority {
	t.Helper()
	authority, err := runtimedelivery.NewNormalExecutionAuthority(fact, uuid.NewString(), 1)
	if err != nil {
		t.Fatalf("construct delivery authority: %v", err)
	}
	return authority
}

func newFencedSourceMutationOccurrence(t testing.TB) worklifetime.Occurrence {
	t.Helper()
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        "foreign-fenced-source",
	})
	if err != nil {
		t.Fatalf("create fenced runtime occurrence: %v", err)
	}
	if err := runtimeOwner.Fence(); err != nil {
		t.Fatalf("fence foreign runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = runtimeOwner.RetireAndWait(ctx)
		process.Retire()
		_, _ = process.Join(ctx)
	})
	return runtimeOwner
}

func sourceMutationEvent() events.Event {
	return eventtest.RuntimeControl(
		uuid.NewString(),
		events.EventType("test.source_mutation"),
		"test",
		"",
		[]byte(`{}`),
		0,
		uuid.NewString(),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}

func TestDurableEventBusConstructionRequiresImmutableBundleSourceFact(t *testing.T) {
	owner := newSourceMutationProbeOwner()
	if _, err := NewEventBusWithOptions(InMemoryEventStore{}, EventBusOptions{
		PipelineObligations: owner,
		ExecutionPosture:    executionposture.Live,
	}); err == nil || !strings.Contains(err.Error(), "immutable bundle source fact") {
		t.Fatalf("durable constructor error = %v, want immutable source rejection", err)
	}
	if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
		t.Fatalf("pipeline mutations after constructor rejection = %#v, want zero", got)
	}
	if _, err := NewEphemeralEventBus(InMemoryEventStore{}); err != nil {
		t.Fatalf("ownerless ephemeral constructor: %v", err)
	}
}

func TestPublishRootsRejectForeignSourceBeforeWorkOccurrenceAdmission(t *testing.T) {
	owned := sourceMutationFact(t, "a")
	foreign := sourceMutationFact(t, "b")
	owner := newSourceMutationProbeOwner()
	bus := newSourceMutationProbeBus(t, owned, owner)
	fenced := newFencedSourceMutationOccurrence(t)

	for _, tc := range []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "publish", run: func(ctx context.Context) error {
			return bus.Publish(ctx, sourceMutationEvent())
		}},
		{name: "publish and wait", run: func(ctx context.Context) error {
			return bus.PublishAndWait(ctx, sourceMutationEvent())
		}},
		{name: "publish acknowledged", run: func(ctx context.Context) error {
			return bus.PublishAcknowledged(ctx, sourceMutationEvent())
		}},
		{name: "direct", run: func(ctx context.Context) error {
			return bus.PublishDirect(ctx, sourceMutationEvent(), []string{"agent-a"})
		}},
		{name: "direct routes", run: func(ctx context.Context) error {
			return bus.PublishDirectRoutes(ctx, sourceMutationEvent(), []events.DeliveryRoute{{
				Recipient: events.MustAgentDeliveryRecipient("agent-a"),
			}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
			ctx = worklifetime.WithOccurrence(ctx, fenced)
			err := tc.run(ctx)
			if err == nil || !strings.Contains(err.Error(), "bundle source fact conflicts") {
				t.Fatalf("error = %v, want source conflict before work admission", err)
			}
			if strings.Contains(err.Error(), "work admission is fenced") {
				t.Fatalf("error = %v, source admission lost precedence to work fencing", err)
			}
			if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
				t.Fatalf("pipeline mutations = %#v, want zero", got)
			}
		})
	}
}

func TestAdjacentDurableMutationRootsRejectForeignSourceBeforeMutation(t *testing.T) {
	owned := sourceMutationFact(t, "c")
	foreign := sourceMutationFact(t, "d")
	owner := newSourceMutationProbeOwner()
	store := &sourceBoundaryProbeStore{}
	bus := newSourceMutationProbeBusWithStore(t, store, owned, owner, sourceMutationRouteSource())
	foreignCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
	req := FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("work", "instance-a"),
	}

	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	rollback := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	routeCtx := runtimepipelinefixture.WithSQLTx(foreignCtx, &sql.Tx{})
	routeCtx = runtimepipelinefixture.WithPostCommitActions(routeCtx, &postCommit)
	routeCtx = runtimepipelinefixture.WithRollbackActions(routeCtx, &rollback)
	if err := bus.AddFlowInstanceRouteContext(routeCtx, req); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("foreign route add error = %v, want source conflict", err)
	}
	if store.upsertCalls != 0 || len(postCommit) != 0 || len(rollback) != 0 || bus.HasFlowInstanceRoute(req.Identity) {
		t.Fatalf(
			"route add mutations = upsert:%d post_commit:%d rollback:%d local:%v, want zero",
			store.upsertCalls, len(postCommit), len(rollback), bus.HasFlowInstanceRoute(req.Identity),
		)
	}

	if err := bus.AddFlowInstanceRouteContext(context.Background(), req); err != nil {
		t.Fatalf("owner-bound route add: %v", err)
	}
	if store.upsertCalls == 0 || !owned.Matches(store.upsertFact) || !bus.HasFlowInstanceRoute(req.Identity) {
		t.Fatal("owner-bound route add did not persist and publish with the immutable source")
	}
	beforeDeletes := store.deleteCalls
	if err := bus.RemoveFlowInstanceRouteContext(foreignCtx, req.Identity); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("foreign route remove error = %v, want source conflict", err)
	}
	if store.deleteCalls != beforeDeletes || !bus.HasFlowInstanceRoute(req.Identity) {
		t.Fatal("foreign route removal mutated persistence or the local route table")
	}
	if err := bus.RemoveFlowInstanceRoute(req.Identity); err != nil {
		t.Fatalf("owner-bound no-context route removal: %v", err)
	}
	if store.deleteCalls != beforeDeletes+1 || !owned.Matches(store.deleteFact) || bus.HasFlowInstanceRoute(req.Identity) {
		t.Fatal("owner-bound route removal did not use the immutable bus source")
	}

	if _, err := bus.MarkDeliveryInProgress(foreignCtx, "agent-a", "session-a"); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("foreign delivery-session error = %v, want source conflict", err)
	}
	if store.bindCalls != 0 {
		t.Fatalf("delivery-session store mutations = %d, want zero", store.bindCalls)
	}

}

func TestDeliverySessionBindingRejectsForeignSourceWithExactClaimBeforeStoreMutation(t *testing.T) {
	owned := sourceMutationFact(t, "7")
	foreign := sourceMutationFact(t, "8")
	store := newExactHandoffProofStore(t, false)
	bus := newSourceMutationProbeBusWithStore(t, store, owned, newSourceMutationProbeOwner())
	eventID, runID := uuid.NewString(), uuid.NewString()
	sessionID := uuid.NewString()
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: testAgentRouteIdentity(t, "agent-a", "")}
	store.seed(t, eventID, runID, route)
	claim := store.claim(t, eventID, runID, route)
	store.seedSession(t, sessionID, runID, route.AgentIdentity)

	foreignCtx := runtimedelivery.WithClaim(context.Background(), claim)
	foreignCtx = runtimecorrelation.WithBundleSourceFact(foreignCtx, foreign)
	if _, err := bus.MarkDeliveryInProgress(foreignCtx, "agent-a", sessionID); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("foreign session binding error = %v, want source conflict", err)
	}
	store.mu.Lock()
	binds := store.binds
	store.mu.Unlock()
	if binds != 0 {
		t.Fatalf("foreign session binding mutated store %d times", binds)
	}

	ownerCtx := runtimedelivery.WithClaim(context.Background(), claim)
	bound, err := bus.MarkDeliveryInProgress(ownerCtx, "agent-a", sessionID)
	if err != nil {
		t.Fatalf("owner-bound session binding: %v", err)
	}
	store.mu.Lock()
	binds, bindFact := store.binds, store.bindFact
	store.mu.Unlock()
	if !bound || binds != 1 || !owned.Matches(bindFact) {
		t.Fatal("owner-bound session binding did not carry the immutable bus source")
	}
}

func TestSubscriptionAndQuiescenceRejectForeignSourceBeforeLifecycleMutation(t *testing.T) {
	owned := sourceMutationFact(t, "e")
	foreign := sourceMutationFact(t, "f")
	bus := newSourceMutationProbeBus(t, owned, newSourceMutationProbeOwner())
	foreignCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)

	if _, err := bus.SubscribeInternal(foreignCtx, "foreign-subscription", events.EventType("test.work")); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("SubscribeInternal error = %v, want source conflict", err)
	}
	bus.mu.RLock()
	installed := bus.internalHandles["foreign-subscription"]
	bus.mu.RUnlock()
	if installed != nil {
		t.Fatal("foreign subscription installed a retained lifecycle handle")
	}

	lifecycleCtx, err := bus.admitBundleSourceFact(context.Background())
	if err != nil {
		t.Fatalf("derive owner lifecycle context: %v", err)
	}
	handle := newInternalSubscriptionHandle(lifecycleCtx, bus, "quiescence-proof", nil)
	bus.mu.Lock()
	bus.retiringInternalHandles = append(bus.retiringInternalHandles, handle)
	bus.mu.Unlock()
	if err := bus.WaitForQuiescence(foreignCtx); err == nil ||
		!strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("WaitForQuiescence error = %v, want source conflict", err)
	}
	handle.mu.RLock()
	active := handle.active
	handle.mu.RUnlock()
	bus.mu.RLock()
	retained := len(bus.retiringInternalHandles)
	bus.mu.RUnlock()
	if !active || retained != 1 {
		t.Fatalf("foreign quiescence changed retained lifecycle state: active=%v retained=%d", active, retained)
	}
	close(handle.receiverDone)
	if err := bus.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("owner-bound quiescence: %v", err)
	}
}

func commitSourceMutationEnginePublications(ctx context.Context, bus *EventBus, intents []runtimeengine.EmitIntent) error {
	plans, err := bus.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return err
	}
	evidence := make([]runtimeengine.CommittedDurablePublication, 0, len(plans))
	for _, value := range plans {
		plan, ok := value.(EnginePublicationPlan)
		if !ok {
			return fmt.Errorf("unexpected engine publication plan %T", value)
		}
		owner, ok := bus.store.(CommitPublicationOwner)
		if !ok {
			return fmt.Errorf("event store %T has no closed publication owner", bus.store)
		}
		committed, err := owner.CommitPublication(ctx, plan.PublicationCommand())
		if err != nil {
			_ = bus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return err
		}
		proof, err := NewCommittedEnginePublication(plan, committed)
		if err != nil {
			_ = bus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return err
		}
		evidence = append(evidence, proof)
	}
	return bus.FinalizeEnginePublications(ctx, evidence)
}

func TestEnginePublicationPlanAdmitsExactBundleSourceBeforePersistenceOrClaim(t *testing.T) {
	owned := sourceMutationFact(t, "a")
	foreign := sourceMutationFact(t, "b")
	for _, tc := range []struct {
		name      string
		busFact   runtimecorrelation.BundleSourceFact
		context   func(context.Context) context.Context
		wantError bool
	}{
		{
			name:    "foreign",
			busFact: owned,
			context: func(ctx context.Context) context.Context {
				return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
			},
			wantError: true,
		},
		{name: "bus owned", busFact: owned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newSourceMutationProbeOwner()
			bus := newSourceMutationProbeBus(t, tc.busFact, owner)
			ctx := context.Background()
			if tc.context != nil {
				ctx = tc.context(ctx)
			}
			event := sourceMutationEvent()

			err := commitSourceMutationEnginePublications(ctx, bus, []runtimeengine.EmitIntent{{Event: event}})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
					t.Fatalf("PrepareEnginePublications error = %v, want source admission failure", err)
				}
				if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
					t.Fatalf("pipeline mutations = %#v, want zero", got)
				}
				bus.mu.RLock()
				pending := len(bus.pendingOutboxByID[event.ID()])
				bus.mu.RUnlock()
				if pending != 0 {
					t.Fatalf("pending operations = %d, want zero", pending)
				}
				return
			}
			if err != nil {
				t.Fatalf("commit engine publications: %v", err)
			}
			if got := owner.counts().claimPublication; got != 1 {
				t.Fatalf("publication claims = %d, want one", got)
			}
			bus.mu.RLock()
			pending := len(bus.pendingOutboxByID[event.ID()])
			bus.mu.RUnlock()
			if pending != 1 {
				t.Fatalf("pending operations = %d, want one", pending)
			}
			if err := bus.clearPendingOutboxOperation(context.Background(), event.ID()); err != nil {
				t.Fatalf("clear pending outbox operation: %v", err)
			}
		})
	}
}

func TestPostCommitAndDeferredDispatchRetainPendingWorkOnSourceRejection(t *testing.T) {
	owned := sourceMutationFact(t, "c")
	foreign := sourceMutationFact(t, "d")
	for _, surface := range []string{"post_commit", "deferred_private_root"} {
		t.Run(surface, func(t *testing.T) {
			owner := newSourceMutationProbeOwner()
			bus := newSourceMutationProbeBus(t, owned, owner)
			event := sourceMutationEvent()
			claim, err := bus.claimPipelinePublication(context.Background(), event.ID())
			if err != nil {
				t.Fatalf("claim staged publication: %v", err)
			}
			bus.stageCommittedOutboxOperation(
				runtimeengine.EmitIntent{Event: event},
				EventAppendInserted,
				claim,
				nil,
			)
			before := owner.counts()
			ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
			switch surface {
			case "post_commit":
				err = bus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: event}})
			case "deferred_private_root":
				err = bus.publishDeferred(ctx, event)
			}
			if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
				t.Fatalf("%s error = %v, want source admission failure", surface, err)
			}
			if got := owner.counts(); got != before {
				t.Fatalf("pipeline mutations after rejection = %#v, want unchanged %#v", got, before)
			}
			bus.mu.RLock()
			pending := len(bus.pendingOutboxByID[event.ID()])
			bus.mu.RUnlock()
			if pending != 1 || !owner.owns(event.ID()) {
				t.Fatalf("pending operation/claim = %d/%v, want exact staged work retained", pending, owner.owns(event.ID()))
			}
			if err := bus.clearPendingOutboxOperation(context.Background(), event.ID()); err != nil {
				t.Fatalf("clear pending outbox operation: %v", err)
			}
		})
	}
}

func TestPostCommitDispatchIgnoresAmbientSQLContextAndPreservesBusOwnedSourceFact(t *testing.T) {
	owned := sourceMutationFact(t, "4")
	owner := newSourceMutationProbeOwner()
	store := newTargetRouteMemoryStore()
	bus := newSourceMutationProbeBusWithStore(t, store, owned, owner)
	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	rollback := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	ctx := context.Background()
	ctx = runtimepipelinefixture.WithSQLTx(ctx, &sql.Tx{})
	ctx = runtimepipelinefixture.WithPostCommitActions(ctx, &postCommit)
	ctx = runtimepipelinefixture.WithRollbackActions(ctx, &rollback)
	event := sourceMutationEvent()
	seedCommittedNoDeliveryForTest(t, store, event)

	if err := bus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: event}}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommit) != 0 || len(rollback) != 0 {
		t.Fatalf("ambient transaction actions = commit:%d rollback:%d, want none", len(postCommit), len(rollback))
	}
	if got := owner.counts(); got.claimEvent != 1 || got.settle != 1 {
		t.Fatalf("closed post-commit dispatch mutations = %#v, want one claim and settlement", got)
	}
}

func TestPreparedPublishRejectsCrossBusAndForeignSourceWithoutConsumingClaim(t *testing.T) {
	owned := sourceMutationFact(t, "e")
	foreign := sourceMutationFact(t, "f")
	actions := []struct {
		name string
		run  func(*EventBus, context.Context, PreparedPublish) error
	}{
		{name: "abandon", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.AbandonPreparedPublish(ctx, prepared)
		}},
		{name: "sync", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublish(ctx, prepared)
		}},
		{name: "wait", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublishAndWait(ctx, prepared)
		}},
		{name: "async", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublishAsync(ctx, prepared)
		}},
	}
	for _, action := range actions {
		for _, transfer := range []struct {
			name           string
			targetBus      func(testing.TB, *sourceMutationProbeOwner) *EventBus
			context        func(context.Context) context.Context
			mutatePrepared func(PreparedPublish) PreparedPublish
		}{
			{
				name: "same source different bus",
				targetBus: func(t testing.TB, owner *sourceMutationProbeOwner) *EventBus {
					return newSourceMutationProbeBus(t, owned, owner)
				},
			},
			{
				name: "different source different bus",
				targetBus: func(t testing.TB, owner *sourceMutationProbeOwner) *EventBus {
					return newSourceMutationProbeBus(t, foreign, owner)
				},
			},
			{
				name:      "same bus foreign context",
				targetBus: nil,
				context: func(ctx context.Context) context.Context {
					return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
				},
			},
		} {
			t.Run(action.name+"/"+transfer.name, func(t *testing.T) {
				ownerA := newSourceMutationProbeOwner()
				busA := newSourceMutationProbeBus(t, owned, ownerA)
				event := sourceMutationEvent()
				prepareCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), owned)
				plans, err := busA.PrepareEnginePublications(prepareCtx, []runtimeengine.EmitIntent{{Event: event}})
				if err != nil {
					t.Fatalf("prepare publication through production path: %v", err)
				}
				plan := plans[0].(EnginePublicationPlan)
				prepared := plan.prepared
				ownedPrepared := prepared
				target := busA
				if transfer.targetBus != nil {
					target = transfer.targetBus(t, newSourceMutationProbeOwner())
				}
				if transfer.mutatePrepared != nil {
					prepared = transfer.mutatePrepared(prepared)
				}
				ctx := context.Background()
				if transfer.context != nil {
					ctx = transfer.context(ctx)
				}

				err = action.run(target, ctx, prepared)
				if err == nil {
					t.Fatal("invalid prepared handoff unexpectedly succeeded")
				}
				if got := ownerA.counts(); got.release != 0 || got.settle != 0 || !ownerA.owns(event.ID()) {
					t.Fatalf("preparing claim after rejection = %#v owned:%v, want intact", got, ownerA.owns(event.ID()))
				}
				if err := busA.AbandonPreparedPublish(context.Background(), ownedPrepared); err != nil {
					t.Fatalf("preparing bus abandon after rejection: %v", err)
				}
				if got := ownerA.counts().release; got != 1 {
					t.Fatalf("preparing bus release count = %d, want one", got)
				}
			})
		}
	}
}

func TestRetainedPendingAndScanCapabilitiesRejectSourceSwitchAndPreserveOwnership(t *testing.T) {
	owned := sourceMutationFact(t, "5")
	foreign := sourceMutationFact(t, "6")

	t.Run("pending outbox", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), owned)
		event := sourceMutationEvent()
		if err := commitSourceMutationEnginePublications(ctx, bus, []runtimeengine.EmitIntent{{Event: event}}); err != nil {
			t.Fatalf("commit engine publications: %v", err)
		}
		before := owner.counts()
		foreignCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
		if err := bus.EngineDispatcher().DispatchPostCommit(foreignCtx, []runtimeengine.EmitIntent{{Event: event}}); err == nil ||
			!strings.Contains(err.Error(), "bundle source fact conflicts") {
			t.Fatalf("DispatchPostCommit error = %v, want source conflict", err)
		}
		if got := owner.counts(); got != before {
			t.Fatalf("pipeline mutations after source switch = %#v, want unchanged %#v", got, before)
		}
		bus.mu.RLock()
		pending := len(bus.pendingOutboxByID[event.ID()])
		bus.mu.RUnlock()
		if pending != 1 || !owner.owns(event.ID()) {
			t.Fatalf("pending operation/claim = %d/%v, want retained", pending, owner.owns(event.ID()))
		}
		if err := bus.clearPendingOutboxOperation(context.Background(), event.ID()); err != nil {
			t.Fatalf("owner-bound pending cleanup: %v", err)
		}
		if got := owner.sourceFact("release"); !owned.Matches(got) {
			t.Fatal("pending cleanup did not carry the immutable bus source fact")
		}
	})

	t.Run("pipeline scan", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		owner.retainScan = true
		bus := newSourceMutationProbeBus(t, owned, owner)
		if _, err := bus.SweepPipelineObligations(context.Background(), 1); err != nil {
			t.Fatalf("open retained scan: %v", err)
		}
		before := owner.counts()
		if before.openScan != 1 || before.claimBatch != 1 || before.closeScan != 0 {
			t.Fatalf("retained scan mutations = %#v, want open and batch only", before)
		}
		foreignCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
		if _, err := bus.SweepPipelineObligations(foreignCtx, 1); err == nil ||
			!strings.Contains(err.Error(), "bundle source fact conflicts") {
			t.Fatalf("second sweep error = %v, want source conflict", err)
		}
		if got := owner.counts(); got != before {
			t.Fatalf("scan mutations after source switch = %#v, want unchanged %#v", got, before)
		}
		if err := bus.closeAllPipelineScans(context.Background()); err != nil {
			t.Fatalf("background scan cleanup: %v", err)
		}
		if got := owner.counts().closeScan; got != 1 {
			t.Fatalf("close scan calls = %d, want one", got)
		}
		if got := owner.sourceFact("close"); !owned.Matches(got) {
			t.Fatal("background scan cleanup did not carry the immutable bus source fact")
		}
	})

	t.Run("reset pending cleanup", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		event := sourceMutationEvent()
		claim, err := bus.claimPipelinePublication(context.Background(), event.ID())
		if err != nil {
			t.Fatalf("claim publication: %v", err)
		}
		bus.stageCommittedOutboxOperation(
			runtimeengine.EmitIntent{Event: event},
			EventAppendInserted,
			claim,
			nil,
		)
		if err := bus.ResetInMemoryState(); err != nil {
			t.Fatalf("ResetInMemoryState: %v", err)
		}
		if got := owner.sourceFact("release"); !owned.Matches(got) {
			t.Fatal("reset cleanup did not carry the immutable bus source fact")
		}
		bus.mu.RLock()
		pending := len(bus.pendingOutboxByID[event.ID()])
		bus.mu.RUnlock()
		if pending != 0 {
			t.Fatalf("pending operations after reset = %d, want zero", pending)
		}
	})
}

func TestPublicationClaimCleanupRejectsForeignSourceBeforeCapabilityMutation(t *testing.T) {
	owned := sourceMutationFact(t, "7")
	foreign := sourceMutationFact(t, "8")
	for _, action := range []string{"release", "settle", "decision"} {
		t.Run(action, func(t *testing.T) {
			owner := newSourceMutationProbeOwner()
			bus := newSourceMutationProbeBus(t, owned, owner)
			eventID := uuid.NewString()
			claim, err := bus.claimPipelinePublication(context.Background(), eventID)
			if err != nil {
				t.Fatalf("claim publication: %v", err)
			}
			before := owner.counts()
			foreignCtx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
			switch action {
			case "release":
				err = claim.Release(foreignCtx)
			case "settle":
				err = claim.Settle(foreignCtx, runtimepipelineobligation.Acknowledged("test"))
			case "decision":
				err = claim.MarkDecisionProcessed(foreignCtx)
			}
			if err == nil || !strings.Contains(err.Error(), "bundle source fact conflicts") {
				t.Fatalf("%s error = %v, want source conflict", action, err)
			}
			if got := owner.counts(); got != before {
				t.Fatalf("pipeline mutations after rejected %s = %#v, want unchanged %#v", action, got, before)
			}
			if !owner.owns(eventID) {
				t.Fatalf("publication claim was consumed by rejected %s", action)
			}

			switch action {
			case "release":
				err = claim.Release(context.Background())
			case "settle":
				err = claim.Settle(context.Background(), runtimepipelineobligation.Acknowledged("test"))
			case "decision":
				err = claim.MarkDecisionProcessed(context.Background())
				if err == nil {
					err = claim.Release(context.Background())
				}
			}
			if err != nil {
				t.Fatalf("owner-bound %s: %v", action, err)
			}
			if got := owner.sourceFact(action); !owned.Matches(got) {
				t.Fatalf("%s did not carry the immutable bus source fact", action)
			}
		})
	}
}

func TestPipelineSweepRootsAdmitSourceBeforeWorkOccurrenceOrStoreMutation(t *testing.T) {
	owned := sourceMutationFact(t, "1")
	foreign := sourceMutationFact(t, "2")
	for _, surface := range []string{"start", "global", "shared_internal", "run_release"} {
		t.Run(surface, func(t *testing.T) {
			owner := newSourceMutationProbeOwner()
			bus := newSourceMutationProbeBus(t, owned, owner)
			ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)
			var err error
			switch surface {
			case "start":
				err = bus.StartOutboxSweeper(ctx, OutboxSweeperConfig{Interval: time.Hour, Limit: 1})
			case "global":
				_, err = bus.SweepPipelineObligations(ctx, 1)
			case "shared_internal":
				_, err = bus.sweepPipelineObligations(ctx, runtimepipelineobligation.GlobalScanRequest(), 1)
			case "run_release":
				_, err = bus.ReleaseRunQueue(ctx, uuid.NewString(), 1)
			}
			if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
				t.Fatalf("%s error = %v, want source admission failure", surface, err)
			}
			if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
				t.Fatalf("pipeline store mutations = %#v, want zero", got)
			}
			if bus.OutboxSweeperActive() || bus.outboxSweeperDone != nil {
				t.Fatal("source rejection created a sweeper occurrence")
			}
		})
	}
}

func TestPipelineSweepRootsExecuteForImmutableBusOwnedSource(t *testing.T) {
	owned := sourceMutationFact(t, "3")
	t.Run("bus owned global", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		result, err := bus.SweepPipelineObligations(context.Background(), 1)
		if err != nil {
			t.Fatalf("SweepPipelineObligations: %v", err)
		}
		if !result.Exhausted {
			t.Fatal("global sweep did not execute to exhaustion")
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("global sweep mutations = %#v, want one open/batch/close", got)
		}
	})
	t.Run("run release", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		result, err := bus.ReleaseRunQueue(context.Background(), uuid.NewString(), 1)
		if err != nil {
			t.Fatalf("ReleaseRunQueue: %v", err)
		}
		if !result.Exhausted {
			t.Fatal("run release did not execute to exhaustion")
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("run release mutations = %#v, want one open/batch/close", got)
		}
	})
	t.Run("periodic", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		ctx, cancel := context.WithCancel(context.Background())
		if err := bus.StartOutboxSweeper(ctx, OutboxSweeperConfig{Interval: time.Hour, Limit: 1}); err != nil {
			cancel()
			t.Fatalf("StartOutboxSweeper: %v", err)
		}
		select {
		case <-owner.opened:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("periodic sweeper did not execute")
		}
		cancel()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
		defer waitCancel()
		if err := bus.WaitForOutboxSweeper(waitCtx); err != nil {
			t.Fatalf("WaitForOutboxSweeper: %v", err)
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("periodic sweep mutations = %#v, want one open/batch/close", got)
		}
		if got := owner.sourceFact("close"); !owned.Matches(got) {
			t.Fatal("periodic background close did not carry the immutable bus source fact")
		}
	})
}
