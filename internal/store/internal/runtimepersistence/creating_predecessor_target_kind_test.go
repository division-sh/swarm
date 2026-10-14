package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestCreatingPredecessorTargetKindsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"entityless_receiver", "materializing_entity", "materializing_agent_without_initialization"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				proveCreatingPredecessorTargetKind(t, backend, kind)
			})
		}
	}
}

func proveCreatingPredecessorTargetKind(t *testing.T, backend, kind string) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
	f.constructKeylessRoot(t)
	req := f.request("target-kinds", "target-kinds", "initial")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
	}
	store := f.store.(deliverylifecycle.Store)
	creator := req.TriggerEvent
	target := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
	creatorRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "creator")), Target: events.MustExistingEntityTarget(target)}
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, creator); err != nil {
		t.Fatal(err)
	}
	if err := commitDeliveryObligationFixture(f.ctx, store, creator, creatorRoute); err != nil {
		t.Fatal(err)
	}
	creatorBefore := loadDeliverySnapshotFixture(t, f.ctx, store, creator.ID(), creatorRoute)
	follower := eventtest.ExistingRunRootIngress(uuid.NewString(), creator.Type(), creator.SourceAgent(), creator.TaskID(), creator.Payload(), creator.ChainDepth(), creator.RunID(), creator.Envelope(), creator.CreatedAt())
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, follower); err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "follower")), Target: events.MustMaterializingEntityTarget(target)}
	if kind == "entityless_receiver" {
		target.EntityID = ""
		route.Target = events.MustEntitylessReceiverTarget(target)
	}
	if kind == "materializing_agent_without_initialization" {
		agent := mustTestAgentIdentityForRun(follower.RunID(), "worker", plan.Identity.InstancePath)
		route.Recipient, route.AgentIdentity = events.MustAgentDeliveryRecipient(agent.AgentID()), agent
	}
	err = commitDeliveryObligationFixture(f.ctx, store, follower, route)
	if kind == "materializing_agent_without_initialization" {
		assertUnadmittedMaterializingAgent(t, f.ctx, store, follower, route, creatorBefore, err)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := runDeliveryFixtureAttempt(f.ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		adapter := sqliteDeliveryAdapter
		if backend == "postgres" {
			adapter = postgresDeliveryAdapter
		}
		return adapter.CommitPipelineHandoff(ctx, attempt, follower.ID())
	}); err != nil {
		t.Fatal(err)
	}
	before := loadDeliverySnapshotFixture(t, f.ctx, store, follower.ID(), route)
	assertTargetKindContinuationEligibility(t, f.ctx, store, before)
	var claim deliverylifecycle.ClaimResult
	if kind == "entityless_receiver" {
		// The clock/claim fixture legitimately has no constructor binding.
		err = runDeliveryFixtureAttempt(f.ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
			adapter := sqliteDeliveryAdapter
			if backend == "postgres" {
				adapter = postgresDeliveryAdapter
			}
			var err error
			claim, err = adapter.ClaimExactResult(ctx, attempt, before.Authority, follower, route, deliverylifecycle.DefaultLeaseTTL)
			return err
		})
	} else {
		claim, err = store.ClaimDelivery(f.ctx, before.Authority, follower, route)
	}
	acquired, ok := claim.Acquired()
	if err != nil || !ok || acquired.Claim.Version() != 1 {
		t.Fatalf("%s target-kind claim did not mint its own first version: %+v err=%v", kind, claim, err)
	}
	if _, err := store.SettleSuccess(f.ctx, acquired.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatal(err)
	}
	creatorAfter, err := store.Snapshot(f.ctx, creatorBefore.DeliveryID)
	if err != nil || !reflect.DeepEqual(creatorBefore, creatorAfter) || creatorAfter.Status != deliverylifecycle.StatusPending || creatorAfter.ClaimVersion != 0 {
		t.Fatalf("target-kind admission changed the unfinished creator: before=%+v after=%+v err=%v", creatorBefore, creatorAfter, err)
	}
}

func assertUnadmittedMaterializingAgent(t *testing.T, ctx context.Context, store deliverylifecycle.Store, event events.Event, route events.DeliveryRoute, creator deliverylifecycle.Snapshot, admissionErr error) {
	t.Helper()
	if admissionErr == nil || !strings.Contains(admissionErr.Error(), "constructing receiver omitted canonical construction receipt") {
		t.Fatalf("materializing agent bypassed construction admission: %v", admissionErr)
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(ctx, id); !errors.Is(err, deliverylifecycle.ErrNotFound) {
		t.Fatalf("refused materializing agent acquired a durable obligation: %v", err)
	}
	after, err := store.Snapshot(ctx, creator.DeliveryID)
	if err != nil || !reflect.DeepEqual(creator, after) {
		t.Fatalf("refused agent changed creator evidence: before=%+v after=%+v err=%v", creator, after, err)
	}
}

func assertTargetKindContinuationEligibility(t *testing.T, ctx context.Context, store deliverylifecycle.Store, before deliverylifecycle.Snapshot) {
	t.Helper()
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
	if err != nil || single.Disposition != deliverylifecycle.ClaimAcquired || single.Invariant != nil {
		t.Fatalf("target-kind single observation: %+v err=%v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
	if err != nil || len(batch) != 1 || batch[0].Disposition != deliverylifecycle.ClaimAcquired || batch[0].Invariant != nil {
		t.Fatalf("target-kind batch observation: %+v err=%v", batch, err)
	}
	page, err := store.ScanDeliveryContinuations(ctx, before.Authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, item := range page.Items {
		if item.DeliveryID == before.DeliveryID {
			found = item.Disposition == deliverylifecycle.ClaimAcquired && item.Invariant == nil && item.Snapshot.ClaimVersion == 0
		}
	}
	after, err := store.Snapshot(ctx, before.DeliveryID)
	if !found || err != nil || !reflect.DeepEqual(before, after) || after.Status != deliverylifecycle.StatusPending || after.ClaimVersion != 0 {
		t.Fatalf("target-kind scan changed or deferred the unclaimed delivery: page=%+v before=%+v after=%+v err=%v", page, before, after, err)
	}
}
