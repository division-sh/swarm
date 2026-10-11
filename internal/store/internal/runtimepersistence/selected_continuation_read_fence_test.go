package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestSelectedContinuationReadFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"existing_busy", "existing_retry", "entityless_pending"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				proveSelectedContinuationReadFence(t, backend, state)
			})
		}
	}
}

func proveSelectedContinuationReadFence(t *testing.T, backend, state string) {
	t.Helper()
	selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
	f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
	ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
	ctx = correlation.WithRunID(ctx, f.forkRun)
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "continuation-read-fence", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	plan := admitSelectedProviderFixture(t, ctx, f, issued, execution)
	authority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash), execution.ID, f.forkRun, execution.SelectedFork.Generation)
	if err != nil {
		t.Fatal(err)
	}
	target := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "global", "receiver")), Target: events.MustExistingEntityTarget(target)}
	if state == "entityless_pending" {
		target.EntityID = ""
		route.Target = events.MustEntitylessReceiverTarget(target)
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, f.forkRun, events.EventEnvelope{}, time.Now().UTC())
	event = eventtest.TargetRouted(event, route.Target.Route())
	store := selected.(deliverylifecycle.Store)
	commitSelectedReceiverClaimEvent(t, ctx, selected.(agentFixtureFlowStore), event, route, authority)
	if err := runDeliveryFixtureAttempt(ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		adapter := sqliteDeliveryAdapter
		if !sqlite {
			adapter = postgresDeliveryAdapter
		}
		return adapter.CommitPipelineHandoff(ctx, attempt, event.ID())
	}); err != nil {
		t.Fatal(err)
	}
	want := deliverylifecycle.ClaimAcquired
	if state != "entityless_pending" {
		result, err := store.ClaimDelivery(ctx, authority, event, route)
		claimed, acquired := result.Acquired()
		if err != nil || !acquired {
			t.Fatalf("claim selected history: %+v err=%v", result, err)
		}
		want = deliverylifecycle.ClaimBusy
		if state == "existing_retry" {
			failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "selected_read_retry", "selected_fence_test", "settle", nil))
			if !ok {
				t.Fatal("missing retry failure envelope")
			}
			if _, err := store.SettleFailure(ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
				t.Fatal(err)
			}
			want = deliverylifecycle.ClaimDeferred
		}
	}
	before := loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
	if state == "entityless_pending" {
		assertTargetKindContinuationEligibility(t, ctx, store, before)
	} else {
		assertClaimHistoryContinuations(t, ctx, store, before, want, true)
	}
	if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, execution); err != nil {
		t.Fatal(err)
	}
	assertFencedSelectedContinuations(t, ctx, store, event, route, before)
}

func assertFencedSelectedContinuations(t *testing.T, ctx context.Context, store deliverylifecycle.Store, event events.Event, route events.DeliveryRoute, before deliverylifecycle.Snapshot) {
	t.Helper()
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
	_, singleWake := single.Wake.After()
	if err != nil || single.Disposition != deliverylifecycle.ClaimWrongAuthority || singleWake || single.Invariant != nil {
		t.Errorf("fenced selected single observation remained live: %+v err=%v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("fenced selected batch observation: %+v err=%v", batch, err)
	}
	_, batchWake := batch[0].Wake.After()
	if batch[0].Disposition != deliverylifecycle.ClaimWrongAuthority || batchWake || batch[0].Invariant != nil {
		t.Errorf("fenced selected batch observation remained live: %+v err=%v", batch, err)
	}
	if _, err := store.ScanDeliveryContinuations(ctx, before.Authority, deliverylifecycle.ContinuationCursor{}, 64); err == nil || !strings.Contains(err.Error(), "selected delivery continuation authority is fenced") {
		t.Errorf("fenced selected scan: %v", err)
	}
	direct, err := store.ClaimDelivery(ctx, before.Authority, event, route)
	if _, acquired := direct.Acquired(); err != nil || acquired || direct.Disposition != deliverylifecycle.ClaimWrongAuthority || direct.Renewal.Acknowledged {
		t.Errorf("fenced selected direct claim: %+v err=%v", direct, err)
	}
	after, err := store.Snapshot(ctx, before.DeliveryID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Errorf("selected fence changed immutable history or claim state: before=%+v after=%+v err=%v", before, after, err)
	}
}
