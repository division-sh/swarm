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
		for _, state := range []string{"existing_busy", "existing_retry", "entityless_pending", "existing_delivered", "existing_dead_letter", "existing_terminalized"} {
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
		want = settleSelectedReadFenceFixture(t, ctx, store, state, claimed.Claim)
	}
	before := loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
	if want == deliverylifecycle.ClaimTerminal {
		assertTerminalSelectedContinuations(t, ctx, store, before, true)
	} else if state == "entityless_pending" {
		assertTargetKindContinuationEligibility(t, ctx, store, before)
	} else {
		assertClaimHistoryContinuations(t, ctx, store, before, want, true)
	}
	if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, execution); err != nil {
		t.Fatal(err)
	}
	wantFenced := deliverylifecycle.ClaimWrongAuthority
	if want == deliverylifecycle.ClaimTerminal {
		wantFenced = deliverylifecycle.ClaimTerminal
	}
	assertFencedSelectedContinuations(t, ctx, store, event, route, before, wantFenced)
	if want == deliverylifecycle.ClaimTerminal {
		assertTerminalSelectedContinuations(t, ctx, store, before, false)
	}
}

func settleSelectedReadFenceFixture(t *testing.T, ctx context.Context, store deliverylifecycle.Store, state string, claim deliverylifecycle.Claim) deliverylifecycle.ClaimDisposition {
	t.Helper()
	switch state {
	case "existing_busy":
		return deliverylifecycle.ClaimBusy
	case "existing_delivered":
		if _, err := store.SettleSuccess(ctx, claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
			t.Fatal(err)
		}
	case "existing_terminalized":
		rows, err := store.TerminalizeRun(ctx, claim.RunID(), "selected_read_terminalized")
		if err != nil || len(rows) != 1 || rows[0].Current.Status != deliverylifecycle.StatusDeadLetter || rows[0].Current.ClaimVersion != claim.Version()+1 {
			t.Fatalf("exact parent terminalization: %+v err=%v", rows, err)
		}
	case "existing_retry", "existing_dead_letter":
		failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "selected_read_fixture", "selected_fence_test", "settle", nil))
		if !ok {
			t.Fatal("missing typed fixture failure")
		}
		disposition := deliverylifecycle.FailureDeadLetter
		if state == "existing_retry" {
			disposition = deliverylifecycle.FailureRetry
		}
		if _, err := store.SettleFailure(ctx, claim, deliverylifecycle.Settlement{Disposition: disposition, ReasonCode: "selected_read_fixture", Failure: &failure, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
			t.Fatal(err)
		}
		if state == "existing_retry" {
			return deliverylifecycle.ClaimDeferred
		}
	default:
		t.Fatalf("unsupported selected fence fixture state %q", state)
	}
	return deliverylifecycle.ClaimTerminal
}

func assertTerminalSelectedContinuations(t *testing.T, ctx context.Context, store deliverylifecycle.Store, before deliverylifecycle.Snapshot, executionLive bool) {
	t.Helper()
	if !before.Terminal() {
		t.Fatalf("fixture lacks exact terminal delivery evidence: %+v", before)
	}
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
	if _, wake := single.Wake.After(); err != nil || single.Disposition != deliverylifecycle.ClaimTerminal || wake || single.Invariant != nil {
		t.Fatalf("exact terminal read lost settled evidence: %+v err=%v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("exact terminal batch: %+v err=%v", batch, err)
	}
	if _, wake := batch[0].Wake.After(); batch[0].Disposition != deliverylifecycle.ClaimTerminal || wake || batch[0].Invariant != nil {
		t.Fatalf("exact terminal batch lost settled evidence: %+v", batch)
	}
	if executionLive {
		page, err := store.ScanDeliveryContinuations(ctx, before.Authority, deliverylifecycle.ContinuationCursor{}, 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			if item.DeliveryID == before.DeliveryID {
				t.Fatalf("terminal delivery survived recovery enumeration: %+v", item)
			}
		}
	}
	for _, coordinates := range []struct {
		name, executionID string
		generation        uint64
	}{
		{"wrong_generation", before.Authority.ExecutionID(), before.Authority.Generation() + 1},
		{"foreign_execution", uuid.NewString(), before.Authority.Generation()},
	} {
		foreign, err := deliverylifecycle.NewSelectedExecutionAuthority(before.Authority.SourceArtifact(), coordinates.executionID, before.Authority.ForkRunID(), coordinates.generation)
		if err != nil {
			t.Fatal(err)
		}
		single, err = store.ObserveDeliveryContinuation(ctx, foreign, before.DeliveryID)
		if _, wake := single.Wake.After(); err != nil || single.Disposition != deliverylifecycle.ClaimWrongAuthority || wake || single.Invariant != nil {
			t.Fatalf("terminal read accepted %s row authority: %+v err=%v", coordinates.name, single, err)
		}
		batch, err = store.ObserveDeliveryContinuations(ctx, foreign, []string{before.DeliveryID})
		if err != nil || len(batch) != 1 {
			t.Fatalf("terminal %s batch: %+v err=%v", coordinates.name, batch, err)
		}
		if _, wake := batch[0].Wake.After(); batch[0].Disposition != deliverylifecycle.ClaimWrongAuthority || wake || batch[0].Invariant != nil {
			t.Fatalf("terminal batch accepted %s row authority: %+v", coordinates.name, batch)
		}
	}
	absentID := uuid.NewString()
	single, err = store.ObserveDeliveryContinuation(ctx, before.Authority, absentID)
	if _, wake := single.Wake.After(); err != nil || single.Disposition != deliverylifecycle.ClaimAbsent || wake || single.Invariant != nil {
		t.Fatalf("absent continuation acquired terminal/execution evidence: %+v err=%v", single, err)
	}
	batch, err = store.ObserveDeliveryContinuations(ctx, before.Authority, []string{absentID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("absent continuation batch: %+v err=%v", batch, err)
	}
	if _, wake := batch[0].Wake.After(); batch[0].Disposition != deliverylifecycle.ClaimAbsent || wake || batch[0].Invariant != nil {
		t.Fatalf("absent batch acquired terminal/execution evidence: %+v", batch)
	}
}

func assertFencedSelectedContinuations(t *testing.T, ctx context.Context, store deliverylifecycle.Store, event events.Event, route events.DeliveryRoute, before deliverylifecycle.Snapshot, want deliverylifecycle.ClaimDisposition) {
	t.Helper()
	outcomes, err := store.Outcomes(ctx, before.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
	_, singleWake := single.Wake.After()
	if err != nil || single.Disposition != want || singleWake || single.Invariant != nil {
		t.Errorf("fenced selected single observation: %+v want=%s err=%v", single, want, err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("fenced selected batch observation: %+v err=%v", batch, err)
	}
	_, batchWake := batch[0].Wake.After()
	if batch[0].Disposition != want || batchWake || batch[0].Invariant != nil {
		t.Errorf("fenced selected batch observation: %+v want=%s err=%v", batch, want, err)
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
	afterOutcomes, err := store.Outcomes(ctx, before.DeliveryID)
	if err != nil || !reflect.DeepEqual(outcomes, afterOutcomes) {
		t.Errorf("selected fence changed immutable outcomes: before=%+v after=%+v err=%v", outcomes, afterOutcomes, err)
	}
}
