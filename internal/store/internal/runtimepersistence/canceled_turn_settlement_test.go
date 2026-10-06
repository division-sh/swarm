package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
)

type canceledTurnTestStore interface {
	runtimeeffects.CanceledTurnStore
	DeliveryLifecycleSnapshotPageForAgent(context.Context, deliverylifecycle.AgentLifecyclePageQuery) (deliverylifecycle.SnapshotPage, error)
}

func TestCanceledDeliveryTurnRequiresSettledPhysicalTailBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		canceled, ok := store.(canceledTurnTestStore)
		if !ok {
			t.Fatal("selected store has no authored canceled-origin settlement")
		}
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		authority := fixture.authority
		authority.BudgetScopes = nil
		ctx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		ctx = withManagedCompletionTestSurface(t, ctx, authority, "claude_cli")
		first := beginLogicalClockCompletion(t, ctx, "canceled-physical-tail")
		launch, err := store.MarkExternalAttemptLaunched(ctx, first.Attempt(), time.Now().UTC())
		if err != nil || !launch.Committed || launch.Turn == nil {
			t.Fatalf("launch: %+v err=%v", launch, err)
		}
		second := beginLogicalClockCompletion(t, ctx, "canceled-authorized-tool-round")
		command := runtimeeffects.CanceledTurnCommandForAttempt(first.Attempt(), nil)
		if result, err := canceled.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
			t.Fatalf("missing authored intent admitted cancellation: %+v err=%v", result, err)
		}
		intent, err := store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, first.Attempt(), launch.Turn.DeadlineAt)
		if err != nil || !intent.Committed || !intent.Requested {
			t.Fatalf("timeout: %+v err=%v", intent, err)
		}
		command.Publication = prepareCanceledReactionForTest(t, ctx, fixture, *launch.Turn, intent.RequestedAt)
		if _, err := store.(deliverylifecycle.Store).SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err == nil {
			t.Fatal("ordinary success bypassed committed cancellation intent")
		}
		if result, err := canceled.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
			t.Fatalf("unsettled physical work admitted cancellation: %+v err=%v", result, err)
		}
		requireExternalAttemptState(t, selected.db, !selected.postgres, first.Attempt().AttemptID, runtimeeffects.StateLaunched)
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		settlement := completionSettlementForTest(t, first.Attempt().Authority.Target, fixture, "claude_cli", "", "")
		settlement.ProviderHead = nil
		settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateOutcomeUncertain, Failure: &failure, Evidence: map[string]any{"physical_joined": true}}
		settlement.AgentTurn.Failure = &failure
		settlement.AgentTurn.ResponsePayload = []byte(`{"observed":"kept after cancellation"}`)
		physical, err := first.SettleCompletion(ctx, settlement)
		if err != nil || !physical.Committed {
			t.Fatalf("settle accepted physical evidence: %+v err=%v", physical, err)
		}
		if result, err := canceled.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
			t.Fatalf("another accepted attempt was overlooked: %+v err=%v", result, err)
		}
		if err := second.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
			t.Fatal(err)
		}
		var responseBefore string
		if err := selected.db.QueryRowContext(ctx, `SELECT CAST(response_payload AS TEXT) FROM agent_turns WHERE turn_id=$1`, settlement.AgentTurn.TurnID).Scan(&responseBefore); err != nil {
			t.Fatal(err)
		}
		settled, err := canceled.CommitCanceledTurn(ctx, command)
		if err != nil || !settled.Acknowledged || settled.Delivery.Status != deliverylifecycle.StatusCanceled ||
			settled.Delivery.ReasonCode != string(deliverylifecycle.CancellationTurnTimeout) || !settled.Delivery.MatchesSettlementClaim(fixture.origin) {
			t.Fatalf("canceled exact origin: %+v err=%v", settled, err)
		}
		if err := deliverylifecycle.ValidateCanceledSnapshot(settled.Delivery); err != nil {
			t.Fatal(err)
		}
		repeated, err := canceled.CommitCanceledTurn(ctx, command)
		if err != nil || !repeated.Acknowledged || !repeated.Delivery.SettledAt.Equal(settled.Delivery.SettledAt) {
			t.Fatalf("cancellation retry changed the receipt: %+v err=%v", repeated, err)
		}
		var outcomes, deadLetters int
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND outcome='canceled'`, fixture.origin.DeliveryID()).Scan(&outcomes); err != nil || outcomes != 1 {
			t.Fatalf("exact canceled attempt: count=%d err=%v", outcomes, err)
		}
		if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE original_event_id=$1`, settled.Delivery.EventID).Scan(&deadLetters); err != nil || deadLetters != 0 {
			t.Fatalf("authored cancellation became a dead letter: count=%d err=%v", deadLetters, err)
		}
		reader := store.(deliverylifecycle.Store)
		history, err := reader.Outcomes(ctx, fixture.origin.DeliveryID())
		if err != nil || len(history) != 1 || history[0].Outcome != "canceled" || history[0].Failure != nil || history[0].ReasonCode != settled.Delivery.ReasonCode {
			t.Fatalf("canceled outcome history: %+v err=%v", history, err)
		}
		summary, err := reader.SummarizeRun(ctx, fixture.origin.RunID())
		if err != nil || summary.Canceled != 1 || summary.DeadLetter != 0 || !summary.Settled() {
			t.Fatalf("canceled conservation: %+v err=%v", summary, err)
		}
		page, err := canceled.DeliveryLifecycleSnapshotPageForAgent(ctx, deliverylifecycle.AgentLifecyclePageQuery{
			AgentIdentity: authority.Normal.Identity, Statuses: []deliverylifecycle.Status{deliverylifecycle.StatusCanceled}, Limit: 10,
		})
		if err != nil || len(page.Snapshots) != 1 || page.Snapshots[0].Status != deliverylifecycle.StatusCanceled {
			t.Fatalf("canceled diagnostic page: %+v err=%v", page, err)
		}
		proveCanceledDeliveryFanOutFold(t, ctx, selected, fixture, settled.Delivery)
		requireExternalAttemptState(t, selected.db, !selected.postgres, first.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
		requireCompletionSettlementRows(t, fixture, first.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)
		var response string
		if err := selected.db.QueryRowContext(ctx, `SELECT CAST(response_payload AS TEXT) FROM agent_turns WHERE turn_id=$1`, settlement.AgentTurn.TurnID).Scan(&response); err != nil {
			t.Fatal(err)
		}
		if response != responseBefore {
			t.Fatalf("canceled settlement changed observed response: %s", response)
		}
	})
}

func proveCanceledDeliveryFanOutFold(t *testing.T, ctx context.Context, selected exactFactStore, completion completionSettlementFixture, snapshot deliverylifecycle.Snapshot) {
	t.Helper()
	parent := fanOutOwnerFixture{runID: snapshot.RunID, flowPath: ".", plan: fanOutOwnerTypedPlanFixture(t)}
	if err := selected.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, snapshot.RunID).Scan(&parent.bundleHash); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	fixture := seedFanOutOwnerChildFixture(t, ctx, selected.db, completion.store, selected.postgres, parent, 1, at)
	handle := seedFanOutDeliveryBarrier(t, ctx, selected.db, fixture, at)
	seedFanOutBarrierOutcomes(t, ctx, selected.db, fixture, []string{snapshot.EventID}, false, at)
	advanceFanOutBarriersForTest(t, ctx, completion.store.(storeTestDurableEventBusStore), selected.db, fixture.runID, at.Add(time.Second))
	want := fanoutbarrier.Summary{Total: 1, Canceled: 1}
	assertFanOutBarrierState(t, ctx, selected.db, fixture.runID, fixture.deliveryID, fixture.semanticPath, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
}

func TestCanceledDeliveryTurnRollsBackAllOriginEvidenceBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		authority := fixture.authority
		authority.BudgetScopes = nil
		ctx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		ctx = withManagedCompletionTestSurface(t, ctx, authority, "claude_cli")
		handle := beginLogicalClockCompletion(t, ctx, "canceled-rollback")
		launch, err := store.MarkExternalAttemptLaunched(ctx, handle.Attempt(), time.Now().UTC())
		if err != nil || !launch.Committed || launch.Turn == nil {
			t.Fatalf("launch: %+v err=%v", launch, err)
		}
		intent, err := store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), launch.Turn.DeadlineAt)
		if err != nil || !intent.Requested {
			t.Fatalf("timeout: %+v err=%v", intent, err)
		}
		command := runtimeeffects.CanceledTurnCommandForAttempt(handle.Attempt(), prepareCanceledReactionForTest(t, ctx, fixture, *launch.Turn, intent.RequestedAt))
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		if err := handle.Settle(ctx, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		install := []string{`CREATE TRIGGER canceled_origin_cut BEFORE UPDATE OF settled_at ON runtime_agent_turn_lifetimes WHEN NEW.settled_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'canceled_origin_cut'); END`}
		uninstall := []string{`DROP TRIGGER canceled_origin_cut`}
		if selected.postgres {
			install = []string{`CREATE FUNCTION canceled_origin_cut_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'canceled_origin_cut'; END $$`, `CREATE TRIGGER canceled_origin_cut BEFORE UPDATE OF settled_at ON runtime_agent_turn_lifetimes FOR EACH ROW WHEN (NEW.settled_at IS NOT NULL) EXECUTE FUNCTION canceled_origin_cut_fn()`}
			uninstall = []string{`DROP TRIGGER canceled_origin_cut ON runtime_agent_turn_lifetimes`, `DROP FUNCTION canceled_origin_cut_fn()`}
		}
		for _, statement := range install {
			if _, err := selected.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		canceled := store.(canceledTurnTestStore)
		result, err := canceled.CommitCanceledTurn(ctx, command)
		if err == nil || result.Acknowledged {
			t.Fatalf("late failure acknowledged partial cancellation: %+v err=%v", result, err)
		}
		reader := store.(deliverylifecycle.Store)
		snapshot, err := reader.Snapshot(ctx, fixture.origin.DeliveryID())
		if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != fixture.origin.Version() || !snapshot.SettledAt.IsZero() {
			t.Fatalf("late rollback changed origin: %+v err=%v", snapshot, err)
		}
		history, err := reader.Outcomes(ctx, fixture.origin.DeliveryID())
		if err != nil || len(history) != 0 {
			t.Fatalf("rollback retained canceled attempt: %+v err=%v", history, err)
		}
		for _, statement := range uninstall {
			if _, err := selected.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		result, err = canceled.CommitCanceledTurn(ctx, command)
		if err != nil || !result.Acknowledged || result.Delivery.Status != deliverylifecycle.StatusCanceled {
			t.Fatalf("rollback did not retain cancellation responsibility: %+v err=%v", result, err)
		}
	})
}
