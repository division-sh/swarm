package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestHumanTaskDecisionAndBudgetLifecycleParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			humanStore := cardStore.(decisioncard.HumanTaskStore)
			now := time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC)

			approved, approvedContinuation := newHumanTaskDecisionCardTestFixture(t, runID, "approve-first", now, 1, now.Add(48*time.Hour))
			if err := humanStore.CreateHumanTaskCard(ctx, approved, approvedContinuation); err != nil {
				t.Fatalf("CreateHumanTaskCard approved: %v", err)
			}
			decisionEventID := uuid.NewString()
			decisionAt := now.Add(time.Minute).Add(789 * time.Nanosecond)
			wantDecisionAt := decisioncard.CanonicalTimestamp(decisionAt)
			outcome, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: approved.CardID, Verdict: "approve", ActorTokenID: "operator-a",
				ObservedContentHash: approved.CardContentHash, DecisionEventID: decisionEventID, Now: decisionAt,
			})
			if err != nil || outcome.ForcedDeferred || outcome.Card.Status != decisioncard.StatusDecided || !outcome.Card.DecidedAt.Equal(wantDecisionAt) || !outcome.Card.UpdatedAt.Equal(wantDecisionAt) {
				t.Fatalf("DecideDecisionCard approved = %#v, %v", outcome, err)
			}
			persistedCard, err := cardStore.GetDecisionCard(ctx, approved.CardID)
			if err != nil || !persistedCard.DecidedAt.Equal(wantDecisionAt) || !persistedCard.UpdatedAt.Equal(wantDecisionAt) {
				t.Fatalf("persisted decision card = %#v, %v", persistedCard, err)
			}
			continuation, err := humanStore.LoadHumanTaskContinuation(ctx, approved.CardID)
			if err != nil || continuation.State != decisioncard.HumanTaskContinuationDecisionCommitted || continuation.OutcomeEventID != decisionEventID || !continuation.UpdatedAt.Equal(wantDecisionAt) {
				t.Fatalf("decision continuation = %#v, %v", continuation, err)
			}
			completionAt := now.Add(2 * time.Minute).Add(789 * time.Nanosecond)
			wantCompletionAt := decisioncard.CanonicalTimestamp(completionAt)
			dispatched, err := humanStore.CompleteHumanTaskOutcome(ctx, approved.CardID, decisionEventID, completionAt)
			if err != nil || dispatched.State != decisioncard.HumanTaskContinuationOutcomeDispatched || !dispatched.UpdatedAt.Equal(wantCompletionAt) {
				t.Fatalf("CompleteHumanTaskOutcome = %#v, %v", dispatched, err)
			}
			if replayed, err := humanStore.CompleteHumanTaskOutcome(ctx, approved.CardID, decisionEventID, now.Add(3*time.Minute)); err != nil || replayed.State != decisioncard.HumanTaskContinuationOutcomeDispatched || !replayed.UpdatedAt.Equal(wantCompletionAt) {
				t.Fatalf("replayed CompleteHumanTaskOutcome = %#v, %v", replayed, err)
			}
			if _, err := humanStore.CompleteHumanTaskOutcome(ctx, approved.CardID, uuid.NewString(), now.Add(3*time.Minute)); err == nil {
				t.Fatal("conflicting human-task outcome identity was accepted")
			}

			budgeted, budgetedContinuation := newHumanTaskDecisionCardTestFixture(t, runID, "approve-budgeted", now.Add(4*time.Minute), 1, now.Add(48*time.Hour))
			if err := humanStore.CreateHumanTaskCard(ctx, budgeted, budgetedContinuation); err != nil {
				t.Fatalf("CreateHumanTaskCard budgeted: %v", err)
			}
			forced, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: budgeted.CardID, Verdict: "approve", ActorTokenID: "operator-a",
				ObservedContentHash: budgeted.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(5 * time.Minute),
			})
			if err != nil || !forced.ForcedDeferred || forced.Card.Status != decisioncard.StatusPending {
				t.Fatalf("budget-forced decision = %#v, %v", forced, err)
			}
			continuation, err = humanStore.LoadHumanTaskContinuation(ctx, budgeted.CardID)
			if err != nil || continuation.State != decisioncard.HumanTaskContinuationPending || continuation.DeferCause != "weekly_budget_exhausted" || continuation.RequeueCount != 1 || !continuation.DeferredUntil.Equal(continuation.BudgetWindowEnd) {
				t.Fatalf("budget-forced continuation = %#v, %v", continuation, err)
			}
		})
	}
}

func TestPostgresHumanTaskWeeklyBudgetSerializesConcurrentApprovals(t *testing.T) {
	ctx := context.Background()
	cardStore, runID := decisionCardTestStore(t, "postgres")
	humanStore := cardStore.(decisioncard.HumanTaskStore)
	postgres := cardStore.(*PostgresStore)
	now := time.Date(2026, 7, 14, 9, 30, 0, 0, time.UTC)
	windowStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	cards := make([]decisioncard.Card, 0, 2)
	for _, operationID := range []string{"concurrent-approval-a", "concurrent-approval-b"} {
		card, continuation := newHumanTaskDecisionCardTestFixture(t, runID, operationID, now, 1, now.Add(48*time.Hour))
		if err := humanStore.CreateHumanTaskCard(ctx, card, continuation); err != nil {
			t.Fatalf("CreateHumanTaskCard %s: %v", operationID, err)
		}
		cards = append(cards, card)
	}

	if _, err := postgres.DB.ExecContext(ctx, `
		CREATE FUNCTION test_delay_human_task_budget_commit() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_sleep(0.5);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_delay_human_task_budget_commit
		BEFORE UPDATE OF state ON human_task_continuations
		FOR EACH ROW
		WHEN (OLD.state = 'pending' AND NEW.state = 'decision_committed')
		EXECUTE FUNCTION test_delay_human_task_budget_commit();
	`); err != nil {
		t.Fatalf("install concurrent budget probe: %v", err)
	}

	type result struct {
		outcome decisioncard.DecisionOutcome
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, len(cards))
	var workers sync.WaitGroup
	for index, card := range cards {
		index, card := index, card
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			outcome, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator-a",
				ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(),
				Now: now.Add(time.Duration(index+1) * time.Minute),
			})
			results <- result{outcome: outcome, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	approved, forcedDeferred := 0, 0
	for item := range results {
		if item.err != nil {
			t.Fatalf("concurrent DecideDecisionCard: %v", item.err)
		}
		if item.outcome.ForcedDeferred {
			forcedDeferred++
			continue
		}
		if item.outcome.Card.Status == decisioncard.StatusDecided {
			approved++
		}
	}
	if approved != 1 || forcedDeferred != 1 {
		t.Fatalf("concurrent budget outcomes approved=%d forced_deferred=%d, want 1/1", approved, forcedDeferred)
	}

	var committed int
	if err := postgres.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM human_task_continuations h
		JOIN decision_cards c ON c.card_id = h.card_id
		WHERE h.budget_bundle_hash = $1 AND h.budget_window_start = $2
		  AND c.verdict = 'approve'
		  AND h.state IN ('decision_committed', 'outcome_dispatched')
	`, cards[0].BundleHash, windowStart).Scan(&committed); err != nil {
		t.Fatalf("count committed approvals: %v", err)
	}
	if committed != 1 {
		t.Fatalf("committed approvals = %d, want 1", committed)
	}
}

func TestHumanTaskExpiryAndRunSupersessionParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			humanStore := cardStore.(decisioncard.HumanTaskStore)
			expiryStore := cardStore.(decisioncard.HumanTaskExpiryStore)
			now := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)

			due, dueContinuation := newHumanTaskDecisionCardTestFixture(t, runID, "expires", now, 0, now.Add(time.Hour))
			pending, pendingContinuation := newHumanTaskDecisionCardTestFixture(t, runID, "run-terminal", now.Add(time.Minute), 0, now.Add(24*time.Hour))
			for _, item := range []struct {
				card         decisioncard.Card
				continuation decisioncard.HumanTaskContinuation
			}{{due, dueContinuation}, {pending, pendingContinuation}} {
				if err := humanStore.CreateHumanTaskCard(ctx, item.card, item.continuation); err != nil {
					t.Fatalf("CreateHumanTaskCard %s: %v", item.card.CardID, err)
				}
			}

			if err := cardStore.SupersedeDecisionCardsForStage(ctx, runID, uuid.NewString(), uuid.NewString(), "stage_exited", now.Add(2*time.Minute)); err != nil {
				t.Fatalf("stage supersession against human-task cards: %v", err)
			}
			if card, err := cardStore.GetDecisionCard(ctx, pending.CardID); err != nil || card.Status != decisioncard.StatusPending {
				t.Fatalf("stage lifecycle changed human-task card = %#v, %v", card, err)
			}

			deferredUntil := now.Add(30 * time.Minute).Add(789 * time.Nanosecond)
			wantDeferredUntil := decisioncard.CanonicalTimestamp(deferredUntil)
			if _, err := cardStore.DeferDecisionCard(ctx, decisioncard.DeferRequest{CardID: due.CardID, ActorTokenID: "operator-a", Until: deferredUntil, Now: now.Add(3 * time.Minute)}); err != nil {
				t.Fatalf("DeferDecisionCard: %v", err)
			}
			deferred, err := humanStore.LoadHumanTaskContinuation(ctx, due.CardID)
			if err != nil || deferred.DeferCause != "operator_deferred" || deferred.RequeueCount != 1 || !deferred.DeferredUntil.Equal(wantDeferredUntil) {
				t.Fatalf("deferred continuation = %#v, %v", deferred, err)
			}

			expiryAt := now.Add(2 * time.Hour).Add(789 * time.Nanosecond)
			wantExpiryAt := decisioncard.CanonicalTimestamp(expiryAt)
			if _, err := expiryStore.ExpireHumanTaskCardsInMutation(ctx, expiryAt, 10); err == nil {
				t.Fatal("ExpireHumanTaskCardsInMutation without a pipeline transaction succeeded")
			}
			expiredEvents := expireHumanTaskCardsInTestMutation(t, ctx, cardStore, expiryStore, expiryAt, 10)
			if len(expiredEvents) != 1 || expiredEvents[0].ID() == "" || expiredEvents[0].Type() != events.EventType("mailbox.card_expired") {
				t.Fatalf("ExpireHumanTaskCardsInMutation events = %#v", expiredEvents)
			}
			if replayed := expireHumanTaskCardsInTestMutation(t, ctx, cardStore, expiryStore, expiryAt, 10); len(replayed) != 0 {
				t.Fatalf("replayed ExpireHumanTaskCardsInMutation events = %#v", replayed)
			}
			expiredCard, err := cardStore.GetDecisionCard(ctx, due.CardID)
			if err != nil || expiredCard.Status != decisioncard.StatusExpired || !expiredCard.DecidedAt.Equal(wantExpiryAt) || !expiredCard.UpdatedAt.Equal(wantExpiryAt) {
				t.Fatalf("expired card = %#v, %v", expiredCard, err)
			}
			expiredContinuation, err := humanStore.LoadHumanTaskContinuation(ctx, due.CardID)
			if err != nil || expiredContinuation.State != decisioncard.HumanTaskContinuationExpired || expiredContinuation.DeferCause != "deadline_elapsed" || expiredContinuation.OutcomeEventID == "" || !expiredContinuation.UpdatedAt.Equal(wantExpiryAt) {
				t.Fatalf("expired continuation = %#v, %v", expiredContinuation, err)
			}

			supersededAt := now.Add(3 * time.Hour).Add(789 * time.Nanosecond)
			wantSupersededAt := decisioncard.CanonicalTimestamp(supersededAt)
			if _, err := markDecisionCardRunTerminal(ctx, cardStore, runID, "cancelled", supersededAt); err != nil {
				t.Fatalf("MarkRunTerminal cancelled: %v", err)
			}
			supersededCard, err := cardStore.GetDecisionCard(ctx, pending.CardID)
			if err != nil || supersededCard.Status != decisioncard.StatusSuperseded || supersededCard.SupersededReason != "run_cancelled" || !supersededCard.UpdatedAt.Equal(wantSupersededAt) {
				t.Fatalf("run-superseded card = %#v, %v", supersededCard, err)
			}
			supersededContinuation, err := humanStore.LoadHumanTaskContinuation(ctx, pending.CardID)
			if err != nil || supersededContinuation.State != decisioncard.HumanTaskContinuationSuperseded || !supersededContinuation.UpdatedAt.Equal(wantSupersededAt) {
				t.Fatalf("run-superseded continuation = %#v, %v", supersededContinuation, err)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: pending.CardID, Verdict: "approve", ObservedContentHash: pending.CardContentHash}); !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("decision after run supersession error = %v", err)
			}
		})
	}
}

func expireHumanTaskCardsInTestMutation(t *testing.T, ctx context.Context, cardStore decisioncard.Store, expiryStore decisioncard.HumanTaskExpiryStore, at time.Time, limit int) []events.Event {
	t.Helper()
	db, postgres := decisionCardStoreDB(t, cardStore)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	if !postgres {
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, cardStore.(*SQLiteRuntimeStore))
	}
	var eventsOut []events.Event
	if err := workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		var err error
		eventsOut, err = expiryStore.ExpireHumanTaskCardsInMutation(txctx, at, limit)
		return err
	}); err != nil {
		t.Fatalf("ExpireHumanTaskCardsInMutation: %v", err)
	}
	return eventsOut
}

func newHumanTaskDecisionCardTestFixture(t *testing.T, runID, operationID string, createdAt time.Time, budgetLimit int, deadline time.Time) (decisioncard.Card, decisioncard.HumanTaskContinuation) {
	t.Helper()
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "requester", OperationID: operationID, Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/instance-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review the result", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve"},
		"reject": {
			Verdict: "reject", Label: "Reject",
			Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text", Required: true, Label: "Reason"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
		BundleHash:       "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	windowStart := time.Date(createdAt.Year(), createdAt.Month(), createdAt.Day(), 0, 0, 0, 0, time.UTC)
	return card, decisioncard.HumanTaskContinuation{
		CardID: card.CardID, RunID: runID, SourceEventID: uuid.NewString(), DeadlineAt: deadline,
		BudgetBundleHash: card.BundleHash, BudgetLimit: budgetLimit,
		BudgetWindowStart: windowStart, BudgetWindowEnd: windowStart.Add(7 * 24 * time.Hour),
		State: decisioncard.HumanTaskContinuationPending, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}
