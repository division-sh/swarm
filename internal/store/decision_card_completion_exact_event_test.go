package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestNormalRunCompletionRequiresExactDecisionOutcomeEventIDParity(t *testing.T) {
	testCases := []struct {
		name    string
		kind    string
		verdict string
	}{
		{name: "human_approve", kind: "human", verdict: "approve"},
		{name: "human_reject", kind: "human", verdict: "reject"},
		{name: "human_expiry", kind: "human", verdict: "expired"},
		{name: "proposed_revise", kind: "proposed", verdict: "revise"},
		{name: "proposed_reject", kind: "proposed", verdict: "reject"},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, testCase := range testCases {
			backend, testCase := backend, testCase
			t.Run(backend+"/"+testCase.name, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				cards, runID := decisionCardTestStore(t, backend)
				db, postgres := decisionCardStoreDB(t, cards)
				now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

				eventName, sourceEventID, expectedEventID := "", "", ""
				switch testCase.kind {
				case "human":
					card, continuation := newHumanTaskDecisionCardTestFixture(t, runID, testCase.name, now, 1, now.Add(time.Hour))
					humanStore := cards.(decisioncard.HumanTaskStore)
					if err := humanStore.CreateHumanTaskCard(ctx, card, continuation); err != nil {
						t.Fatalf("CreateHumanTaskCard: %v", err)
					}
					if testCase.verdict == "expired" {
						expiryStore := cards.(decisioncard.HumanTaskExpiryStore)
						if eventsOut := expireHumanTaskCardsInTestMutation(t, ctx, cards, expiryStore, now.Add(2*time.Hour), 1); len(eventsOut) != 1 {
							t.Fatalf("expired events = %d, want 1", len(eventsOut))
						}
						expired, err := humanStore.LoadHumanTaskContinuation(ctx, card.CardID)
						if err != nil {
							t.Fatalf("LoadHumanTaskContinuation: %v", err)
						}
						eventName = "human_task.expired"
						sourceEventID = expired.OutcomeEventID
					} else {
						sourceEventID = uuid.NewString()
						fields := semanticvalue.EmptyObject()
						if testCase.verdict == "reject" {
							fields, _ = canonicaljson.FromGo(map[string]any{"reason": "not approved"})
						}
						if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
							CardID: card.CardID, Verdict: testCase.verdict, Fields: fields, ActorTokenID: "operator",
							ObservedContentHash: card.CardContentHash, DecisionEventID: sourceEventID, Now: now.Add(time.Minute),
						}); err != nil {
							t.Fatalf("DecideDecisionCard: %v", err)
						}
						eventName = "human_task.approved"
						if testCase.verdict == "reject" {
							eventName = "human_task.rejected"
						}
					}
					expectedEventID = decisioncard.HumanTaskOutcomeEventID(card.CardID, sourceEventID)
					forceDecisionCardContinuationState(t, ctx, db, postgres,
						`UPDATE human_task_continuations SET state = 'outcome_dispatched' WHERE card_id = ?`,
						`UPDATE human_task_continuations SET state = 'outcome_dispatched' WHERE card_id = $1`, card.CardID)

				case "proposed":
					card, continuation := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
					store := cards.(decisioncard.ProposedEffectStore)
					if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
						t.Fatalf("CreateProposedEffectCard: %v", err)
					}
					fields := semanticvalue.EmptyObject()
					if testCase.verdict == "revise" {
						fields, _ = canonicaljson.FromGo(map[string]any{"feedback": "revise this"})
					} else {
						fields, _ = canonicaljson.FromGo(map[string]any{"reason": "not approved"})
					}
					sourceEventID = uuid.NewString()
					if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
						CardID: card.CardID, Verdict: testCase.verdict, Fields: fields, ActorTokenID: "operator",
						ObservedContentHash: card.CardContentHash, DecisionEventID: sourceEventID, Now: now.Add(time.Minute),
					}); err != nil {
						t.Fatalf("DecideDecisionCard: %v", err)
					}
					eventName = continuation.RejectedEvent
					if testCase.verdict == "revise" {
						eventName = continuation.RevisionEvent
					}
					expectedEventID = decisioncard.ProposedEffectOutcomeEventID(card.CardID, sourceEventID, testCase.verdict)
					forceDecisionCardContinuationState(t, ctx, db, postgres,
						`UPDATE proposed_effect_continuations SET state = 'outcome_dispatched', route_event_id = ? WHERE card_id = ?`,
						`UPDATE proposed_effect_continuations SET state = 'outcome_dispatched', route_event_id = $1::uuid WHERE card_id = $2`, sourceEventID, card.CardID)
				}

				wrongEventID := uuid.NewString()
				if wrongEventID == expectedEventID {
					t.Fatal("wrong event ID unexpectedly matched deterministic identity")
				}
				wrongEvent := eventtest.PersistedProjection(wrongEventID, events.EventType(eventName), "test", "", []byte(`{}`), 1,
					runID, sourceEventID, events.EventEnvelope{}, now.Add(2*time.Hour+time.Minute))
				if err := runDecisionCardTestPipelineMutation(t, ctx, cards, func(txctx context.Context, tx *sql.Tx) error {
					return appendDecisionCardTestEvent(t, txctx, cards, tx, wrongEvent)
				}); err != nil {
					t.Fatalf("append wrong-ID outcome event: %v", err)
				}

				entityID := uuid.NewString()
				seedDecisionCardCompletionEntity(t, db, postgres, runID, entityID, "done", now)
				completionEventID := seedDecisionCardCompletionEvent(t, ctx, cards, runID, entityID, now.Add(3*time.Hour))
				if err := convergeDecisionCardRunCompletion(ctx, cards, completionEventID); err != nil {
					t.Fatalf("ConvergeNormalRunCompletion: %v", err)
				}
				assertDecisionCardRunStatus(t, ctx, db, postgres, runID, "running")
			})
		}
	}
}

func forceDecisionCardContinuationState(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, sqliteQuery, postgresQuery string, args ...any) {
	t.Helper()
	query := sqliteQuery
	if postgres {
		query = postgresQuery
	}
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("force decision-card continuation state: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("forced decision-card continuation rows = %d, %v; want 1", affected, err)
	}
}

func assertDecisionCardRunStatus(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, runID, want string) {
	t.Helper()
	query := `SELECT status FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status FROM runs WHERE run_id = $1::uuid`
	}
	var status string
	if err := db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil {
		t.Fatalf("load run status: %v", err)
	}
	if status != want {
		t.Fatalf("run status = %q, want %q", status, want)
	}
}
