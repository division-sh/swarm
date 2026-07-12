package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestDecisionCardStoreLifecycleParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
			card, err := decisioncard.New(decisioncard.Card{
				CardID: uuid.NewString(), RunID: runID, FlowInstance: "launch/review-1", FlowID: "launch", EntityID: uuid.NewString(),
				Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
				Snapshot: decisioncard.Snapshot{
					Decision: "launch_review", Context: map[string]any{"summary": "ready"},
					Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
						"accept": {Verdict: "accept", AdvancesTo: "operating"},
						"revise": {Verdict: "revise", AdvancesTo: "building", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
					},
				},
				BundleHash:      "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				WorkflowVersion: "1", EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"},
				CreatedAt: now,
			})
			if err != nil {
				t.Fatalf("New decision card: %v", err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatalf("CreateDecisionCard: %v", err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatalf("idempotent CreateDecisionCard: %v", err)
			}
			loaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || loaded.CardContentHash != card.CardContentHash || loaded.Snapshot.Context["summary"] != "ready" {
				t.Fatalf("GetDecisionCard = %#v, %v", loaded, err)
			}

			draft, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now, TTL: 10 * time.Minute})
			if err != nil {
				t.Fatalf("BeginDecisionCardInput: %v", err)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "revise", Fields: map[string]any{"feedback": "fix tests"}, ActorTokenID: "operator-a",
				ObservedContentHash: "sha256:stale", InputDraftID: draft.InputDraftID, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			}); !errors.Is(err, decisioncard.ErrStaleContent) {
				t.Fatalf("stale decide error = %v", err)
			}
			outcome, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "revise", Fields: map[string]any{"feedback": "fix tests"}, ActorTokenID: "operator-a",
				ObservedContentHash: card.CardContentHash, InputDraftID: draft.InputDraftID, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			})
			if err != nil {
				t.Fatalf("DecideDecisionCard: %v", err)
			}
			if outcome.Card.Status != decisioncard.StatusDecided || outcome.Card.Verdict != "revise" || outcome.ChangeID < 1 {
				t.Fatalf("decision outcome = %#v", outcome)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "accept", ObservedContentHash: card.CardContentHash}); !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("second decide error = %v", err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 20})
			if err != nil {
				t.Fatalf("ListDecisionCardChanges: %v", err)
			}
			if len(changes) != 3 || changes[0].ChangeType != decisioncard.ChangeCreated || changes[2].ChangeType != decisioncard.ChangeDecided {
				t.Fatalf("changes = %#v", changes)
			}
		})
	}
}

func TestDecisionCardStorePaginationUsesCreationOrderOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			base := time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC)
			want := make([]string, 0, 3)
			for i := 0; i < 3; i++ {
				card := newDecisionCardTestCard(t, runID, base.Add(time.Duration(i)*time.Second))
				want = append(want, card.CardID)
				if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
					t.Fatalf("CreateDecisionCard(%d): %v", i, err)
				}
			}
			var got []string
			cursor := ""
			for page := 0; page < 4; page++ {
				items, next, err := cardStore.ListDecisionCards(ctx, decisioncard.ListOptions{Status: decisioncard.StatusPending, Limit: 1, Cursor: cursor})
				if err != nil {
					t.Fatalf("ListDecisionCards page %d: %v", page, err)
				}
				for _, item := range items {
					got = append(got, item.CardID)
				}
				if next == "" {
					break
				}
				if next == cursor {
					t.Fatalf("cursor did not advance on page %d", page)
				}
				cursor = next
			}
			if len(got) != len(want) {
				t.Fatalf("paginated card ids = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("paginated card ids = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestDecisionCardStoreDeferDraftCancelAndSupersedeParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 7, 12, 14, 0, 0, 0, time.UTC)
			card := newDecisionCardTestCard(t, runID, now)
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatalf("CreateDecisionCard: %v", err)
			}
			deferred, err := cardStore.DeferDecisionCard(ctx, decisioncard.DeferRequest{CardID: card.CardID, ActorTokenID: "operator-a", Until: now.Add(time.Hour), Now: now})
			if err != nil || deferred.Card.Status != decisioncard.StatusPending || !deferred.Card.DeferredUntil.Equal(now.Add(time.Hour)) {
				t.Fatalf("DeferDecisionCard = %#v, %v", deferred, err)
			}
			draft, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now, TTL: 10 * time.Minute})
			if err != nil {
				t.Fatalf("BeginDecisionCardInput: %v", err)
			}
			cancelled, err := cardStore.CancelDecisionCardInput(ctx, decisioncard.CancelInputRequest{CardID: card.CardID, InputDraftID: draft.InputDraftID, ActorTokenID: "operator-a", Now: now.Add(time.Minute)})
			if err != nil || cancelled.Status != decisioncard.DraftStatusCancelled {
				t.Fatalf("CancelDecisionCardInput = %#v, %v", cancelled, err)
			}
			if err := cardStore.SupersedeDecisionCardsForStage(ctx, runID, card.EntityID, card.StageActivationID, "stage_exited", now.Add(2*time.Minute)); err != nil {
				t.Fatalf("SupersedeDecisionCardsForStage: %v", err)
			}
			loaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || loaded.Status != decisioncard.StatusSuperseded || loaded.SupersededReason != "stage_exited" {
				t.Fatalf("superseded card = %#v, %v", loaded, err)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "accept", ObservedContentHash: card.CardContentHash}); !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("decide superseded card error = %v", err)
			}
		})
	}
}

func newDecisionCardTestCard(t *testing.T, runID string, now time.Time) decisioncard.Card {
	t.Helper()
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, FlowInstance: "launch/review", FlowID: "launch", EntityID: uuid.NewString(),
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
		Snapshot: decisioncard.Snapshot{Decision: "launch_review", Context: map[string]any{"summary": "ready"}, Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"accept": {Verdict: "accept", AdvancesTo: "operating"},
			"revise": {Verdict: "revise", AdvancesTo: "building", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
		}},
		BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkflowVersion: "1",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("New decision card: %v", err)
	}
	return card
}

func decisionCardTestStore(t *testing.T, backend string) (decisioncard.Store, string) {
	t.Helper()
	ctx := context.Background()
	spec := loadPlatformSpecForSQLiteSchemaTest(t)
	plans, err := GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	runID := uuid.NewString()
	switch backend {
	case "sqlite":
		store, err := NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "runtime.db"))
		if err != nil {
			t.Fatalf("NewSQLiteRuntimeStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.EnsureSchemaTables(ctx, plans); err != nil {
			t.Fatalf("EnsureSchemaTables sqlite: %v", err)
		}
		if _, err := store.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
			t.Fatalf("insert sqlite run: %v", err)
		}
		return store, runID
	case "postgres":
		_, db, _ := testutil.StartPostgres(t)
		store := &PostgresStore{DB: db}
		if err := store.EnsureSchemaTables(ctx, plans); err != nil {
			t.Fatalf("EnsureSchemaTables postgres: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("insert postgres run: %v", err)
		}
		return store, runID
	default:
		t.Fatalf("unknown backend %s", backend)
		return nil, ""
	}
}
