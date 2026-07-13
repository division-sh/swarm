package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
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
				Snapshot: freezeDecisionCardTestSnapshot(t, "launch_review", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"accept": {Verdict: "accept", AdvancesTo: "operating"},
					"revise": {Verdict: "revise", AdvancesTo: "building", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
				}),
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
			summary, _ := loaded.Snapshot.Context.Lookup("summary")
			if err != nil || loaded.CardContentHash != card.CardContentHash || summary.Interface() != "ready" {
				t.Fatalf("GetDecisionCard = %#v, %v", loaded, err)
			}

			draft, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now, TTL: 10 * time.Minute})
			if err != nil {
				t.Fatalf("BeginDecisionCardInput: %v", err)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "revise", Fields: admitDecisionCardTestObject(t, map[string]any{"feedback": "fix tests"}), ActorTokenID: "operator-a",
				ObservedContentHash: "sha256:stale", InputDraftID: draft.InputDraftID, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			}); !errors.Is(err, decisioncard.ErrStaleContent) {
				t.Fatalf("stale decide error = %v", err)
			}
			outcome, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "revise", Fields: admitDecisionCardTestObject(t, map[string]any{"feedback": "fix tests"}), ActorTokenID: "operator-a",
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
			if len(changes) != 4 || changes[0].ChangeType != decisioncard.ChangeCreated || changes[2].ChangeType != decisioncard.ChangeDraftConsumed || changes[3].ChangeType != decisioncard.ChangeDecided {
				t.Fatalf("changes = %#v", changes)
			}
		})
	}
}

func TestDecisionCardStoreRejectsNonCanonicalDecisionIdentityWithoutPersistenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			card := newDecisionCardTestCard(t, runID, time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC))
			card.DecisionID = " launch_review "

			if err := cardStore.CreateDecisionCard(ctx, card); err == nil || !strings.Contains(err.Error(), "decision_id") || !strings.Contains(err.Error(), "not canonical") {
				t.Fatalf("CreateDecisionCard error = %v, want noncanonical decision identity rejection", err)
			}
			if _, err := cardStore.GetDecisionCard(ctx, card.CardID); !errors.Is(err, decisioncard.ErrNotFound) {
				t.Fatalf("GetDecisionCard after rejected create error = %v, want ErrNotFound", err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10})
			if err != nil || len(changes) != 0 {
				t.Fatalf("changes after rejected create = %#v, %v", changes, err)
			}
		})
	}
}

func TestDecisionCardStoreRejectsSnapshotHashDriftOnCreateAndReadbackOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 7, 13, 11, 0, 0, 0, time.UTC)

			staleCreate := newDecisionCardTestCard(t, runID, now)
			setDecisionCardFeedbackRequired(staleCreate.Snapshot.Outcomes, false)
			if err := cardStore.CreateDecisionCard(ctx, staleCreate); err == nil || !strings.Contains(err.Error(), "content hash does not match") {
				t.Fatalf("CreateDecisionCard with changed snapshot error = %v, want stale content hash rejection", err)
			}
			if _, err := cardStore.GetDecisionCard(ctx, staleCreate.CardID); !errors.Is(err, decisioncard.ErrNotFound) {
				t.Fatalf("GetDecisionCard after rejected changed snapshot error = %v, want ErrNotFound", err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10})
			if err != nil || len(changes) != 0 {
				t.Fatalf("changes after rejected changed snapshot = %#v, %v", changes, err)
			}

			persisted := newDecisionCardTestCard(t, runID, now.Add(time.Minute))
			if err := cardStore.CreateDecisionCard(ctx, persisted); err != nil {
				t.Fatalf("CreateDecisionCard valid card: %v", err)
			}
			setDecisionCardFeedbackRequired(persisted.Snapshot.Outcomes, false)
			snapshot, err := json.Marshal(persisted.Snapshot)
			if err != nil {
				t.Fatal(err)
			}
			db, postgres := decisionCardStoreDB(t, cardStore)
			updateSnapshot := `UPDATE decision_cards SET snapshot = ? WHERE card_id = ?`
			args := []any{string(snapshot), persisted.CardID}
			if postgres {
				updateSnapshot = `UPDATE decision_cards SET snapshot = $1::jsonb WHERE card_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, updateSnapshot, args...); err != nil {
				t.Fatalf("corrupt persisted snapshot: %v", err)
			}
			if _, err := cardStore.GetDecisionCard(ctx, persisted.CardID); err == nil || !strings.Contains(err.Error(), "content hash does not match") {
				t.Fatalf("changed snapshot readback error = %v, want stale content hash rejection", err)
			}

			contentHash, err := canonicaljson.Hash(persisted.Snapshot)
			if err != nil {
				t.Fatal(err)
			}
			updateContentHash := `UPDATE decision_cards SET card_content_hash = ? WHERE card_id = ?`
			args = []any{contentHash, persisted.CardID}
			if postgres {
				updateContentHash = `UPDATE decision_cards SET card_content_hash = $1 WHERE card_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, updateContentHash, args...); err != nil {
				t.Fatalf("align corrupted card content hash: %v", err)
			}
			if _, err := cardStore.GetDecisionCard(ctx, persisted.CardID); err == nil || !strings.Contains(err.Error(), "schema hash does not match") {
				t.Fatalf("changed semantic schema readback error = %v, want stale schema hash rejection", err)
			}
		})
	}
}

func TestDecisionCardStoreEnforcesSafeNumericSnapshotCarriersOnBothStores(t *testing.T) {
	const safeInteger = int64(9007199254740991)
	const outcomeEvent = "review.completed"
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			card, err := decisioncard.New(decisioncard.Card{
				CardID: uuid.NewString(), RunID: runID, FlowInstance: "launch/review", FlowID: "launch", EntityID: uuid.NewString(),
				Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
				Snapshot: freezeDecisionCardTestSnapshot(t, "launch_review", map[string]any{"large_integer": safeInteger, "subnormal": math.SmallestNonzeroFloat64}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {
						Verdict: "approve", AdvancesTo: "operating",
						Input: map[string]runtimecontracts.WorkflowGateInputField{"score": {Type: "integer", Required: true}},
						Emit: runtimecontracts.EmitSpec{Event: outcomeEvent, Fields: map[string]runtimecontracts.ExpressionValue{
							"large_integer": runtimecontracts.LiteralExpression(safeInteger),
							"score":         runtimecontracts.CELExpression("decision.score"),
						}},
						EmitSchema: map[string]any{
							"type": "object",
							"properties": map[string]any{
								"large_integer": map[string]any{"type": "integer", "minimum": safeInteger},
								"score":         map[string]any{"type": "integer"},
							},
							"required": []string{"large_integer", "score"}, "additionalProperties": false,
						},
					},
				}),
				BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkflowVersion: "1",
				EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"},
				Provenance:       admitDecisionCardTestObject(t, map[string]any{"safe_integer": safeInteger, "subnormal": math.SmallestNonzeroFloat64}),
				CreatedAt:        time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
			})
			if err != nil {
				t.Fatalf("New decision card: %v", err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatalf("CreateDecisionCard: %v", err)
			}
			loaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil {
				t.Fatalf("GetDecisionCard: %v", err)
			}
			if loaded.CardContentHash != card.CardContentHash || loaded.DecisionSchemaHash != card.DecisionSchemaHash {
				t.Fatalf("round-trip hashes = %q/%q, want %q/%q", loaded.CardContentHash, loaded.DecisionSchemaHash, card.CardContentHash, card.DecisionSchemaHash)
			}
			contextNumber, _ := loaded.Snapshot.Context.Lookup("large_integer")
			assertStoreSnapshotNumber(t, "context.large_integer", contextNumber.Interface(), float64(safeInteger))
			contextSubnormal, _ := loaded.Snapshot.Context.Lookup("subnormal")
			assertStoreSnapshotNumber(t, "context.subnormal", contextSubnormal.Interface(), math.SmallestNonzeroFloat64)
			provenanceNumber, _ := loaded.Provenance.Lookup("safe_integer")
			assertStoreSnapshotNumber(t, "provenance.safe_integer", provenanceNumber.Interface(), float64(safeInteger))
			provenanceSubnormal, _ := loaded.Provenance.Lookup("subnormal")
			assertStoreSnapshotNumber(t, "provenance.subnormal", provenanceSubnormal.Interface(), math.SmallestNonzeroFloat64)
			outcome := loaded.Snapshot.Outcomes["approve"]
			assertStoreSnapshotNumber(t, "outcome literal", outcome.Emit.Fields["large_integer"].Literal.Interface(), float64(safeInteger))
			schema := outcome.EmitSchema.Interface().(map[string]any)
			properties, ok := schema["properties"].(map[string]any)
			if !ok {
				t.Fatalf("emit schema properties = %#v", schema["properties"])
			}
			property, ok := properties["large_integer"].(map[string]any)
			if !ok {
				t.Fatalf("large_integer schema = %#v", properties["large_integer"])
			}
			assertStoreSnapshotNumber(t, "schema minimum", property["minimum"], float64(safeInteger))

			decisionEventID := uuid.NewString()
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{"score": safeInteger}),
				ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID,
				Now: card.CreatedAt.Add(time.Minute),
			}); err != nil {
				t.Fatalf("DecideDecisionCard with safe field: %v", err)
			}
			decided, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil {
				t.Fatalf("GetDecisionCard after safe decision: %v", err)
			}
			fieldNumber, _ := decided.Fields.Lookup("score")
			assertStoreSnapshotNumber(t, "fields.score", fieldNumber.Interface(), float64(safeInteger))

			db, postgres := decisionCardStoreDB(t, cardStore)
			changePayload := admitDecisionCardTestObject(t, map[string]any{"safe_integer": safeInteger, "subnormal": math.SmallestNonzeroFloat64})
			if _, err := appendDecisionCardChange(ctx, db, runID, card.CardID, decisioncard.ChangeDeferred, changePayload, card.CreatedAt.Add(2*time.Minute), postgres); err != nil {
				t.Fatalf("append semantic decision-card change: %v", err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10})
			if err != nil {
				t.Fatalf("ListDecisionCardChanges: %v", err)
			}
			last := changes[len(changes)-1]
			changeNumber, _ := last.Payload.Lookup("safe_integer")
			assertStoreSnapshotNumber(t, "change.safe_integer", changeNumber.Interface(), float64(safeInteger))
			changeSubnormal, _ := last.Payload.Lookup("subnormal")
			assertStoreSnapshotNumber(t, "change.subnormal", changeSubnormal.Interface(), math.SmallestNonzeroFloat64)

			unsafeID := uuid.NewString()
			if _, err := decisioncard.FreezeSnapshot("launch_review", "", map[string]any{"large_integer": int64(9007199254740992)}, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}); err == nil || !strings.Contains(err.Error(), "safe range") {
				t.Fatalf("FreezeSnapshot unsafe integer error = %v", err)
			}
			if _, err := cardStore.GetDecisionCard(ctx, unsafeID); !errors.Is(err, decisioncard.ErrNotFound) {
				t.Fatalf("GetDecisionCard after unsafe create error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestDecisionCardStoreRejectsUnadmittedPersistedCarriersOnBothStores(t *testing.T) {
	const unsafeJSON = `{"unsafe":9007199254740992}`
	const scalarJSON = `"not-an-object"`
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			for _, carrier := range []string{"provenance", "fields", "change_payload"} {
				carrier := carrier
				t.Run(carrier, func(t *testing.T) {
					ctx := context.Background()
					cardStore, runID := decisionCardTestStore(t, backend)
					card := newDecisionCardTestCard(t, runID, time.Date(2026, 7, 13, 13, 0, 0, 0, time.UTC))
					if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
						t.Fatal(err)
					}
					db, postgres := decisionCardStoreDB(t, cardStore)
					switch carrier {
					case "provenance", "fields":
						query := fmt.Sprintf("UPDATE decision_cards SET %s = ? WHERE card_id = ?", carrier)
						args := []any{unsafeJSON, card.CardID}
						if postgres {
							query = fmt.Sprintf("UPDATE decision_cards SET %s = $1::jsonb WHERE card_id = $2::uuid", carrier)
						}
						if _, err := db.ExecContext(ctx, query, args...); err != nil {
							t.Fatalf("corrupt %s: %v", carrier, err)
						}
						if _, err := cardStore.GetDecisionCard(ctx, card.CardID); err == nil || !strings.Contains(err.Error(), "safe range") {
							t.Fatalf("GetDecisionCard after %s corruption error = %v, want semantic admission failure", carrier, err)
						}
						args[0] = scalarJSON
						if _, err := db.ExecContext(ctx, query, args...); err != nil {
							t.Fatalf("corrupt %s with scalar: %v", carrier, err)
						}
						if _, err := cardStore.GetDecisionCard(ctx, card.CardID); err == nil || !strings.Contains(err.Error(), carrier+" must be an object") {
							t.Fatalf("GetDecisionCard after scalar %s corruption error = %v, want object-shape failure", carrier, err)
						}
					case "change_payload":
						query := "UPDATE decision_card_changes SET payload = ? WHERE card_id = ?"
						args := []any{unsafeJSON, card.CardID}
						if postgres {
							query = "UPDATE decision_card_changes SET payload = $1::jsonb WHERE card_id = $2::uuid"
						}
						if _, err := db.ExecContext(ctx, query, args...); err != nil {
							t.Fatalf("corrupt change payload: %v", err)
						}
						if _, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10}); err == nil || !strings.Contains(err.Error(), "safe range") {
							t.Fatalf("ListDecisionCardChanges after payload corruption error = %v, want semantic admission failure", err)
						}
						args[0] = scalarJSON
						if _, err := db.ExecContext(ctx, query, args...); err != nil {
							t.Fatalf("corrupt change payload with scalar: %v", err)
						}
						if _, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10}); err == nil || !strings.Contains(err.Error(), "payload must be an object") {
							t.Fatalf("ListDecisionCardChanges after scalar payload corruption error = %v, want object-shape failure", err)
						}
					}
				})
			}
		})
	}
}

func assertStoreSnapshotNumber(t *testing.T, name string, value any, want float64) {
	t.Helper()
	number, ok := value.(float64)
	if !ok || number != want {
		t.Fatalf("%s = %#v, want float64(%v)", name, value, want)
	}
}

func setDecisionCardFeedbackRequired(outcomes map[string]decisioncard.FrozenOutcome, required bool) {
	outcome := outcomes["revise"]
	outcome.Input["feedback"] = runtimecontracts.WorkflowGateInputField{Type: "text", Required: required}
	outcomes["revise"] = outcome
}

func TestDecisionCardInvalidFrozenOutcomeNeverCommitsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 7, 13, 5, 30, 0, 0, time.UTC)
			properties := map[string]any{
				"code":      map[string]any{"type": "string", "pattern": "^[a-z]+$"},
				"component": map[string]any{"type": "string"},
				"owner":     map[string]any{"type": "string", "x-swarm-equalTo": "component"},
			}
			card, err := decisioncard.New(decisioncard.Card{
				CardID: uuid.NewString(), RunID: runID, FlowInstance: "root", FlowID: "launch", EntityID: uuid.NewString(),
				Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
				Snapshot: freezeDecisionCardTestSnapshot(t, "launch_review", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {
						Verdict: "approve", AdvancesTo: "operating",
						Input: map[string]runtimecontracts.WorkflowGateInputField{
							"code": {Type: "text", Required: true}, "component": {Type: "text", Required: true}, "owner": {Type: "text", Required: true},
						},
						Emit: runtimecontracts.EmitSpec{Event: "review.completed", Fields: map[string]runtimecontracts.ExpressionValue{
							"code": runtimecontracts.CELExpression("decision.code"), "component": runtimecontracts.CELExpression("decision.component"), "owner": runtimecontracts.CELExpression("decision.owner"),
						}},
						EmitSchema: map[string]any{"type": "object", "properties": properties, "required": []string{"code", "component", "owner"}, "additionalProperties": false},
					},
				}),
				BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkflowVersion: "1",
				EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			_, err = cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{"code": "NOT-LOWER", "component": "api", "owner": "worker"}),
				ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			})
			if !errors.Is(err, decisioncard.ErrInvalidFields) {
				t.Fatalf("invalid frozen outcome error = %v, want ErrInvalidFields", err)
			}
			loaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || loaded.Status != decisioncard.StatusPending || loaded.Verdict != "" || !loaded.DecidedAt.IsZero() {
				t.Fatalf("card after rejected settlement = %#v, %v", loaded, err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 10})
			if err != nil || len(changes) != 1 || changes[0].ChangeType != decisioncard.ChangeCreated {
				t.Fatalf("changes after rejected settlement = %#v, %v", changes, err)
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

func TestDecisionCardDraftReplacementExpiryAndSupersessionAreCursorVisibleOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 7, 12, 15, 0, 0, 0, time.UTC)
			card := newDecisionCardTestCard(t, runID, now)
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			first, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now, TTL: 5 * time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			second, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-a", Now: now.Add(time.Minute), TTL: 5 * time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			if first.InputDraftID == second.InputDraftID {
				t.Fatal("replacement reused draft identity")
			}
			expirer, ok := cardStore.(interface {
				ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error)
			})
			if !ok {
				t.Fatal("decision-card store lacks durable draft expiry owner")
			}
			if count, err := expirer.ExpireDecisionCardInputDrafts(ctx, now.Add(7*time.Minute)); err != nil || count != 1 {
				t.Fatalf("ExpireDecisionCardInputDrafts = %d, %v", count, err)
			}
			if _, err := cardStore.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "operator-b", Now: now.Add(8 * time.Minute), TTL: 5 * time.Minute}); err != nil {
				t.Fatal(err)
			}
			if err := cardStore.SupersedeDecisionCardsForStage(ctx, runID, card.EntityID, card.StageActivationID, "timer_fired", now.Add(9*time.Minute)); err != nil {
				t.Fatal(err)
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 50})
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, change := range changes {
				counts[change.ChangeType]++
			}
			if counts[decisioncard.ChangeDraftCancelled] != 2 || counts[decisioncard.ChangeDraftExpired] != 1 || counts[decisioncard.ChangeSuperseded] != 1 {
				t.Fatalf("draft lifecycle changes = %#v; all closures must be cursor-visible", counts)
			}
		})
	}
}

func TestRunTerminalizationAtomicallyFencesGateActivationsAndCardsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			db, postgres := decisionCardStoreDB(t, cardStore)
			now := time.Date(2026, 7, 12, 16, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			activation, err := gateruntime.New(runID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "state:awaiting_review", now)
			if err != nil {
				t.Fatal(err)
			}
			card := newDecisionCardTestCard(t, runID, now)
			card.CardID, card.EntityID, card.FlowInstance, card.FlowID = activation.CardID, entityID, "launch/review", "launch"
			card.StageActivationID, card.Stage, card.DecisionID, card.BundleHash = activation.ActivationID, activation.Stage, activation.DecisionID, activation.BundleHash
			card, err = decisioncard.New(card)
			if err != nil {
				t.Fatal(err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			seedDecisionCardGateEntity(t, db, postgres, runID, entityID, activation, now)
			if _, err := markDecisionCardRunTerminal(ctx, cardStore, runID, "cancelled", now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			loaded, err := cardStore.GetDecisionCard(ctx, card.CardID)
			if err != nil || loaded.Status != decisioncard.StatusSuperseded {
				t.Fatalf("terminal card = %#v, %v", loaded, err)
			}
			stored := loadDecisionCardGateActivation(t, db, postgres, runID, entityID)
			if stored.Status != gateruntime.StatusSuperseded {
				t.Fatalf("terminal activation = %#v", stored)
			}
			eventStore, ok := cardStore.(runtimebus.EventStore)
			if !ok {
				t.Fatalf("decision card store %T is not an EventBus store", cardStore)
			}
			bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{BundleFingerprint: card.BundleHash})
			if err != nil {
				t.Fatal(err)
			}
			const subscriber = "decision-card-lifecycle-recorder"
			bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: subscriber})
			deliveries := bus.Subscribe(subscriber, events.EventType("mailbox.card_superseded"))
			t.Cleanup(func() { bus.Unsubscribe(subscriber) })
			if released, err := bus.ReleaseDecisionCardLifecycleEvents(ctx, 10); err != nil || released != 1 {
				t.Fatalf("release lifecycle outbox = %d, %v", released, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				t.Fatal(err)
			}
			var lifecycle events.Event
			select {
			case lifecycle = <-deliveries:
			case <-waitCtx.Done():
				t.Fatal("timed out waiting for run supersession lifecycle event")
			}
			if lifecycle.RunID() != runID || lifecycle.EntityID() != entityID || lifecycle.FlowInstance() != card.FlowInstance {
				t.Fatalf("lifecycle identity = run:%q entity:%q flow:%q", lifecycle.RunID(), lifecycle.EntityID(), lifecycle.FlowInstance())
			}
			recipients, err := eventStore.ListEventDeliveryRecipients(ctx, lifecycle.ID())
			if err != nil || len(recipients) != 1 || recipients[0] != subscriber {
				t.Fatalf("lifecycle recipients = %#v, %v", recipients, err)
			}
			scopeReader, ok := cardStore.(runtimereplayclaim.ScopeReader)
			if !ok {
				t.Fatalf("decision card store %T lacks replay scope reader", cardStore)
			}
			if scope, err := scopeReader.LoadCommittedReplayScope(ctx, lifecycle.ID()); err != nil || scope != runtimereplayclaim.CommittedReplayScopeSubscribed {
				t.Fatalf("lifecycle replay scope = %q, %v", scope, err)
			}
		})

		t.Run(backend+"/committed_verdict_blocks_terminalization", func(t *testing.T) {
			ctx := context.Background()
			cardStore, runID := decisionCardTestStore(t, backend)
			db, postgres := decisionCardStoreDB(t, cardStore)
			now := time.Date(2026, 7, 12, 17, 0, 0, 0, time.UTC)
			entityID := uuid.NewString()
			activation, err := gateruntime.New(runID, "launch/review", entityID, "launch", "awaiting_review", "launch_review", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "state:awaiting_review", now)
			if err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			if err := activation.CommitDecision(decisionEventID, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			card := newDecisionCardTestCard(t, runID, now)
			card.CardID, card.EntityID, card.FlowInstance, card.FlowID = activation.CardID, entityID, "launch/review", "launch"
			card.StageActivationID, card.Stage, card.DecisionID, card.BundleHash = activation.ActivationID, activation.Stage, activation.DecisionID, activation.BundleHash
			card, err = decisioncard.New(card)
			if err != nil {
				t.Fatal(err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			if _, err := cardStore.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "accept", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute)}); err != nil {
				t.Fatal(err)
			}
			seedDecisionCardGateEntity(t, db, postgres, runID, entityID, activation, now)
			if _, err := markDecisionCardRunTerminal(ctx, cardStore, runID, "cancelled", now.Add(2*time.Minute)); err == nil {
				t.Fatal("run terminalization discarded a committed verdict")
			}
			stored := loadDecisionCardGateActivation(t, db, postgres, runID, entityID)
			if stored.Status != gateruntime.StatusDecisionCommitted {
				t.Fatalf("blocked terminal activation = %#v", stored)
			}
			var status string
			query := `SELECT status FROM runs WHERE run_id = ?`
			if postgres {
				query = `SELECT status FROM runs WHERE run_id = $1::uuid`
			}
			if err := db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil || status != "running" {
				t.Fatalf("blocked terminal run status = %q, %v", status, err)
			}
		})
	}
}

func decisionCardStoreDB(t *testing.T, cards decisioncard.Store) (*sql.DB, bool) {
	t.Helper()
	switch store := cards.(type) {
	case *PostgresStore:
		return store.DB, true
	case *SQLiteRuntimeStore:
		return store.DB, false
	default:
		t.Fatalf("unexpected decision card store %T", cards)
		return nil, false
	}
}

func seedDecisionCardGateEntity(t *testing.T, db *sql.DB, postgres bool, runID, entityID string, activation gateruntime.Activation, now time.Time) {
	t.Helper()
	buckets := map[string]map[string]any{}
	if err := gateruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	accumulator, err := json.Marshal(runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
	if err != nil {
		t.Fatal(err)
	}
	query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, 'launch/review', 'default', 'launch', 'Launch', 'awaiting_review', '{}', '{}', ?, 1, ?, ?, ?)`
	args := []any{runID, entityID, string(accumulator), now, now, now}
	if postgres {
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, 'launch/review', 'default', 'launch', 'Launch', 'awaiting_review', '{}'::jsonb, '{}'::jsonb, $3::jsonb, 1, $4, $5, $6)`
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func loadDecisionCardGateActivation(t *testing.T, db *sql.DB, postgres bool, runID, entityID string) gateruntime.Activation {
	t.Helper()
	query := `SELECT accumulator FROM entity_state WHERE run_id = ? AND entity_id = ?`
	if postgres {
		query = `SELECT accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
	}
	var raw any
	if err := db.QueryRowContext(context.Background(), query, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	accumulator, err := toolDecodeJSONMap(raw)
	if err != nil {
		t.Fatal(err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, accumulator)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil || len(activations) != 1 {
		t.Fatalf("gate activations = %#v, %v", activations, err)
	}
	return activations[0]
}

func markDecisionCardRunTerminal(ctx context.Context, cards decisioncard.Store, runID, status string, now time.Time) (any, error) {
	switch store := cards.(type) {
	case *PostgresStore:
		return store.MarkRunTerminal(ctx, runID, status, nil, now)
	case *SQLiteRuntimeStore:
		return store.MarkRunTerminal(ctx, runID, status, nil, now)
	default:
		return nil, fmt.Errorf("unexpected decision card store %T", cards)
	}
}

func newDecisionCardTestCard(t *testing.T, runID string, now time.Time) decisioncard.Card {
	t.Helper()
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, FlowInstance: "launch/review", FlowID: "launch", EntityID: uuid.NewString(),
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
		Snapshot: freezeDecisionCardTestSnapshot(t, "launch_review", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"accept": {Verdict: "accept", AdvancesTo: "operating"},
			"revise": {Verdict: "revise", AdvancesTo: "building", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
		}),
		BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkflowVersion: "1",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"}, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("New decision card: %v", err)
	}
	return card
}

func freezeDecisionCardTestSnapshot(t *testing.T, decision string, context map[string]any, outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) decisioncard.Snapshot {
	t.Helper()
	snapshot, err := decisioncard.FreezeSnapshot(decision, "", context, outcomes)
	if err != nil {
		t.Fatalf("FreezeSnapshot: %v", err)
	}
	return snapshot
}

func admitDecisionCardTestObject(t *testing.T, object map[string]any) semanticvalue.Value {
	t.Helper()
	value, err := canonicaljson.FromGo(object)
	if err != nil {
		t.Fatalf("admit semantic object: %v", err)
	}
	return value
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
