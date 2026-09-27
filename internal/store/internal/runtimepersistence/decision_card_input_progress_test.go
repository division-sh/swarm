package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
)

func TestDecisionCardInputProgressIsDurableAndOrderedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			cardStore, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 9, 27, 20, 0, 0, 0, time.UTC)
			card := newDecisionCardTestCard(t, runID, now)
			outcome := card.Snapshot.Outcomes["revise"]
			outcome.Input = map[string]runtimecontracts.WorkflowGateInputField{
				"zeta":  {Type: "text", Required: true},
				"alpha": {Type: "integer", Required: true},
			}
			outcome.InputOrder = []string{"zeta", "alpha"}
			card.Snapshot.Outcomes["revise"] = outcome
			var err error
			card, err = decisioncard.New(card)
			if err != nil {
				t.Fatal(err)
			}
			if err := cardStore.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			draft, err := DecisionCardDomainForTest(cardStore).BeginInputForTest(ctx, decisioncard.BeginInputRequest{
				CardID: card.CardID, Verdict: "revise", PrincipalID: "operator-a", Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}

			advance := func(principal, answer string, at time.Time) (decisioncard.InputFieldProgress, error) {
				var progress decisioncard.InputFieldProgress
				var err error
				operation := func(txctx context.Context, tx *sql.Tx) error {
					progress, err = decisionpersistence.AdvanceInputDraftTextTx(txctx, tx, draft.InputDraftID, principal, answer, at, backend == "postgres")
					return err
				}
				switch store := cardStore.(type) {
				case *PostgresStore:
					err = store.backend.RunTransaction(ctx, operation)
				case *SQLiteRuntimeStore:
					err = store.backend.RunTransaction(ctx, "decision input progress", operation)
				default:
					t.Fatal("unexpected selected store")
				}
				return progress, err
			}
			read := func() (string, int, error) {
				var fields string
				var index int
				operation := func(txctx context.Context, tx *sql.Tx) error {
					query := `SELECT input_fields, next_field_index FROM decision_card_input_drafts WHERE input_draft_id=?`
					if backend == "postgres" {
						query = `SELECT input_fields::text, next_field_index FROM decision_card_input_drafts WHERE input_draft_id=$1::uuid`
					}
					return tx.QueryRowContext(txctx, query, draft.InputDraftID).Scan(&fields, &index)
				}
				var err error
				switch store := cardStore.(type) {
				case *PostgresStore:
					err = store.backend.RunReadTransaction(ctx, operation)
				case *SQLiteRuntimeStore:
					err = store.backend.RunReadTransaction(ctx, operation)
				}
				if err != nil {
					return "", 0, err
				}
				value, err := canonicaljson.Decode([]byte(fields))
				if err != nil {
					return "", 0, err
				}
				canonical, err := canonicaljson.Encode(value)
				return string(canonical), index, err
			}
			if _, err := advance("operator-b", "wrong actor", now.Add(time.Second)); err == nil {
				t.Fatal("foreign principal advanced the draft")
			}
			first, err := advance("operator-a", "why", now.Add(2*time.Second))
			if err != nil || first.AcceptedField != "zeta" || first.NextField != "alpha" || first.Complete {
				t.Fatalf("first ordered input = %+v, %v", first, err)
			}
			if fields, index, err := read(); err != nil || fields != `{"zeta":"why"}` || index != 1 {
				t.Fatalf("durable first field = %q, %d, %v", fields, index, err)
			}
			if _, err := advance("operator-a", "01", now.Add(3*time.Second)); err == nil {
				t.Fatal("invalid integer advanced the draft")
			}
			if fields, index, err := read(); err != nil || fields != `{"zeta":"why"}` || index != 1 {
				t.Fatalf("invalid answer changed durable progress = %q, %d, %v", fields, index, err)
			}
			last, err := advance("operator-a", "17", now.Add(4*time.Second))
			if err != nil || !last.Complete || last.AcceptedField != "alpha" {
				t.Fatalf("final ordered input = %+v, %v", last, err)
			}
			if fields, index, err := read(); err != nil || fields != `{"alpha":17,"zeta":"why"}` || index != 2 {
				t.Fatalf("durable complete fields = %q, %d, %v", fields, index, err)
			}
			if _, err := advance("operator-a", "extra", now.Add(5*time.Second)); err == nil {
				t.Fatal("completed input draft advanced again")
			}
			changes, err := cardStore.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 50})
			if err != nil {
				t.Fatal(err)
			}
			progressCount := 0
			for _, change := range changes {
				if change.ChangeType != decisioncard.ChangeDraftProgressed {
					continue
				}
				progressCount++
				payload, ok := change.Payload.Interface().(map[string]any)
				if !ok || len(payload) != 3 || payload["input_draft_id"] != draft.InputDraftID || payload["field"] == nil || payload["next_field_index"] == nil {
					t.Fatalf("change stream exposed unexpected input progress payload: %#v", change.Payload.Interface())
				}
			}
			if progressCount != 2 {
				t.Fatalf("durable progress changes = %d, want 2", progressCount)
			}
		})
	}
}
