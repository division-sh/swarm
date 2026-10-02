package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/google/uuid"
)

// These are controlled downstream negatives, not public draft-creation journeys:
// public selected-fork begin_input already refuses before a draft can be created.
func TestDecisionCardSelectedControlMutationBoundariesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, control := range []string{"decide", "defer", "begin_input", "cancel_input", "partial_text", "final_text", "partial_skip", "final_skip"} {
			t.Run(backend+"/"+control, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				store, runID := decisionCardTestStore(t, backend)
				now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
				card := newDecisionCardTestCard(t, runID, now)
				outcome := card.Snapshot.Outcomes["revise"]
				outcome.Input = map[string]runtimecontracts.WorkflowGateInputField{"answer": {Type: "text"}}
				outcome.InputOrder = []string{"answer"}
				if control == "partial_text" || control == "partial_skip" {
					outcome.Input["later"] = runtimecontracts.WorkflowGateInputField{Type: "text", Required: true}
					outcome.InputOrder = append(outcome.InputOrder, "later")
				}
				card.Snapshot.Outcomes["revise"] = outcome
				card, err := decisioncard.New(card)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				domain := DecisionCardDomainForTest(store)
				draft, err := domain.BeginInputForTest(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", PrincipalID: "operator-a", Now: now})
				if err != nil {
					t.Fatal(err)
				}
				bindingID := uuid.NewString()
				if err := selectedDecisionControlTx(ctx, store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings
						(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,mode,created_at)
						VALUES ($1,$2,$3,'deployment_revision',1,'selected_contracts',$4)`, bindingID, runID, runID, now)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				beforeCard, err := store.GetDecisionCard(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				readDraft := func() [4]string {
					var row [4]string
					if err := selectedDecisionControlTx(ctx, store, func(ctx context.Context, tx *sql.Tx) error {
						return tx.QueryRowContext(ctx, `SELECT status,CAST(input_fields AS TEXT),CAST(next_field_index AS TEXT),CAST(updated_at AS TEXT)
							FROM decision_card_input_drafts WHERE input_draft_id=$1`, draft.InputDraftID).Scan(&row[0], &row[1], &row[2], &row[3])
					}); err != nil {
						t.Fatal(err)
					}
					return row
				}
				beforeDraft := readDraft()
				beforeChanges, err := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
				if err != nil {
					t.Fatal(err)
				}
				operation := runfork.ControlMailboxDecide
				switch control {
				case "decide":
					_, err = domain.ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "accept", Fields: semanticvalue.EmptyObject(), PrincipalID: "operator-a", ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now})
				case "defer":
					operation = runfork.ControlMailboxDefer
					_, err = domain.ApplyDeferralForTest(ctx, decisioncard.DeferRequest{CardID: card.CardID, Until: now.Add(time.Hour), Now: now})
				case "begin_input":
					operation = runfork.ControlMailboxBeginInput
					_, err = domain.BeginInputForTest(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", PrincipalID: "operator-a", Now: now})
				case "cancel_input":
					operation = runfork.ControlMailboxCancelInput
					_, err = domain.CancelInputForTest(ctx, decisioncard.CancelInputRequest{CardID: card.CardID, InputDraftID: draft.InputDraftID, PrincipalID: "operator-a", Now: now})
				default:
					err = selectedDecisionControlTx(ctx, store, func(ctx context.Context, tx *sql.Tx) error {
						if control == "partial_skip" || control == "final_skip" {
							_, err := decisionpersistence.AdvanceInputDraftSkipTx(ctx, tx, draft.InputDraftID, "operator-a", now, backend == "postgres")
							return err
						}
						_, err := decisionpersistence.AdvanceInputDraftTextTx(ctx, tx, draft.InputDraftID, "operator-a", "answer", now, backend == "postgres")
						return err
					})
				}
				var refusal *runfork.SelectedForkControlUnsupported
				if !errors.As(err, &refusal) || refusal.RunID != runID || refusal.BindingID != bindingID || refusal.Operation != operation {
					t.Fatalf("exact selected control refusal = %#v, %v", refusal, err)
				}
				afterCard, err := store.GetDecisionCard(ctx, card.CardID)
				if err != nil || !reflect.DeepEqual(beforeCard, afterCard) {
					t.Fatalf("refusal mutated card: %v", err)
				}
				if afterDraft := readDraft(); beforeDraft != afterDraft {
					t.Fatalf("refusal mutated draft: %v -> %v", beforeDraft, afterDraft)
				}
				afterChanges, err := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
				if err != nil || !reflect.DeepEqual(beforeChanges, afterChanges) {
					t.Fatalf("refusal emitted a card change: %v", err)
				}
			})
		}
	}
}

func selectedDecisionControlTx(ctx context.Context, store decisioncard.Store, fn func(context.Context, *sql.Tx) error) error {
	switch store := store.(type) {
	case *PostgresStore:
		return store.backend.RunTransaction(ctx, fn)
	case *SQLiteRuntimeStore:
		return store.backend.RunTransaction(ctx, "selected decision control proof", fn)
	default:
		return errors.New("selected decision control proof requires a selected store")
	}
}
