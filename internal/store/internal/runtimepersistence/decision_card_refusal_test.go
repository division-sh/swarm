package runtimepersistence

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

func execDecisionCardRefusalFixture(t *testing.T, db *sql.DB, postgres bool, query, cardID string) error {
	t.Helper()
	if postgres {
		query = strings.Replace(query, "?", "$1", 1)
	}
	_, err := db.Exec(query, cardID)
	return err
}

func TestDecisionCardRefusalReasonAcrossAnchorsAndOperationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			cards, runID := decisionCardTestStore(t, backend)
			db, postgres := decisionCardStoreDB(t, cards)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
			for _, anchor := range []string{"stage_gate", "human_task", "proposed_effect"} {
				for _, state := range []string{"superseded", "decided", "anchor_superseded"} {
					if anchor == "stage_gate" && state == "anchor_superseded" {
						continue // Real workflow-anchor transaction/preparation proof is separate.
					}
					for _, operation := range []string{"decide", "defer", "begin_input"} {
						t.Run(anchor+"/"+state+"/"+operation, func(t *testing.T) {
							var card decisioncard.Card
							verdict, inputVerdict := "approve", "reject"
							var err error
							switch anchor {
							case "stage_gate":
								card = newDecisionCardTestCard(t, runID, now)
								verdict, inputVerdict = "accept", "revise"
								err = cards.CreateDecisionCard(ctx, card)
							case "human_task":
								var continuation decisioncard.HumanTaskContinuation
								card, continuation = newHumanTaskDecisionCardTestFixture(t, runID, uuid.NewString(), now, 0, now.Add(time.Hour))
								err = cards.(decisioncard.HumanTaskStore).CreateHumanTaskCard(ctx, card, continuation)
							case "proposed_effect":
								var continuation decisioncard.ProposedEffectContinuation
								card, continuation = newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
								inputVerdict = "revise"
								err = cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(ctx, card, continuation)
							}
							if err != nil {
								t.Fatal(err)
							}
							domain := DecisionCardDomainForTest(cards)
							want := error(decisioncard.ErrSuperseded)
							switch state {
							case "decided":
								_, err = domain.ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: verdict, PrincipalID: "winner", ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now})
								want = decisioncard.ErrAlreadyTerminal
							case "superseded":
								if anchor == "stage_gate" {
									a := mustDecisionCardTestStageAnchor(t, card)
									err = cards.SupersedeDecisionCardsForStage(ctx, runID, a.EntityID, a.StageActivationID, "stage_exited", now)
								} else {
									// Pin the card cut without terminalizing the run, which is a
									// separate earlier admission fence.
									err = execDecisionCardRefusalFixture(t, db, postgres, `UPDATE decision_cards SET status='superseded', superseded_reason='owner_replaced' WHERE card_id=?`, card.CardID)
								}
							case "anchor_superseded":
								query := `UPDATE human_task_continuations SET state='superseded' WHERE card_id=?`
								if anchor == "proposed_effect" {
									query = `UPDATE proposed_effect_continuations SET state='superseded', superseded_reason='owner_replaced' WHERE card_id=?`
								}
								err = execDecisionCardRefusalFixture(t, db, postgres, query, card.CardID)
							}
							if err != nil {
								t.Fatal(err)
							}
							before := snapshotForkHistoricalExecutionTables(t, db, postgres)
							switch operation {
							case "decide":
								_, err = domain.ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: verdict, PrincipalID: "loser", ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute)})
							case "defer":
								_, err = domain.ApplyDeferralForTest(ctx, decisioncard.DeferRequest{CardID: card.CardID, PrincipalID: "loser", Until: now.Add(time.Hour), Now: now.Add(time.Minute)})
							case "begin_input":
								_, err = domain.BeginInputForTest(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: inputVerdict, PrincipalID: "loser", TTL: time.Minute, Now: now.Add(time.Minute)})
							}
							if !errors.Is(err, want) {
								t.Errorf("refusal = %v, want %v", err, want)
							}
							if after := snapshotForkHistoricalExecutionTables(t, db, postgres); !reflect.DeepEqual(before, after) {
								t.Fatal("refused operation changed persisted domain, cursor, completion, event or delivery evidence")
							}
						})
					}
				}
			}
		})
	}
}
