package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestMailboxRefusalAtPreparationAndCommitBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				for _, winner := range []string{"decided", "superseded", "anchor_superseded"} {
					if winner == "superseded" && kind != decisioncard.AnchorKindStageGate {
						continue // Continuation supersession is covered by the locked-store matrix.
					}
					for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input"} {
						for _, cut := range []string{"before_prepare", "before_commit"} {
							t.Run(string(kind)+"/"+winner+"/"+method+"/"+cut, func(t *testing.T) {
								f := mailboxCompletionFixtureInRuntime(t, rt, owner)
								if kind == decisioncard.AnchorKindStageGate && winner == "decided" {
									// Keep the run active after its gate moves: terminal-run
									// admission is a different, earlier refusal contract.
									mailboxCompletionAnchorCard(t, f, decisioncard.AnchorKindHumanTask)
								}
								params := mailboxPrincipalMutationParams(t, f, kind, method)
								req := mailboxPrincipalRequest(t, rt, method, params)
								card, err := owner.GetDecisionCard(f.ctx, req.ResourceID)
								if err != nil {
									t.Fatal(err)
								}
								want := error(decisioncard.ErrAlreadyTerminal)
								code := "MAILBOX_ALREADY_DECIDED"
								if winner != "decided" {
									want, code = decisioncard.ErrSuperseded, "MAILBOX_CARD_SUPERSEDED"
								}
								winnerParams := map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": card.CardContentHash, "idempotency_key": uuid.NewString()}
								if cut == "before_commit" {
									// SQLite serializes keyed requests per store. A lawful
									// unkeyed winner reaches the domain while that lease is held.
									delete(winnerParams, "idempotency_key")
								}
								var winResult map[string]any
								var before []string
								hits := 0
								win := func() {
									hits++
									if winner == "anchor_superseded" {
										storetest.SupersedeDecisionCardAnchor(t, f.ctx, owner, card, time.Now())
									} else if winner == "superseded" {
										a, err := card.Anchor.StageGate()
										if err != nil {
											t.Fatal(err)
										}
										if err := owner.SupersedeDecisionCardsForStage(f.ctx, card.RunID, a.EntityID, a.StageActivationID, "stage_exited", time.Now()); err != nil {
											t.Fatal(err)
										}
									} else {
										requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", winnerParams, &winResult)
										waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, card.RunID)
									}
									before = mailboxCompletionRunEffects(t, rt, card.RunID)
								}
								fault := &mailboxPostCommitFault{}
								if cut == "before_prepare" {
									win()
								} else {
									fault.beforeCommit = func(context.Context, runtimepipeline.DecisionCardMutationCommand) error {
										win()
										return nil
									}
								}
								coordinator := mailboxFaultCoordinator(t, rt, fault)
								mutation, err := mailboxCardMutation(req, params)
								if err != nil {
									t.Fatal(err)
								}
								_, replayed, err := coordinator.CommitDecisionCardMutation(f.ctx, req, mutation)
								if !errors.Is(err, want) || replayed || hits != 1 {
									t.Fatalf("cut refusal: err=%v want=%v replay=%t winner_hits=%d", err, want, replayed, hits)
								}
								if after := mailboxCompletionRunEffects(t, rt, card.RunID); !reflect.DeepEqual(before, after) {
									t.Fatalf("losing commit mutated domain: %s", mailboxEffectsDifference(before, after))
								}
								if refusal := requireServedJSONRPCError(t, rt.Endpoint, method, params); refusal.Data["code"] != code {
									t.Fatalf("served refusal = %+v, want %s", refusal, code)
								}
								if after := mailboxCompletionRunEffects(t, rt, card.RunID); !reflect.DeepEqual(before, after) {
									t.Fatalf("served refusal mutated domain: %s", mailboxEffectsDifference(before, after))
								}
								var completions int
								if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&completions); err != nil || completions != 0 {
									t.Fatalf("loser minted completion: count=%d err=%v", completions, err)
								}
								if winner == "decided" && cut == "before_prepare" {
									var replay map[string]any
									requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", winnerParams, &replay)
									if replay["idempotency_replayed"] != true {
										t.Fatalf("terminal card blocked completed replay: %v", replay)
									}
									delete(replay, "idempotency_replayed")
									delete(winResult, "idempotency_replayed")
									if !reflect.DeepEqual(replay, winResult) {
										t.Fatalf("completed response changed: %v / %v", replay, winResult)
									}
								}
							})
						}
					}
				}
			}
		})
	}
}
