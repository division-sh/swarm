package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestProposedEffectCardLifecycleParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, verdict := range []string{"approve", "revise", "reject"} {
			t.Run(backend+"/"+verdict, func(t *testing.T) {
				ctx := context.Background()
				cards, runID := decisionCardTestStore(t, backend)
				store := cards.(decisioncard.ProposedEffectStore)
				now := time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)
				card, continuation := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
				if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatalf("CreateProposedEffectCard: %v", err)
				}
				if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatalf("idempotent CreateProposedEffectCard: %v", err)
				}
				readback, err := store.ProposedEffectReadback(ctx, card.CardID)
				if err != nil || readback.DispatchState != "held" {
					t.Fatalf("pending readback = %#v, %v", readback, err)
				}
				changed := continuation
				changed.Input, _ = canonicaljson.FromGo(map[string]any{"chat_id": "support", "text": "changed"})
				changedEffect, err := changed.EffectValue()
				if err != nil {
					t.Fatal(err)
				}
				changed.EffectContentHash, err = canonicaljson.HashValue(changedEffect)
				if err != nil {
					t.Fatal(err)
				}
				changedCard := card
				changedCard.EffectContentHash = changed.EffectContentHash
				changedCard.CardContentHash = ""
				changedCard, err = decisioncard.New(changedCard)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CreateProposedEffectCard(ctx, changedCard, changed); !isProposedEffectFailureClass(err, runtimefailures.ClassConflictingDuplicate) {
					t.Fatalf("changed-content create error = %v, want conflicting duplicate", err)
				}
				fields := semanticvalue.EmptyObject()
				if verdict == "revise" {
					fields, _ = canonicaljson.FromGo(map[string]any{"feedback": "Please remove the secret."})
				} else if verdict == "reject" {
					fields, _ = canonicaljson.FromGo(map[string]any{"reason": "Not authorized."})
				}
				decisionEventID := uuid.NewString()
				if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
					CardID: card.CardID, Verdict: verdict, Fields: fields, ActorTokenID: "operator",
					ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
				}); err != nil {
					t.Fatalf("DecideDecisionCard: %v", err)
				}
				committed, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
				if err != nil || committed.State != decisioncard.ProposedEffectDecisionCommitted || committed.Verdict != verdict {
					t.Fatalf("committed continuation = %#v, %v", committed, err)
				}
				assertProposedEffectAuthorActivity(t, cards, runID, card.CardID, continuation.RequestEventID, []string{"created", "decided"}, verdict, "")
				if _, err := store.CompleteProposedEffectRoute(ctx, card.CardID, decisionEventID, now.Add(2*time.Minute)); err == nil || !strings.Contains(err.Error(), "active pipeline transaction") {
					t.Fatalf("standalone CompleteProposedEffectRoute error = %v", err)
				}
				if err := runDecisionCardTestPipelineMutation(t, ctx, cards, func(txctx context.Context, _ *sql.Tx) error {
					_, err := store.CompleteProposedEffectRoute(txctx, card.CardID, decisionEventID, now.Add(2*time.Minute))
					return err
				}); err == nil || !strings.Contains(err.Error(), "outcome event is not persisted") {
					t.Fatalf("eventless CompleteProposedEffectRoute error = %v", err)
				}
				if err := runDecisionCardTestPipelineMutation(t, ctx, cards, func(txctx context.Context, _ *sql.Tx) error {
					_, err := store.CompleteProposedEffectRoute(txctx, card.CardID, uuid.NewString(), now.Add(2*time.Minute))
					return err
				}); err == nil {
					t.Fatal("mismatched proposed-effect route identity was accepted")
				}
				completeProposedEffectRouteInTestMutation(t, ctx, cards, card.CardID, decisionEventID, now.Add(2*time.Minute))
				readback, err = store.ProposedEffectReadback(ctx, card.CardID)
				wantDispatch := "released"
				wantContinuation := decisioncard.ProposedEffectRequestReleased
				if verdict != "approve" {
					wantDispatch = "not_dispatched"
					wantContinuation = decisioncard.ProposedEffectOutcomeDispatched
				}
				if err != nil || readback.DispatchState != wantDispatch || readback.ContinuationState != wantContinuation {
					t.Fatalf("routed readback = %#v, %v; want %s/%s", readback, err, wantContinuation, wantDispatch)
				}
			})
		}
	}
}

func TestNormalRunCompletionRequiresSettledProposedEffectsParity(t *testing.T) {
	testCases := []struct {
		name          string
		state         string
		wantCompleted bool
	}{
		{name: "missing_continuation", state: "missing"},
		{name: "pending", state: decisioncard.ProposedEffectPending},
		{name: "decision_committed", state: decisioncard.ProposedEffectDecisionCommitted},
		{name: "request_released", state: decisioncard.ProposedEffectRequestReleased, wantCompleted: true},
		{name: "request_released_without_event", state: "request_released_without_event"},
		{name: "request_released_mismatched_route", state: "request_released_mismatched_route"},
		{name: "outcome_dispatched", state: decisioncard.ProposedEffectOutcomeDispatched, wantCompleted: true},
		{name: "superseded", state: decisioncard.ProposedEffectSuperseded, wantCompleted: true},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, testCase := range testCases {
			backend, testCase := backend, testCase
			t.Run(backend+"/"+testCase.name, func(t *testing.T) {
				ctx := context.Background()
				cards, runID := decisionCardTestStore(t, backend)
				store := cards.(decisioncard.ProposedEffectStore)
				db, postgres := decisionCardStoreDB(t, cards)
				now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
				generation := attemptgeneration.Generation{
					LoopID: "revision", ActivationID: uuid.NewString(), RevisionField: "revision_id",
					RevisionID: uuid.NewString(), Attempt: 1,
				}
				card, continuation := newProposedEffectTestCard(t, runID, now, generation)
				if testCase.state == "missing" {
					if err := cards.CreateDecisionCard(ctx, card); err != nil {
						t.Fatalf("CreateDecisionCard malformed proposed effect: %v", err)
					}
				} else if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatalf("CreateProposedEffectCard: %v", err)
				}

				switch testCase.state {
				case decisioncard.ProposedEffectDecisionCommitted, decisioncard.ProposedEffectRequestReleased, decisioncard.ProposedEffectOutcomeDispatched,
					"request_released_without_event", "request_released_mismatched_route":
					verdict := "approve"
					if testCase.state == decisioncard.ProposedEffectOutcomeDispatched {
						verdict = "reject"
					}
					decisionEventID := uuid.NewString()
					if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
						CardID: card.CardID, Verdict: verdict, Fields: semanticvalue.EmptyObject(), ActorTokenID: "operator",
						ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
					}); err != nil {
						t.Fatalf("DecideDecisionCard: %v", err)
					}
					if testCase.state == "request_released_without_event" {
						query := `UPDATE proposed_effect_continuations SET state = 'request_released', route_event_id = ? WHERE card_id = ?`
						if postgres {
							query = `UPDATE proposed_effect_continuations SET state = 'request_released', route_event_id = $1::uuid WHERE card_id = $2`
						}
						if _, err := db.ExecContext(ctx, query, decisionEventID, card.CardID); err != nil {
							t.Fatal(err)
						}
					} else if testCase.state == "request_released_mismatched_route" {
						completeProposedEffectRouteInTestMutation(t, ctx, cards, card.CardID, decisionEventID, now.Add(2*time.Minute))
						query := `UPDATE proposed_effect_continuations SET route_event_id = ? WHERE card_id = ?`
						if postgres {
							query = `UPDATE proposed_effect_continuations SET route_event_id = $1::uuid WHERE card_id = $2`
						}
						if _, err := db.ExecContext(ctx, query, uuid.NewString(), card.CardID); err != nil {
							t.Fatal(err)
						}
						if err := runDecisionCardTestPipelineMutation(t, ctx, cards, func(txctx context.Context, _ *sql.Tx) error {
							_, err := store.CompleteProposedEffectRoute(txctx, card.CardID, decisionEventID, now.Add(3*time.Minute))
							return err
						}); err == nil || !strings.Contains(err.Error(), "inconsistent route identity") {
							t.Fatalf("inconsistent persisted route identity error = %v", err)
						}
					} else if testCase.state != decisioncard.ProposedEffectDecisionCommitted {
						completeProposedEffectRouteInTestMutation(t, ctx, cards, card.CardID, decisionEventID, now.Add(2*time.Minute))
					}
				case decisioncard.ProposedEffectSuperseded:
					next := generation
					next.RevisionID = uuid.NewString()
					next.Attempt++
					if err := store.SupersedeProposedEffectsForLoopGenerations(ctx, runID, continuation.EntityID, []attemptgeneration.Generation{next}, "loop_generation_superseded", now.Add(2*time.Minute)); err != nil {
						t.Fatalf("SupersedeProposedEffectsForLoopGenerations: %v", err)
					}
				}

				entityID := uuid.NewString()
				seedDecisionCardCompletionEntity(t, db, postgres, runID, entityID, "done", now)
				eventID := seedDecisionCardCompletionEvent(t, ctx, cards, runID, entityID, now.Add(3*time.Hour))
				if err := convergeDecisionCardRunCompletion(ctx, cards, eventID); err != nil {
					t.Fatalf("ConvergeNormalRunCompletion: %v", err)
				}
				query := `SELECT status FROM runs WHERE run_id = ?`
				if postgres {
					query = `SELECT status FROM runs WHERE run_id = $1::uuid`
				}
				var status string
				if err := db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil {
					t.Fatalf("load run status: %v", err)
				}
				wantStatus := "running"
				if testCase.wantCompleted {
					wantStatus = "completed"
				}
				if status != wantStatus {
					t.Fatalf("run status = %q, want %q", status, wantStatus)
				}
			})
		}
	}
}

func assertProposedEffectAuthorActivity(t *testing.T, cards decisioncard.Store, runID, cardID, requestEventID string, wantTransitions []string, verdict, supersedeReason string) {
	t.Helper()
	reader, ok := cards.(interface {
		ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error)
	})
	if !ok {
		t.Fatalf("decision card store %T lacks author activity readback", cards)
	}
	result, err := reader.ListAuthorActivity(context.Background(), runtimeauthoractivity.ListOptions{RunID: runID, Limit: 100})
	if err != nil {
		t.Fatalf("list proposed-effect author activity: %v", err)
	}
	var found []runtimeauthoractivity.Occurrence
	for _, occurrence := range result.Occurrences {
		if occurrence.Kind == runtimeauthoractivity.KindCardLifecycle && occurrence.Projection.CardID == cardID {
			found = append(found, occurrence)
		}
	}
	if len(found) != len(wantTransitions) {
		t.Fatalf("proposed-effect author activity occurrences = %#v, want exactly %v", found, wantTransitions)
	}
	for index, transition := range wantTransitions {
		occurrence := found[index]
		if occurrence.Transition != transition {
			t.Fatalf("proposed-effect author activity transition[%d] = %q, want %q: %#v", index, occurrence.Transition, transition, found)
		}
		if occurrence.Projection.AnchorKind != string(decisioncard.AnchorKindProposedEffect) || occurrence.Projection.AnchorID != requestEventID {
			t.Fatalf("proposed-effect %s anchor projection = %#v", transition, occurrence.Projection)
		}
		if transition == "decided" && occurrence.Projection.Verdict != verdict {
			t.Fatalf("proposed-effect decided verdict = %q, want %q", occurrence.Projection.Verdict, verdict)
		}
		if transition == "superseded" && occurrence.Projection.SupersedeReason != supersedeReason {
			t.Fatalf("proposed-effect superseded reason = %q, want %q", occurrence.Projection.SupersedeReason, supersedeReason)
		}
	}
}

func TestProposedEffectContractPinParticipatesInDuplicateIdentityOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutate := range []struct {
			name  string
			apply func(*decisioncard.Card, *decisioncard.ProposedEffectContinuation)
		}{
			{
				name: "bundle_hash",
				apply: func(card *decisioncard.Card, continuation *decisioncard.ProposedEffectContinuation) {
					continuation.BundleHash = "bundle-v1:sha256:" + strings.Repeat("b", 64)
					card.BundleHash = continuation.BundleHash
				},
			},
			{
				name: "workflow_version",
				apply: func(card *decisioncard.Card, continuation *decisioncard.ProposedEffectContinuation) {
					continuation.WorkflowVersion = "2"
					card.WorkflowVersion = continuation.WorkflowVersion
				},
			},
		} {
			t.Run(backend+"/"+mutate.name, func(t *testing.T) {
				ctx := context.Background()
				cards, runID := decisionCardTestStore(t, backend)
				proposed := cards.(decisioncard.ProposedEffectStore)
				card, continuation := newProposedEffectTestCard(t, runID, time.Date(2026, 7, 14, 16, 0, 0, 0, time.UTC), attemptgeneration.Generation{})
				if err := proposed.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				mutate.apply(&card, &continuation)
				effect, err := continuation.EffectValue()
				if err != nil {
					t.Fatal(err)
				}
				continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
				if err != nil {
					t.Fatal(err)
				}
				card.EffectContentHash = continuation.EffectContentHash
				card.CardContentHash = ""
				card, err = decisioncard.New(card)
				if err != nil {
					t.Fatal(err)
				}
				if err := proposed.CreateProposedEffectCard(ctx, card, continuation); !isProposedEffectFailureClass(err, runtimefailures.ClassConflictingDuplicate) {
					t.Fatalf("changed %s create error = %v, want conflicting duplicate", mutate.name, err)
				}
			})
		}
	}
}

func TestProposedEffectReadbackKeepsAuthorizationAndDispatchAxesSeparateOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, status := range []string{
			runtimepipeline.ActivityAttemptStatusStarted,
			runtimepipeline.ActivityAttemptStatusSucceeded,
			runtimepipeline.ActivityAttemptStatusFailed,
			runtimepipeline.ActivityAttemptStatusUncertain,
		} {
			t.Run(backend+"/"+status, func(t *testing.T) {
				ctx := context.Background()
				cards, runID := decisionCardTestStore(t, backend)
				store := cards.(decisioncard.ProposedEffectStore)
				now := time.Date(2026, 7, 14, 17, 0, 0, 0, time.UTC)
				card, continuation := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
				if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				decisionEventID := uuid.NewString()
				if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
					CardID: card.CardID, Verdict: "approve", Fields: semanticvalue.EmptyObject(), ActorTokenID: "operator",
					ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
				completeProposedEffectRouteInTestMutation(t, ctx, cards, card.CardID, decisionEventID, now.Add(2*time.Minute))

				journal := proposedEffectTestJournal(cards)
				started, inserted, err := journal.StartActivityAttempt(ctx, runtimepipeline.ActivityAttemptRecord{
					RequestEventID: continuation.RequestEventID, RunID: runID, SourceEventID: continuation.SourceEventID,
					EntityID: continuation.EntityID, FlowInstance: continuation.FlowInstance, NodeID: continuation.NodeID,
					HandlerEventKey: continuation.HandlerEventKey, ActivityID: continuation.ActivityID, Tool: continuation.Tool,
					EffectClass: string(continuation.EffectClass), Attempt: 1, Status: runtimepipeline.ActivityAttemptStatusStarted,
					SuccessEvent: continuation.SuccessEvent, FailureEvent: continuation.FailureEvent, InputHash: continuation.EffectContentHash,
				})
				if err != nil || !inserted {
					t.Fatalf("start attempt = %#v, %v, inserted=%v", started, err, inserted)
				}
				if status != runtimepipeline.ActivityAttemptStatusStarted {
					terminal := started
					terminal.Status = status
					terminal.ResultEventID = uuid.NewString()
					terminal.ResultEventType = continuation.SuccessEvent
					terminal.ResultPayload = map[string]any{"activity_id": continuation.ActivityID}
					if status != runtimepipeline.ActivityAttemptStatusSucceeded {
						failure := runtimefailures.Normalize(runtimefailures.New(
							map[string]runtimefailures.Class{
								runtimepipeline.ActivityAttemptStatusFailed:    runtimefailures.ClassConnectorFailure,
								runtimepipeline.ActivityAttemptStatusUncertain: runtimefailures.ClassOutcomeUncertain,
							}[status],
							"proposed_effect_readback_fixture", "test", "dispatch", nil,
						), "test", "dispatch")
						terminal.Failure = &failure
						terminal.ResultEventType = continuation.FailureEvent
					}
					if _, err := journal.CompleteActivityAttempt(ctx, terminal); err != nil {
						t.Fatal(err)
					}
				}
				readback, err := store.ProposedEffectReadback(ctx, card.CardID)
				if err != nil || readback.DispatchState != status || readback.ContinuationState != decisioncard.ProposedEffectRequestReleased {
					t.Fatalf("readback = %#v, %v; want authorization=decided continuation=released dispatch=%s", readback, err, status)
				}
			})
		}
	}
}

func proposedEffectTestJournal(cards decisioncard.Store) *runtimepipeline.WorkflowInstanceStore {
	switch selected := cards.(type) {
	case *SQLiteRuntimeStore:
		return runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected)
	case *PostgresStore:
		return runtimepipeline.NewWorkflowInstanceStore(selected.DB)
	default:
		panic("unsupported proposed-effect test store")
	}
}

func isProposedEffectFailureClass(err error, class runtimefailures.Class) bool {
	var failure *runtimefailures.Error
	return errors.As(err, &failure) && failure.Failure.Class == class
}

func TestProposedEffectDecisionAndSupersessionWinnerParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"decision", "run_supersession", "loop_supersession"} {
			t.Run(backend+"/"+winner, func(t *testing.T) {
				ctx := context.Background()
				cards, runID := decisionCardTestStore(t, backend)
				store := cards.(decisioncard.ProposedEffectStore)
				now := time.Date(2026, 7, 14, 15, 30, 0, 0, time.UTC)
				generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: uuid.NewString(), RevisionField: "revision_id", RevisionID: uuid.NewString(), Attempt: 1}
				card, continuation := newProposedEffectTestCard(t, runID, now, generation)
				if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				decide := func() error {
					_, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
						CardID: card.CardID, Verdict: "approve", Fields: semanticvalue.EmptyObject(), ActorTokenID: "operator",
						ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
					})
					return err
				}
				supersede := func() error {
					switch winner {
					case "loop_supersession":
						next := generation
						next.RevisionID = uuid.NewString()
						next.Attempt++
						return store.SupersedeProposedEffectsForLoopGenerations(ctx, runID, continuation.EntityID, []attemptgeneration.Generation{next}, "loop_generation_superseded", now.Add(2*time.Minute))
					default:
						_, err := markDecisionCardRunTerminal(ctx, cards, runID, "cancelled", now.Add(2*time.Minute))
						return err
					}
				}
				if winner == "decision" {
					if err := decide(); err != nil {
						t.Fatal(err)
					}
					if err := supersede(); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := supersede(); err != nil {
						t.Fatal(err)
					}
					if err := decide(); !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
						t.Fatalf("decision after supersession = %v, want already terminal", err)
					}
				}
				storedCard, err := cards.GetDecisionCard(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				storedContinuation, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				if winner == "decision" {
					if storedCard.Status != decisioncard.StatusDecided || storedContinuation.State != decisioncard.ProposedEffectDecisionCommitted {
						t.Fatalf("decision winner = card:%s continuation:%s", storedCard.Status, storedContinuation.State)
					}
					assertProposedEffectAuthorActivity(t, cards, runID, card.CardID, continuation.RequestEventID, []string{"created", "decided"}, "approve", "")
				} else if storedCard.Status != decisioncard.StatusSuperseded || storedContinuation.State != decisioncard.ProposedEffectSuperseded {
					t.Fatalf("supersession winner = card:%s continuation:%s", storedCard.Status, storedContinuation.State)
				} else {
					reason := "run_cancelled"
					if winner == "loop_supersession" {
						reason = "loop_generation_superseded"
					}
					assertProposedEffectAuthorActivity(t, cards, runID, card.CardID, continuation.RequestEventID, []string{"created", "superseded"}, "", reason)
				}
			})
		}
	}
}

func TestProposedEffectSupersessionScopesParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			cards, runID := decisionCardTestStore(t, backend)
			store := cards.(decisioncard.ProposedEffectStore)
			now := time.Date(2026, 7, 14, 16, 0, 0, 0, time.UTC)
			generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: uuid.NewString(), RevisionField: "revision_id", RevisionID: uuid.NewString(), Attempt: 1}
			card, continuation := newProposedEffectTestCard(t, runID, now, generation)
			if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
				t.Fatal(err)
			}
			if err := cards.SupersedeDecisionCardsForStage(ctx, runID, continuation.EntityID, uuid.NewString(), "stage_exited", now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			stillPending, _ := cards.GetDecisionCard(ctx, card.CardID)
			if stillPending.Status != decisioncard.StatusPending {
				t.Fatalf("ordinary stage exit status = %s", stillPending.Status)
			}
			if err := store.SupersedeProposedEffectsForLoopGenerations(ctx, runID, continuation.EntityID, []attemptgeneration.Generation{generation}, "loop_generation_superseded", now.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			stillPending, _ = cards.GetDecisionCard(ctx, card.CardID)
			if stillPending.Status != decisioncard.StatusPending {
				t.Fatalf("current generation status = %s", stillPending.Status)
			}
			next := generation
			next.RevisionID = uuid.NewString()
			next.Attempt = 2
			if err := store.SupersedeProposedEffectsForLoopGenerations(ctx, runID, continuation.EntityID, []attemptgeneration.Generation{next}, "loop_generation_superseded", now.Add(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			superseded, _ := cards.GetDecisionCard(ctx, card.CardID)
			stored, _ := store.LoadProposedEffectContinuation(ctx, card.CardID)
			if superseded.Status != decisioncard.StatusSuperseded || stored.State != decisioncard.ProposedEffectSuperseded {
				t.Fatalf("supersession = card:%#v continuation:%#v", superseded, stored)
			}
		})
	}
}

func newProposedEffectTestCard(t *testing.T, runID string, now time.Time, generation attemptgeneration.Generation) (decisioncard.Card, decisioncard.ProposedEffectContinuation) {
	t.Helper()
	requestID := uuid.NewString()
	entityID := uuid.NewString()
	input, err := canonicaljson.FromGo(map[string]any{"chat_id": "support", "text": "Approved reply"})
	if err != nil {
		t.Fatal(err)
	}
	continuation := decisioncard.ProposedEffectContinuation{
		CardID: decisioncard.ProposedEffectCardID(requestID, "support_reply"), RunID: runID,
		RequestEventID: requestID, ActivityID: "send_support_reply", Tool: "telegram.send_message", Input: input,
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), WorkflowVersion: "1",
		EffectClass:  runtimecontracts.ActivityEffectClassNonIdempotentWrite,
		SuccessEvent: "support_reply.succeeded", FailureEvent: "support_reply.failed",
		RevisionEvent: "support_reply.revision_requested", RejectedEvent: "support_reply.rejected",
		RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation,
		EntityID: entityID, NodeID: "support", FlowID: "", FlowInstance: "root",
		HandlerEventKey: "support.drafted", SourceEventID: uuid.NewString(), SourceRunID: runID,
		Generation: generation, ReplyContextID: "reply-context-source", State: decisioncard.ProposedEffectPending, CreatedAt: now, UpdatedAt: now,
	}.Canonical()
	effect, err := continuation.EffectValue()
	if err != nil {
		t.Fatal(err)
	}
	continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: requestID, ActivityID: continuation.ActivityID, Decision: "support_reply",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root", EntityID: entityID},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("support_reply", "", map[string]any{"input": input.Interface()}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve"},
		"revise":  {Verdict: "revise", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
		"reject":  {Verdict: "reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: continuation.CardID, RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode:     "live",
		EffectContentHash: continuation.EffectContentHash,
		BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64), WorkflowVersion: "1",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return card, continuation
}
