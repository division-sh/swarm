package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestDecisionCompletionPreservesCommittedHandoffOutcome(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"human", "proposed"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				cards, runID := decisionCardTestStore(t, backend)
				now := time.Now().UTC()
				var card decisioncard.Card
				var err error
				if kind == "human" {
					var continuation decisioncard.HumanTaskContinuation
					card, continuation = newHumanTaskDecisionCardTestFixture(t, runID, "handoff", now, 1, now.Add(48*time.Hour))
					err = cards.(decisioncard.HumanTaskStore).CreateHumanTaskCard(ctx, card, continuation)
				} else {
					var continuation decisioncard.ProposedEffectContinuation
					card, continuation = newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
					err = cards.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(ctx, card, continuation)
				}
				if err != nil {
					t.Fatal(err)
				}
				decisionID := uuid.NewString()
				if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
					CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
					ObservedContentHash: card.CardContentHash, DecisionEventID: decisionID, Now: now.Add(time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
				parentID := decisionID
				eventID := decisioncard.HumanTaskOutcomeEventID(card.CardID, decisionID)
				eventName := "human_task.approved"
				if kind == "proposed" {
					continuation, err := cards.(decisioncard.ProposedEffectStore).LoadProposedEffectContinuation(ctx, card.CardID)
					if err != nil {
						t.Fatal(err)
					}
					parentID, eventID, eventName = continuation.SourceEventID, continuation.RequestEventID, "platform.activity_requested"
				}
				at := now.Add(2 * time.Minute)
				if err := commitSemanticParentFixture(ctx, cards, runID, parentID, at.Add(-time.Microsecond)); err != nil {
					t.Fatal(err)
				}
				evt := eventtest.PersistedProjection(eventID, events.EventType(eventName), "test", "", []byte(`{}`), 1, runID, parentID, events.EventEnvelope{}, at)
				if err := commitSemanticPipelineProcessedEventFixture(ctx, cards, evt); err != nil {
					t.Fatal(err)
				}
				db, postgres := decisionCardStoreDB(t, cards)
				query := "SELECT bundle_hash FROM runs WHERE run_id=?"
				if postgres {
					query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
				}
				var bundleHash string
				if err := db.QueryRow(query, runID).Scan(&bundleHash); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected decision completion handoff failure")
				submits := 0
				sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
					submits++
					if candidate.RunID != runID {
						t.Errorf("foreign candidate: %+v", candidate)
					}
					return injected
				}}
				registration, err := cards.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				if kind == "human" {
					result, completionErr := cards.(decisioncard.HumanTaskStore).CompleteHumanTaskOutcome(ctx, card.CardID, decisionID, at)
					if !errors.Is(completionErr, injected) || result.CardID != card.CardID || result.State != decisioncard.HumanTaskContinuationOutcomeDispatched {
						t.Fatalf("committed human result lost: %+v, %v", result, completionErr)
					}
				} else {
					result, completionErr := cards.(decisioncard.ProposedEffectStore).CompleteProposedEffectRoute(ctx, card.CardID, decisionID, at)
					if !errors.Is(completionErr, injected) || result.CardID != card.CardID || result.State != decisioncard.ProposedEffectRequestReleased {
						t.Fatalf("committed proposed result lost: %+v, %v", result, completionErr)
					}
				}
				if submits != 1 {
					t.Fatalf("handoff submissions=%d want=1", submits)
				}
			})
		}
	}
}
