package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type forkedDecisionConsumerSurface interface {
	decisioncard.Store
	CreateHumanTaskCard(context.Context, decisioncard.Card, decisioncard.HumanTaskContinuation) error
	CompleteHumanTaskOutcome(context.Context, string, string, time.Time) (decisioncard.HumanTaskContinuation, error)
	ExpireHumanTaskCardsInMutation(context.Context, time.Time, int) ([]events.Event, error)
	CreateProposedEffectCard(context.Context, decisioncard.Card, decisioncard.ProposedEffectContinuation) error
	CompleteProposedEffectRoute(context.Context, string, string, time.Time) (decisioncard.ProposedEffectContinuation, error)
	SupersedeProposedEffectsForLoopGenerations(context.Context, string, string, []attemptgeneration.Generation, string, time.Time) error
	PipelineObligations() runtimepipelineobligation.Store
}

func TestForkedSourceDecisionCardsContinuationsDraftsAndRoutesCannotAdvance(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkedConsumerTestBackend(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			now := fixture.forkedAt.Add(-time.Minute)
			var surface forkedDecisionConsumerSurface
			if fixture.postgres != nil {
				surface = fixture.postgres
			} else {
				surface = fixture.sqlite
			}

			stageCard := newDecisionCardTestCard(t, fixture.sourceRun, now)
			if err := surface.CreateDecisionCard(ctx, stageCard); err != nil {
				t.Fatal(err)
			}
			draft, err := surface.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{
				CardID: stageCard.CardID, Verdict: "revise", ActorTokenID: "operator", Now: now, TTL: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}

			humanCard, humanContinuation := newHumanTaskDecisionCardTestFixture(t, fixture.sourceRun, "frozen-human", now, 1, now.Add(24*time.Hour))
			if err := surface.CreateHumanTaskCard(ctx, humanCard, humanContinuation); err != nil {
				t.Fatal(err)
			}
			decisionEventID := uuid.NewString()
			insertForkedConsumerEvent(t, fixture, decisionEventID, "mailbox.item_"+"decided", now)
			if _, err := surface.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: humanCard.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: humanCard.CardContentHash, DecisionEventID: decisionEventID, Now: now,
			}); err != nil {
				t.Fatal(err)
			}

			generation := attemptgeneration.Generation{
				LoopID: "revision", ActivationID: uuid.NewString(), RevisionField: "revision_id", RevisionID: uuid.NewString(), Attempt: 1,
			}
			effectCard, effectContinuation := newProposedEffectTestCard(t, fixture.sourceRun, now, generation)
			if err := surface.CreateProposedEffectCard(ctx, effectCard, effectContinuation); err != nil {
				t.Fatal(err)
			}

			fixture.freeze(t)

			newCard := newDecisionCardTestCard(t, fixture.sourceRun, now.Add(time.Minute))
			requireForkedSourceRefusal(t, "create decision card", surface.CreateDecisionCard(ctx, newCard))
			newHuman, newHumanContinuation := newHumanTaskDecisionCardTestFixture(t, fixture.sourceRun, "late-human", now.Add(time.Minute), 1, now.Add(25*time.Hour))
			requireForkedSourceRefusal(t, "create human task", surface.CreateHumanTaskCard(ctx, newHuman, newHumanContinuation))
			newEffect, newEffectContinuation := newProposedEffectTestCard(t, fixture.sourceRun, now.Add(time.Minute), attemptgeneration.Generation{})
			requireForkedSourceRefusal(t, "create proposed effect", surface.CreateProposedEffectCard(ctx, newEffect, newEffectContinuation))

			_, err = surface.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: stageCard.CardID, Verdict: "accept", ActorTokenID: "operator",
				ObservedContentHash: stageCard.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Minute),
			})
			if !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("decide frozen card error = %v, want typed terminal refusal", err)
			}
			_, err = surface.DeferDecisionCard(ctx, decisioncard.DeferRequest{CardID: stageCard.CardID, Now: now.Add(time.Minute), Until: now.Add(time.Hour)})
			if !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("defer frozen card error = %v, want typed terminal refusal", err)
			}
			_, err = surface.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: stageCard.CardID, Verdict: "revise", ActorTokenID: "operator", Now: now.Add(time.Minute), TTL: time.Hour})
			if !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("begin input on frozen card error = %v, want typed terminal refusal", err)
			}
			_, err = surface.CancelDecisionCardInput(ctx, decisioncard.CancelInputRequest{InputDraftID: draft.InputDraftID, CardID: stageCard.CardID, ActorTokenID: "operator", Now: now.Add(time.Minute)})
			if !errors.Is(err, decisioncard.ErrDraftNotAuthority) {
				t.Fatalf("cancel frozen input error = %v, want draft-not-authority", err)
			}

			err = runDecisionCardTestPipelineMutation(t, ctx, surface, func(txctx context.Context, _ *sql.Tx) error {
				_, err := surface.CompleteHumanTaskOutcome(txctx, humanCard.CardID, decisionEventID, now.Add(time.Minute))
				return err
			})
			if !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("complete frozen human outcome error = %v, want typed terminal refusal", err)
			}
			err = runDecisionCardTestPipelineMutation(t, ctx, surface, func(txctx context.Context, _ *sql.Tx) error {
				_, err := surface.CompleteProposedEffectRoute(txctx, effectCard.CardID, uuid.NewString(), now.Add(time.Minute))
				return err
			})
			if !errors.Is(err, decisioncard.ErrAlreadyTerminal) {
				t.Fatalf("complete frozen proposed effect error = %v, want typed terminal refusal", err)
			}
			requireForkedSourceRefusal(t, "supersede proposed effect", surface.SupersedeProposedEffectsForLoopGenerations(
				ctx, fixture.sourceRun, uuid.NewString(), nil, "loop_advanced", now.Add(time.Minute),
			))

			due, ok, err := surface.PipelineObligations().ClaimNext(ctx, runtimepipelineobligation.DecisionRouteQuery())
			if err != nil || ok {
				t.Fatalf("frozen decision-route selector = %#v, %t, %v", due, ok, err)
			}

			var expired []events.Event
			err = runDecisionCardTestPipelineMutation(t, ctx, surface, func(txctx context.Context, _ *sql.Tx) error {
				var err error
				expired, err = surface.ExpireHumanTaskCardsInMutation(txctx, now.Add(48*time.Hour), 20)
				return err
			})
			if err != nil || len(expired) != 0 {
				t.Fatalf("frozen human-task expiry = %#v, %v", expired, err)
			}

			persisted, err := surface.GetDecisionCard(ctx, stageCard.CardID)
			if err != nil {
				t.Fatal(err)
			}
			if fixture.postgres != nil && persisted.Status != decisioncard.StatusSuperseded {
				t.Fatalf("postgres source card status = %q, want superseded", persisted.Status)
			}
			if fixture.sqlite != nil && persisted.Status != decisioncard.StatusPending {
				t.Fatalf("sqlite canonical frozen-row card status = %q, want preserved pending lineage", persisted.Status)
			}
			if !errors.Is(surface.CreateDecisionCard(ctx, newCard), storerunlifecycle.ErrRunNotActive) {
				t.Fatal("repeated frozen decision create did not remain fail-closed")
			}
		})
	}
}

func insertForkedConsumerEvent(t *testing.T, fixture *forkedConsumerTestBackend, eventID, eventName string, at time.Time) {
	t.Helper()
	event := eventtest.PersistedProjectionForProducer(
		eventID, events.EventType(eventName), eventtest.Producer(events.EventProducerPlatform, "test"),
		"", []byte(`{}`), 0, fixture.sourceRun, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, at,
	)
	if fixture.postgres != nil {
		if err := insertCanonicalEventRecordFixture(context.Background(), fixture.postgres, event); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := insertCanonicalEventRecordFixture(context.Background(), fixture.sqlite, event); err != nil {
		t.Fatal(err)
	}
}
