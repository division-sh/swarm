package runtimepersistence

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

func TestProposedEffectCompletionUsesFrozenPublicationIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scope := range []struct{ name, flow, instance, prefix string }{
			{"root", ".", "root", ""}, {"static", "worker", "worker", "worker/"},
			{"template", "worker", "worker/first", "worker/first/"},
			{"nested", "outer/worker", "outer/worker/second", "outer/worker/second/"},
		} {
			for _, verdict := range []string{"revise", "reject"} {
				t.Run(backend+"/"+scope.name+"/"+verdict, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					cards, runID := decisionCardTestStore(t, backend)
					store := cards.(decisioncard.ProposedEffectStore)
					now := time.Now().UTC()
					card, continuation := newProposedEffectTestCard(t, runID, now, attemptgeneration.Generation{})
					source := eventtest.RootRoutingSource(continuation.EntityID)
					if scope.flow != "." {
						if scope.name == "static" {
							source = eventtest.StaticFlowRoutingSource(scope.flow, scope.instance, continuation.EntityID)
						} else {
							source = eventtest.ConcreteTemplateRoutingSource(scope.flow, scope.instance, continuation.EntityID)
						}
						continuation.FlowID, continuation.FlowInstance = scope.flow, scope.instance
						continuation.NodeID = activityidentity.MustNodeOwner(mustPersistenceNode(scope.flow, "support")).Key()
						anchor, err := card.Anchor.ProposedEffect()
						if err != nil {
							t.Fatal(err)
						}
						anchor.Scope.FlowInstance, anchor.Source = scope.instance, source
						card.Anchor, err = decisioncard.NewProposedEffectAnchor(anchor)
						if err != nil {
							t.Fatal(err)
						}
						effect, err := continuation.EffectValue()
						if err != nil {
							t.Fatal(err)
						}
						continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
						if err != nil {
							t.Fatal(err)
						}
						card.EffectContentHash, card.CardContentHash = continuation.EffectContentHash, ""
						card, err = decisioncard.New(card)
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := store.CreateProposedEffectCard(ctx, card, continuation); err != nil {
						t.Fatal(err)
					}
					field := "reason"
					if verdict == "revise" {
						field = "feedback"
					}
					fields, err := canonicaljson.FromGo(map[string]any{field: verdict})
					if err != nil {
						t.Fatal(err)
					}
					decisionID := uuid.NewString()
					if _, err := cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: verdict,
						Fields: fields, ActorTokenID: "operator", ObservedContentHash: card.CardContentHash,
						DecisionEventID: decisionID, Now: now.Add(time.Minute)}); err != nil {
						t.Fatal(err)
					}
					local := continuation.RevisionEvent
					if verdict == "reject" {
						local = continuation.RejectedEvent
					}
					publication, err := pinrouting.AdmitPublicationIdentity(scope.flow, local, source)
					if err != nil {
						t.Fatal(err)
					}
					if want := scope.prefix + local; string(publication) != want {
						t.Fatalf("publication identity = %q, want %q", publication, want)
					}
					at := now.Add(2 * time.Minute)
					if err := commitSemanticParentFixture(ctx, cards, runID, decisionID, at.Add(-time.Microsecond)); err != nil {
						t.Fatal(err)
					}
					eventID := decisioncard.ProposedEffectOutcomeEventID(card.CardID, decisionID, verdict)
					event := eventtest.PersistedProjectionWithRoutingSource(eventID, publication, "test", "", []byte(`{}`), 1,
						runID, decisionID, events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route()), source, at)
					if err := commitSemanticPipelineProcessedEventFixture(ctx, cards, event); err != nil {
						t.Fatal(err)
					}
					db, _ := decisionCardStoreDB(t, cards)
					for _, hostile := range []string{local, "sibling/" + local, "worker/other/" + local} {
						if hostile == string(publication) {
							continue
						}
						if _, err := db.ExecContext(ctx, `UPDATE events SET event_name=$1 WHERE event_id=$2`, hostile, eventID); err != nil {
							t.Fatal(err)
						}
						if _, err := store.CompleteProposedEffectRoute(ctx, card.CardID, decisionID, at); err == nil || !strings.Contains(err.Error(), "outcome event is not persisted") {
							t.Fatalf("foreign/local-only publication %q accepted: %v", hostile, err)
						}
						current, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
						if err != nil || current.State != decisioncard.ProposedEffectDecisionCommitted || current.RouteEventID != "" {
							t.Fatalf("failed identity check mutated continuation: %+v %v", current, err)
						}
					}
					if _, err := db.ExecContext(ctx, `UPDATE events SET event_name=$1 WHERE event_id=$2`, string(publication), eventID); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < 2; i++ {
						current, err := store.CompleteProposedEffectRoute(ctx, card.CardID, decisionID, at)
						if err != nil || current.State != decisioncard.ProposedEffectOutcomeDispatched || current.RouteEventID != decisionID {
							t.Fatalf("canonical completion/retry: %+v %v", current, err)
						}
					}
				})
			}
		}
	}
}
