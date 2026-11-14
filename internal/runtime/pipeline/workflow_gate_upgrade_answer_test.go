package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Both linearization orders use native request/source-switch owners. The real
// component bus is deliberately not autonomous; committed routing remains held.
func VerifyGateAnswerUpgradeOrdersBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"answer_first_equal", "answer_first_changed", "upgrade_first", "forged_observation"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				bundle := gateLifecycleBundle(t)
				fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, bundle, "drafting", open)
				if _, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "draft.ready"); err != nil {
					t.Fatal(err)
				}
				card := nativeGateCardForTest(t, fixture, ctx)
				now := time.Now().UTC()
				principal, err := pc.decisionCards.(interface {
					EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
				}).EnsureOperatorPrincipal(ctx, now)
				if err != nil {
					t.Fatalf("admit actual operator principal: %v", err)
				}
				request := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: card.CardID, IdempotencyKey: uuid.NewString(), RequestHash: "sha256:" + strings.Repeat("b", 64), Now: now, TTL: time.Hour}
				mutation := NewDecisionCardDecision(decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal.ID, ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: uuid.NewString(), Now: now})
				lease, err := pc.workflowStore.cardMutations.AcquireDecisionCardMutation(ctx, request, mutation)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := lease.Release(ctx); err != nil {
						t.Error(err)
					}
				})
				command, _, err := pc.prepareDecisionCardMutation(ctx, mutation)
				if err != nil {
					t.Fatal(err)
				}
				if order == "forged_observation" {
					instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, card.RunID))
					if err != nil || !found {
						t.Fatalf("read exact gate instance: %t %v", found, err)
					}
					for _, field := range []string{"source", "card", "run", "life", "offer"} {
						p := command.GateOffer
						switch field {
						case "source":
							p.root = PreparedGateOffer{}.root
						case "card":
							p.cardID = uuid.NewString()
						case "run":
							p.runID = uuid.NewString()
						case "life":
							p.life++
						case "offer":
							p.offer = decisioncard.GateOffer{}
						}
						if err := p.RequireMutation(ctx, card, instance); err == nil {
							t.Fatalf("forged %s observation admitted", field)
						}
					}
					command.GateOffer = PreparedGateOffer{}
					committed, err := lease.Commit(ctx, command)
					if err == nil || committed.Acknowledged || fixture.EventIDCount(ctx, mutation.decide.DecisionEventID) != 0 {
						t.Fatalf("zero observation crossed native admission: %+v %v", committed, err)
					}
					current, err := fixture.Cards.GetDecisionCard(ctx, card.CardID)
					if err != nil || current.Status != decisioncard.StatusPending || current.DecisionEventID != "" {
						t.Fatalf("zero observation consumed issuance: %+v %v", current, err)
					}
					return
				}
				files := gateLifecycleDocuments()
				files["schema.yaml"] += "\n# changed immutable source\n"
				if order != "answer_first_equal" {
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "advances_to: operating", "advances_to: drafting", 1)
				}
				candidate := semanticview.Wrap(loadWorkflowTempBundle(t, files))
				if order == "upgrade_first" {
					if err := fixture.UpgradeSource(ctx, candidate); err != nil {
						t.Fatal(err)
					}
					committed, err := lease.Commit(ctx, command)
					if err == nil || committed.Acknowledged {
						t.Fatalf("stale prepared answer crossed native source switch: %+v %v", committed, err)
					}
					current, err := fixture.Cards.GetDecisionCard(ctx, card.CardID)
					if err != nil || current.Status != decisioncard.StatusPending || current.DecisionEventID != "" {
						t.Fatalf("rejected answer changed issuance: %+v %v", current, err)
					}
					if fixture.EventIDCount(ctx, mutation.decide.DecisionEventID) != 0 {
						t.Fatal("rejected answer published its decision")
					}
					return
				}
				committed, err := lease.Commit(ctx, command)
				if err != nil || !committed.Acknowledged {
					t.Fatalf("native answer commit: %+v %v", committed, err)
				}
				assertGateLifecycleState(t, pc.workflowStore, ctx, correlation.RunIDFromContext(ctx), "awaiting_review", gateruntime.StatusDecisionCommitted)
				if err := lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				owner := testRunScopedWorkflowInstanceFromContext(ctx, card.RunID)
				afterAnswer, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("read committed native answer: %t %v", found, err)
				}
				discovered, err := fixture.Store.(runbundle.UpgradeReader).ReadBundleUpgradeState(ctx, card.RunID, semanticview.Wrap(bundle))
				if err != nil || len(discovered.CommittedGateCards) != 1 || discovered.CommittedGateCards[0].CardID != card.CardID || discovered.CommittedGateCards[0].DecisionEventID != mutation.decide.DecisionEventID {
					t.Fatalf("upgrade discovery lost the exact committed card: %+v err=%v", discovered.CommittedGateCards, err)
				}
				anchor := mustStageGateAnchor(t, card)
				var owners []runbundle.UpgradeInstance
				for _, instance := range discovered.Instances {
					if instance.EntityID == anchor.EntityID && instance.Route.InstancePath == anchor.Route.InstancePath {
						owners = append(owners, instance)
					}
				}
				if len(owners) != 1 || owners[0].Route != anchor.Route {
					t.Fatalf("upgrade discovery changed the exact anchor route: anchor=%+v selected=%+v all=%+v", anchor, owners, discovered.Instances)
				}
				err = fixture.UpgradeSource(ctx, candidate)
				if order == "answer_first_equal" {
					if err != nil {
						t.Fatalf("equal offer stranded committed answer: %v", err)
					}
				} else {
					var refused *runbundle.UpgradeConflictError
					if !errors.As(err, &refused) {
						t.Fatalf("changed source crossed committed answer: %v", err)
					}
					afterRefusal, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(afterRefusal, afterAnswer) {
						t.Fatalf("source refusal partially mutated accepted answer: %v", err)
					}
					availability, err := fixture.Store.(runbundle.AvailabilityStore).LoadRunBundleAvailability(ctx, card.RunID)
					if err != nil || availability.BundleHash != bundle.SourceArtifact.BundleHash() {
						t.Fatalf("refused source switch changed exact run: %+v %v", availability, err)
					}
				}
				current, err := fixture.Cards.GetDecisionCard(ctx, card.CardID)
				if err != nil || current.DecisionEventID != committed.Outcome.Card.DecisionEventID || current.Status != decisioncard.StatusDecided {
					t.Fatalf("upgrade withdrew accepted answer: %+v %v", current, err)
				}
			})
		}
	}
}
