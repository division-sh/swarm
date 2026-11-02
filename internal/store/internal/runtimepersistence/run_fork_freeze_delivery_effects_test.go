package runtimepersistence

import (
	"context"

	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestForkSourceDeliveryEffectMatrixBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, state := range []runlifecycle.State{runlifecycle.StateFailed, runlifecycle.StateCancelled} {
			t.Run(backend.name+"/"+string(state), func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				ctx = originalStoreFixtureContext(t, ctx, fixture.store)
				runID := uuid.NewString()
				ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				store := fixture.store.(interface {
					deliverylifecycle.Store
					runlifecycle.OperationOwner
					CreateDecisionCard(context.Context, decisioncard.Card) error
					GetDecisionCard(context.Context, string) (decisioncard.Card, error)
				})
				card := newDecisionCardTestCard(t, runID, time.Now().UTC())
				if err := store.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				draft, err := DecisionCardDomainForTest(store).BeginInputForTest(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", PrincipalID: "freeze-proof", Now: time.Now().UTC(), TTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				beforeDeliveries := map[string]deliverylifecycle.Snapshot{}
				for _, deliveryState := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateRetrying, deliverylifecycle.StateDelivered, deliverylifecycle.StateExhausted} {
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, event); err != nil {
						t.Fatal(err)
					}
					failure := testFailureEnvelope(failures.ClassRetryExhausted, "fixture_failure", nil)
					snapshot := seedDeliveryStateFixture(t, ctx, store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("freeze-" + string(deliveryState))}, deliveryState, &failure)
					beforeDeliveries[snapshot.DeliveryID] = snapshot
				}
				at := time.Now().UTC()
				{
					request := runlifecycle.TerminalRequest{RunID: runID, State: state, EndedAt: at}
					if state == runlifecycle.StateFailed {
						failure := testFailureEnvelope(failures.ClassRetryExhausted, "run_failed", nil)
						request.Failure = &failure
					}
					_, _, err = store.MarkTerminalRun(ctx, request)
				}
				if err != nil {
					t.Fatal(err)
				}
				afterCard, err := store.GetDecisionCard(ctx, card.CardID)
				if err != nil || afterCard.Status != decisioncard.StatusSuperseded || afterCard.SupersededReason != "run_"+string(state) {
					t.Fatalf("terminal transition skipped decision supersession: %+v %v", afterCard, err)
				}
				var draftState string
				if err := fixture.db.QueryRowContext(ctx, `SELECT status FROM decision_card_input_drafts WHERE input_draft_id=$1`, draft.InputDraftID).Scan(&draftState); err != nil || draftState != decisioncard.DraftStatusCancelled {
					t.Fatalf("draft supersession = %s %v", draftState, err)
				}
				for id, expected := range beforeDeliveries {
					got, err := store.Snapshot(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if expected.Status == deliverylifecycle.StatusDelivered || expected.Status == deliverylifecycle.StatusDeadLetter {
						if !reflect.DeepEqual(expected, got) {
							t.Fatalf("terminal effect rewrote retained delivery %s: before=%+v after=%+v", id, expected, got)
						}
					} else if got.Status != deliverylifecycle.StatusDeadLetter || got.ReasonCode != "run_"+string(state) {
						t.Fatalf("legitimate terminalization lost delivery effect: %+v", got)
					}
				}
			})
		}
	}
}

func TestForkMaterializationLiveClaimRefusalBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newForkActivationFrontierFixture(t, backend, false)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.received", "freeze-test", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
			if err := insertCanonicalEventRecordFixture(f.ctx, f.store, event); err != nil {
				t.Fatal(err)
			}
			seedDeliveryStateFixture(t, f.ctx, f.store.(deliverylifecycle.Store), event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("held-source-agent")}, deliverylifecycle.StateLaunching, nil)
			f.eventID = uuid.NewString()
			frontier := eventtest.ExistingRunRootIngress(f.eventID, "item.received", "freeze-frontier", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, frontier); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			result, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
			if err == nil || !strings.Contains(err.Error(), "delivery_history_unproven") || result.ForkRunID != "" {
				t.Fatalf("live source claim did not refuse materialization: %+v %v", result, err)
			}
			if after := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
				t.Fatal("live-authority refusal mutated source or child")
			}
		})
	}
}
