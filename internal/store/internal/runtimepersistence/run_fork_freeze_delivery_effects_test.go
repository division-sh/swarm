package runtimepersistence

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestForkSourceDeliveryEffectMatrixBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, state := range []runlifecycle.State{runlifecycle.StateForked, runlifecycle.StateFailed, runlifecycle.StateCancelled} {
			t.Run(backend.name+"/"+string(state), func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				runID, childID := uuid.NewString(), uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				seedAuthorActivityReceiptRun(t, fixture, ctx, childID)
				store := fixture.store.(interface {
					deliverylifecycle.Store
					runlifecycle.OperationOwner
					CreateDecisionCard(context.Context, decisioncard.Card) error
					GetDecisionCard(context.Context, string) (decisioncard.Card, error)
					BeginDecisionCardInput(context.Context, decisioncard.BeginInputRequest) (decisioncard.InputDraft, error)
				})
				card := newDecisionCardTestCard(t, runID, time.Now().UTC())
				if err := store.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				draft, err := store.BeginDecisionCardInput(ctx, decisioncard.BeginInputRequest{CardID: card.CardID, Verdict: "revise", ActorTokenID: "freeze-proof", Now: time.Now().UTC(), TTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				beforeDeliveries := map[string]deliverylifecycle.Snapshot{}
				sourceEvents := map[string]events.Event{}
				for _, deliveryState := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateRetrying, deliverylifecycle.StateDelivered, deliverylifecycle.StateExhausted} {
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, event); err != nil {
						t.Fatal(err)
					}
					failure := testFailureEnvelope(failures.ClassRetryExhausted, "fixture_failure", nil)
					snapshot := seedDeliveryStateFixture(t, ctx, store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("freeze-" + string(deliveryState))}, deliveryState, &failure)
					beforeDeliveries[snapshot.DeliveryID] = snapshot
					sourceEvents[snapshot.DeliveryID] = event
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				at := time.Now().UTC()
				if state == runlifecycle.StateForked {
					_, _, err = store.ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: runID, ContinuedAsRunID: childID, EndedAt: at})
				} else {
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
					if state == runlifecycle.StateForked || expected.Status == deliverylifecycle.StatusDelivered || expected.Status == deliverylifecycle.StatusDeadLetter {
						if !reflect.DeepEqual(expected, got) {
							t.Fatalf("terminal effect rewrote retained delivery %s: before=%+v after=%+v", id, expected, got)
						}
					} else if got.Status != deliverylifecycle.StatusDeadLetter || got.ReasonCode != "run_"+string(state) {
						t.Fatalf("legitimate terminalization lost delivery effect: %+v", got)
					}
				}
				if state != runlifecycle.StateForked {
					return
				}
				after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				for _, table := range []string{"events", "event_deliveries", "dead_letters", "event_receipts"} {
					if _, exists := before[table]; !exists {
						t.Fatalf("missing history table %s", table)
					}
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Fatalf("freeze rewrote retained %s", table)
					}
				}
				for id, snapshot := range beforeDeliveries {
					if _, err := store.ClaimDelivery(ctx, snapshot.Authority, sourceEvents[id], snapshot.Route); err == nil {
						t.Fatal("frozen source admitted delivery claim")
					}
					page, err := store.ScanDeliveryContinuations(ctx, snapshot.Authority, deliverylifecycle.ContinuationCursor{}, 200)
					if err != nil {
						t.Fatal(err)
					}
					for _, item := range page.Items {
						if item.Snapshot.RunID == runID {
							t.Fatal("recovery scan returned frozen source work")
						}
					}
				}
				summary, err := store.SummarizeRun(ctx, runID)
				if err != nil || len(summary.ActiveDeliveryIDs) != 0 || summary.Pending != 1 || summary.RetryScheduled != 1 {
					t.Fatalf("frozen source lost retained history or active-authority exclusion: %+v %v", summary, err)
				}
				_, disposition, err := store.ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: runID, ContinuedAsRunID: childID, EndedAt: at})
				if err != nil || disposition != runlifecycle.MutationExactNoop {
					t.Fatalf("exact freeze retry: %s %v", disposition, err)
				}
				if _, _, err := store.ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: runID, ContinuedAsRunID: uuid.NewString(), EndedAt: at}); err == nil {
					t.Fatal("conflicting source pointer accepted")
				}
				if !reflect.DeepEqual(after, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("duplicate/conflicting freeze changed application facts")
				}
			})
		}
	}
}

func TestForkFreezeActivationDeliveryHistoryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, selected := range []bool{false, true} {
			for _, rollback := range []bool{false, true} {
				for _, deliveryState := range []deliverylifecycle.State{deliverylifecycle.StateQueued, deliverylifecycle.StateDelivered} {
					t.Run(fmt.Sprintf("%s/selected=%t/rollback=%t/%s", backend.name, selected, rollback, deliveryState), func(t *testing.T) {
						f := newForkContentionFixture(t, backend)
						descriptors, err := runtimepkg.AuthorActivityEventDescriptors(semanticview.Wrap(loadCanonicalSelectedContractStoreSource(t)))
						if err != nil {
							t.Fatal(err)
						}
						scope, found := authoractivity.ScopeFromContext(f.ctx)
						if !found {
							t.Fatal("missing exact author scope")
						}
						catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(catalog.Release)
						store := f.store.(deliverylifecycle.Store)
						// Retained history predates the selected frontier. No post-frontier
						// writer is allowed to turn this into the source-advanced branch.
						event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.received", "freeze-test", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
						if err := insertCanonicalEventRecordFixture(f.ctx, f.store, event); err != nil {
							t.Fatal(err)
						}
						snapshot := seedDeliveryStateFixture(t, f.ctx, store, event, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("retained-agent")}, deliveryState, nil)
						f.eventID = uuid.NewString()
						point := eventtest.ExistingRunRootIngress(f.eventID, "item.received", "freeze-frontier", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
						if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, point); err != nil {
							t.Fatal(err)
						}
						// Ordinary replay needs the existing agent-history admission port;
						// selected admission uses the real compiled frontier proof below.
						staged, req := stageForkContentionFixture(t, f, selected)
						if selected {
							for _, sourceID := range req.AllowedSourceEventIDs {
								lineage, err := events.NewSelectedForkLineage(staged.ForkRunID, f.runID, sourceID, runfork.RunForkSelectedContractExecutionOwner, "", "live")
								if err != nil {
									t.Fatal(err)
								}
								replayed := eventtest.SelectedForkReplay(uuid.NewString(), "item.received", eventtest.Producer(events.EventProducerPlatform, runfork.RunForkSelectedContractExecutionOwner), "", []byte(`{}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC())
								if err := commitSelectedForkEventFixture(f.ctx, f.store.(selectedForkEventFixtureStore), replayed, runfork.RunForkSelectedContractExecutionLineage{ForkRunID: staged.ForkRunID, SourceRunID: f.runID, SourceEventID: sourceID, ForkEventID: replayed.ID(), EventName: string(replayed.Type()), SelectionAuthority: runfork.RunForkSelectedContractExecutionOwner, CreatedAt: replayed.CreatedAt()}); err != nil {
									t.Fatal(err)
								}
							}
						}
						if rollback {
							query := fmt.Sprintf(`CREATE TRIGGER freeze_child_failure BEFORE UPDATE ON runs WHEN OLD.run_id='%s' AND NEW.status='running' BEGIN SELECT RAISE(ABORT, 'freeze_child_failure'); END`, staged.ForkRunID)
							if backend.name == "postgres" {
								query = fmt.Sprintf(`CREATE FUNCTION freeze_child_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'freeze_child_failure'; END $$; CREATE TRIGGER freeze_child_failure BEFORE UPDATE ON runs FOR EACH ROW WHEN (OLD.run_id='%s' AND NEW.status='running') EXECUTE FUNCTION freeze_child_failure()`, staged.ForkRunID)
							}
							if _, err := f.db.Exec(query); err != nil {
								t.Fatal(err)
							}
						}
						before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
						var result runfork.RunForkActivation
						if selected {
							result, err = f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(f.ctx, req)
						} else {
							admitter := &fakeRunForkHistoricalReplayExecutionAdmitter{work: func(r runfork.RunForkHistoricalReplayExecutionRequest) []runfork.RunForkHistoricalReplayExecutableWork {
								var work []runfork.RunForkHistoricalReplayExecutableWork
								for _, pending := range r.PendingWork {
									if pending.SubscriberType == "agent" && pending.Status == "pending" {
										work = append(work, runForkHistoricalReplayWorkFromPending(pending))
									}
								}
								return work
							}}
							result, err = f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, HistoricalReplayExecutionAdmitter: admitter, OriginalLoopCarriage: originalCarriageForRun(t, f.store.(selectedActivityProjectionStore), f.runID)})
						}
						if rollback {
							if err == nil || !strings.Contains(err.Error(), "freeze_child_failure") {
								t.Fatalf("wrong rollback frontier: %v", err)
							}
							if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
								t.Fatal("child activation failure left partial source/decision/history changes")
							}
							return
						}
						if err != nil || !result.Activated || !result.SourceFrozen || result.SourceAdvancedAfterFork {
							t.Fatalf("activation did not freeze unadvanced source: %+v %v", result, err)
						}
						got, err := store.Snapshot(f.ctx, snapshot.DeliveryID)
						if err != nil || !reflect.DeepEqual(snapshot, got) {
							t.Fatalf("activation rewrote retained source history: %+v %v", got, err)
						}
					})
				}
			}
		}
	}
}
