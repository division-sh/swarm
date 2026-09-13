package runtimepersistence

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestRetryDoesNotFreezeFinalSelectionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, retry := range []bool{false, true} {
			name := "no_fault"
			if retry {
				name = "retry"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				fixture := backend.open(t)
				selected := fixture.store.(deliveryReadProjectionStore)
				ctx := testAuthorActivityContext()
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				event := eventtest.PersistedProjection(uuid.NewString(), "projection.retry", "gateway", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("worker")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				if retry {
					snapshot, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour, RuleSelection: handlerselection.NotReached()})
					if err != nil {
						t.Fatal(err)
					}
					if snapshot.Status != runtimedelivery.StatusFailed {
						t.Fatalf("not a retry: %s", snapshot.Status)
					}
					var count int
					if err := fixture.db.QueryRowContext(ctx, `SELECT count(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, snapshot.DeliveryID).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Errorf("nonterminal retry froze %d final selection fact(s)", count)
					}
					if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET next_eligible_at=$1 WHERE delivery_id=$2`, time.Now().UTC().Add(-time.Second), snapshot.DeliveryID); err != nil {
						t.Fatal(err)
					}
					claimed, err = claimDeliveryFixture(ctx, selected, event, route)
					if err != nil {
						t.Fatal(err)
					}
				}
				fact, err := handlerselection.NoMatch(handlerselection.ContextRules)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := selected.SettleSuccess(ctx, claimed.Claim, nil, 0, fact); err != nil {
					t.Fatalf("final actual evaluation must settle: %v", err)
				}
			})
		}
	}
}

func TestFinalSelectionLiveTraceAndHistoricalLifetimeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.PersistedProjection(uuid.NewString(), "trace.retry", "gateway", "", json.RawMessage("{\n \"fixed\": true\n}"), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			route := testEntitylessNodeDeliveryRoute("selection-worker")
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatal(err)
			}
			id := claimed.Snapshot.DeliveryID
			ref, err := runtimeidentity.AdmitDeclarationIdentity(".", "handler_rule", `nodes["selection-worker"].handlers["trace.retry"].rules[0]`)
			if err != nil {
				t.Fatal(err)
			}
			first, err := handlerselection.Selected(handlerselection.ContextRules, ref, "attempt-one")
			if err != nil {
				t.Fatal(err)
			}
			failed, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour, RuleSelection: handlerselection.Resolved(first)})
			if err != nil || failed.Status != runtimedelivery.StatusFailed || failed.FinalSelection.Present() {
				t.Fatalf("failed attempt = %#v, %v", failed, err)
			}
			retryRevision := routeEvidenceHead(t, ctx, fixture.db, runID)
			assertTrace := func(present bool) {
				t.Helper()
				rows, _, err := selected.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10})
				if err != nil || len(rows) != 1 || (rows[0].HandlerRuleSelection != nil) != present {
					t.Fatalf("trace presence=%v rows=%#v err=%v", present, rows, err)
				}
			}
			assertTrace(false)
			if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET next_eligible_at=$1 WHERE delivery_id=$2`, time.Now().UTC().Add(-time.Second), id); err != nil {
				t.Fatal(err)
			}
			second, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil || second.Claim.Version() != claimed.Claim.Version()+1 {
				t.Fatalf("reclaim: %#v, %v", second, err)
			}
			final, err := handlerselection.NoMatch(handlerselection.ContextRules)
			if err != nil {
				t.Fatal(err)
			}
			settled, err := selected.SettleSuccess(ctx, second.Claim, []string{"one-final-effect"}, 0, final)
			if err != nil || settled.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("final settle: %#v, %v", settled, err)
			}
			finalRevision := routeEvidenceHead(t, ctx, fixture.db, runID)
			if finalRevision <= retryRevision {
				t.Fatal("final fact did not advance the delivery family")
			}
			for _, revision := range []int64{retryRevision, finalRevision} {
				history := routeEvidenceSnapshot(t, ctx, fixture.db, runID, id, revision)
				if history.FinalSelection.Present() != (revision == finalRevision) {
					t.Fatalf("as-of selection leaked: revision=%d snapshot=%#v", revision, history)
				}
				if history.FinalSelection.Present() {
					fact, err := history.FinalSelection.Fact()
					if err != nil || !fact.Equal(final) {
						t.Fatalf("historical final identity=%#v, %v", fact, err)
					}
				}
			}
			assertTrace(true)
			if _, err := selected.SettleSuccess(ctx, second.Claim, nil, 0, first); !errors.Is(err, runtimedelivery.ErrConflict) {
				t.Fatalf("stale final rewrite: %v", err)
			}
			if _, err := fixture.db.ExecContext(ctx, `DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.Snapshot(ctx, id); !errors.Is(err, runtimedelivery.ErrConflict) {
				t.Fatalf("live terminal absence accepted: %v", err)
			}
			if _, _, err := selected.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10}); err == nil {
				t.Fatal("public trace hid corrupt terminal absence")
			}
			// Historical evidence is independent of the corrupted current projection.
			history := routeEvidenceSnapshot(t, ctx, fixture.db, runID, id, finalRevision)
			fact, err := history.FinalSelection.Fact()
			if err != nil || !fact.Equal(final) {
				t.Fatalf("live corruption changed as-of final selection: %#v %v", fact, err)
			}
		})
	}
}

func TestNonterminalFinalSelectionCorruptionFailsClosedBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			for _, status := range []runtimedelivery.Status{runtimedelivery.StatusPending, runtimedelivery.StatusInProgress, runtimedelivery.StatusFailed} {
				t.Run(string(status), func(t *testing.T) {
					event := eventtest.PersistedProjection(uuid.NewString(), "trace.corrupt", "gateway", "", []byte("{}"), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
					route := testEntitylessNodeDeliveryRoute("worker")
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					id, err := runtimedelivery.DeliveryID(event.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					if status != runtimedelivery.StatusPending {
						claim, err := claimDeliveryFixture(ctx, selected, event, route)
						if err != nil {
							t.Fatal(err)
						}
						if status == runtimedelivery.StatusFailed {
							if _, err := selected.SettleFailure(ctx, claim.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour, RuleSelection: handlerselection.NotReached()}); err != nil {
								t.Fatal(err)
							}
						}
					}
					if _, err := fixture.db.ExecContext(ctx, `INSERT INTO event_delivery_handler_rule_selections (delivery_id,selection_context,disposition,display_label) VALUES ($1,'none','not_applicable','')`, id); err != nil {
						t.Fatal(err)
					}
					if _, err := selected.Snapshot(ctx, id); !errors.Is(err, runtimedelivery.ErrConflict) {
						t.Fatalf("%s accepted premature final fact: %v", status, err)
					}
					if _, _, err := selected.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10}); err == nil {
						t.Fatal("trace admitted premature final fact")
					}
					// Remove only injected corruption so the next fixture's canonical revision can validate.
					if _, err := fixture.db.ExecContext(ctx, `DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestFinalSelectionTerminalPresenceReadMatrixBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			for _, terminal := range []runtimedelivery.Status{runtimedelivery.StatusDelivered, runtimedelivery.StatusDeadLetter} {
				t.Run(string(terminal), func(t *testing.T) {
					runID := uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					event := eventtest.PersistedProjection(uuid.NewString(), "terminal.read", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
					route := testEntitylessNodeDeliveryRoute("worker")
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					claimed, err := claimDeliveryFixture(ctx, selected, event, route)
					if err != nil {
						t.Fatal(err)
					}
					if terminal == runtimedelivery.StatusDelivered {
						_, err = selected.SettleSuccess(ctx, claimed.Claim, nil, 0, handlerselection.NotApplicable())
					} else {
						_, err = selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "never_reached", Failure: testRetryableFailure(), RuleSelection: handlerselection.NotReached()})
					}
					if err != nil {
						t.Fatal(err)
					}
					id := claimed.Claim.DeliveryID()
					assertRead := func(valid bool) {
						t.Helper()
						snapshot, snapshotErr := selected.Snapshot(ctx, id)
						rows, _, traceErr := selected.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10})
						if valid {
							if snapshotErr != nil || traceErr != nil || snapshot.Status != terminal || !snapshot.FinalSelection.Present() || len(rows) != 1 || rows[0].HandlerRuleSelection == nil {
								t.Fatalf("legal terminal evidence rejected: snapshot=%+v errors=%v/%v rows=%+v", snapshot, snapshotErr, traceErr, rows)
							}
						} else if snapshotErr == nil || traceErr == nil {
							t.Fatalf("illegal terminal evidence accepted: snapshot=%+v errors=%v/%v rows=%+v", snapshot, snapshotErr, traceErr, rows)
						}
					}
					assertRead(true)
					if _, err := fixture.db.Exec(`DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id); err != nil {
						t.Fatal(err)
					}
					assertRead(false)
					if _, err := fixture.db.Exec(`INSERT INTO event_delivery_handler_rule_selections (delivery_id,selection_context,disposition,flow_path,declaration_family,semantic_path,display_label) VALUES ($1,'handler_rules','evaluation_failed','.','handler_rule','nodes["worker"].handlers["terminal.read"].rules[0]','failed')`, id); err != nil {
						t.Fatal(err)
					}
					assertRead(terminal == runtimedelivery.StatusDeadLetter)
				})
			}
		})
	}
}

func TestUnreachedSelectionRetryExhaustionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.PersistedProjection(uuid.NewString(), "retry.exhaust", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			route := testEntitylessNodeDeliveryRoute("worker")
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			id, err := runtimedelivery.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := selected.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt <= initial.MaxRetries; attempt++ {
				claim, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				after, err := selected.SettleFailure(ctx, claim.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour, RuleSelection: handlerselection.NotReached()})
				if err != nil {
					t.Fatal(err)
				}
				terminal := attempt == initial.MaxRetries
				outcomes, err := selected.Outcomes(ctx, id)
				if err != nil || len(outcomes) != attempt+1 || after.FinalSelection.Present() != terminal || after.ClaimVersion != int64(attempt+1) {
					t.Fatalf("incorrect bounded retry history: after=%+v outcomes=%+v err=%v", after, outcomes, err)
				}
				if terminal {
					fact, err := after.FinalSelection.Fact()
					if err != nil || !fact.Equal(handlerselection.NotApplicable()) || after.Status != runtimedelivery.StatusDeadLetter || after.ReasonCode != "retry_exhausted" || !after.NextEligibleAt.IsZero() || !after.ClaimExpiresAt.IsZero() {
						t.Fatalf("effective exhaustion lost final nonexecution or claim cleanup: %+v %v", after, err)
					}
					if _, err := selected.SettleSuccess(ctx, claim.Claim, nil, 0, handlerselection.NotApplicable()); !errors.Is(err, runtimedelivery.ErrConflict) {
						t.Fatalf("exhausted capability accepted a second settlement: %v", err)
					}
				} else {
					if after.Status != runtimedelivery.StatusFailed || after.NextEligibleAt.IsZero() {
						t.Fatalf("retry was discarded: %+v", after)
					}
					// Component-only clock acceleration; the HTTP proof waits for
					// ordinary retry eligibility without modifying store timestamps.
					if _, err := fixture.db.Exec(`UPDATE event_deliveries SET next_eligible_at=$1 WHERE delivery_id=$2`, time.Now().UTC().Add(-time.Second), id); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestFinalSelectionAdmissionFailureRollsBackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			for _, cut := range []string{"insert", "hydrate", "equality"} {
				t.Run(cut, func(t *testing.T) {
					runID := uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					event := eventtest.PersistedProjection(uuid.NewString(), "selection.atomic", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
					route := testEntitylessNodeDeliveryRoute("worker")
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					claimed, err := claimDeliveryFixture(ctx, selected, event, route)
					if err != nil {
						t.Fatal(err)
					}
					ref, err := runtimeidentity.AdmitDeclarationIdentity(".", "handler_rule", `nodes["worker"].handlers["selection.atomic"].rules[0]`)
					if err != nil {
						t.Fatal(err)
					}
					fact, err := handlerselection.Selected(handlerselection.ContextRules, ref, "selected")
					if err != nil {
						t.Fatal(err)
					}
					id := claimed.Claim.DeliveryID()
					head := routeEvidenceHead(t, ctx, fixture.db, runID)
					body := "SELECT RAISE(ABORT,'selection_insert_cut');"
					pgBody := "RAISE EXCEPTION 'selection_insert_cut';"
					wantError := "persist delivery handler rule selection"
					if cut == "hydrate" {
						body = "UPDATE event_delivery_handler_rule_selections SET flow_path='..' WHERE delivery_id=NEW.delivery_id;"
						pgBody = "NEW.flow_path := '..';"
						wantError = "hydrate delivery handler rule selection"
					}
					if cut == "equality" {
						body = "UPDATE event_delivery_handler_rule_selections SET display_label='contradiction' WHERE delivery_id=NEW.delivery_id;"
						pgBody = "NEW.display_label := 'contradiction';"
						wantError = "contradicts the canonical fact"
					}
					install := []string{"CREATE TRIGGER selection_cut AFTER INSERT ON event_delivery_handler_rule_selections BEGIN " + body + " END"}
					remove := []string{"DROP TRIGGER selection_cut"}
					if backend.name == "postgres" {
						install = []string{"CREATE FUNCTION selection_cut_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " + pgBody + " RETURN NEW; END $$", "CREATE TRIGGER selection_cut BEFORE INSERT ON event_delivery_handler_rule_selections FOR EACH ROW EXECUTE FUNCTION selection_cut_fn()"}
						remove = []string{"DROP TRIGGER selection_cut ON event_delivery_handler_rule_selections", "DROP FUNCTION selection_cut_fn()"}
					}
					for _, sql := range install {
						if _, err := fixture.db.Exec(sql); err != nil {
							t.Fatal(err)
						}
					}
					_, settleErr := selected.SettleSuccess(ctx, claimed.Claim, []string{"must-rollback"}, 0, fact)
					for _, sql := range remove {
						if _, err := fixture.db.Exec(sql); err != nil {
							t.Fatal(err)
						}
					}
					if settleErr == nil || !strings.Contains(settleErr.Error(), wantError) {
						t.Fatalf("wrong fault boundary: %v, want %s", settleErr, wantError)
					}
					after, err := selected.Snapshot(ctx, id)
					if err != nil || after.Status != runtimedelivery.StatusInProgress || after.ClaimVersion != claimed.Claim.Version() || after.FinalSelection.Present() || !after.ClaimExpiresAt.Equal(claimed.Snapshot.ClaimExpiresAt) {
						t.Fatalf("admission failure lost recoverable exact claim: %+v %v", after, err)
					}
					outcomes, err := selected.Outcomes(ctx, id)
					if err != nil || len(outcomes) != 0 || routeEvidenceHead(t, ctx, fixture.db, runID) != head {
						t.Fatalf("admission failure leaked outcome or historical revision: %+v %v", outcomes, err)
					}
					settled, err := selected.SettleSuccess(ctx, claimed.Claim, []string{"once"}, 0, fact)
					if err != nil || settled.Status != runtimedelivery.StatusDelivered {
						t.Fatalf("unchanged exact-claim retry failed: %+v %v", settled, err)
					}
					final, err := settled.FinalSelection.Fact()
					if err != nil || !final.Equal(fact) {
						t.Fatalf("final selection changed: %+v %v", final, err)
					}
					outcomes, err = selected.Outcomes(ctx, id)
					if err != nil || len(outcomes) != 1 || len(outcomes[0].SideEffects) != 1 || outcomes[0].SideEffects[0] != "once" {
						t.Fatalf("rollback/retry duplicated outcome: %+v %v", outcomes, err)
					}
				})
			}
		})
	}
}
