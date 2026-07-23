package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type deliveryLifecycleConformanceBackend struct {
	name     string
	store    runtimedelivery.Store
	restart  runtimedelivery.Store
	selected any
	db       *sql.DB
	postgres bool
}

func TestExecutableDeliveryLifecycleParity(t *testing.T) {
	for _, backend := range deliveryLifecycleConformanceBackends(t) {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			t.Run("exact_route_claim_settlement_and_outcome", func(t *testing.T) {
				event := deliveryLifecycleEvent("exact-" + backend.name)
				agent := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "agent-a"}
				sibling := events.DeliveryRoute{
					SubscriberType: "agent",
					SubscriberID:   "agent-a",
					Target:         events.RouteIdentity{FlowID: "flow-a"},
				}
				node := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "node-a"}
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{agent, sibling, node}, runtimereplayclaim.CommittedReplayScopeSubscribed)

				agentProof, err := backend.store.ProveHandoff(ctx, event.ID(), agent)
				if err != nil {
					t.Fatalf("prove agent handoff: %v", err)
				}
				siblingProof, err := backend.store.ProveHandoff(ctx, event.ID(), sibling)
				if err != nil {
					t.Fatalf("prove sibling handoff: %v", err)
				}
				if agentProof.DeliveryID() == siblingProof.DeliveryID() {
					t.Fatal("distinct exact routes collapsed to one delivery obligation")
				}

				claimed, err := backend.store.ClaimAgentDelivery(ctx, event, agent)
				if err != nil {
					t.Fatalf("claim agent delivery: %v", err)
				}
				if claimed.Snapshot.Status != runtimedelivery.StatusInProgress || claimed.Snapshot.MaxRetries != runtimedelivery.AgentMaxRetries {
					t.Fatalf("claimed agent snapshot = %#v", claimed.Snapshot)
				}
				if _, err := backend.store.ClaimAgentDelivery(ctx, event, agent); !errors.Is(err, runtimedelivery.ErrIneligible) {
					t.Fatalf("second live claim error = %v, want ErrIneligible", err)
				}
				sessionID := uuid.NewString()
				seedDeliveryAgentSession(t, ctx, backend, sessionID, event.RunID(), agent.SubscriberID)
				bound, err := backend.store.BindAgentSession(ctx, claimed.Claim, sessionID)
				if err != nil {
					t.Fatalf("bind agent session: %v", err)
				}
				if bound.ActiveSessionID != sessionID {
					t.Fatalf("bound session = %q, want %q", bound.ActiveSessionID, sessionID)
				}
				settled, err := backend.store.SettleSuccess(ctx, claimed.Claim, []string{"message.sent"}, 25*time.Millisecond)
				if err != nil {
					t.Fatalf("settle agent success: %v", err)
				}
				if settled.Status != runtimedelivery.StatusDelivered || !settled.Terminal() {
					t.Fatalf("settled status = %q", settled.Status)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("stale settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, agentProof.DeliveryID())
				if err != nil {
					t.Fatalf("read exact outcomes: %v", err)
				}
				if len(outcomes) != 1 || outcomes[0].Outcome != "delivered" || outcomes[0].ClaimVersion != claimed.Claim.Version() || len(outcomes[0].SideEffects) != 1 || outcomes[0].SideEffects[0] != "message.sent" {
					t.Fatalf("agent outcomes = %#v", outcomes)
				}

				// Exact duplicate event admission cannot mint, replace, or reset obligations.
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{agent, sibling, node}, runtimereplayclaim.CommittedReplayScopeSubscribed)
				afterDuplicate, err := backend.store.Snapshot(ctx, agentProof.DeliveryID())
				if err != nil {
					t.Fatalf("snapshot after exact duplicate: %v", err)
				}
				if afterDuplicate.Status != runtimedelivery.StatusDelivered || afterDuplicate.ClaimVersion != claimed.Claim.Version() {
					t.Fatalf("exact duplicate changed lifecycle: %#v", afterDuplicate)
				}
			})

			t.Run("class_retry_budgets_are_structural", func(t *testing.T) {
				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberAgent, "retry-agent", runtimedelivery.AgentMaxRetries)
				assertDeliveryRetryBudget(t, ctx, backend, runtimedelivery.SubscriberNode, "retry-node", runtimedelivery.NodeMaxRetries)
			})

			t.Run("claim_renewal_fences_reclaim_and_preserves_settlement", func(t *testing.T) {
				event := deliveryLifecycleEvent("claim-renewal-" + backend.name)
				route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "renewal-agent"}
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
				claimed, err := backend.store.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatalf("claim delivery: %v", err)
				}
				agedAt := ageDeliveryClaimForConformance(t, ctx, backend, claimed.Snapshot.DeliveryID)
				renewed, err := backend.store.RenewClaim(ctx, claimed.Claim)
				if err != nil {
					t.Fatalf("renew claim: %v", err)
				}
				if renewed.ClaimVersion != claimed.Claim.Version() || renewed.Status != runtimedelivery.StatusInProgress || renewed.ClaimExpiresAt.Before(claimed.Snapshot.ClaimExpiresAt) || !renewed.UpdatedAt.After(agedAt) {
					t.Fatalf("renewed claim = %#v, original = %#v", renewed, claimed.Snapshot)
				}
				if lease := renewed.ClaimExpiresAt.Sub(renewed.UpdatedAt); lease != runtimedelivery.DefaultLeaseTTL {
					t.Fatalf("renewed lease window = %s, want %s from exact database renewal time", lease, runtimedelivery.DefaultLeaseTTL)
				}
				assertDeliveryAttemptLeaseMatchesObligation(t, ctx, backend, renewed.DeliveryID, renewed.ClaimVersion)
				if _, err := backend.restart.ClaimAgentDelivery(ctx, event, route); !errors.Is(err, runtimedelivery.ErrIneligible) {
					t.Fatalf("claim before renewed expiry = %v, want ErrIneligible", err)
				}

				expireDeliveryClaimForConformance(t, ctx, backend, renewed.DeliveryID)
				if _, err := backend.store.RenewClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("expired claim renewal = %v, want ErrConflict", err)
				}
				reclaimed, err := backend.restart.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatalf("reclaim after renewed lease expiry: %v", err)
				}
				if reclaimed.Claim.Version() != claimed.Claim.Version()+1 {
					t.Fatalf("reclaimed version = %d, want %d", reclaimed.Claim.Version(), claimed.Claim.Version()+1)
				}
				if _, err := backend.store.RenewClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("superseded claim renewal = %v, want ErrConflict", err)
				}
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"renewal.proven"}, time.Millisecond)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("settle renewed lifecycle = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("parent_terminalization_fences_late_writer", func(t *testing.T) {
				event := deliveryLifecycleEvent("terminalize-" + backend.name)
				route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "terminal-agent"}
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
				claimed, err := backend.store.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatal(err)
				}
				removeFault := installDeliveryDeadLetterFault(t, ctx, backend)
				if _, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal"); err == nil {
					t.Fatal("run terminalization succeeded while required diagnostic writer was faulted")
				}
				assertDeliverySettlementRolledBack(t, ctx, backend, claimed)
				removeFault()
				transitions, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal")
				if err != nil {
					t.Fatalf("terminalize run: %v", err)
				}
				if len(transitions) != 1 || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {
					t.Fatalf("terminalizations = %#v", transitions)
				}
				current := transitions[0].Current
				if current.ClaimVersion != claimed.Claim.Version()+1 || current.Failure == nil || current.Failure.Detail.Code != "delivery_parent_terminalized" {
					t.Fatalf("terminalized snapshot = %#v, want new exact fence and typed parent failure", current)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("late settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, transitions[0].Current.DeliveryID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "terminalized" || outcomes[0].ClaimVersion != current.ClaimVersion || outcomes[0].Failure == nil {
					t.Fatalf("terminalization outcomes = %#v, err=%v", outcomes, err)
				}
				assertExactDeliveryDeadLetter(t, ctx, backend, event, current)
			})

			t.Run("concurrent_claim_and_restart_reclaim_are_fenced", func(t *testing.T) {
				event := deliveryLifecycleEvent("claim-race-" + backend.name)
				route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "race-agent"}
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)

				type claimResult struct {
					claimed runtimedelivery.ClaimedObligation
					err     error
				}
				const contenders = 8
				start := make(chan struct{})
				results := make(chan claimResult, contenders)
				for index := 0; index < contenders; index++ {
					claimStore := backend.store
					if index%2 == 1 {
						claimStore = backend.restart
					}
					go func() {
						<-start
						claimed, err := claimStore.ClaimAgentDelivery(ctx, event, route)
						results <- claimResult{claimed: claimed, err: err}
					}()
				}
				close(start)

				var winner runtimedelivery.ClaimedObligation
				wins := 0
				for index := 0; index < contenders; index++ {
					result := <-results
					if result.err == nil {
						winner = result.claimed
						wins++
						continue
					}
					if !errors.Is(result.err, runtimedelivery.ErrIneligible) && !errors.Is(result.err, runtimedelivery.ErrConflict) {
						t.Fatalf("claim race loser error = %v, want typed ineligible/conflict", result.err)
					}
				}
				if wins != 1 {
					t.Fatalf("claim race winners = %d, want exactly one", wins)
				}
				if winner.Claim.Version() != 1 || winner.Snapshot.Status != runtimedelivery.StatusInProgress {
					t.Fatalf("initial winning claim = %#v", winner)
				}

				if _, err := backend.restart.ClaimAgentDelivery(ctx, event, route); !errors.Is(err, runtimedelivery.ErrIneligible) {
					t.Fatalf("pre-expiry reconstructed-store claim error = %v, want ErrIneligible", err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, winner.Snapshot.DeliveryID)
				reclaimed, err := backend.restart.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatalf("post-expiry reconstructed-store reclaim: %v", err)
				}
				if reclaimed.Claim.Version() != winner.Claim.Version()+1 || reclaimed.Snapshot.DeliveryID != winner.Snapshot.DeliveryID {
					t.Fatalf("reclaimed delivery = %#v, first = %#v", reclaimed, winner)
				}
				if _, err := backend.store.SettleSuccess(ctx, winner.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("expired claimant settlement error = %v, want ErrConflict", err)
				}
				settled, err := backend.restart.SettleSuccess(ctx, reclaimed.Claim, []string{"race.proven"}, time.Millisecond)
				if err != nil || settled.Status != runtimedelivery.StatusDelivered {
					t.Fatalf("current claimant settlement = %#v, err=%v", settled, err)
				}
				assertDeliveryAttemptHistory(t, ctx, backend, settled.DeliveryID)
			})

			t.Run("fresh_schema_rejects_disconnected_lifecycle_facts", func(t *testing.T) {
				assertDeliverySchemaRejectsDisconnectedFacts(t, ctx, backend)
			})

			t.Run("expired_claim_precedes_continuous_pending_backlog", func(t *testing.T) {
				agentID := "selector-agent-" + backend.name
				expiredEvent := deliveryLifecycleEvent("selector-expired-" + backend.name)
				route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: agentID}
				storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, expiredEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
				claimed, err := backend.store.ClaimAgentDelivery(ctx, expiredEvent, route)
				if err != nil {
					t.Fatalf("claim selector expiry candidate: %v", err)
				}
				expireDeliveryClaimForConformance(t, ctx, backend, claimed.Snapshot.DeliveryID)
				for index := 0; index < 12; index++ {
					pending := deliveryLifecycleEvent(fmt.Sprintf("selector-pending-%s-%02d", backend.name, index))
					storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, pending, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
				}
				backlog, err := backend.restart.ClaimAgentBacklog(ctx, agentID, 1)
				if err != nil {
					t.Fatalf("claim saturated backlog: %v", err)
				}
				if len(backlog) != 1 || backlog[0].Snapshot.DeliveryID != claimed.Snapshot.DeliveryID || backlog[0].Claim.Version() != claimed.Claim.Version()+1 {
					t.Fatalf("first saturated claim = %#v, want expired delivery %s version %d", backlog, claimed.Snapshot.DeliveryID, claimed.Claim.Version()+1)
				}
			})

			t.Run("terminal_settlement_commits_exact_diagnostic_atomically", func(t *testing.T) {
				for _, class := range []runtimedelivery.SubscriberClass{runtimedelivery.SubscriberAgent, runtimedelivery.SubscriberNode} {
					class := class
					t.Run(string(class), func(t *testing.T) {
						event := deliveryLifecycleEvent(fmt.Sprintf("atomic-diagnostic-%s-%s", backend.name, class))
						route := events.DeliveryRoute{SubscriberType: string(class), SubscriberID: "diagnostic-" + string(class)}
						storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
						var claimed runtimedelivery.ClaimedObligation
						var err error
						if class == runtimedelivery.SubscriberAgent {
							claimed, err = backend.store.ClaimAgentDelivery(ctx, event, route)
						} else {
							claimed, err = backend.store.ClaimNodeDelivery(ctx, event, route)
						}
						if err != nil {
							t.Fatalf("claim %s diagnostic delivery: %v", class, err)
						}
						removeFault := installDeliveryDeadLetterFault(t, ctx, backend)
						settlement := runtimedelivery.Settlement{
							Disposition: runtimedelivery.FailureDeadLetter,
							ReasonCode:  "terminal_test_failure",
							Failure:     testFailure("terminal_test_failure"),
						}
						if _, err := backend.store.SettleFailure(ctx, claimed.Claim, settlement); err == nil {
							t.Fatal("terminal settlement succeeded while required diagnostic writer was faulted")
						}
						assertDeliverySettlementRolledBack(t, ctx, backend, claimed)
						removeFault()
						settled, err := backend.store.SettleFailure(ctx, claimed.Claim, settlement)
						if err != nil {
							t.Fatalf("settle %s diagnostic delivery: %v", class, err)
						}
						if settled.Failure == nil || settled.Failure.Class != settlement.Failure.Class || settled.Failure.Detail.Code != settlement.Failure.Detail.Code || settled.RetryCount != 0 {
							t.Fatalf("direct terminal settlement changed original failure or retry count: %#v", settled)
						}
						assertExactDeliveryDeadLetter(t, ctx, backend, event, settled)
					})
				}
			})
		})
	}
}

func installDeliveryDeadLetterFault(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend) func() {
	t.Helper()
	if backend.postgres {
		if _, err := backend.db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_delivery_dead_letter_insert() RETURNS trigger AS $$ BEGIN IF NEW.delivery_id IS NOT NULL THEN RAISE EXCEPTION 'forced delivery diagnostic failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql`); err != nil {
			t.Fatalf("create delivery diagnostic fault function: %v", err)
		}
		if _, err := backend.db.ExecContext(ctx, `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters FOR EACH ROW EXECUTE FUNCTION fail_delivery_dead_letter_insert()`); err != nil {
			t.Fatalf("create delivery diagnostic fault trigger: %v", err)
		}
		cleanup := func() {
			if _, err := backend.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_delivery_dead_letter_insert ON dead_letters`); err != nil {
				t.Fatalf("drop delivery diagnostic fault trigger: %v", err)
			}
			if _, err := backend.db.ExecContext(ctx, `DROP FUNCTION IF EXISTS fail_delivery_dead_letter_insert()`); err != nil {
				t.Fatalf("drop delivery diagnostic fault function: %v", err)
			}
		}
		t.Cleanup(cleanup)
		return cleanup
	}
	if _, err := backend.db.ExecContext(ctx, `CREATE TRIGGER fail_delivery_dead_letter_insert BEFORE INSERT ON dead_letters WHEN NEW.delivery_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'forced delivery diagnostic failure'); END`); err != nil {
		t.Fatalf("create sqlite delivery diagnostic fault trigger: %v", err)
	}
	cleanup := func() {
		if _, err := backend.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fail_delivery_dead_letter_insert`); err != nil {
			t.Fatalf("drop sqlite delivery diagnostic fault trigger: %v", err)
		}
	}
	t.Cleanup(cleanup)
	return cleanup
}

func assertDeliverySettlementRolledBack(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, claimed runtimedelivery.ClaimedObligation) {
	t.Helper()
	snapshot, err := backend.store.Snapshot(ctx, claimed.Snapshot.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimVersion != claimed.Claim.Version() {
		t.Fatalf("faulted settlement snapshot = %#v, want original in-progress claim", snapshot)
	}
	query := `SELECT (SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1::uuid), (SELECT COUNT(*) FROM dead_letters WHERE delivery_id=$1::uuid), (SELECT COUNT(*) FROM author_activity_occurrences WHERE source_identity=$1::text AND transition IN ('dead_letter', 'terminalized'))`
	if !backend.postgres {
		query = `SELECT (SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=?), (SELECT COUNT(*) FROM dead_letters WHERE delivery_id=?), (SELECT COUNT(*) FROM author_activity_occurrences WHERE source_identity=? AND transition IN ('dead_letter', 'terminalized'))`
	}
	args := []any{claimed.Snapshot.DeliveryID}
	if !backend.postgres {
		args = []any{claimed.Snapshot.DeliveryID, claimed.Snapshot.DeliveryID, claimed.Snapshot.DeliveryID}
	}
	var outcomes, diagnostics, transitions int
	if err := backend.db.QueryRowContext(ctx, query, args...).Scan(&outcomes, &diagnostics, &transitions); err != nil {
		t.Fatalf("read faulted settlement evidence: %v", err)
	}
	if outcomes != 0 || diagnostics != 0 || transitions != 0 {
		t.Fatalf("faulted settlement evidence outcomes=%d diagnostics=%d transitions=%d, want all zero", outcomes, diagnostics, transitions)
	}
}

func assertExactDeliveryDeadLetter(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, event events.Event, settled runtimedelivery.Snapshot) {
	t.Helper()
	query := `SELECT delivery_id::text, claim_version, original_event, original_payload, failure, retry_count, chain_depth, handler_node FROM dead_letters WHERE delivery_id=$1::uuid AND claim_version=$2`
	if !backend.postgres {
		query = `SELECT delivery_id, claim_version, original_event, original_payload, failure, retry_count, chain_depth, handler_node FROM dead_letters WHERE delivery_id=? AND claim_version=?`
	}
	var deliveryID, eventType, handler string
	var claimVersion int64
	var payload, failureRaw []byte
	var retryCount, chainDepth int
	if err := backend.db.QueryRowContext(ctx, query, settled.DeliveryID, settled.ClaimVersion).Scan(&deliveryID, &claimVersion, &eventType, &payload, &failureRaw, &retryCount, &chainDepth, &handler); err != nil {
		t.Fatalf("read exact terminal delivery diagnostic: %v", err)
	}
	deadLetterFailure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
	if err != nil {
		t.Fatalf("decode exact terminal delivery failure: %v", err)
	}
	var gotPayload, wantPayload any
	if err := json.Unmarshal(payload, &gotPayload); err != nil {
		t.Fatalf("decode terminal diagnostic payload: %v", err)
	}
	if err := json.Unmarshal(event.Payload(), &wantPayload); err != nil {
		t.Fatal(err)
	}
	if deliveryID != settled.DeliveryID || claimVersion != settled.ClaimVersion || eventType != string(event.Type()) ||
		!reflect.DeepEqual(gotPayload, wantPayload) || settled.Failure == nil || !reflect.DeepEqual(deadLetterFailure, *settled.Failure) || retryCount != settled.RetryCount || chainDepth != event.ChainDepth() || handler != settled.SubscriberID {
		t.Fatalf("terminal diagnostic = delivery:%s version:%d type:%s payload:%v retry:%d depth:%d handler:%s; want exact settled/event facts", deliveryID, claimVersion, eventType, gotPayload, retryCount, chainDepth, handler)
	}
	attemptQuery := `SELECT failure FROM event_delivery_attempts WHERE delivery_id=$1::uuid AND claim_version=$2`
	activityQuery := `SELECT failure FROM author_activity_occurrences WHERE source_owner='event_deliveries' AND source_identity=$1 ORDER BY sequence DESC LIMIT 1`
	if !backend.postgres {
		attemptQuery = `SELECT failure FROM event_delivery_attempts WHERE delivery_id=? AND claim_version=?`
		activityQuery = `SELECT failure FROM author_activity_occurrences WHERE source_owner='event_deliveries' AND source_identity=? ORDER BY sequence DESC LIMIT 1`
	}
	for owner, queryAndArgs := range map[string]struct {
		query string
		args  []any
	}{
		"attempt":         {query: attemptQuery, args: []any{settled.DeliveryID, settled.ClaimVersion}},
		"author activity": {query: activityQuery, args: []any{settled.DeliveryID}},
	} {
		var raw []byte
		if err := backend.db.QueryRowContext(ctx, queryAndArgs.query, queryAndArgs.args...).Scan(&raw); err != nil {
			t.Fatalf("read exact terminal %s failure: %v", owner, err)
		}
		persisted, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil || !reflect.DeepEqual(persisted, *settled.Failure) {
			t.Fatalf("terminal %s failure = %#v, err=%v; want %#v", owner, persisted, err, *settled.Failure)
		}
	}
}

func assertDeliverySchemaRejectsDisconnectedFacts(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend) {
	t.Helper()
	route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "schema-agent-" + backend.name}
	event := deliveryLifecycleEvent("schema-event-" + backend.name)
	other := deliveryLifecycleEvent("schema-other-run-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, other, nil, runtimereplayclaim.CommittedReplayScopeDirect)
	proof, err := backend.store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "event/run mismatch",
		`UPDATE event_deliveries SET run_id = $1::uuid WHERE delivery_id = $2::uuid`,
		`UPDATE event_deliveries SET run_id = ? WHERE delivery_id = ?`,
		[]any{other.RunID(), proof.DeliveryID()})

	missingAttemptEvent := deliveryLifecycleEvent("schema-missing-attempt-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, missingAttemptEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	missingAttempt, err := backend.store.ProveHandoff(ctx, missingAttemptEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	assertDeliverySQLRejected(t, backend, "in-progress without open attempt",
		`UPDATE event_deliveries SET status='in_progress', next_eligible_at=NULL, claim_version=1, current_attempt_version=1, current_attempt_open=TRUE, started_at=$1, updated_at=$1 WHERE delivery_id=$2::uuid`,
		`UPDATE event_deliveries SET status='in_progress', next_eligible_at=NULL, claim_version=1, current_attempt_version=1, current_attempt_open=TRUE, started_at=?, updated_at=? WHERE delivery_id=?`,
		[]any{now, missingAttempt.DeliveryID()})

	sessionEvent := deliveryLifecycleEvent("schema-session-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, sessionEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	claimed, err := backend.store.ClaimAgentDelivery(ctx, sessionEvent, route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "nonexistent exact agent session",
		`UPDATE event_delivery_attempts SET active_session_id=$1::uuid, session_run_id=$2::uuid, session_agent_id=$3 WHERE delivery_id=$4::uuid AND claim_version=$5`,
		`UPDATE event_delivery_attempts SET active_session_id=?, session_run_id=?, session_agent_id=? WHERE delivery_id=? AND claim_version=?`,
		[]any{uuid.NewString(), sessionEvent.RunID(), route.SubscriberID, claimed.Snapshot.DeliveryID, claimed.Claim.Version()})

	otherSessionEvent := deliveryLifecycleEvent("schema-session-other-owner-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, otherSessionEvent, nil, runtimereplayclaim.CommittedReplayScopeDirect)
	otherSessionID := uuid.NewString()
	otherAgentID := "schema-other-agent-" + backend.name
	seedDeliveryAgentSession(t, ctx, backend, otherSessionID, otherSessionEvent.RunID(), otherAgentID)
	assertDeliverySQLRejected(t, backend, "session owned by another delivery run and agent",
		`UPDATE event_delivery_attempts SET active_session_id=$1::uuid, session_delivery_id=$2::uuid, session_run_id=$3::uuid, session_subscriber_type='agent', session_agent_id=$4 WHERE delivery_id=$2::uuid AND claim_version=$5`,
		`UPDATE event_delivery_attempts SET active_session_id=?1, session_delivery_id=?2, session_run_id=?3, session_subscriber_type='agent', session_agent_id=?4 WHERE delivery_id=?2 AND claim_version=?5`,
		[]any{otherSessionID, claimed.Snapshot.DeliveryID, otherSessionEvent.RunID(), otherAgentID, claimed.Claim.Version()})
	if _, err := backend.store.BindAgentSession(ctx, claimed.Claim, otherSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind session owned by another delivery error = %v, want ErrConflict", err)
	}

	assertDeliverySQLRejected(t, backend, "unreferenced second open attempt",
		`INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $1::uuid, TRUE)`,
		`INSERT INTO event_delivery_attempts (delivery_id, claim_version, claim_token, started_at, lease_expires_at, current_delivery_id, open_marker) VALUES (?1, ?2, ?3, ?4, ?5, ?1, TRUE)`,
		[]any{claimed.Snapshot.DeliveryID, claimed.Claim.Version() + 1, uuid.NewString(), now, now.Add(time.Minute)})

	terminatedRoute := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "terminated-session-agent-" + backend.name}
	terminatedEvent := deliveryLifecycleEvent("schema-terminated-session-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, terminatedEvent, []events.DeliveryRoute{terminatedRoute}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	terminatedClaim, err := backend.store.ClaimAgentDelivery(ctx, terminatedEvent, terminatedRoute)
	if err != nil {
		t.Fatal(err)
	}
	terminatedSessionID := uuid.NewString()
	seedDeliveryAgentSession(t, ctx, backend, terminatedSessionID, terminatedEvent.RunID(), terminatedRoute.SubscriberID)
	terminateSessionQuery := `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=$2, updated_at=$2 WHERE session_id=$1::uuid`
	if !backend.postgres {
		terminateSessionQuery = `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=?, updated_at=? WHERE session_id=?`
	}
	var terminateErr error
	if backend.postgres {
		_, terminateErr = backend.db.ExecContext(ctx, terminateSessionQuery, terminatedSessionID, now)
	} else {
		_, terminateErr = backend.db.ExecContext(ctx, terminateSessionQuery, now, now, terminatedSessionID)
	}
	if terminateErr != nil {
		t.Fatalf("terminate exact delivery session: %v", terminateErr)
	}
	if _, err := backend.store.BindAgentSession(ctx, terminatedClaim.Claim, terminatedSessionID); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("bind terminated exact session error = %v, want ErrConflict", err)
	}

	outcomeEvent := deliveryLifecycleEvent("schema-outcome-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, outcomeEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	outcomeProof, err := backend.store.ProveHandoff(ctx, outcomeEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "outcome without exact attempt",
		`INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, side_effects, duration_ms, settled_at) VALUES ($1::uuid, 99, 'delivered', '[]'::jsonb, 0, $2)`,
		`INSERT INTO event_delivery_outcomes (delivery_id, claim_version, outcome, side_effects, duration_ms, settled_at) VALUES (?, 99, 'delivered', '[]', 0, ?)`,
		[]any{outcomeProof.DeliveryID(), now})

	deadLetterEvent := deliveryLifecycleEvent("schema-dead-letter-failure-" + backend.name)
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, deadLetterEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	deadLetterProof, err := backend.store.ProveHandoff(ctx, deadLetterEvent.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliverySQLRejected(t, backend, "dead-letter without typed failure",
		`UPDATE event_deliveries SET status='dead_letter', next_eligible_at=NULL, reason_code='raw_terminal', settled_at=created_at, updated_at=created_at WHERE delivery_id=$1::uuid`,
		`UPDATE event_deliveries SET status='dead_letter', next_eligible_at=NULL, reason_code='raw_terminal', settled_at=created_at, updated_at=created_at WHERE delivery_id=?`,
		[]any{deadLetterProof.DeliveryID()})
}

func assertDeliverySQLRejected(t *testing.T, backend deliveryLifecycleConformanceBackend, name, postgresQuery, sqliteQuery string, args []any) {
	t.Helper()
	query := postgresQuery
	if !backend.postgres {
		query = sqliteQuery
		if name == "in-progress without open attempt" {
			args = []any{args[0], args[0], args[1]}
		}
	}
	if _, err := backend.db.Exec(query, args...); err == nil {
		t.Fatalf("%s mutation succeeded; fresh schema must reject it", name)
	}
}

func assertDeliveryRetryBudget(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, class runtimedelivery.SubscriberClass, subscriberID string, maxRetries int) {
	t.Helper()
	event := deliveryLifecycleEvent(fmt.Sprintf("retry-%s-%s", backend.name, class))
	route := events.DeliveryRoute{SubscriberType: string(class), SubscriberID: subscriberID}
	storetest.CommitSemanticEventWithRoutes(t, ctx, backend.selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	proof, err := backend.store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	var exhausted runtimedelivery.Snapshot
	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		var claimed runtimedelivery.ClaimedObligation
		if class == runtimedelivery.SubscriberAgent {
			claimed, err = backend.store.ClaimAgentDelivery(ctx, event, route)
		} else {
			claimed, err = backend.store.ClaimNodeDelivery(ctx, event, route)
		}
		if err != nil {
			t.Fatalf("claim %s attempt %d: %v", class, attempt, err)
		}
		settled, settleErr := backend.store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     testFailure("handler_failed"),
			Duration:    time.Duration(attempt) * time.Millisecond,
			RetryBase:   time.Nanosecond,
		})
		if settleErr != nil {
			t.Fatalf("settle %s attempt %d: %v", class, attempt, settleErr)
		}
		if attempt <= maxRetries {
			if settled.Status != runtimedelivery.StatusFailed || settled.RetryCount != attempt {
				t.Fatalf("retry %d snapshot = %#v", attempt, settled)
			}
			makeDeliveryImmediatelyEligible(t, ctx, backend, proof.DeliveryID())
		} else {
			if settled.Status != runtimedelivery.StatusDeadLetter || settled.RetryCount != maxRetries || settled.ReasonCode != "retry_exhausted" {
				t.Fatalf("exhausted snapshot = %#v", settled)
			}
			exhausted = settled
			assertRetryExhaustedFailure(t, settled.Failure, maxRetries)
			assertExactDeliveryDeadLetter(t, ctx, backend, event, settled)
		}
	}
	outcomes, err := backend.store.Outcomes(ctx, proof.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != maxRetries+1 {
		t.Fatalf("outcome count = %d, want %d", len(outcomes), maxRetries+1)
	}
	for index, outcome := range outcomes {
		want := "retry_scheduled"
		if index == maxRetries {
			want = "dead_letter"
		}
		if outcome.ClaimVersion != int64(index+1) || outcome.Outcome != want {
			t.Fatalf("outcome %d = %#v, want version=%d outcome=%s", index, outcome, index+1, want)
		}
		if index < maxRetries {
			if outcome.Failure == nil || outcome.Failure.Class != runtimefailures.ClassConnectorFailure || outcome.Failure.Detail.Code != "handler_failed" {
				t.Fatalf("retry outcome %d failure = %#v, want original failure", index, outcome.Failure)
			}
		} else if exhausted.Failure == nil || outcome.Failure == nil || !reflect.DeepEqual(*outcome.Failure, *exhausted.Failure) {
			t.Fatalf("terminal outcome failure = %#v, want synthesized exhausted failure %#v", outcome.Failure, exhausted.Failure)
		}
	}
}

func assertRetryExhaustedFailure(t *testing.T, failure *runtimefailures.Envelope, maxRetries int) {
	t.Helper()
	if failure == nil || failure.Class != runtimefailures.ClassRetryExhausted || failure.Detail.Code != "delivery_retry_exhausted" || failure.Retryable || !failure.Deterministic {
		t.Fatalf("retry-exhausted failure = %#v", failure)
	}
	raw, err := json.Marshal(failure.Detail.Attributes)
	if err != nil {
		t.Fatal(err)
	}
	var evidence struct {
		MaxRetries   int `json:"max_retries"`
		RetryHistory []struct {
			ClaimVersion int                      `json:"claim_version"`
			Failure      runtimefailures.Envelope `json:"failure"`
		} `json:"retry_history"`
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatalf("decode retry-exhausted evidence: %v", err)
	}
	if evidence.MaxRetries != maxRetries || len(evidence.RetryHistory) != maxRetries+1 {
		t.Fatalf("retry-exhausted evidence = %#v, want max=%d attempts=%d", evidence, maxRetries, maxRetries+1)
	}
	for index, attempt := range evidence.RetryHistory {
		if attempt.ClaimVersion != index+1 || attempt.Failure.Class != runtimefailures.ClassConnectorFailure || attempt.Failure.Detail.Code != "handler_failed" {
			t.Fatalf("retry history attempt %d = %#v", index, attempt)
		}
	}
}

func makeDeliveryImmediatelyEligible(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	query := `UPDATE event_deliveries SET next_eligible_at = $1 WHERE delivery_id = $2::uuid AND status = 'failed'`
	if !backend.postgres {
		query = `UPDATE event_deliveries SET next_eligible_at = ? WHERE delivery_id = ? AND status = 'failed'`
	}
	result, err := backend.db.ExecContext(ctx, query, time.Now().Add(-time.Hour).UTC(), deliveryID)
	if err != nil {
		t.Fatalf("make retry eligible: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("make retry eligible affected %d rows, err=%v", rows, err)
	}
}

func ageDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) time.Time {
	t.Helper()
	agedAt := time.Now().Add(-15 * time.Minute).UTC()
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin aged-claim proof: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $1 WHERE delivery_id = $2::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1 WHERE delivery_id = $2::uuid AND open_marker = TRUE`
	if !backend.postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ? WHERE delivery_id = ? AND open_marker = TRUE`
	}
	var deliveryResult sql.Result
	if backend.postgres {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, agedAt, deliveryID)
	} else {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, agedAt, agedAt, agedAt, deliveryID)
	}
	if err != nil {
		t.Fatalf("age delivery lifecycle timestamp: %v", err)
	}
	if rows, rowsErr := deliveryResult.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age delivery lifecycle affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, agedAt, deliveryID); execErr != nil {
		t.Fatalf("age delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("age delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit aged-claim proof: %v", err)
	}
	return agedAt
}

func expireDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin claim-expiry proof: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND open_marker = TRUE`
	if !backend.postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND open_marker = TRUE`
	}
	var deliveryResult sql.Result
	if backend.postgres {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, expiresAt, deliveryID)
	} else {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, startedAt, expiresAt, deliveryID)
	}
	if err != nil {
		t.Fatalf("expire delivery claim: %v", err)
	}
	if rows, rowsErr := deliveryResult.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire delivery claim affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, startedAt, expiresAt, deliveryID); execErr != nil {
		t.Fatalf("expire delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit claim-expiry proof: %v", err)
	}
}

func assertDeliveryAttemptHistory(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	query := `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = $1::uuid ORDER BY claim_version`
	if !backend.postgres {
		query = `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = ? ORDER BY claim_version`
	}
	rows, err := backend.db.QueryContext(ctx, query, deliveryID)
	if err != nil {
		t.Fatalf("load delivery attempt history: %v", err)
	}
	defer rows.Close()
	type attempt struct {
		version int64
		outcome string
	}
	var attempts []attempt
	for rows.Next() {
		var current attempt
		if err := rows.Scan(&current.version, &current.outcome); err != nil {
			t.Fatalf("scan delivery attempt history: %v", err)
		}
		attempts = append(attempts, current)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read delivery attempt history: %v", err)
	}
	if len(attempts) != 2 || attempts[0] != (attempt{version: 1, outcome: "lease_expired"}) || attempts[1] != (attempt{version: 2, outcome: "delivered"}) {
		t.Fatalf("delivery attempt history = %#v", attempts)
	}
}

func assertDeliveryAttemptLeaseMatchesObligation(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string, version int64) {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id AND d.current_attempt_version = a.claim_version AND d.current_attempt_open = TRUE WHERE a.delivery_id = $1::uuid AND a.claim_version = $2 AND a.open_marker = TRUE`
	if !backend.postgres {
		query = `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id AND d.current_attempt_version = a.claim_version AND d.current_attempt_open = TRUE WHERE a.delivery_id = ? AND a.claim_version = ? AND a.open_marker = TRUE`
	}
	var matches int
	if err := backend.db.QueryRowContext(ctx, query, deliveryID, version).Scan(&matches); err != nil {
		t.Fatalf("compare renewed delivery attempt lease: %v", err)
	}
	if matches != 1 {
		t.Fatalf("matching renewed attempt and obligation leases = %d, want 1", matches)
	}
}

func deliveryLifecycleEvent(label string) events.Event {
	return eventtest.RunCreatingRootIngress(
		eventtest.UUID(label),
		events.EventType("delivery.conformance"),
		"conformance-ingress",
		"",
		json.RawMessage(`{"ok":true}`),
		0,
		eventtest.UUID(label+"-run"),
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID(label+"-entity")),
		time.Now().UTC(),
	)
}

func seedDeliveryAgentSession(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, sessionID, runID, agentID string) {
	t.Helper()
	agentQuery := `INSERT INTO agents (agent_id, role, model) VALUES ($1, 'delivery-test', 'test') ON CONFLICT (agent_id) DO NOTHING`
	sessionQuery := `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance) VALUES ($1::uuid, $2::uuid, $3, $4)`
	if !backend.postgres {
		agentQuery = `INSERT OR IGNORE INTO agents (agent_id, role, model) VALUES (?, 'delivery-test', 'test')`
		sessionQuery = `INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance) VALUES (?, ?, ?, ?)`
	}
	if _, err := backend.db.ExecContext(ctx, agentQuery, agentID); err != nil {
		t.Fatalf("seed delivery agent: %v", err)
	}
	if _, err := backend.db.ExecContext(ctx, sessionQuery, sessionID, runID, agentID, "delivery-conformance"); err != nil {
		t.Fatalf("seed delivery agent session: %v", err)
	}
}

func deliveryLifecycleConformanceBackends(t *testing.T) []deliveryLifecycleConformanceBackend {
	t.Helper()
	sqlite, sqliteRestart := storetest.StartSQLiteRuntimeStorePair(t)
	_, postgresDB, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	postgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
	postgresRestart := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
	return []deliveryLifecycleConformanceBackend{
		{name: "sqlite", store: sqlite, restart: sqliteRestart, selected: sqlite, db: sqlite.DB},
		{name: "postgres", store: postgres, restart: postgresRestart, selected: postgres, db: postgres.DB, postgres: true},
	}
}

func requireCanonicalDeliveryLifecycleSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	storetest.BootstrapPostgresRuntimeStore(t, pg)
	requireTableColumns(t, ctx, pg.DB, "event_deliveries",
		"delivery_id", "event_id", "route_identity", "subscriber_type", "subscriber_id",
		"status", "retry_count", "max_retries", "claim_version", "current_attempt_version",
		"current_attempt_open", "settled_at")
	requireTableColumns(t, ctx, pg.DB, "event_delivery_attempts",
		"delivery_id", "claim_version", "claim_token", "started_at", "lease_expires_at", "current_delivery_id",
		"active_session_id", "session_delivery_id", "session_run_id", "session_subscriber_type", "session_agent_id", "open_marker", "outcome")
	requireTableColumns(t, ctx, pg.DB, "event_delivery_outcomes",
		"delivery_id", "claim_version", "outcome", "side_effects", "duration_ms", "settled_at")
}

var (
	_ runtimedelivery.Store = (*store.PostgresStore)(nil)
	_ runtimedelivery.Store = (*store.SQLiteRuntimeStore)(nil)
)
