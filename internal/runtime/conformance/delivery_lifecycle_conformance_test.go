package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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
				renewed, err := backend.store.RenewClaim(ctx, claimed.Claim)
				if err != nil {
					t.Fatalf("renew claim: %v", err)
				}
				if renewed.ClaimVersion != claimed.Claim.Version() || renewed.Status != runtimedelivery.StatusInProgress || renewed.ClaimExpiresAt.Before(claimed.Snapshot.ClaimExpiresAt) {
					t.Fatalf("renewed claim = %#v, original = %#v", renewed, claimed.Snapshot)
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
				transitions, err := backend.store.TerminalizeRun(ctx, event.RunID(), "run_terminal")
				if err != nil {
					t.Fatalf("terminalize run: %v", err)
				}
				if len(transitions) != 1 || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {
					t.Fatalf("terminalizations = %#v", transitions)
				}
				if _, err := backend.store.SettleSuccess(ctx, claimed.Claim, nil, 0); !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("late settlement error = %v, want ErrConflict", err)
				}
				outcomes, err := backend.store.Outcomes(ctx, transitions[0].Current.DeliveryID)
				if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "terminalized" {
					t.Fatalf("terminalization outcomes = %#v, err=%v", outcomes, err)
				}
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
		})
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
		} else if settled.Status != runtimedelivery.StatusDeadLetter || settled.RetryCount != maxRetries || settled.ReasonCode != "retry_exhausted" {
			t.Fatalf("exhausted snapshot = %#v", settled)
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

func expireDeliveryClaimForConformance(t *testing.T, ctx context.Context, backend deliveryLifecycleConformanceBackend, deliveryID string) {
	t.Helper()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin claim-expiry proof: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2, claim_expires_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND outcome IS NULL`
	if !backend.postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ?, claim_expires_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND outcome IS NULL`
	}
	var deliveryResult sql.Result
	if backend.postgres {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, expiresAt, deliveryID)
	} else {
		deliveryResult, err = transaction.ExecContext(ctx, deliveryQuery, startedAt, startedAt, expiresAt, expiresAt, deliveryID)
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
	query := `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id WHERE a.delivery_id = $1::uuid AND a.claim_version = $2 AND a.outcome IS NULL AND a.lease_expires_at = d.claim_expires_at`
	if !backend.postgres {
		query = `SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id = a.delivery_id WHERE a.delivery_id = ? AND a.claim_version = ? AND a.outcome IS NULL AND a.lease_expires_at = d.claim_expires_at`
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
		"status", "retry_count", "max_retries", "claim_token", "claim_version",
		"claim_expires_at", "settled_at")
	requireTableColumns(t, ctx, pg.DB, "event_delivery_attempts",
		"delivery_id", "claim_version", "claim_token", "started_at", "lease_expires_at", "outcome")
	requireTableColumns(t, ctx, pg.DB, "event_delivery_outcomes",
		"delivery_id", "claim_version", "outcome", "side_effects", "duration_ms", "settled_at")
}

var (
	_ runtimedelivery.Store = (*store.PostgresStore)(nil)
	_ runtimedelivery.Store = (*store.SQLiteRuntimeStore)(nil)
)
