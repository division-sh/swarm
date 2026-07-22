package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type managerDeliveryTestStore struct {
	db      *sql.DB
	adapter *runtimedelivery.Adapter
	seedMu  sync.Mutex
	mu      sync.RWMutex
	events  map[string]events.Event
}

func newManagerDeliveryTestStore(t *testing.T) *managerDeliveryTestStore {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+uuid.NewString()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open manager delivery test store: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE runs (
			run_id TEXT PRIMARY KEY,
			bundle_hash TEXT
		)`,
		`CREATE TABLE events (
			event_class TEXT NOT NULL,
			event_id TEXT PRIMARY KEY,
			run_id TEXT,
			event_name TEXT NOT NULL,
			task_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			scope TEXT NOT NULL,
			payload BLOB NOT NULL,
			execution_mode TEXT NOT NULL,
			chain_depth INTEGER NOT NULL,
			produced_by TEXT NOT NULL,
			produced_by_type TEXT NOT NULL,
			source_event_id TEXT,
			created_at TIMESTAMP NOT NULL,
			routing_source_kind TEXT NOT NULL,
			routing_source_authority TEXT,
			source_route BLOB NOT NULL,
			target_route BLOB NOT NULL,
			target_set BLOB NOT NULL,
			operator_reference_event_id TEXT
		)`,
		`CREATE TABLE event_deliveries (
			delivery_id TEXT PRIMARY KEY,
			run_id TEXT,
			event_id TEXT NOT NULL,
			route_identity TEXT NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			delivery_target_route TEXT NOT NULL,
			delivery_context TEXT NOT NULL,
			delivery_payload_projection TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL,
			max_retries INTEGER NOT NULL,
			next_eligible_at TIMESTAMP,
			claim_version INTEGER NOT NULL,
			current_attempt_version INTEGER,
			current_attempt_open BOOLEAN,
			reason_code TEXT,
			failure TEXT,
			started_at TIMESTAMP,
			settled_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			UNIQUE(event_id, route_identity)
		)`,
		`CREATE TABLE event_delivery_attempts (
			delivery_id TEXT NOT NULL,
			claim_version INTEGER NOT NULL,
			claim_token TEXT NOT NULL UNIQUE,
			started_at TIMESTAMP NOT NULL,
			lease_expires_at TIMESTAMP NOT NULL,
			active_session_id TEXT,
			session_run_id TEXT,
			session_agent_id TEXT,
			open_marker BOOLEAN NOT NULL,
			outcome TEXT,
			reason_code TEXT,
			failure TEXT,
			side_effects TEXT NOT NULL DEFAULT '[]',
			duration_ms INTEGER,
			completed_at TIMESTAMP,
			PRIMARY KEY(delivery_id, claim_version)
		)`,
		`CREATE TABLE event_delivery_outcomes (
			delivery_id TEXT NOT NULL,
			claim_version INTEGER NOT NULL,
			outcome TEXT NOT NULL,
			reason_code TEXT,
			failure TEXT,
			side_effects TEXT NOT NULL DEFAULT '[]',
			duration_ms INTEGER NOT NULL,
			settled_at TIMESTAMP NOT NULL,
			PRIMARY KEY(delivery_id, claim_version)
		)`,
		`CREATE TABLE author_activity_order (
			singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
			last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0)
		)`,
		`CREATE TABLE author_activity_occurrences (
			occurrence_id TEXT PRIMARY KEY,
			sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0),
			kind TEXT NOT NULL,
			version INTEGER NOT NULL CHECK (version = 2),
			transition TEXT NOT NULL,
			source_owner TEXT NOT NULL,
			source_identity TEXT NOT NULL,
			dedup_key TEXT NOT NULL UNIQUE,
			run_id TEXT,
			entity_id TEXT,
			agent_id TEXT,
			flow_id TEXT,
			scope_kind TEXT NOT NULL,
			runtime_instance_id TEXT,
			bundle_hash TEXT,
			author_safe_summary TEXT,
			projection TEXT NOT NULL DEFAULT '{}',
			failure TEXT,
			occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("create manager delivery test schema: %v", err)
		}
	}
	adapter, err := runtimedelivery.NewAdapter(runtimedelivery.DialectSQLite)
	if err != nil {
		t.Fatalf("create manager delivery adapter: %v", err)
	}
	return &managerDeliveryTestStore{db: db, adapter: adapter, events: make(map[string]events.Event)}
}

func (s *managerDeliveryTestStore) seedAgentDeliveries(t *testing.T, agentID string, pending []events.Event) {
	t.Helper()
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}
	for _, evt := range pending {
		if _, err := uuid.Parse(evt.ID()); err != nil {
			t.Fatalf("manager delivery fixture event id %q is not durable: %v", evt.ID(), err)
		}
		if err := eventfixture.Insert(context.Background(), s.db, runtimeauthoractivity.DialectSQLite, evt); err != nil {
			t.Fatalf("seed manager delivery event %s: %v", evt.ID(), err)
		}
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin manager delivery seed: %v", err)
		}
		if _, err := s.adapter.CommitInitial(context.Background(), tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed manager delivery obligation %s: %v", evt.ID(), err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit manager delivery obligation %s: %v", evt.ID(), err)
		}
		s.mu.Lock()
		s.events[evt.ID()] = evt
		s.mu.Unlock()
	}
}

func (s *managerDeliveryTestStore) ensureDelivery(evt events.Event, route events.DeliveryRoute) error {
	s.seedMu.Lock()
	defer s.seedMu.Unlock()
	if _, err := uuid.Parse(evt.ID()); err != nil {
		return fmt.Errorf("manager delivery fixture event id %q is not durable: %w", evt.ID(), err)
	}
	if snapshot, err := s.adapter.SnapshotExact(context.Background(), s.db, evt, route); err == nil {
		s.mu.Lock()
		s.events[evt.ID()] = evt
		s.mu.Unlock()
		_ = snapshot
		return nil
	} else if !errors.Is(err, runtimedelivery.ErrNotFound) {
		return err
	}
	runID := evt.RunID()
	if _, err := uuid.Parse(runID); err != nil {
		runID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("manager-delivery-test-run:"+evt.ID())).String()
	}
	if err := eventfixture.Insert(context.Background(), s.db, runtimeauthoractivity.DialectSQLite, evt); err != nil {
		return fmt.Errorf("seed manager delivery event %s: %w", evt.ID(), err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if _, err := s.adapter.CommitInitial(context.Background(), tx, evt.ID(), runID, []events.DeliveryRoute{route}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.mu.Lock()
	s.events[evt.ID()] = evt
	s.mu.Unlock()
	return nil
}

func (s *managerDeliveryTestStore) mutate(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	story, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(story, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := runtimeauthoractivity.Finalize(story); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *managerDeliveryTestStore) claimExact(ctx context.Context, evt events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := s.ensureDelivery(evt, route); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var claimed runtimedelivery.ClaimedObligation
	err := s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		var err error
		claimed, err = s.adapter.ClaimExact(story, tx, evt, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return claimed, err
}

func (s *managerDeliveryTestStore) ClaimAgentDelivery(ctx context.Context, evt events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if route.SubscriberType != string(runtimedelivery.SubscriberAgent) {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("agent claim requires an agent route")
	}
	return s.claimExact(ctx, evt, route)
}

func (s *managerDeliveryTestStore) ClaimNodeDelivery(ctx context.Context, evt events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if route.SubscriberType != string(runtimedelivery.SubscriberNode) {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("node claim requires a node route")
	}
	return s.claimExact(ctx, evt, route)
}

func (s *managerDeliveryTestStore) claimBacklog(ctx context.Context, class runtimedelivery.SubscriberClass, subscriberID string, limit int) ([]runtimedelivery.AgentExecution, error) {
	var claimed []runtimedelivery.ClaimedObligation
	err := s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		var err error
		if class == runtimedelivery.SubscriberAgent {
			claimed, err = s.adapter.ClaimPendingAgent(story, tx, subscriberID, limit, runtimedelivery.DefaultLeaseTTL)
		} else {
			claimed, err = s.adapter.ClaimPendingNode(story, tx, subscriberID, limit, runtimedelivery.DefaultLeaseTTL)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	executions := make([]runtimedelivery.AgentExecution, 0, len(claimed))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, obligation := range claimed {
		evt, ok := s.events[obligation.Snapshot.EventID]
		if !ok {
			return nil, fmt.Errorf("manager delivery fixture event %s is missing", obligation.Snapshot.EventID)
		}
		executions = append(executions, runtimedelivery.AgentExecution{Event: evt, Snapshot: obligation.Snapshot, Claim: obligation.Claim})
	}
	return executions, nil
}

func (s *managerDeliveryTestStore) ClaimAgentBacklog(ctx context.Context, agentID string, limit int) ([]runtimedelivery.AgentExecution, error) {
	return s.claimBacklog(ctx, runtimedelivery.SubscriberAgent, agentID, limit)
}

func (s *managerDeliveryTestStore) ClaimNodeBacklog(ctx context.Context, nodeID string, limit int) ([]runtimedelivery.NodeExecution, error) {
	return s.claimBacklog(ctx, runtimedelivery.SubscriberNode, nodeID, limit)
}

func (s *managerDeliveryTestStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.BindAgentSession(story, tx, claim, sessionID)
		return err
	})
	return snapshot, err
}

func (s *managerDeliveryTestStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.RenewClaim(story, tx, claim, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return snapshot, err
}

func (s *managerDeliveryTestStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.SettleSuccess(story, tx, claim, sideEffects, duration)
		return err
	})
	return snapshot, err
}

func (s *managerDeliveryTestStore) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.SettleFailure(story, tx, claim, settlement)
		return err
	})
	return snapshot, err
}

func (s *managerDeliveryTestStore) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	return s.adapter.Snapshot(ctx, s.db, deliveryID)
}

func (s *managerDeliveryTestStore) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	return s.adapter.Outcomes(ctx, s.db, deliveryID)
}

func (s *managerDeliveryTestStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	return s.adapter.ProveHandoff(ctx, s.db, eventID, route)
}

func (s *managerDeliveryTestStore) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return s.adapter.SummarizeRun(ctx, s.db, runID)
}

func (s *managerDeliveryTestStore) TerminalizeRun(ctx context.Context, runID, reason string) (terminalizations []runtimedelivery.Terminalization, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		terminalizations, err = s.adapter.TerminalizeRun(story, tx, runID, reason)
		return err
	})
	return terminalizations, err
}

func (s *managerDeliveryTestStore) markDelivered(t *testing.T, evt events.Event, agentID string) {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	claimed, err := s.ClaimAgentDelivery(ctx, evt, events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID})
	if err != nil {
		t.Fatalf("claim delivered manager fixture event %s: %v", evt.ID(), err)
	}
	if claimed.Claim.DeliveryID() == "" || claimed.Claim.Version() == 0 {
		t.Fatalf("claim delivered manager fixture event %s returned an empty capability: %#v", evt.ID(), claimed)
	}
	if _, err := s.SettleSuccess(ctx, claimed.Claim, nil, 0); err != nil {
		t.Fatalf("settle delivered manager fixture event %s: %v", evt.ID(), err)
	}
}

func (s *managerDeliveryTestStore) markInProgress(t *testing.T, evt events.Event, agentID string) {
	t.Helper()
	if _, err := s.ClaimAgentDelivery(testAuthorActivityContext(context.Background()), evt, events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}); err != nil {
		t.Fatalf("claim in-progress manager fixture event %s: %v", evt.ID(), err)
	}
}

func managerAgentDeliveryRoute(agentID string) events.DeliveryRoute {
	return events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}
}

func managerAgentDeliveryContext(ctx context.Context, agentID string) context.Context {
	return runtimedelivery.WithRoute(ctx, managerAgentDeliveryRoute(agentID))
}

func (s *managerDeliveryTestStore) makeRetryEligible(t *testing.T, evt events.Event, agentID string) {
	t.Helper()
	identity, err := managerAgentDeliveryRoute(agentID).Identity()
	if err != nil {
		t.Fatalf("derive manager delivery fixture route: %v", err)
	}
	if _, err := s.db.Exec(
		`UPDATE event_deliveries SET next_eligible_at = ? WHERE event_id = ? AND route_identity = ? AND status = 'failed'`,
		time.Now().Add(-time.Minute), evt.ID(), identity.String(),
	); err != nil {
		t.Fatalf("make manager delivery fixture retry eligible: %v", err)
	}
}

func (s *managerDeliveryTestStore) activityTransitions(t *testing.T) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT transition FROM author_activity_occurrences ORDER BY sequence`)
	if err != nil {
		t.Fatalf("list manager delivery fixture activity: %v", err)
	}
	defer rows.Close()
	var transitions []string
	for rows.Next() {
		var transition string
		if err := rows.Scan(&transition); err != nil {
			t.Fatalf("scan manager delivery fixture activity: %v", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read manager delivery fixture activity: %v", err)
	}
	return transitions
}
