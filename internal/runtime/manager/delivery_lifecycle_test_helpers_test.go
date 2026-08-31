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
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	deliveryfixture "github.com/division-sh/swarm/internal/store/testutil/deliveryfixture"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type managerDeliveryTestStore struct {
	db        *sql.DB
	adapter   *deliveryfixture.Adapter
	authority runtimedelivery.ExecutionAuthority
	seedMu    sync.Mutex
	mu        sync.RWMutex
	events    map[string]events.Event
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
			payload_bytes BLOB NOT NULL,
			payload_schema_bundle_hash TEXT NOT NULL,
			payload_schema_bundle_source TEXT NOT NULL,
			payload_schema_flow_id TEXT,
			payload_schema_event_key TEXT NOT NULL,
			payload_schema_digest TEXT NOT NULL,
			payload_schema_class TEXT NOT NULL,
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
				operator_reference_event_id TEXT,
				route_settlement BLOB NOT NULL
			)`,
		`CREATE TABLE event_deliveries (
			delivery_id TEXT PRIMARY KEY,
			run_id TEXT,
			event_id TEXT NOT NULL,
			route_identity TEXT NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			agent_name_owner TEXT NOT NULL,
			agent_name_source TEXT NOT NULL,
			agent_route_presence TEXT NOT NULL,
			agent_flow_scope_key TEXT NOT NULL,
			agent_flow_instance_id TEXT NOT NULL,
			agent_flow_instance_path TEXT NOT NULL,
			delivery_target_route TEXT NOT NULL,
			delivery_context TEXT NOT NULL,
			connect_execution_claim TEXT NOT NULL,
			delivery_payload_projection TEXT NOT NULL,
			execution_authority_kind TEXT NOT NULL,
			authority_bundle_hash TEXT NOT NULL,
			authority_bundle_source TEXT NOT NULL,
			execution_authority_id TEXT NOT NULL,
			execution_authority_generation INTEGER NOT NULL,
			selected_execution_id TEXT,
			selected_fork_run_id TEXT,
			selected_execution_generation INTEGER,
			continuation_handoff_at TIMESTAMP,
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
		`CREATE TABLE event_delivery_handler_rule_selections (
			delivery_id TEXT PRIMARY KEY REFERENCES event_deliveries(delivery_id),
			selection_context TEXT NOT NULL CHECK (selection_context IN ('none', 'handler_rules', 'handler_on_complete', 'join_on_complete', 'join_timeout')),
			disposition TEXT NOT NULL CHECK (disposition IN ('selected', 'no_match', 'evaluation_failed', 'not_applicable')),
			package_coordinate TEXT,
			element_id TEXT,
			display_label TEXT NOT NULL DEFAULT '',
			CHECK ((disposition = 'selected' AND selection_context <> 'none' AND NULLIF(TRIM(COALESCE(package_coordinate, '')), '') IS NOT NULL AND element_id IS NOT NULL) OR (disposition = 'evaluation_failed' AND selection_context IN ('handler_rules', 'handler_on_complete') AND NULLIF(TRIM(COALESCE(package_coordinate, '')), '') IS NOT NULL AND element_id IS NOT NULL) OR (disposition = 'no_match' AND selection_context IN ('handler_rules', 'handler_on_complete') AND package_coordinate IS NULL AND element_id IS NULL AND display_label = '') OR (disposition = 'not_applicable' AND selection_context = 'none' AND package_coordinate IS NULL AND element_id IS NULL AND display_label = ''))
		)`,
		`CREATE TABLE event_delivery_attempts (
			delivery_id TEXT NOT NULL,
			claim_version INTEGER NOT NULL,
			claim_token TEXT NOT NULL UNIQUE,
			started_at TIMESTAMP NOT NULL,
			lease_expires_at TIMESTAMP NOT NULL,
			current_delivery_id TEXT,
			active_session_id TEXT,
			session_delivery_id TEXT,
			session_run_id TEXT,
			session_subscriber_type TEXT,
			session_agent_id TEXT,
			session_agent_name_owner TEXT,
			session_agent_name_source TEXT,
			session_agent_route_presence TEXT,
			session_agent_flow_scope_key TEXT,
			session_agent_flow_instance_id TEXT,
			session_agent_flow_instance_path TEXT,
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
	adapter, err := deliveryfixture.NewAdapter(deliveryfixture.DialectSQLite)
	if err != nil {
		t.Fatalf("create manager delivery adapter: %v", err)
	}
	source, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		t.Fatalf("create manager delivery source: %v", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "manager-delivery-test", 1)
	if err != nil {
		t.Fatalf("create manager delivery authority: %v", err)
	}
	return &managerDeliveryTestStore{db: db, adapter: adapter, authority: authority, events: make(map[string]events.Event)}
}

func (s *managerDeliveryTestStore) seedAgentDeliveries(t *testing.T, agentID string, pending []events.Event) {
	t.Helper()
	route := managerAgentDeliveryRoute(agentID)
	for _, evt := range pending {
		if _, err := uuid.Parse(evt.ID()); err != nil {
			t.Fatalf("manager delivery fixture event id %q is not durable: %v", evt.ID(), err)
		}
		if err := eventfixture.Insert(context.Background(), s.db, authoractivityfixture.DialectSQLite, evt); err != nil {
			t.Fatalf("seed manager delivery event %s: %v", evt.ID(), err)
		}
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin manager delivery seed: %v", err)
		}
		if _, err := s.adapter.CommitInitial(context.Background(), tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, s.authority); err != nil {
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

func (s *managerDeliveryTestStore) ensureDelivery(
	evt events.Event,
	route events.DeliveryRoute,
	authority runtimedelivery.ExecutionAuthority,
) error {
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
	if err := eventfixture.Insert(context.Background(), s.db, authoractivityfixture.DialectSQLite, evt); err != nil {
		return fmt.Errorf("seed manager delivery event %s: %w", evt.ID(), err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if _, err := s.adapter.CommitInitial(context.Background(), tx, evt.ID(), runID, []events.DeliveryRoute{route}, authority); err != nil {
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
	story, err := authoractivityfixture.Begin(ctx, tx, authoractivityfixture.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(story, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := authoractivityfixture.Finalize(story); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *managerDeliveryTestStore) claimExact(ctx context.Context, evt events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	if err := s.ensureDelivery(evt, route, s.authority); err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	var result runtimedelivery.ClaimResult
	err := s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.adapter.ClaimExactResult(story, tx, s.authority, evt, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("manager test delivery disposition = %s", result.Disposition)
	}
	return claimed, nil
}

func (s *managerDeliveryTestStore) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) error {
	s.authority = authority
	return s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		return s.adapter.ActivateNormalAuthority(story, tx, authority)
	})
}

func (s *managerDeliveryTestStore) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	return s.adapter.InspectRecovery(ctx, s.db, source)
}

func (s *managerDeliveryTestStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, evt events.Event, route events.DeliveryRoute) (result runtimedelivery.ClaimResult, err error) {
	if err := s.ensureDelivery(evt, route, authority); err != nil {
		return runtimedelivery.ClaimResult{}, err
	}
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		result, err = s.adapter.ClaimExactResult(story, tx, authority, evt, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return result, err
}

func (s *managerDeliveryTestStore) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (page runtimedelivery.ContinuationPage, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		page, err = s.adapter.ScanContinuations(story, tx, authority, cursor, limit)
		return err
	})
	if err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for index := range page.Items {
		page.Items[index].Event = s.events[page.Items[index].Snapshot.EventID]
	}
	return page, nil
}

func (s *managerDeliveryTestStore) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	return s.adapter.ObserveContinuation(ctx, s.db, authority, deliveryID)
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

func (s *managerDeliveryTestStore) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, sideEffects []string, duration time.Duration, selection runtimedelivery.HandlerRuleSelectionFact) (snapshot runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(story context.Context, tx *sql.Tx) error {
		snapshot, err = s.adapter.SettleSuccess(story, tx, claim, sideEffects, duration, selection)
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

func managerAgentDeliveryRoute(agentID string) events.DeliveryRoute {
	identity := managerAgentIdentity(agentID)
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
}

func managerAgentIdentity(agentID string) runtimeagentidentity.Identity {
	name, err := runtimeagentidentity.RuntimeName(agentID, "manager.delivery_test")
	if err != nil {
		panic(err)
	}
	identity, err := runtimeagentidentity.New(name, runtimeagentidentity.RootRoute())
	if err != nil {
		panic(err)
	}
	return identity
}

func managerAgentDeliveryContext(ctx context.Context, agentID string) context.Context {
	return runtimedelivery.WithRoute(ctx, managerAgentDeliveryRoute(agentID))
}

func managerAgentClaimContext(ctx context.Context, claim runtimedelivery.Claim, agentID string) context.Context {
	return runtimedelivery.WithClaim(managerAgentDeliveryContext(ctx, agentID), claim)
}

func managerClaimedDeliveryContext(
	t *testing.T,
	am *AgentManager,
	ctx context.Context,
	evt events.Event,
	agentID string,
) context.Context {
	t.Helper()
	ctx = managerAgentDeliveryContext(ctx, agentID)
	store, ok := am.deliveryStore.(interface {
		ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error)
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	})
	if !ok {
		t.Fatal("manager test delivery store does not expose typed claim authority")
	}
	authority := store.managerTestDeliveryAuthority()
	if admission, admitted := managedexecution.FromContext(ctx); admitted {
		var err error
		authority, err = runtimedelivery.NewExecutionAuthority(authority.BundleSource(), admission)
		if err != nil {
			t.Fatalf("construct managed delivery authority: %v", err)
		}
	}
	result, err := store.ClaimDelivery(ctx, authority, evt, managerAgentDeliveryRoute(agentID))
	if err != nil {
		t.Fatalf("claim manager test delivery: %v", err)
	}
	claimed, ok := result.Acquired()
	if !ok {
		t.Fatalf("manager test delivery claim disposition = %s", result.Disposition)
	}
	return runtimedelivery.WithClaim(ctx, claimed.Claim)
}

func (s *managerDeliveryTestStore) managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority {
	return s.authority
}

func (s *managerDeliveryTestStore) makeDeliveryDueNow(t *testing.T, evt events.Event, agentID string) {
	t.Helper()
	identity, err := managerAgentDeliveryRoute(agentID).Identity()
	if err != nil {
		t.Fatalf("derive manager delivery fixture route: %v", err)
	}
	if _, err := s.db.Exec(
		`UPDATE event_deliveries SET next_eligible_at = ? WHERE event_id = ? AND route_identity = ? AND status = 'failed'`,
		time.Now().Add(-time.Minute), evt.ID(), events.EncodeDeliveryRouteIdentity(identity),
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
