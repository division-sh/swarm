package bus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type exactHandoffProofStore struct {
	InMemoryEventStore
	runtimedelivery.Store
	db          *sql.DB
	adapter     *runtimedelivery.Adapter
	mu          sync.Mutex
	attempts    int
	binds       int
	failOnce    bool
	handoffFact runtimecorrelation.BundleSourceFact
	bindFact    runtimecorrelation.BundleSourceFact
}

func (s *exactHandoffProofStore) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	s.mu.Lock()
	s.attempts++
	s.handoffFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	fail := s.failOnce && s.attempts == 1
	s.mu.Unlock()
	if fail {
		return runtimedelivery.DurableHandoffProof{}, errors.New("injected handoff proof failure")
	}
	return s.adapter.ProveHandoff(ctx, s.db, eventID, route)
}

func (s *exactHandoffProofStore) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (runtimedelivery.Snapshot, error) {
	s.mu.Lock()
	s.binds++
	s.bindFact, _ = runtimecorrelation.BundleSourceFactFromContext(ctx)
	s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	ctx, err = runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		return runtimedelivery.Snapshot{}, err
	}
	snapshot, err := s.adapter.BindAgentSession(ctx, tx, claim, sessionID)
	if err != nil {
		_ = tx.Rollback()
		return runtimedelivery.Snapshot{}, err
	}
	if err := runtimeauthoractivity.Finalize(ctx); err != nil {
		_ = tx.Rollback()
		return runtimedelivery.Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	return snapshot, nil
}

func newExactHandoffProofStore(t *testing.T, failOnce bool) *exactHandoffProofStore {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+uuid.NewString()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open exact handoff proof store: %v", err)
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
			delivery_target_route BLOB NOT NULL,
			delivery_context BLOB NOT NULL,
			delivery_payload_projection BLOB NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL,
			max_retries INTEGER NOT NULL,
			next_eligible_at TIMESTAMP,
			claim_version INTEGER NOT NULL,
			current_attempt_version INTEGER,
			current_attempt_open BOOLEAN,
			reason_code TEXT,
			failure BLOB,
			started_at TIMESTAMP,
			settled_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			UNIQUE (event_id, route_identity)
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
			open_marker BOOLEAN NOT NULL,
			outcome TEXT,
			reason_code TEXT,
			failure BLOB,
			side_effects BLOB NOT NULL DEFAULT '[]',
			duration_ms INTEGER,
			completed_at TIMESTAMP,
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
		`CREATE TABLE agent_sessions (
			session_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			agent_id TEXT NOT NULL,
			status TEXT NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("create exact handoff proof schema: %v", err)
		}
	}
	adapter, err := runtimedelivery.NewAdapter(runtimedelivery.DialectSQLite)
	if err != nil {
		t.Fatalf("create exact handoff adapter: %v", err)
	}
	return &exactHandoffProofStore{db: db, adapter: adapter, failOnce: failOnce}
}

func (s *exactHandoffProofStore) seed(t *testing.T, eventID, runID string, route events.DeliveryRoute) {
	t.Helper()
	ctx := context.Background()
	evt := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	if err := eventfixture.Insert(ctx, s.db, runtimeauthoractivity.DialectSQLite, evt); err != nil {
		t.Fatalf("seed exact handoff event: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin exact handoff obligation: %v", err)
	}
	if _, err := s.adapter.CommitInitial(ctx, tx, eventID, runID, []events.DeliveryRoute{route}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit exact handoff obligation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact handoff transaction: %v", err)
	}
}

func (s *exactHandoffProofStore) seedSession(t *testing.T, sessionID, runID, agentID string) {
	t.Helper()
	if _, err := s.db.Exec(
		`INSERT INTO agent_sessions (session_id, run_id, agent_id, status) VALUES (?, ?, ?, 'active')`,
		sessionID, runID, agentID,
	); err != nil {
		t.Fatalf("seed exact delivery agent session: %v", err)
	}
}

func (s *exactHandoffProofStore) claim(t *testing.T, eventID, runID string, route events.DeliveryRoute) runtimedelivery.Claim {
	t.Helper()
	ctx := runtimeauthoractivity.WithScope(
		context.Background(),
		runtimeauthoractivity.BundleScope("exact-claim-runtime", "exact-claim-bundle"),
	)
	evt := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin exact delivery claim: %v", err)
	}
	ctx, err = runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("begin exact claim author activity: %v", err)
	}
	claimed, err := s.adapter.ClaimExact(ctx, tx, evt, route, runtimedelivery.DefaultLeaseTTL)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("claim exact delivery: %v", err)
	}
	if err := runtimeauthoractivity.Finalize(ctx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("finalize exact claim author activity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact delivery claim: %v", err)
	}
	return claimed.Claim
}

func TestEventBusAgentRouteReplacementWaitsForExactDequeuedPredecessor(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	if oldCh == nil {
		t.Fatal("predecessor route was not installed")
	}
	evt := eventtest.RuntimeControl("work-old", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver predecessor event: %v", err)
	}
	delivery := <-oldCh

	replaced := make(chan (<-chan *LocalDelivery), 1)
	go func() {
		replaced <- eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work")))
	}()
	select {
	case <-replaced:
		t.Fatal("replacement published before dequeued predecessor completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete predecessor delivery: %v", err)
	}
	var newCh <-chan *LocalDelivery
	select {
	case newCh = <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish after predecessor completion")
	}
	if newCh == nil || newCh == oldCh {
		t.Fatal("replacement did not publish an exact fresh route")
	}

	newEvent := eventtest.RuntimeControl("work-new", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), newEvent, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver successor event: %v", err)
	}
	newDelivery := <-newCh
	if newDelivery.ID() != "work-new" {
		t.Fatalf("successor event id = %q", newDelivery.ID())
	}
	if err := newDelivery.Complete(); err != nil {
		t.Fatalf("complete successor delivery: %v", err)
	}
}

func TestEventBusAgentRouteRemovalWaitsForDequeuedWork(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-1", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	delivery := <-ch
	done := make(chan struct{})
	go func() { eb.RemoveAgentRoute(token); close(done) }()
	select {
	case <-done:
		t.Fatal("route removal returned before dequeued work completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete route delivery: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route removal did not join completed work")
	}
}

func TestEventBusSnapshottedAgentRouteSendLinearizesWithRemoval(t *testing.T) {
	store := newExactHandoffProofStore(t, false)
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	for generation := uint64(1); generation <= 64; generation++ {
		token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-race", Generation: generation}
		if ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work"))); ch == nil {
			t.Fatalf("generation %d route was not installed", generation)
		}
		recipients := eb.snapshotRecipientChans([]string{token.AgentID})
		if len(recipients) != 1 || recipients[0].route == nil {
			t.Fatalf("generation %d snapshot = %#v, want exact route handle", generation, recipients)
		}
		eventID, runID := uuid.NewString(), uuid.NewString()
		route := events.DeliveryRoute{SubscriberType: "agent", SubscriberID: token.AgentID}
		evt := eventtest.RuntimeControl(
			eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0,
			runID, "", events.EventEnvelope{}, time.Now(),
		)
		store.seed(t, eventID, runID, route)
		start := make(chan struct{})
		sendResult := make(chan agentRouteSendResult, 1)
		removed := make(chan struct{})
		go func(recipient agentRecipient) {
			<-start
			sendResult <- recipient.send(context.Background(), evt, route)
		}(recipients[0])
		go func() {
			<-start
			eb.RemoveAgentRoute(token)
			close(removed)
		}()
		close(start)
		if result := <-sendResult; result != agentRouteSendDelivered && result != agentRouteSendInactive {
			t.Fatalf("generation %d send result = %v, want delivered-before-retirement or inactive-after-retirement", generation, result)
		}
		select {
		case <-removed:
		case <-time.After(time.Second):
			t.Fatalf("generation %d route removal did not join linearized send", generation)
		}
	}
	joinCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := eb.WaitForQuiescence(joinCtx); err != nil {
		t.Fatalf("WaitForQuiescence after route races: %v", err)
	}
}

func TestAgentRecipientWithoutExactLifecycleHandleFailsClosed(t *testing.T) {
	recipient := agentRecipient{agentID: "orphan", kind: inMemorySubscriberAgent}
	evt := eventtest.RuntimeControl("orphan-send", events.EventType("test.work"), "test", "", []byte(`{}`), 0,
		"run-1", "", events.EventEnvelope{}, time.Now())
	if result := recipient.send(context.Background(), evt, events.DeliveryRoute{}); result != agentRouteSendInactive {
		t.Fatalf("send result = %v, want inactive without exact lifecycle handle", result)
	}
}

func TestEventBusAgentRouteBufferedRemovalFailsClosedWithoutDurableHandoff(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-buffered", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after unproven buffered handoff")
	}
}

func TestEventBusAgentRouteFailedHandoffRetainsCarrierForExactRetry(t *testing.T) {
	store := newExactHandoffProofStore(t, true)
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	eventID, runID := uuid.NewString(), uuid.NewString()
	evt := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now())
	store.seed(t, eventID, runID, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: oldToken.AgentID})
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after first handoff proof failed")
	}
	if err := eb.WaitForQuiescence(context.Background()); err != nil {
		t.Fatalf("retry retained handoff and join: %v", err)
	}
	store.mu.Lock()
	attempts := store.attempts
	store.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("handoff proof attempts = %d, want exact fail-once retry", attempts)
	}
}

func TestNoContextRouteCleanupCarriesImmutableBundleSourceToHandoff(t *testing.T) {
	owned := sourceMutationFact(t, "9")
	for _, tc := range []struct {
		name           string
		subscriberType string
		cleanup        func(*EventBus, string, runtimeeffects.LifecycleToken, worklifetime.InternalSubscription) error
	}{
		{
			name:           "agent route removal",
			subscriberType: "agent",
			cleanup: func(bus *EventBus, _ string, token runtimeeffects.LifecycleToken, _ worklifetime.InternalSubscription) error {
				bus.RemoveAgentRoute(token)
				return nil
			},
		},
		{
			name:           "internal natural completion",
			subscriberType: "node",
			cleanup: func(_ *EventBus, _ string, _ runtimeeffects.LifecycleToken, subscription worklifetime.InternalSubscription) error {
				return subscription.Complete(false)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newExactHandoffProofStore(t, false)
			bus := newSourceMutationProbeBusWithStore(t, store, owned, newSourceMutationProbeOwner())
			subscriberID := "cleanup-" + strings.ReplaceAll(tc.name, " ", "-")
			token := runtimeeffects.LifecycleToken{
				RuntimeEpoch: 7,
				AgentID:      subscriberID,
				Generation:   1,
			}
			var subscription worklifetime.InternalSubscription
			if tc.subscriberType == "agent" {
				if ch := bus.ReplaceAgentRoute(
					token,
					testAgentSubscriptionAdmission(t, subscriberID, events.EventType("test.work")),
				); ch == nil {
					t.Fatal("install agent route")
				}
			} else {
				var err error
				subscription, err = bus.SubscribeInternal(context.Background(), subscriberID, events.EventType("test.work"))
				if err != nil {
					t.Fatalf("subscribe internal route: %v", err)
				}
			}

			eventID, runID := uuid.NewString(), uuid.NewString()
			route := events.DeliveryRoute{SubscriberType: tc.subscriberType, SubscriberID: subscriberID}
			store.seed(t, eventID, runID, route)
			evt := eventtest.RuntimeControl(
				eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if tc.subscriberType == "agent" {
				if err := bus.deliverToAgents(context.Background(), evt, []string{subscriberID}); err != nil {
					t.Fatalf("buffer agent delivery: %v", err)
				}
			} else {
				if err := bus.deliverLiveRecipientsWithRoutes(
					context.Background(),
					evt,
					[]RoutePlanLiveRecipient{{
						RecipientID:       subscriberID,
						SubscriberType:    routePlanSubscriberInternal,
						PersistAsDelivery: false,
						liveAuthority:     liveRecipientAuthorityIdentity,
					}},
					[]events.DeliveryRoute{route},
				); err != nil {
					t.Fatalf("buffer internal delivery: %v", err)
				}
			}
			if err := tc.cleanup(bus, subscriberID, token, subscription); err != nil {
				t.Fatalf("cleanup retained route: %v", err)
			}
			store.mu.Lock()
			attempts, handoffFact := store.attempts, store.handoffFact
			store.mu.Unlock()
			if attempts != 1 || !owned.Matches(handoffFact) {
				t.Fatalf("handoff proof = attempts:%d source:%#v, want exact immutable source", attempts, handoffFact)
			}
		})
	}
}

func TestEventBusResetRetiresAndRestartsInternalSubscriptionGeneration(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan int, 2)
	received := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for generation := 1; ; generation++ {
			subscription, err := eb.SubscribeInternal(ctx, "reset-proof", events.EventType("test.work"))
			if err != nil {
				return
			}
			subscription.MarkReady()
			ready <- generation
			for {
				select {
				case <-ctx.Done():
					_ = subscription.Complete(false)
					return
				case <-subscription.Retiring():
					restart := ctx.Err() == nil
					_ = subscription.Complete(restart)
					if !restart {
						return
					}
					goto nextGeneration
				case delivery := <-subscription.Deliveries():
					if delivery != nil {
						received <- delivery.ID()
						_ = delivery.Complete()
					}
				}
			}
		nextGeneration:
		}
	}()
	if generation := <-ready; generation != 1 {
		t.Fatalf("initial generation = %d", generation)
	}
	if err := eb.ResetInMemoryState(); err != nil {
		t.Fatalf("ResetInMemoryState: %v", err)
	}
	if generation := <-ready; generation != 2 {
		t.Fatalf("replacement generation = %d", generation)
	}
	evt := eventtest.RuntimeControl("after-reset", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"reset-proof"}); err != nil {
		t.Fatalf("deliver after reset: %v", err)
	}
	select {
	case eventID := <-received:
		if eventID != evt.ID() {
			t.Fatalf("event id = %q, want %q", eventID, evt.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("replacement internal subscription did not receive event")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("internal subscription receiver did not exit")
	}
}

func TestEventBusResetStopsWaitingWhenRestartingSubscriberContextIsCancelled(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	retired := make(chan struct{})
	allowResubscribe := make(chan struct{})
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		for {
			subscription, subscribeErr := eb.SubscribeInternal(ctx, "cancelled-restart-proof", events.EventType("test.work"))
			if subscribeErr != nil {
				return
			}
			subscription.MarkReady()
			select {
			case <-ctx.Done():
				_ = subscription.Complete(false)
				return
			case <-subscription.Retiring():
				_ = subscription.Complete(true)
				close(retired)
				<-allowResubscribe
			}
		}
	}()

	if err := eb.waitForInternalSubscriptionReady(ctx, "cancelled-restart-proof"); err != nil {
		t.Fatalf("wait for initial subscription: %v", err)
	}
	resetDone := make(chan error, 1)
	go func() { resetDone <- eb.ResetInMemoryState() }()
	<-retired
	cancel()
	close(allowResubscribe)
	select {
	case err := <-resetDone:
		if err != nil {
			t.Fatalf("ResetInMemoryState: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reset waited for a subscriber generation whose lifecycle context was cancelled")
	}
	select {
	case <-receiverDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled internal subscriber did not exit")
	}
}

func TestEventBusSnapshottedInternalSendLinearizesWithReset(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			subscription, subscribeErr := eb.SubscribeInternal(ctx, "reset-race-proof", events.EventType("test.work"))
			if subscribeErr != nil {
				return
			}
			subscription.MarkReady()
			ready <- struct{}{}
			for {
				select {
				case <-ctx.Done():
					_ = subscription.Complete(false)
					return
				case <-subscription.Retiring():
					_ = subscription.Complete(true)
					goto nextGeneration
				case delivery := <-subscription.Deliveries():
					if delivery != nil {
						_ = delivery.Complete()
					}
				}
			}
		nextGeneration:
		}
	}()
	<-ready

	for i := 0; i < 64; i++ {
		recipients := eb.snapshotRoutePlanRecipientChans(
			[]string{"reset-race-proof"},
			[]RoutePlanLiveRecipient{{RecipientID: "reset-race-proof", SubscriberType: routePlanSubscriberAgent}},
		)
		if len(recipients) != 1 || recipients[0].internal == nil {
			t.Fatalf("iteration %d snapshot = %#v, want one internal generation", i, recipients)
		}
		evt := eventtest.RuntimeControl(
			fmt.Sprintf("reset-race-%d", i), events.EventType("test.work"), "test", "", []byte(`{}`), 0,
			"run-1", "", events.EventEnvelope{}, time.Now(),
		)
		start := make(chan struct{})
		result := make(chan agentRouteSendResult, 1)
		resetErr := make(chan error, 1)
		go func(recipient agentRecipient) {
			<-start
			result <- recipient.send(context.Background(), evt, events.DeliveryRoute{})
		}(recipients[0])
		go func() {
			<-start
			resetErr <- eb.ResetInMemoryState()
		}()
		close(start)
		if sendResult := <-result; sendResult != agentRouteSendDelivered && sendResult != agentRouteSendInactive {
			t.Fatalf("iteration %d send result = %v, want delivered or retirement-fenced", i, sendResult)
		}
		if err := <-resetErr; err != nil {
			t.Fatalf("iteration %d ResetInMemoryState: %v", i, err)
		}
		<-ready
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("internal subscription receiver did not exit")
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	if err := eb.WaitForQuiescence(joinCtx); err != nil {
		t.Fatalf("join after reset/send races: %v", err)
	}
}
