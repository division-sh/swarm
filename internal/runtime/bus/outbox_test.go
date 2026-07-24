package bus_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type recordingEventStore struct {
	mu     sync.Mutex
	events []events.Event
}

type directRecipientTransactionalStore struct {
	mu            sync.Mutex
	descriptors   []runtimebus.ActiveAgentDescriptor
	events        []events.Event
	deliveries    map[string][]string
	routes        map[string][]events.DeliveryRoute
	deadLetterErr error
	active        []string
	scopes        map[string]runtimepipelineobligation.CommittedScope
	receipts      map[string]runtimepipelineobligation.DispositionKind
}

type outboxClaimStore struct {
	directRecipientTransactionalStore
	claimMu    sync.Mutex
	claimOwner *runtimepipelineobligation.ClaimIssuer
	claims     map[string]runtimepipelineobligation.Claim
}

func (s *outboxClaimStore) PipelineObligations() runtimepipelineobligation.Store {
	return s
}

func (s *outboxClaimStore) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	return s.issueClaim(eventID, runtimepipelineobligation.PurposePublication)
}

func (s *outboxClaimStore) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	var event events.Event
	for _, candidate := range s.events {
		if candidate.ID() == eventID {
			event = candidate
			break
		}
	}
	scope := s.scopes[eventID]
	if event.ID() == "" || scope == "" || s.receipts[eventID] != "" {
		return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
	}
	claim, err := s.issueClaim(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return runtimepipelineobligation.ClaimedWork{Event: event, Scope: scope, Claim: claim}, nil
}

func (s *outboxClaimStore) ClaimNext(context.Context, runtimepipelineobligation.ClaimQuery) (runtimepipelineobligation.ClaimedWork, bool, error) {
	return runtimepipelineobligation.ClaimedWork{}, false, nil
}

func (s *outboxClaimStore) issueClaim(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	eventID = strings.TrimSpace(eventID)
	if s.claimOwner == nil {
		s.claimOwner = runtimepipelineobligation.NewClaimIssuer()
	}
	if s.claims == nil {
		s.claims = make(map[string]runtimepipelineobligation.Claim)
	}
	if _, exists := s.claims[eventID]; exists {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	claim, err := s.claimOwner.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	s.claims[eventID] = claim
	return claim, nil
}

func (s *outboxClaimStore) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return runtimepipelineobligation.ErrIneligible
}

func (s *outboxClaimStore) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) error {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if err := s.verifyClaim(claim); err != nil {
		return err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	s.mu.Lock()
	if s.receipts == nil {
		s.receipts = map[string]runtimepipelineobligation.DispositionKind{}
	}
	s.receipts[claim.EventID()] = disposition.Kind()
	s.mu.Unlock()
	delete(s.claims, claim.EventID())
	return nil
}

func (s *outboxClaimStore) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if err := s.verifyClaim(claim); err != nil {
		return err
	}
	delete(s.claims, claim.EventID())
	return nil
}

func (s *outboxClaimStore) verifyClaim(claim runtimepipelineobligation.Claim) error {
	stored, ok := s.claims[claim.EventID()]
	if !ok || s.claimOwner == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	storedToken, err := s.claimOwner.Token(stored)
	if err != nil {
		return err
	}
	claimToken, err := s.claimOwner.Token(claim)
	if err != nil {
		return err
	}
	if storedToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	return nil
}

func (*outboxClaimStore) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func (*outboxClaimStore) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}, nil
}

func (*outboxClaimStore) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

func (s *recordingEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{finalize: func(context.Context, runtimebus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return nil
	}, begin: func(_ context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events = append(s.events, admitted.Event())
		return runtimebus.EventAppendInserted, nil
	}})
}

func (*recordingEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}
func (*recordingEventStore) SupportsPersistedReplay() bool { return false }

func (s *recordingEventStore) eventTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.events))
	for _, evt := range s.events {
		out = append(out, string(evt.Type()))
	}
	return out
}

func (s *directRecipientTransactionalStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries[eventID]...), nil
}
func (*directRecipientTransactionalStore) SupportsPersistedReplay() bool { return true }

func (s *directRecipientTransactionalStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (s *directRecipientTransactionalStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return prepareTestCommitPublish(ctx, plan, &testCommitPublishTransaction{
		begin:    s.beginPreparedPublish,
		finalize: s.finalizePreparedPublish,
	})
}

func (s *directRecipientTransactionalStore) BeginPreparedPublish(ctx context.Context, prepared runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	return s.beginPreparedPublish(ctx, prepared.AdmittedEvent())
}

func (s *directRecipientTransactionalStore) beginPreparedPublish(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	s.mu.Lock()
	evt := admitted.Event()
	for _, existing := range s.events {
		if existing.ID() == evt.ID() {
			s.mu.Unlock()
			if existing.Type() != evt.Type() {
				return runtimebus.EventAppendOutcomeUnknown, errors.New("conflicting event identity")
			}
			return runtimebus.EventAppendExactDuplicate, nil
		}
	}
	previousEvents := append([]events.Event(nil), s.events...)
	previousRoutes := append([]events.DeliveryRoute(nil), s.routes[evt.ID()]...)
	previousDeliveries := append([]string(nil), s.deliveries[evt.ID()]...)
	s.events = append(s.events, evt)
	s.active = append(s.active, evt.ID())
	s.mu.Unlock()
	_ = runtimepipeline.QueuePipelineRollbackAction(ctx, func(context.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events = previousEvents
		if s.routes != nil {
			s.routes[evt.ID()] = previousRoutes
		}
		if s.deliveries != nil {
			s.deliveries[evt.ID()] = previousDeliveries
		}
	})
	return runtimebus.EventAppendInserted, nil
}

func (s *directRecipientTransactionalStore) FinalizePreparedPublish(_ context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	return s.finalizePreparedPublish(context.Background(), finalization.Request())
}

func (s *directRecipientTransactionalStore) finalizePreparedPublish(_ context.Context, req runtimebus.CommitPublishRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	evt := req.Event.Event()
	if len(s.active) == 0 || s.active[len(s.active)-1] != evt.ID() {
		return errors.New("prepared event finalization does not match the active event")
	}
	if req.DeadLetter != nil && s.deadLetterErr != nil {
		return s.deadLetterErr
	}
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	if s.deliveries == nil {
		s.deliveries = map[string][]string{}
	}
	if s.scopes == nil {
		s.scopes = map[string]runtimepipelineobligation.CommittedScope{}
	}
	if s.receipts == nil {
		s.receipts = map[string]runtimepipelineobligation.DispositionKind{}
	}
	s.routes[evt.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
	s.scopes[evt.ID()] = req.ReplayScope
	if req.Disposition != nil {
		s.receipts[evt.ID()] = req.Disposition.Kind()
	}
	s.deliveries[evt.ID()] = nil
	for _, route := range s.routes[evt.ID()] {
		if route.SubscriberType == "agent" {
			s.deliveries[evt.ID()] = append(s.deliveries[evt.ID()], route.SubscriberID)
		}
	}
	s.active = s.active[:len(s.active)-1]
	return nil
}

func (s *directRecipientTransactionalStore) deliveryRoutes(eventID string) []events.DeliveryRoute {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...)
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == want.SubscriberType &&
			route.SubscriberID == want.SubscriberID &&
			route.Target.Normalized() == want.Target.Normalized() {
			return true
		}
	}
	return false
}

type interceptingTestHandler struct{}

func (interceptingTestHandler) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() != events.EventType("custom.emitted") {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, []events.Event{eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.followup"),
		"runtime",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()),
		time.Now().UTC(),
	)}, runtimepipelineobligation.Continue(), nil
}

type recordingDeliveryRouteInterceptor struct {
	mu     sync.Mutex
	routes []events.DeliveryRoute
}

func (*recordingDeliveryRouteInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func (r *recordingDeliveryRouteInterceptor) InterceptDeliveryRoute(_ context.Context, _ events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route.Normalized())
	return false, nil, runtimepipelineobligation.Continue(), nil
}

func (r *recordingDeliveryRouteInterceptor) seen(route events.DeliveryRoute) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, got := range events.NormalizeDeliveryRoutes(r.routes) {
		if got.SubscriberType == route.SubscriberType && got.SubscriberID == route.SubscriberID && got.Target.Normalized() == route.Target.Normalized() {
			return true
		}
	}
	return false
}

func TestEngineDispatcherCollectsEmitIntentsWithChainDepth(t *testing.T) {
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eventCollector := make([]events.Event, 0, 1)
	intentCollector := make([]runtimeengine.EmitIntent, 0, 1)
	ctx := runtimepipeline.WithPipelineEmitCollectors(context.Background(), &eventCollector, &intentCollector)

	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress("", events.EventType("custom.emitted"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Time{}),
		ChainDepth: 3,
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if got := len(intentCollector); got != 1 {
		t.Fatalf("intent collector count = %d, want 1", got)
	}
	if got := intentCollector[0].ChainDepth; got != 3 {
		t.Fatalf("intent chain depth = %d, want 3", got)
	}
	if got := len(eventCollector); got != 0 {
		t.Fatalf("event collector count = %d, want 0", got)
	}
}

func TestEngineDispatcherQueuesWhenPipelineSQLTxActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a", EntityID: eventtest.UUID("ent-1")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
	defer runtimebustest.Unsubscribe(eb, "agent-a")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-post-commit-dispatch"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	postCommitActions := make([]runtimepipeline.OwnerAction, 0, 1)
	rollbackActions := make([]runtimepipeline.OwnerAction, 0, 1)
	txctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	txctx = runtimebus.WithCommitPublishTransaction(txctx, store)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)
	txctx = runtimepipeline.WithPipelineRollbackActions(txctx, &rollbackActions)

	if err := eb.EngineOutbox().WriteOutbox(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) == 0 {
		_ = tx.Rollback()
		t.Fatal("post-commit dispatch was not queued")
	}
	requireNoBusEvent(t, ch, "post-commit delivery before flush")

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	got := requireBusEvent(t, ch, "post-commit outbox dispatch")
	if got.ID() != intent.Event.ID() {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), intent.Event.ID())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherQueuesImmutableIntentSnapshotWhenPipelineSQLTxActive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	originalCh := runtimebustest.Subscribe(t, eb, "agent-original", events.EventType("custom.snapshot"))
	defer runtimebustest.Unsubscribe(eb, "agent-original")
	mutatedCh := runtimebustest.Subscribe(t, eb, "agent-mutated", events.EventType("custom.snapshot"))
	defer runtimebustest.Unsubscribe(eb, "agent-mutated")

	payload := []byte(`{"value":"original"}`)
	targetSet := []events.RouteIdentity{{FlowInstance: "flow-original", EntityID: eventtest.UUID("entity-original")}}
	recipients := []string{"agent-original"}
	intents := []runtimeengine.EmitIntent{{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("evt-queued-snapshot"),
			events.EventType("custom.snapshot"), "", "", payload, 0, "", "", events.EventEnvelope{TargetSet: targetSet},
			time.Now().UTC()),

		Recipients: recipients,
	}}
	postCommitActions := make([]runtimepipeline.OwnerAction, 0, 1)
	rollbackActions := make([]runtimepipeline.OwnerAction, 0, 1)
	txctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)
	txctx = runtimepipeline.WithPipelineRollbackActions(txctx, &rollbackActions)

	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, intents); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) != 1 {
		_ = tx.Rollback()
		t.Fatalf("post-commit actions = %d, want 1", len(postCommitActions))
	}
	copy(payload, []byte(`{"value":"mutated!"}`))
	targetSet[0] = events.RouteIdentity{FlowInstance: "flow-mutated", EntityID: "entity-mutated"}
	recipients[0] = "agent-mutated"
	intents[0].Event = eventtest.RunCreatingRootIngress(
		intents[0].Event.ID(),
		intents[0].Event.Type(),
		intents[0].Event.SourceAgent(),
		intents[0].Event.TaskID(),
		[]byte(`{"value":"reassigned"}`),
		intents[0].Event.ChainDepth(),
		intents[0].Event.RunID(),
		intents[0].Event.ParentEventID(),
		events.EventEnvelope{TargetSet: []events.RouteIdentity{{FlowInstance: "flow-reassigned", EntityID: "entity-reassigned"}}},
		intents[0].Event.CreatedAt())

	intents[0].Recipients = []string{"agent-reassigned"}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	got := requireBusEvent(t, originalCh, "immutable intent snapshot delivery")
	if string(got.Payload()) != `{"value":"original"}` {
		t.Fatalf("delivered payload = %s, want original snapshot", string(got.Payload()))
	}
	routes := got.TargetRoutes()
	if len(routes) != 1 || routes[0].FlowInstance != "flow-original" || routes[0].EntityID != eventtest.UUID("entity-original") {
		t.Fatalf("delivered target routes = %#v, want original snapshot", routes)
	}
	requireNoBusEvent(t, mutatedCh, "mutated recipient delivery")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherFailsClosedWithSQLTxAndNoPostCommitQueue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	eb, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	err = eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-no-post-commit-queue"),
			events.EventType("custom.emitted"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected DispatchPostCommit to fail closed without post-commit queue")
	}
	if !strings.Contains(err.Error(), "post-commit dispatch requires pipeline post-commit actions") {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit error = %q, want post-commit queue failure", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	entityID := uuid.NewString()
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	recordingStore := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer", EntityID: entityID},
		},
	}
	eb, err := newScopedTestEventBus(recordingStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, recordingStore)
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-1"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"`+entityID+`"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Now().UTC(),
		),

		Recipients: []string{"reviewer"},
	}
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	intent.Recipients = []string{"late-reviewer"}
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox exact duplicate: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := len(recordingStore.events); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
	gotPersisted, err := recordingStore.ListEventDeliveryRecipients(context.Background(), eventtest.UUID("evt-1"))
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if strings.Join(gotPersisted, ",") != "reviewer" {
		t.Fatalf("persisted recipients after exact duplicate = %v, want original [reviewer]", gotPersisted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxExactDuplicateDispatchIsOperationNoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer", EntityID: eventtest.UUID("ent-1")},
			{AgentID: "late-reviewer", EntityID: eventtest.UUID("ent-1")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
	lateReviewer := runtimebustest.Subscribe(t, eb, "late-reviewer")
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-exact-duplicate-operation"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
		Recipients: []string{"reviewer"},
	}

	persistAndDispatch := func(intent runtimeengine.EmitIntent) {
		t.Helper()
		mock.ExpectBegin()
		mock.ExpectCommit()
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
		ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
		if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
			_ = tx.Rollback()
			t.Fatalf("WriteOutbox: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
			t.Fatalf("DispatchPostCommit: %v", err)
		}
	}

	persistAndDispatch(intent)
	_ = requireBusEvent(t, reviewer, "initial inserted outbox operation")
	requireNoBusEvent(t, lateReviewer, "initial recipient manifest")

	duplicate := intent
	duplicate.Recipients = []string{"late-reviewer"}
	persistAndDispatch(duplicate)
	requireNoBusEvent(t, reviewer, "exact duplicate redelivery")
	requireNoBusEvent(t, lateReviewer, "exact duplicate recipient expansion")
	got, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if strings.Join(got, ",") != "reviewer" {
		t.Fatalf("persisted recipients after exact duplicate = %v, want [reviewer]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "rollback"
		if commit {
			name = "commit_then_dispatch"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			store := &outboxClaimStore{directRecipientTransactionalStore: directRecipientTransactionalStore{
				descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "reviewer", EntityID: eventtest.UUID("ent-claim")}},
			}}
			eb, err := newScopedTestEventBus(store)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{
				Event: eventtest.RunCreatingRootIngress(
					eventtest.UUID("evt-outbox-claim-"+name),
					events.EventType("custom.emitted"),
					"",
					"",
					[]byte(`{"entity_id":"ent-claim"}`),
					0,
					"",
					"",
					events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-claim")),
					time.Now().UTC(),
				),
				Recipients: []string{"reviewer"},
			}

			mock.ExpectBegin()
			if commit {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			rollbackActions := []runtimepipeline.OwnerAction{}
			postCommitActions := []runtimepipeline.OwnerAction{}
			ctx := runtimepipeline.WithPipelineRollbackActions(runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx), &rollbackActions)
			ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
			ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
			if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}

			if commit {
				if err := tx.Commit(); err != nil {
					t.Fatalf("Commit: %v", err)
				}
				runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
				if _, err := store.PipelineObligations().ClaimPublication(context.Background(), intent.Event.ID()); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
					t.Fatalf("publication claim before designated dispatch = %v, want busy", err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatalf("DispatchPostCommit: %v", err)
				}
				_ = requireBusEvent(t, reviewer, "claimed outbox event")
			} else {
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback: %v", err)
				}
				runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
				requireNoBusEvent(t, reviewer, "rolled-back outbox event")
			}

			claim, err := store.PipelineObligations().ClaimPublication(context.Background(), intent.Event.ID())
			if err != nil {
				t.Fatalf("publication claim after lifecycle completion: %v", err)
			}
			if err := store.PipelineObligations().Release(context.Background(), claim); err != nil {
				t.Fatalf("release publication claim: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sql expectations: %v", err)
			}
		})
	}
}

func TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := &directRecipientTransactionalStore{descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "reviewer", EntityID: eventtest.UUID("ent-1")}}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress(eventtest.UUID("evt-same-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()),
		Recipients: []string{"reviewer"},
	}

	mock.ExpectBegin()
	mock.ExpectCommit()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent, intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent, intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if evt := requireBusEvent(t, reviewer, "inserted same-batch intent"); evt.ID() != intent.Event.ID() {
		t.Fatalf("dispatched event = %q, want %q", evt.ID(), intent.Event.ID())
	}
	requireNoBusEvent(t, reviewer, "same-batch exact duplicate")
	if got := len(store.events); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := &directRecipientTransactionalStore{descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "reviewer", EntityID: eventtest.UUID("ent-1")}}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress(eventtest.UUID("evt-preexisting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()),
		Recipients: []string{"reviewer"},
	}
	writeAndDispatch := func(intents []runtimeengine.EmitIntent) {
		t.Helper()
		mock.ExpectBegin()
		mock.ExpectCommit()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
		ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
		if err := eb.EngineOutbox().WriteOutbox(ctx, intents); err != nil {
			t.Fatalf("WriteOutbox: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), intents); err != nil {
			t.Fatalf("DispatchPostCommit: %v", err)
		}
	}

	writeAndDispatch([]runtimeengine.EmitIntent{intent})
	_ = requireBusEvent(t, reviewer, "initial inserted intent")
	writeAndDispatch([]runtimeengine.EmitIntent{intent, intent, intent})
	requireNoBusEvent(t, reviewer, "preexisting exact duplicate batch")
	if got := len(store.events); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := &directRecipientTransactionalStore{descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "reviewer", EntityID: eventtest.UUID("ent-1")}}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress(eventtest.UUID("evt-conflicting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","value":"first"}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()),
		Recipients: []string{"reviewer"},
	}
	conflict := intent
	conflict.Event = eventtest.RunCreatingRootIngress(intent.Event.ID(), events.EventType("custom.conflicting"), "", "", []byte(`{"entity_id":"ent-1","value":"conflict"}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), intent.Event.CreatedAt())

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rollbackActions := []runtimepipeline.OwnerAction{}
	postCommitActions := []runtimepipeline.OwnerAction{}
	ctx := runtimepipeline.WithPipelineRollbackActions(runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx), &rollbackActions)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommitActions)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent, conflict}); err == nil || !strings.Contains(err.Error(), "conflicting event identity") {
		t.Fatalf("WriteOutbox conflict error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	runtimepipeline.FlushPipelineRollbackActions(rollbackActions)
	if got := len(store.events); got != 0 {
		t.Fatalf("events after rollback = %d, want 0", got)
	}
	if got, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID()); err != nil || len(got) != 0 {
		t.Fatalf("deliveries after rollback = %v, %v", got, err)
	}

	writeAndDispatch := func() {
		t.Helper()
		mock.ExpectBegin()
		mock.ExpectCommit()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
		ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
		if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
			t.Fatal(err)
		}
	}
	writeAndDispatch()
	_ = requireBusEvent(t, reviewer, "insert after rolled-back conflict")
	writeAndDispatch()
	requireNoBusEvent(t, reviewer, "exact duplicate after rolled-back conflict")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxDistinctIntentBatchDispatchesEachOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := &directRecipientTransactionalStore{descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "reviewer", EntityID: eventtest.UUID("ent-1")}}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
	intents := []runtimeengine.EmitIntent{
		{Event: eventtest.RunCreatingRootIngress(eventtest.UUID("evt-distinct-1"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":1}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}},
		{Event: eventtest.RunCreatingRootIngress(eventtest.UUID("evt-distinct-2"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":2}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}},
	}
	mock.ExpectBegin()
	mock.ExpectCommit()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, intents); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), intents); err != nil {
		t.Fatal(err)
	}
	if evt := requireBusEvent(t, reviewer, "first distinct intent"); evt.ID() != intents[0].Event.ID() {
		t.Fatalf("first event = %q, want %q", evt.ID(), intents[0].Event.ID())
	}
	if evt := requireBusEvent(t, reviewer, "second distinct intent"); evt.ID() != intents[1].Event.ID() {
		t.Fatalf("second event = %q, want %q", evt.ID(), intents[1].Event.ID())
	}
	requireNoBusEvent(t, reviewer, "distinct intent over-dispatch")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxSubscribedIntentConsumesCanonicalMaterializedRoutePlan(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{}
	want := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "target-node",
		Target: events.RouteIdentity{
			FlowInstance: "review/inst-1",
		},
	}
	guardSawMaterializedRoute := false
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) ([]events.DeliveryRoute, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []events.DeliveryRoute{want}, nil
		},
		RecipientPlanGuard: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
				t.Fatalf("guard delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
			}
			guardSawMaterializedRoute = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("evt-outbox-materialized-route"),
			events.EventType("review/inst-1/task.started"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !guardSawMaterializedRoute {
		t.Fatal("recipient plan guard did not see materialized route")
	}
	if got := store.deliveryRoutes(intent.Event.ID()); !deliveryRoutesContain(got, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "control-plane"},
			{AgentID: "reviewer-ent-1", EntityID: eventtest.UUID("ent-1")},
			{AgentID: "reviewer-ent-2", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controlCh := runtimebustest.Subscribe(t, eb, "control-plane")
	matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1")
	otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-direct-intent"),
			events.EventType("custom.direct"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),

		Recipients: []string{"control-plane", "reviewer-ent-1", "reviewer-ent-2", "missing-agent"},
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	wantPersisted := []string{"control-plane", "reviewer-ent-1"}
	if strings.Join(gotPersisted, ",") != strings.Join(wantPersisted, ",") {
		t.Fatalf("persisted recipients = %v, want %v", gotPersisted, wantPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	_ = requireBusEvent(t, controlCh, "direct intent delivery to control-plane")
	evt := requireBusEvent(t, matchCh, "direct intent delivery to matching entity-scoped agent")
	if got := evt.EntityID(); got != eventtest.UUID("ent-1") {
		t.Fatalf("matched event entity_id = %q, want ent-1", got)
	}
	requireNoBusEvent(t, otherCh, "direct intent delivery to filtered recipient")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutbox_TargetFailureDeadLetterErrorFailsClosed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	deadLetterErr := errors.New("dead letter recorder unavailable")
	store := &directRecipientTransactionalStore{deadLetterErr: deadLetterErr}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-outbox-target-failure"),
			events.EventType("child/output.done"),
			"",
			"",
			[]byte(`{}`),
			0,
			"",
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("missing-entity"), FlowInstance: "missing-flow"}),
			time.Now().UTC(),
		),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	err = eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent})
	if !errors.Is(err, deadLetterErr) {
		t.Fatalf("WriteOutbox error = %v, want dead-letter persistence failure", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "agent-a"},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	internalCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.emitted"))
	agentCh := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-internal-live"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if strings.Join(gotPersisted, ",") != "agent-a" {
		t.Fatalf("persisted recipients = %v, want [agent-a]", gotPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	evt := requireBusEvent(t, internalCh, "outbox event delivery to internal subscriber")
	if got := evt.EntityID(); got != eventtest.UUID("ent-1") {
		t.Fatalf("internal event entity_id = %q, want ent-1", got)
	}
	evt = requireBusEvent(t, agentCh, "outbox event delivery to agent subscriber")
	if got := evt.EntityID(); got != eventtest.UUID("ent-1") {
		t.Fatalf("agent event entity_id = %q, want ent-1", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxAndDispatcher_RoutesPendingInternalDeliveriesToRouteInterceptors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{}
	interceptor := &recordingDeliveryRouteInterceptor{}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	internalCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.emitted"))
	defer runtimebustest.Unsubscribe(eb, "workflow-runtime")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-pending-internal-route"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	txctx = runtimebus.WithCommitPublishTransaction(txctx, store)
	if err := eb.EngineOutbox().WriteOutbox(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	wantRoute := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "workflow-runtime"}
	if !interceptor.seen(wantRoute) {
		t.Fatalf("delivery route interceptor did not receive pending internal route %#v", wantRoute)
	}
	requireNoBusEvent(t, internalCh, "route-intercepted pending internal event")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherRunsInterceptorsForPersistedEmitIntents(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SetInterceptors(interceptingTestHandler{})

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-1"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	got := store.eventTypes()
	if len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcher_FailsClosedWithoutAuthoritativeRecipientManifestOnInMemoryBus(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-missing-manifest"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil {
		t.Fatal("expected DispatchPostCommit to fail without authoritative recipient manifest")
	}
	if got := err.Error(); !strings.Contains(got, "authoritative delivery recipient manifest is unavailable") {
		t.Fatalf("DispatchPostCommit error = %q, want missing authoritative manifest failure", got)
	}
}

func TestEngineDispatcher_DirectIntentUsesExplicitRecipientsWhenManifestWasNotPersisted(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	recipientCh := runtimebustest.Subscribe(t, eb, "agent-a")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-direct-no-tx"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),

		Recipients: []string{"agent-a"},
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}

	evt := requireBusEvent(t, recipientCh, "direct no-tx delivery to explicit recipient")
	if got := evt.EntityID(); got != eventtest.UUID("ent-1") {
		t.Fatalf("delivered event entity_id = %q, want ent-1", got)
	}
}

func TestEngineDispatcher_TransactionalDirectIntentHonorsEmptyPersistedManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{
			{AgentID: "reviewer-ent-2", EntityID: eventtest.UUID("ent-2")},
		},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	filteredCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")

	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-empty-direct-manifest"),
			events.EventType("custom.direct"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),

		Recipients: []string{"reviewer-ent-2"},
	}
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	if err := eb.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotPersisted, err := store.ListEventDeliveryRecipients(context.Background(), intent.Event.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRecipients: %v", err)
	}
	if len(gotPersisted) != 0 {
		t.Fatalf("persisted recipients = %v, want empty authoritative manifest", gotPersisted)
	}

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	requireNoBusEvent(t, filteredCh, "empty authoritative direct manifest delivery")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPublishDirectInMutationRejectsFilteredExplicitRecipient(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{{
			AgentID: "requester-agent", EntityID: "requester-entity", FlowInstance: "provider/instance-a",
		}},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("evt-filtered-direct-mutation"), events.EventType("human_task.approved"), "runtime", "", []byte(`{}`), 0, "", "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("wrong-entity"), FlowInstance: "wrong-flow"}),
		time.Now().UTC(),
	)
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	rollback := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := runtimepipeline.WithPipelineSQLTxContext(context.Background(), tx)
	ctx = runtimebus.WithCommitPublishTransaction(ctx, store)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
	err = eb.PublishDirectInMutation(ctx, evt, []string{"requester-agent"})
	if err == nil || !strings.Contains(err.Error(), "transactional direct delivery rejected recipients: requester-agent") {
		_ = tx.Rollback()
		t.Fatalf("PublishDirectInMutation error = %v", err)
	}
	if len(postCommit) != 1 {
		_ = tx.Rollback()
		t.Fatalf("post-commit actions = %d, want only the rollback-lease settlement counterpart", len(postCommit))
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	runtimepipeline.FlushPipelineRollbackActions(rollback)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
