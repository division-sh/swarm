package bus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

type recordingEventStore struct {
	mu       sync.Mutex
	events   []events.Event
	prepared map[string]runtimebus.PreparedPublishEvent
}

type directRecipientTransactionalStore struct {
	mu            sync.Mutex
	descriptors   []runtimebus.ActiveAgentDescriptor
	targetOwners  []runtimebus.ActiveTargetDescriptor
	targetRunID   string
	events        []events.Event
	settlements   map[string]events.RouteSettlement
	deliveries    map[string][]string
	routes        map[string][]events.DeliveryRoute
	deadLetterErr error
	active        []string
	scopes        map[string]runtimepipelineobligation.CommittedScope
	receipts      map[string]runtimepipelineobligation.DispositionKind
}

func (s *directRecipientTransactionalStore) ListSelectedRunTargetOwners(_ context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if runID != s.targetRunID {
		return nil, nil
	}
	return append([]runtimebus.ActiveTargetDescriptor(nil), s.targetOwners...), nil
}

func (s *directRecipientTransactionalStore) ListSelectedRunTargetOwnersForScope(ctx context.Context, runID string, paths []string, sourceEntityID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	owners, err := s.ListSelectedRunTargetOwners(ctx, runID)
	var selected []runtimebus.ActiveTargetDescriptor
	for _, owner := range owners {
		if sourceEntityID != "" && owner.EntityID == sourceEntityID {
			selected = append(selected, owner)
			continue
		}
		for _, path := range paths {
			if owner.FlowInstance == path {
				selected = append(selected, owner)
				break
			}
		}
	}
	return selected, err
}

type outboxClaimStore struct {
	directRecipientTransactionalStore
	claimMu    sync.Mutex
	claimOwner *runtimepipelineobligation.ClaimIssuer
	scanOwner  *runtimepipelineobligation.ScanIssuer
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

func (s *outboxClaimStore) OpenScan(_ context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	if s.scanOwner == nil {
		s.scanOwner = runtimepipelineobligation.NewScanIssuer()
	}
	return s.scanOwner.Issue()
}

func (*outboxClaimStore) ClaimBatch(context.Context, runtimepipelineobligation.Scan, int) (runtimepipelineobligation.ScanBatch, error) {
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (s *outboxClaimStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	_, err := s.scanOwner.Token(scan)
	return err
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

func (s *outboxClaimStore) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) (runtimepipelineobligation.SettlementOutcome, error) {
	return runtimepipelineobligation.SettlementOutcome{}, runtimepipelineobligation.ErrIneligible
}

func (s *outboxClaimStore) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if err := s.verifyClaim(claim); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	s.mu.Lock()
	if s.receipts == nil {
		s.receipts = map[string]runtimepipelineobligation.DispositionKind{}
	}
	s.receipts[claim.EventID()] = disposition.Kind()
	s.mu.Unlock()
	delete(s.claims, claim.EventID())
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
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

func (s *recordingEventStore) CommitPublication(_ context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	s.mu.Lock()
	s.events = append(s.events, command.Commit.Event.Event())
	s.mu.Unlock()
	return (runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}).WithCommitAcknowledgment(), nil
}

func (*recordingEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *recordingEventStore) LoadPreparedPublishEvent(_ context.Context, eventID string) (runtimebus.PreparedPublishEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared, ok := s.prepared[strings.TrimSpace(eventID)]
	return prepared, ok, nil
}

func (*recordingEventStore) SupportsPersistedReplay() bool { return false }

func seedRecordingNoDelivery(t testing.TB, store *recordingEventStore, event events.Event) {
	t.Helper()
	admitted, err := events.RevalidatePersistedEvent(event)
	if err != nil {
		t.Fatalf("admit recording event: %v", err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit recording evaluation ledger: %v", err)
	}
	settlement, err := events.NewNoDeliverySettlement(
		events.EventWriteNormalPublication,
		events.NoDeliveryDeclaredConsumerNoPlan,
		ledger,
	)
	if err != nil {
		t.Fatalf("admit recording no-delivery settlement: %v", err)
	}
	store.prepared = map[string]runtimebus.PreparedPublishEvent{
		event.ID(): {Event: admitted, Settlement: settlement},
	}
}

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

func (s *directRecipientTransactionalStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *directRecipientTransactionalStore) LoadPreparedPublishEvent(_ context.Context, eventID string) (runtimebus.PreparedPublishEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event.ID() != strings.TrimSpace(eventID) {
			continue
		}
		admitted, err := events.RevalidatePersistedEvent(event)
		if err != nil {
			return runtimebus.PreparedPublishEvent{}, false, err
		}
		routes := append([]events.DeliveryRoute(nil), s.routes[event.ID()]...)
		settlement := s.settlements[event.ID()]
		if err := settlement.Validate(routes); err != nil {
			return runtimebus.PreparedPublishEvent{}, false, err
		}
		return runtimebus.PreparedPublishEvent{Event: admitted, Settlement: settlement, DeliveryRoutes: routes}, true, nil
	}
	return runtimebus.PreparedPublishEvent{}, false, nil
}

func (*directRecipientTransactionalStore) SupportsPersistedReplay() bool { return true }

func (s *directRecipientTransactionalStore) ListActiveAgentDescriptors(context.Context, string) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (s *directRecipientTransactionalStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	outcome, err := s.beginPreparedPublish(ctx, command.Commit.Event)
	if err != nil {
		return runtimebus.CommittedPublication{AppendOutcome: outcome}, err
	}
	if outcome == runtimebus.EventAppendExactDuplicate {
		return (runtimebus.CommittedPublication{AppendOutcome: outcome}).WithCommitAcknowledgment(), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	eventID := command.Commit.Event.ID()
	if command.Commit.DeadLetter != nil && s.deadLetterErr != nil {
		return runtimebus.CommittedPublication{}, s.deadLetterErr
	}
	if s.routes == nil {
		s.routes = map[string][]events.DeliveryRoute{}
	}
	if s.settlements == nil {
		s.settlements = map[string]events.RouteSettlement{}
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
	s.routes[eventID] = events.NormalizeDeliveryRoutes(command.Commit.DeliveryRoutes)
	s.settlements[eventID] = command.Commit.RouteSettlement
	s.scopes[eventID] = command.Commit.ReplayScope
	if command.Commit.Disposition != nil {
		s.receipts[eventID] = command.Commit.Disposition.Kind()
	}
	for _, route := range s.routes[eventID] {
		if route.Recipient.IsAgent() {
			s.deliveries[eventID] = append(s.deliveries[eventID], route.Recipient.LocalID())
		}
	}
	if len(s.active) > 0 {
		s.active = s.active[:len(s.active)-1]
	}
	return (runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}).WithCommitAcknowledgment(), nil
}

func commitEnginePublicationsForTest(
	ctx context.Context,
	eb *runtimebus.EventBus,
	owner runtimebus.CommitPublicationOwner,
	intents []runtimeengine.EmitIntent,
) error {
	plans, err := eb.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return err
	}
	seen := make(map[string]events.Event, len(plans))
	for _, value := range plans {
		plan, ok := value.(runtimebus.EnginePublicationPlan)
		if !ok {
			_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return fmt.Errorf("unexpected engine publication plan %T", value)
		}
		event := plan.PublicationCommand().Commit.Event.Event()
		if previous, exists := seen[event.ID()]; exists && (previous.Type() != event.Type() || string(previous.Payload()) != string(event.Payload())) {
			_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return errors.New("conflicting event identity")
		}
		seen[event.ID()] = event
	}
	evidence := make([]runtimeengine.CommittedDurablePublication, 0, len(plans))
	for _, value := range plans {
		plan, ok := value.(runtimebus.EnginePublicationPlan)
		if !ok {
			_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return fmt.Errorf("unexpected engine publication plan %T", value)
		}
		committed, err := owner.CommitPublication(ctx, plan.PublicationCommand())
		if err != nil {
			_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return err
		}
		proof, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			_ = eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
			return err
		}
		evidence = append(evidence, proof)
	}
	return eb.FinalizeEnginePublications(ctx, evidence)
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
	_ = runtimepipelinefixture.QueueRollbackAction(ctx, func(context.Context) {
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

func (s *directRecipientTransactionalStore) deliveryRoutes(eventID string) []events.DeliveryRoute {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...)
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient == want.Recipient &&
			route.Target == want.Target {
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
		evt.RunID(),
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

func TestEngineDispatcherDispatchesCommittedIntentWithoutHiddenCollector(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.SetInterceptors(interceptingTestHandler{})
	intent := runtimeengine.EmitIntent{
		Event:      eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("custom.emitted"), "", "", nil, 0, eventtest.UUID("committed-intent-run"), "", events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()),
		ChainDepth: 3,
	}
	seedRecordingNoDelivery(t, store, intent.Event)
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if got := store.eventTypes(); len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcherDoesNotConsumeAmbientSQLTransactionAuthority(t *testing.T) {
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
			testActiveAgentDescriptor(t, "agent-a", eventtest.UUID("ent-1"), ""),
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
			runtimebustest.DefaultRunID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	postCommitActions := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	rollbackActions := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	txctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
	txctx = runtimepipelinefixture.WithPostCommitActions(txctx, &postCommitActions)
	txctx = runtimepipelinefixture.WithRollbackActions(txctx, &rollbackActions)

	if err := commitEnginePublicationsForTest(context.Background(), eb, store, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) != 0 || len(rollbackActions) != 0 {
		_ = tx.Rollback()
		t.Fatalf("ambient transaction actions = commit:%d rollback:%d, want none", len(postCommitActions), len(rollbackActions))
	}
	got := requireBusEvent(t, ch, "closed post-commit dispatch")
	if got.ID() != intent.Event.ID() {
		t.Fatalf("delivered event id = %s, want %s", got.ID(), intent.Event.ID())
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCommittedReadinessFailureBlocksOnlyItsExactOutput(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "agent-a", eventtest.UUID("readiness-ent"), "")}...)
			eb := fixture.bus
			ch := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
			defer runtimebustest.Unsubscribe(eb, "agent-a")
			intents := make([]runtimeengine.EmitIntent, 2)
			for i := range intents {
				intents[i].Event = eventtest.ExistingRunRootIngress(eventtest.UUID(fmt.Sprintf("readiness-event-%d", i)), "custom.emitted", "", "", []byte(`{"entity_id":"readiness-ent"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("readiness-ent")), time.Now().UTC())
			}
			fault := errors.New("first committed agent readiness failed")
			seen := make([]string, 0, 2)
			eb.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(_ context.Context, event events.Event, _ []events.DeliveryRoute) error {
				seen = append(seen, event.ID())
				if event.ID() == intents[0].Event.ID() {
					return fault
				}
				return nil
			}))
			if err := fixture.commitError(intents); !errors.Is(err, fault) {
				t.Fatalf("committed finalization = %v, want first readiness fault", err)
			}
			if len(seen) != 2 {
				t.Fatalf("readiness attempted for %d outputs, want 2", len(seen))
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); !errors.Is(err, fault) {
				t.Fatalf("dispatch = %v, want exact blocked readiness error", err)
			}
			if got := requireBusEvent(t, ch, "independent ready output"); got.ID() != intents[1].Event.ID() {
				t.Fatalf("delivered %s, want second output %s", got.ID(), intents[1].Event.ID())
			}
			select {
			case delivery := <-ch:
				if delivery != nil {
					t.Fatalf("failed-readiness output dispatched: %s", delivery.Event().ID())
				}
			default:
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents[:1]); !errors.Is(err, fault) {
				t.Fatalf("repeat dispatch bypassed failed readiness: %v", err)
			}
		})
	}
}

func TestEngineDispatcherConsumesImmutableCommittedIntentWithoutAmbientQueue(t *testing.T) {
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
	originalIdentity := runtimebustest.Identity(t, "agent-original", "flow-original")
	store := &directRecipientTransactionalStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{{
			Identity: originalIdentity, EntityID: eventtest.UUID("entity-original"),
		}},
	}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	originalSubscriptions := []string{"custom.snapshot"}
	originalAdmission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: "agent-original", FlowPath: "flow-original", LocalEvents: testLocalSubscriptionEvents(originalSubscriptions), Subscriptions: originalSubscriptions,
	})
	if err != nil {
		t.Fatalf("admit original snapshot recipient: %v", err)
	}
	originalCh := runtimebustest.SubscribeIdentity(t, eb, originalIdentity, originalAdmission)
	defer runtimebustest.UnsubscribeIdentity(eb, originalIdentity)
	mutatedCh := runtimebustest.Subscribe(t, eb, "agent-mutated", events.EventType("custom.snapshot"))
	defer runtimebustest.Unsubscribe(eb, "agent-mutated")

	payload := []byte(`{"value":"original"}`)
	targetSet := []events.RouteIdentity{{FlowInstance: "flow-original", EntityID: eventtest.UUID("entity-original")}}
	recipients := []string{"agent-original"}
	intents := []runtimeengine.EmitIntent{{
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("evt-queued-snapshot"),
			events.EventType("custom.snapshot"), "", "", payload, 0, runtimebustest.DefaultRunID, "", events.EventEnvelope{TargetSet: targetSet},
			time.Now().UTC()),

		Recipients: recipients,
	}}
	postCommitActions := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	rollbackActions := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	txctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
	txctx = runtimepipelinefixture.WithPostCommitActions(txctx, &postCommitActions)
	txctx = runtimepipelinefixture.WithRollbackActions(txctx, &rollbackActions)

	if err := commitEnginePublicationsForTest(context.Background(), eb, store, intents); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(txctx, intents); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommitActions) != 0 || len(rollbackActions) != 0 {
		_ = tx.Rollback()
		t.Fatalf("ambient transaction actions = commit:%d rollback:%d, want none", len(postCommitActions), len(rollbackActions))
	}
	got := requireBusEvent(t, originalCh, "immutable committed intent delivery")
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
	if string(got.Payload()) != `{"value":"original"}` {
		t.Fatalf("delivered payload = %s, want original snapshot", string(got.Payload()))
	}
	target := got.TargetRoute()
	if target.FlowInstance != "flow-original" || target.EntityID != eventtest.UUID("entity-original") {
		t.Fatalf("delivered target route = %#v, want original snapshot", target)
	}
	requireNoBusEvent(t, mutatedCh, "mutated recipient delivery")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineDispatcherAmbientSQLContextDoesNotBypassDurableManifest(t *testing.T) {
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
	ctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
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
	if !strings.Contains(err.Error(), "prepared event settlement reader is required") {
		_ = tx.Rollback()
		t.Fatalf("DispatchPostCommit error = %q, want prepared event authority failure", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			entityID := uuid.NewString()
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", entityID, "")}...)
			recordingStore := fixture.store
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-1"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"`+entityID+`"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()), Recipients: []string{"reviewer"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			intent.Recipients = []string{"late-reviewer"}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox exact duplicate: %v", err)
			}
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
			gotPersisted, err := recordingStore.ListEventDeliveryRecipients(fixture.ctx, eventtest.UUID("evt-1"))
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(gotPersisted, ",") != "reviewer" {
				t.Fatalf("persisted recipients after exact duplicate = %v, want original [reviewer]", gotPersisted)
			}
		})
	}
}

func TestEngineOutboxExactDuplicateDispatchIsOperationNoOp(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), ""), testActiveAgentDescriptor(t, "late-reviewer", eventtest.UUID("ent-1"), "")}...)
			store := fixture.store
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			lateReviewer := runtimebustest.Subscribe(t, eb, "late-reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-exact-duplicate-operation"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			persistAndDispatch := func(intent runtimeengine.EmitIntent) {
				t.Helper()
				if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatalf("WriteOutbox: %v", err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
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
			got, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(got, ",") != "reviewer" {
				t.Fatalf("persisted recipients after exact duplicate = %v, want [reviewer]", got)
			}
		})
	}
}

func TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack(t *testing.T) {
	for _, commit := range []bool{true} {
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
				descriptors: []runtimebus.ActiveAgentDescriptor{
					testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-claim"), ""),
				},
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
					runtimebustest.DefaultRunID,
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
			rollbackActions := []runtimepipelinefixture.OwnerAction{}
			postCommitActions := []runtimepipelinefixture.OwnerAction{}
			ctx := runtimepipelinefixture.WithRollbackActions(runtimepipelinefixture.WithSQLTx(context.Background(), tx), &rollbackActions)
			ctx = runtimepipelinefixture.WithPostCommitActions(ctx, &postCommitActions)
			if err := commitEnginePublicationsForTest(ctx, eb, store, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}

			if commit {
				if err := tx.Commit(); err != nil {
					t.Fatalf("Commit: %v", err)
				}
				runtimepipelinefixture.FlushPostCommitActions(postCommitActions)
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
				runtimepipelinefixture.FlushRollbackActions(rollbackActions)
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-same-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent, intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent, intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			if evt := requireBusEvent(t, reviewer, "inserted same-batch intent"); evt.ID() != intent.Event.ID() {
				t.Fatalf("dispatched event = %q, want %q", evt.ID(), intent.Event.ID())
			}
			requireNoBusEvent(t, reviewer, "same-batch exact duplicate")
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
		})
	}
}

func TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-preexisting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			writeAndDispatch := func(intents []runtimeengine.EmitIntent) {
				t.Helper()
				if err := fixture.commitError(intents); err != nil {
					t.Fatalf("WriteOutbox: %v", err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); err != nil {
					t.Fatalf("DispatchPostCommit: %v", err)
				}
			}
			writeAndDispatch([]runtimeengine.EmitIntent{intent})
			_ = requireBusEvent(t, reviewer, "initial inserted intent")
			writeAndDispatch([]runtimeengine.EmitIntent{intent, intent, intent})
			requireNoBusEvent(t, reviewer, "preexisting exact duplicate batch")
			if got := fixture.eventCount(t); got != 1 {
				t.Fatalf("persisted events = %d, want 1", got)
			}
		})
	}
}

func TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			store := fixture.store
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-conflicting-batch"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","value":"first"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}
			conflict := intent
			conflict.Event = eventtest.ExistingRunRootIngress(intent.Event.ID(), events.EventType("custom.conflicting"), "", "", []byte(`{"entity_id":"ent-1","value":"conflict"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), intent.Event.CreatedAt())
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent, conflict}); err == nil || !strings.Contains(err.Error(), "conflicting event identity") {
				t.Fatalf("WriteOutbox conflict error = %v", err)
			}
			if got := fixture.eventCount(t); got != 0 {
				t.Fatalf("events after rollback = %d, want 0", got)
			}
			if got, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID()); err != nil || len(got) != 0 {
				t.Fatalf("deliveries after rollback = %v, %v", got, err)
			}
			writeAndDispatch := func() {
				t.Helper()
				if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatal(err)
				}
				if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
					t.Fatal(err)
				}
			}
			writeAndDispatch()
			_ = requireBusEvent(t, reviewer, "insert after rolled-back conflict")
			writeAndDispatch()
			requireNoBusEvent(t, reviewer, "exact duplicate after rolled-back conflict")
		})
	}
}

func TestEngineOutboxDistinctIntentBatchDispatchesEachOnce(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer", eventtest.UUID("ent-1"), "")}...)
			eb := fixture.bus
			reviewer := runtimebustest.Subscribe(t, eb, "reviewer")
			intents := []runtimeengine.EmitIntent{{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-distinct-1"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":1}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}, {Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-distinct-2"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1","ordinal":2}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer"}}}
			if err := fixture.commitError(intents); err != nil {
				t.Fatal(err)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, intents); err != nil {
				t.Fatal(err)
			}
			if evt := requireBusEvent(t, reviewer, "first distinct intent"); evt.ID() != intents[0].Event.ID() {
				t.Fatalf("first event = %q, want %q", evt.ID(), intents[0].Event.ID())
			}
			if evt := requireBusEvent(t, reviewer, "second distinct intent"); evt.ID() != intents[1].Event.ID() {
				t.Fatalf("second event = %q, want %q", evt.ID(), intents[1].Event.ID())
			}
			requireNoBusEvent(t, reviewer, "distinct intent over-dispatch")
		})
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
	store := &directRecipientTransactionalStore{targetRunID: runtimebustest.DefaultRunID, targetOwners: []runtimebus.ActiveTargetDescriptor{{
		ID: "review", FlowInstance: "review/inst-1", EntityID: runtimeflowidentity.EntityID("review/inst-1"),
	}}}
	flow := runtimecontracts.FlowContractView{
		Path: "review", Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Schema: runtimecontracts.FlowSchemaDocument{Instance: semanticviewtest.
			InstanceField("instance_key"),
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.started": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"target-node": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"task.started": {}}},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"review": &root.Children[0]},
	}}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	wantBlueprint := runtimebus.DeliveryRouteBlueprint{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "review", "target-node")), Target: events.RouteIdentity{
		FlowID:       "review",
		FlowInstance: "review/inst-1",
		EntityID:     runtimeflowidentity.EntityID("review/inst-1"),
	}, Handler: runtimepipeline.MustDeliveryTargetHandler(testFlowNode(t, "review", "target-node")).ForEvent("task.started")}
	want := events.DeliveryRoute{Recipient: wantBlueprint.Recipient, Target: events.MustExistingEntityTarget(wantBlueprint.Target)}
	guardSawMaterializedRoute := false
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle),
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []runtimebus.DeliveryRouteBlueprint{wantBlueprint}, nil
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
		Event: eventtest.RuntimeControl(eventtest.UUID("evt-outbox-materialized-route"),
			events.EventType("review/inst-1/task.started"), "", "", []byte(`{}`), 0, runtimebustest.DefaultRunID, "", events.EventEnvelope{}, time.Now().UTC()),
	}
	ctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
	if err := commitEnginePublicationsForTest(ctx, eb, store, []runtimeengine.EmitIntent{intent}); err != nil {
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "control-plane", "", ""), testActiveAgentDescriptor(t, "reviewer-ent-1", eventtest.UUID("ent-1"), ""), testActiveAgentDescriptor(t, "reviewer-ent-2", eventtest.UUID("ent-2"), "")}...)
			store := fixture.store
			eb := fixture.bus
			controlCh := runtimebustest.Subscribe(t, eb, "control-plane")
			matchCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-1")
			otherCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-direct-intent"), events.EventType("custom.direct"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"control-plane", "reviewer-ent-1"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			wantPersisted := []string{"control-plane", "reviewer-ent-1"}
			if strings.Join(gotPersisted, ",") != strings.Join(wantPersisted, ",") {
				t.Fatalf("persisted recipients = %v, want %v", gotPersisted, wantPersisted)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			_ = requireBusEvent(t, controlCh, "direct intent delivery to control-plane")
			evt := requireBusEvent(t, matchCh, "direct intent delivery to matching entity-scoped agent")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless matched event entity_id = %q, want empty route-local target", got)
			}
			requireNoBusEvent(t, otherCh, "direct intent delivery to filtered recipient")
		})
	}
}

func TestEngineOutboxCommittedTargetRefusalPreservesDisposition(t *testing.T) {
	store := &outboxClaimStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	intent := runtimeengine.EmitIntent{Event: eventtest.RunCreatingRootIngress(
		uuid.NewString(), "child/output.done", "", "", []byte(`{}`), 0,
		runtimebustest.DefaultRunID, "",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
			EntityID: uuid.NewString(), FlowInstance: "missing-flow",
		}), time.Now().UTC(),
	)}
	ctx := context.Background()
	if err := commitEnginePublicationsForTest(ctx, eb, store, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatal(err)
	}
	if got := store.receipts[intent.Event.ID()]; got != runtimepipelineobligation.DispositionDeadLetter {
		t.Fatalf("committed disposition = %s, want dead letter", got)
	}
	for i := 0; i < 2; i++ {
		if err := eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
			t.Fatal(err)
		}
		if got := store.receipts[intent.Event.ID()]; got != runtimepipelineobligation.DispositionDeadLetter {
			t.Fatalf("dispatch %d replaced committed refusal with %s", i, got)
		}
		store.claimMu.Lock()
		remaining := len(store.claims)
		store.claimMu.Unlock()
		if remaining != 0 {
			t.Fatalf("dispatch %d retained %d claims", i, remaining)
		}
	}
	if routes := store.routes[intent.Event.ID()]; len(routes) != 0 {
		t.Fatalf("refused publication acquired deliveries: %+v", routes)
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
			runtimebustest.DefaultRunID,
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("missing-entity"), FlowInstance: "missing-flow"}),
			time.Now().UTC(),
		),
	}
	ctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
	err = commitEnginePublicationsForTest(ctx, eb, store, []runtimeengine.EmitIntent{intent})
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "agent-a", "", "")}...)
			store := fixture.store
			eb := fixture.bus
			internalCh := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("custom.emitted"))
			agentCh := runtimebustest.Subscribe(t, eb, "agent-a", events.EventType("custom.emitted"))
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-internal-live"), events.EventType("custom.emitted"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC())}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if strings.Join(gotPersisted, ",") != "agent-a" {
				t.Fatalf("persisted recipients = %v, want [agent-a]", gotPersisted)
			}
			if err := eb.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{intent}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			evt := requireBusEvent(t, internalCh, "outbox event delivery to internal subscriber")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless internal event entity_id = %q, want empty route-local target", got)
			}
			evt = requireBusEvent(t, agentCh, "outbox event delivery to agent subscriber")
			if got := evt.TargetRoute().EntityID; got != "" {
				t.Fatalf("targetless agent event entity_id = %q, want empty route-local target", got)
			}
		})
	}
}

func TestEngineOutboxAndDispatcher_DoesNotFabricateRoutesForPendingInternalDeliveries(t *testing.T) {
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
			runtimebustest.DefaultRunID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	txctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
	if err := commitEnginePublicationsForTest(txctx, eb, store, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(interceptor.routes) != 0 {
		t.Fatalf("delivery route interceptor received fabricated pending internal routes %#v", interceptor.routes)
	}
	requireBusEvent(t, internalCh, "pending internal event without a semantic delivery route")

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
			eventtest.UUID("persisted-interceptor-run"),
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}
	seedRecordingNoDelivery(t, store, intent.Event)
	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	got := store.eventTypes()
	if len(got) == 0 || got[0] != "custom.followup" {
		t.Fatalf("persisted event types = %v, want first event custom.followup", got)
	}
}

func TestEngineDispatcherFailsClosedWithoutPreparedEventAuthority(t *testing.T) {
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
			runtimebustest.DefaultRunID,
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
			time.Now().UTC(),
		),
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil {
		t.Fatal("expected DispatchPostCommit to fail without prepared event authority")
	}
	if got := err.Error(); !strings.Contains(got, "prepared event settlement reader is required") {
		t.Fatalf("DispatchPostCommit error = %q, want missing prepared event authority", got)
	}
}

func TestEngineDispatcher_DirectIntentWithoutPersistedExactRoutesFailsClosed(t *testing.T) {
	intent := runtimeengine.EmitIntent{
		Event: eventtest.RunCreatingRootIngress(
			eventtest.UUID("evt-direct-no-tx"),
			events.EventType("custom.emitted"),
			"",
			"",
			[]byte(`{"entity_id":"ent-1"}`),
			0,
			eventtest.UUID("direct-intent-missing-route-run"),
			"",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: "root"}),
			time.Now().UTC(),
		),

		Recipients: []string{"agent-a"},
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit direct intent evaluation ledger: %v", err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatalf("admit direct intent settlement: %v", err)
	}
	store := &directRecipientTransactionalStore{
		events:      []events.Event{intent.Event},
		settlements: map[string]events.RouteSettlement{intent.Event.ID(): settlement},
		deliveries:  map[string][]string{intent.Event.ID(): {"agent-a"}},
		routes: map[string][]events.DeliveryRoute{intent.Event.ID(): {{
			Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "node-only")),
			Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
				FlowID: ".", FlowInstance: "root",
			}),
		}}},
	}
	eb, err := newScopedTestEventBus(store, runtimebus.EventBusOptions{Durable: runtimebus.ExactDurableTestDependencies(store)})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	err = eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{intent})
	if err == nil || !strings.Contains(err.Error(), "persisted agent recipients without exact identity-bearing delivery routes") {
		t.Fatalf("DispatchPostCommit error = %v, want missing exact route failure", err)
	}
}

func TestEnginePublicationRejectsFilteredDirectIntentBeforePersistence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "reviewer-ent-2", eventtest.UUID("ent-2"), "")}...)
			store := fixture.store
			eb := fixture.bus
			filteredCh := runtimebustest.Subscribe(t, eb, "reviewer-ent-2")
			intent := runtimeengine.EmitIntent{Event: eventtest.ExistingRunRootIngress(eventtest.UUID("evt-empty-direct-manifest"), events.EventType("custom.direct"), "", "", []byte(`{"entity_id":"ent-1"}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")), time.Now().UTC()), Recipients: []string{"reviewer-ent-2"}}
			if err := fixture.commitError([]runtimeengine.EmitIntent{intent}); err == nil || !strings.Contains(err.Error(), "direct delivery rejected recipients: reviewer-ent-2") {
				t.Fatalf("commit engine publications error = %v, want rejected recipient", err)
			}
			gotPersisted, err := store.ListEventDeliveryRecipients(fixture.ctx, intent.Event.ID())
			if err != nil {
				t.Fatalf("ListEventDeliveryRecipients: %v", err)
			}
			if len(gotPersisted) != 0 {
				t.Fatalf("persisted recipients = %v, want empty authoritative manifest", gotPersisted)
			}
			requireNoBusEvent(t, filteredCh, "rejected direct intent delivery")
		})
	}
}

func TestPublishDirectRejectsFilteredExplicitRecipient(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newNativeOutboxFixture(t, backend, []runtimebus.ActiveAgentDescriptor{testActiveAgentDescriptor(t, "requester-agent", "requester-entity", "provider/instance-a")}...)
			eb := fixture.bus
			evt := eventtest.ExistingRunRootIngress(eventtest.UUID("evt-filtered-direct-mutation"), events.EventType("human_task.approved"), "runtime", "", []byte(`{}`), 0, runtimebustest.DefaultRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: eventtest.UUID("wrong-entity"), FlowInstance: "wrong-flow"}), time.Now().UTC())
			err := eb.PublishDirect(fixture.ctx, evt, []string{"requester-agent"})
			if err == nil || !strings.Contains(err.Error(), "direct delivery rejected recipients: requester-agent") {
				t.Fatalf("PublishDirect error = %v", err)
			}
		})
	}
}
