package pipeline_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

type recoveryCapturePublisher struct {
	inner     runtimepipeline.Publisher
	published []events.Event
	direct    []events.Event
}

type recoveryMissingClaimStore struct {
	events      []events.PersistedReplayEvent
	deliveries  map[string][]string
	receiptByID map[string]string
}

func (p *recoveryCapturePublisher) Publish(ctx context.Context, evt events.Event) error {
	p.published = append(p.published, evt)
	return p.inner.Publish(ctx, evt)
}

func (p *recoveryCapturePublisher) PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error {
	p.direct = append(p.direct, evt)
	return p.inner.PublishPersistedRecipients(ctx, evt, recipients)
}

func (s *recoveryMissingClaimStore) AppendEvent(context.Context, events.Event) error { return nil }
func (s *recoveryMissingClaimStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (s *recoveryMissingClaimStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.events...), nil
}
func (s *recoveryMissingClaimStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), s.deliveries[eventID]...), nil
}
func (s *recoveryMissingClaimStore) UpsertPipelineReceipt(_ context.Context, eventID, status, _ string) error {
	if s.receiptByID == nil {
		s.receiptByID = map[string]string{}
	}
	s.receiptByID[eventID] = status
	return nil
}

type blockingRecoveryPublisher struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	count   atomic.Int32
}

func (*blockingRecoveryPublisher) Publish(context.Context, events.Event) error { return nil }

func (p *blockingRecoveryPublisher) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	if p.count.Add(1) == 1 {
		close(p.started)
		<-p.release
	}
	return nil
}

func TestRecoveryManager_ReplaysPersistedCorrelationEnvelope(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}
	recipientID := "agent-recovery"
	recipientCh := bus.Subscribe(recipientID)

	runID := uuid.NewString()
	parentID := uuid.NewString()
	childID := uuid.NewString()

	parent := events.Event{
		ID:          parentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}
	child := events.Event{
		ID:            childID,
		Type:          events.EventType("system.recover"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         runID,
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute).UTC(),
	}

	if err := pg.AppendEvent(ctx, parent); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, childID, []string{recipientID}); err != nil {
		t.Fatalf("InsertEventDeliveries(child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}

	var runsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsBefore); err != nil {
		t.Fatalf("count runs before recovery: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 {
		t.Fatalf("direct replayed events = %#v, want one replayed event", capture.direct)
	}
	replayed := capture.direct[0]
	if replayed.ID != childID {
		t.Fatalf("replayed event id = %q, want %q", replayed.ID, childID)
	}
	if replayed.RunID != runID {
		t.Fatalf("replayed run_id = %q, want %q", replayed.RunID, runID)
	}
	if replayed.ParentEventID != parentID {
		t.Fatalf("replayed parent_event_id = %q, want %q", replayed.ParentEventID, parentID)
	}
	select {
	case delivered := <-recipientCh:
		if delivered.ID != childID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID, childID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected persisted recipient to receive replayed event")
	}

	var runsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsAfter); err != nil {
		t.Fatalf("count runs after recovery: %v", err)
	}
	if runsAfter != runsBefore {
		t.Fatalf("run rows after recovery = %d, want %d", runsAfter, runsBefore)
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, childID).Scan(&receiptStatus); err != nil {
		t.Fatalf("load child receipt: %v", err)
	}
	if receiptStatus != "success" {
		t.Fatalf("child receipt outcome = %q, want success", receiptStatus)
	}
}

func TestRecoveryManager_ReplaysInternalSubscribersWithEmptyPersistedManifest(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}
	internalCh := bus.SubscribeInternal("workflow-runtime", events.EventType("system.recover.internal"))

	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.recover.internal"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-time.Minute).UTC(),
	}.WithEntityID(entityID)

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 {
		t.Fatalf("direct replayed events = %#v, want one replayed event", capture.direct)
	}
	select {
	case delivered := <-internalCh:
		if delivered.ID != eventID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected internal subscriber to receive replayed event")
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&receiptStatus); err != nil {
		t.Fatalf("load replay receipt: %v", err)
	}
	if receiptStatus != "success" {
		t.Fatalf("replay receipt outcome = %q, want success", receiptStatus)
	}
}

func TestRecoveryManager_QuarantinesMissingPersistedRunIDAndContinues(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	badEventID := uuid.NewString()
	goodRunID := uuid.NewString()
	goodParentID := uuid.NewString()
	goodEventID := uuid.NewString()
	goodRecipientID := "agent-good"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, 'system.recover', 'global', '{}'::jsonb, 'runtime', 'platform', now()
		)
	`, badEventID); err != nil {
		t.Fatalf("seed malformed event: %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          goodParentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       goodRunID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(good parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            goodEventID,
		Type:          events.EventType("system.recover.good"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         goodRunID,
		ParentEventID: goodParentID,
		CreatedAt:     time.Now().Add(-1 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(good child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, goodEventID, []string{goodRecipientID}); err != nil {
		t.Fatalf("InsertEventDeliveries(good child): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, goodParentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(good parent): %v", err)
	}
	goodRecipientCh := bus.Subscribe(goodRecipientID)

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 {
		t.Fatalf("direct replayed events = %#v, want one valid replay", capture.direct)
	}
	if capture.direct[0].ID != goodEventID {
		t.Fatalf("published event id = %q, want %q", capture.direct[0].ID, goodEventID)
	}
	select {
	case delivered := <-goodRecipientCh:
		if delivered.ID != goodEventID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID, goodEventID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected valid replay recipient to receive event")
	}

	var badOutcome, badReason string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, badEventID).Scan(&badOutcome, &badReason); err != nil {
		t.Fatalf("load bad receipt: %v", err)
	}
	if badOutcome != "dead_letter" {
		t.Fatalf("bad receipt outcome = %q, want dead_letter", badOutcome)
	}
	if badReason != "pipeline_error" {
		t.Fatalf("bad receipt reason = %q, want pipeline_error", badReason)
	}
}

func TestRecoveryManager_QuarantinesMissingRunIDSchemaCapability(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	if _, err := db.ExecContext(ctx, `ALTER TABLE events DROP COLUMN run_id`); err != nil {
		t.Fatalf("drop events.run_id: %v", err)
	}

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	eventID := uuid.NewString()
	parentID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:            eventID,
		Type:          events.EventType("system.recover.no-run-id"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 0 {
		t.Fatalf("direct replayed events = %#v, want none", capture.direct)
	}

	var outcome, reason string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load receipt: %v", err)
	}
	if outcome != "dead_letter" {
		t.Fatalf("receipt outcome = %q, want dead_letter", outcome)
	}
	if reason != "pipeline_error" {
		t.Fatalf("receipt reason = %q, want pipeline_error", reason)
	}
}

func TestRecoveryManager_ClaimsReplayOwnershipUnderOverlap(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg1 := &store.PostgresStore{DB: db}
	pg2 := &store.PostgresStore{DB: db}

	runID := uuid.NewString()
	parentID := uuid.NewString()
	childID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if err := pg1.AppendEvent(ctx, events.Event{
		ID:          parentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"ok":true}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg1.AppendEvent(ctx, events.Event{
		ID:            childID,
		Type:          events.EventType("system.recover"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"ok":true}`),
		RunID:         runID,
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg1.InsertEventDeliveries(ctx, childID, []string{"agent-recovery"}); err != nil {
		t.Fatalf("InsertEventDeliveries(child): %v", err)
	}
	if err := pg1.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}

	publisher := &blockingRecoveryPublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rm1 := runtimepipeline.NewRecoveryManagerWith(pg1, publisher)
	rm2 := runtimepipeline.NewRecoveryManagerWith(pg2, publisher)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- rm1.Recover(ctx)
	}()

	select {
	case <-publisher.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first recovery replay to start")
	}

	if err := rm2.Recover(ctx); err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if got := publisher.count.Load(); got != 1 {
		t.Fatalf("replay publish count during overlap = %d, want 1", got)
	}

	close(publisher.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	if got := publisher.count.Load(); got != 1 {
		t.Fatalf("replay publish count after overlap = %d, want 1", got)
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, childID).Scan(&receiptStatus); err != nil {
		t.Fatalf("load pipeline receipt: %v", err)
	}
	if receiptStatus != "success" {
		t.Fatalf("pipeline receipt outcome = %q, want success", receiptStatus)
	}
}

func TestRecoveryManager_UsesPersistedDeliveryRecipientsInsteadOfCurrentSubscriptions(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	runID := uuid.NewString()
	parentID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()

	if err := pg.AppendEvent(ctx, events.Event{
		ID:          parentID,
		Type:        events.EventType("system.parent"),
		SourceAgent: "runtime",
		Payload:     []byte(`{"entity_id":"` + entityID + `"}`),
		RunID:       runID,
		CreatedAt:   time.Now().Add(-2 * time.Minute).UTC(),
	}.WithEntityID(entityID)); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}
	child := (events.Event{
		ID:            eventID,
		Type:          events.EventType("system.recover.explicit"),
		SourceAgent:   "runtime",
		Payload:       []byte(`{"entity_id":"` + entityID + `"}`),
		RunID:         runID,
		ParentEventID: parentID,
		CreatedAt:     time.Now().Add(-time.Minute).UTC(),
	}).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"agent-a"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}

	agentA := bus.Subscribe("agent-a")
	agentB := bus.Subscribe("agent-b", events.EventType("system.recover.explicit"))

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 || capture.direct[0].ID != eventID {
		t.Fatalf("direct replayed events = %#v, want [%s]", capture.direct, eventID)
	}
	select {
	case evt := <-agentA:
		if evt.ID != eventID {
			t.Fatalf("agent-a replayed event id = %q, want %q", evt.ID, eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected agent-a to receive replayed event")
	}
	select {
	case evt := <-agentB:
		t.Fatalf("agent-b should not receive replayed event from current subscription drift: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}

	var receiptStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&receiptStatus); err != nil {
		t.Fatalf("load child receipt: %v", err)
	}
	if receiptStatus != "success" {
		t.Fatalf("child receipt outcome = %q, want success", receiptStatus)
	}
}

func TestRecoveryManager_FailsClosedWithoutReplayClaimOwner(t *testing.T) {
	store := &recoveryMissingClaimStore{
		events: []events.PersistedReplayEvent{
			{Event: events.Event{
				ID:        "evt-missing-claim",
				Type:      events.EventType("system.recover"),
				Payload:   []byte(`{"ok":true}`),
				CreatedAt: time.Now().UTC(),
			}},
		},
		deliveries: map[string][]string{"evt-missing-claim": {"agent-a"}},
	}
	rm := runtimepipeline.NewRecoveryManagerWith(store, &recoveryCapturePublisher{})

	err := rm.Recover(context.Background())
	if err == nil {
		t.Fatal("expected Recover to fail without replay claim owner")
	}
	if got := err.Error(); got != "recover pipeline receipts: store does not support explicit pipeline replay claims" {
		t.Fatalf("Recover error = %q, want explicit replay claim owner failure", got)
	}
}

func TestRecoveryManager_NoopsWithoutPersistedReplayStore(t *testing.T) {
	capture := &recoveryCapturePublisher{}
	rm := runtimepipeline.NewRecoveryManagerWith(runtimebus.InMemoryEventStore{}, capture)

	if err := rm.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.published) != 0 || len(capture.direct) != 0 {
		t.Fatalf("recovery capture = published:%#v direct:%#v, want no replay activity", capture.published, capture.direct)
	}
}
