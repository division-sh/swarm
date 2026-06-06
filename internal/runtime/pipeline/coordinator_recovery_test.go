package pipeline_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recoveryCapturePublisher struct {
	inner     runtimepipeline.Publisher
	published []events.Event
	direct    []events.Event
	logs      []runtimepipeline.RuntimeLogEntry
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

func (p *recoveryCapturePublisher) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	p.logs = append(p.logs, entry)
	return nil
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
	logs    []runtimepipeline.RuntimeLogEntry
}

func (*blockingRecoveryPublisher) Publish(context.Context, events.Event) error { return nil }

func (p *blockingRecoveryPublisher) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	if p.count.Add(1) == 1 {
		close(p.started)
		<-p.release
	}
	return nil
}

func (p *blockingRecoveryPublisher) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logs = append(p.logs, entry)
	return nil
}

func recoveryDetailString(v any) string {
	if text, ok := v.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace("")
}

func findRecoveryAftermathLog(t *testing.T, logs []runtimepipeline.RuntimeLogEntry, eventID, outcome, reason string) runtimepipeline.RuntimeLogEntry {
	t.Helper()
	for _, entry := range logs {
		if strings.TrimSpace(entry.Action) != "startup_recovery_pipeline_replay_aftermath" {
			continue
		}
		if strings.TrimSpace(entry.EventID) != strings.TrimSpace(eventID) {
			continue
		}
		detail, _ := entry.Detail.(map[string]any)
		if got := recoveryDetailString(detail["decision_outcome"]); got != strings.TrimSpace(outcome) {
			continue
		}
		if got := recoveryDetailString(detail["decision_reason_code"]); got != strings.TrimSpace(reason) {
			continue
		}
		return entry
	}
	t.Fatalf("missing recovery aftermath log for event=%q outcome=%q reason=%q in %#v", eventID, outcome, reason, logs)
	return runtimepipeline.RuntimeLogEntry{}
}

func persistCommittedReplayScope(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID string, scope runtimereplayclaim.CommittedReplayScope) {
	t.Helper()
	if err := pg.UpsertCommittedReplayScope(ctx, eventID, scope); err != nil {
		t.Fatalf("UpsertCommittedReplayScope(%s): %v", eventID, err)
	}
}

func seedHistoricalReplayRecoverySourceRun(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'fork.ready', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, eventID, entityID, at); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'recovery-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Recovery Entity"'::jsonb, $3::uuid, 'platform', 'recovery-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed source mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Recovery Entity',
			'ready', '{}'::jsonb, '{"name":"Recovery Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
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

	parent := events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC())

	child := events.NewProjectionEvent(childID,
		events.EventType("system.recover"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-1*time.Minute).UTC())

	if err := pg.AppendEvent(ctx, parent); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, childID, []string{recipientID}); err != nil {
		t.Fatalf("InsertEventDeliveries(child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, childID, runtimereplayclaim.CommittedReplayScopeSubscribed)
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
	if replayed.ID() != childID {
		t.Fatalf("replayed event id = %q, want %q", replayed.ID(), childID)
	}
	if replayed.RunID() != runID {
		t.Fatalf("replayed run_id = %q, want %q", replayed.RunID(), runID)
	}
	if replayed.ParentEventID() != parentID {
		t.Fatalf("replayed parent_event_id = %q, want %q", replayed.ParentEventID(), parentID)
	}
	select {
	case delivered := <-recipientCh:
		if delivered.ID() != childID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID(), childID)
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
	logEntry := findRecoveryAftermathLog(t, capture.logs, childID, "replayed", "persisted_recipients_replayed")
	if logEntry.Level != "info" {
		t.Fatalf("recovery aftermath level = %q, want info", logEntry.Level)
	}
	if logEntry.EventType != string(child.Type()) {
		t.Fatalf("recovery aftermath event_type = %q, want %q", logEntry.EventType, child.Type())
	}
	detail, _ := logEntry.Detail.(map[string]any)
	recipients, _ := detail["persisted_recipients"].([]string)
	if len(recipients) != 1 || recipients[0] != recipientID {
		t.Fatalf("persisted_recipients = %#v, want [%q]", detail["persisted_recipients"], recipientID)
	}
}

func TestRecoveryManager_ReplaysHistoricalForkDeliveryEventReplayRows(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700001200, 0).UTC()
	seedHistoricalReplayRecoverySourceRun(t, db, sourceRunID, entityID, sourceEventID, at)

	var sourceDeliveryID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id,
			status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'source_pending', $3)
		RETURNING delivery_id::text
	`, sourceRunID, sourceEventID, at).Scan(&sourceDeliveryID); err != nil {
		t.Fatalf("seed source pending delivery: %v", err)
	}

	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          sourceEventID,
	})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	activated, err := pg.ActivateRunFork(ctx, store.RunForkActivateRequest{
		ForkRunID:                         materialized.ForkRunID,
		HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{},
	})
	if err != nil {
		t.Fatalf("ActivateRunFork: %v", err)
	}
	if activated.DeliveryEventReplay == nil || activated.DeliveryEventReplay.ReplayedEventCount != 1 || activated.DeliveryEventReplay.ReplayedDeliveryCount != 1 {
		t.Fatalf("DeliveryEventReplay = %#v, want one fork-local event and delivery", activated.DeliveryEventReplay)
	}

	var forkEventID, forkDeliveryID string
	if err := db.QueryRowContext(ctx, `
		SELECT fork_event_id::text, fork_delivery_id::text
		FROM run_fork_delivery_event_replays
		WHERE fork_run_id = $1::uuid
		  AND source_delivery_id = $2::uuid
	`, materialized.ForkRunID, sourceDeliveryID).Scan(&forkEventID, &forkDeliveryID); err != nil {
		t.Fatalf("load fork replay lineage: %v", err)
	}
	if forkEventID == sourceEventID || forkDeliveryID == sourceDeliveryID {
		t.Fatalf("fork replay reused source ids: fork_event=%s fork_delivery=%s", forkEventID, forkDeliveryID)
	}

	scope, err := pg.LoadCommittedReplayScope(ctx, forkEventID)
	if err != nil {
		t.Fatalf("LoadCommittedReplayScope(fork event): %v", err)
	}
	if scope != runtimereplayclaim.CommittedReplayScopeDirect {
		t.Fatalf("fork replay scope = %q, want direct", scope)
	}
	if err := pg.UpsertPipelineReceipt(ctx, sourceEventID, "processed", "source_outcome_after_fork"); err != nil {
		t.Fatalf("mark source pipeline receipt: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', 'safe-agent', e.entity_id, e.flow_instance,
			'dead_letter', 'source_outcome_after_fork', '{}'::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
	`, sourceEventID); err != nil {
		t.Fatalf("seed source agent dead-letter receipt: %v", err)
	}

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}
	safeAgent := bus.Subscribe("safe-agent", events.EventType("fork.ready"))
	currentOnly := bus.Subscribe("current-only-agent", events.EventType("fork.ready"))

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 {
		t.Fatalf("direct replayed events = %#v, want one fork-local replay", capture.direct)
	}
	replayed := capture.direct[0]
	if replayed.ID() != forkEventID || replayed.RunID() != materialized.ForkRunID {
		t.Fatalf("replayed event = id:%s run:%s, want fork event %s run %s", replayed.ID(), replayed.RunID(), forkEventID, materialized.ForkRunID)
	}
	select {
	case evt := <-safeAgent:
		if evt.ID() != forkEventID || evt.RunID() != materialized.ForkRunID {
			t.Fatalf("safe-agent received = id:%s run:%s, want fork event %s run %s", evt.ID(), evt.RunID(), forkEventID, materialized.ForkRunID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected safe-agent to receive recovered fork replay event")
	}
	select {
	case evt := <-currentOnly:
		t.Fatalf("current-only subscription should not receive direct fork replay: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}

	var forkPipelineOutcome, forkPipelineReason string
	if err := db.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, forkEventID).Scan(&forkPipelineOutcome, &forkPipelineReason); err != nil {
		t.Fatalf("load fork pipeline receipt: %v", err)
	}
	if forkPipelineOutcome != "success" || forkPipelineReason != "pipeline_persisted" {
		t.Fatalf("fork pipeline receipt = outcome:%s reason:%s, want success/pipeline_persisted", forkPipelineOutcome, forkPipelineReason)
	}

	var sourceDeliveryRun, sourceDeliveryStatus, sourceAgentOutcome string
	if err := db.QueryRowContext(ctx, `
		SELECT d.run_id::text, d.status, r.outcome
		FROM event_deliveries d
		JOIN event_receipts r
		  ON r.event_id = d.event_id
		 AND r.subscriber_type = 'agent'
		 AND r.subscriber_id = d.subscriber_id
		WHERE d.delivery_id = $1::uuid
	`, sourceDeliveryID).Scan(&sourceDeliveryRun, &sourceDeliveryStatus, &sourceAgentOutcome); err != nil {
		t.Fatalf("load source delivery/receipt after recovery: %v", err)
	}
	if sourceDeliveryRun != sourceRunID || sourceDeliveryStatus != "pending" || sourceAgentOutcome != "dead_letter" {
		t.Fatalf("source state changed or suppressed recovery = run:%s status:%s outcome:%s", sourceDeliveryRun, sourceDeliveryStatus, sourceAgentOutcome)
	}

	logEntry := findRecoveryAftermathLog(t, capture.logs, forkEventID, "replayed", "persisted_recipients_replayed")
	detail, _ := logEntry.Detail.(map[string]any)
	if got := recoveryDetailString(detail["persisted_run_id"]); got != materialized.ForkRunID {
		t.Fatalf("recovery persisted_run_id = %q, want fork run %q", got, materialized.ForkRunID)
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
	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(goodParentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, goodRunID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(good parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(goodEventID,
		events.EventType("system.recover.good"),
		"runtime", "", []byte(`{"ok":true}`), 0, goodRunID,
		goodParentID, events.EventEnvelope{}, time.Now().Add(-1*time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(good child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, goodEventID, []string{goodRecipientID}); err != nil {
		t.Fatalf("InsertEventDeliveries(good child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, goodEventID, runtimereplayclaim.CommittedReplayScopeSubscribed)
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
	if capture.direct[0].ID() != goodEventID {
		t.Fatalf("published event id = %q, want %q", capture.direct[0].ID(), goodEventID)
	}
	select {
	case delivered := <-goodRecipientCh:
		if delivered.ID() != goodEventID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID(), goodEventID)
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
	logEntry := findRecoveryAftermathLog(t, capture.logs, badEventID, "dropped", "replay_quarantined")
	if logEntry.Level != "warn" {
		t.Fatalf("recovery aftermath level = %q, want warn", logEntry.Level)
	}
	if got := strings.TrimSpace(logEntry.Error); got == "" {
		t.Fatal("expected dropped recovery aftermath log to carry explicit error text")
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
	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(eventID,
		events.EventType("system.recover.no-run-id"),
		"runtime", "", []byte(`{"ok":true}`), 0, "", parentID, events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
	); err != nil {
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
	if err := pg1.AppendEvent(ctx, events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg1.AppendEvent(ctx, events.NewProjectionEvent(childID,
		events.EventType("system.recover"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg1.InsertEventDeliveries(ctx, childID, []string{"agent-recovery"}); err != nil {
		t.Fatalf("InsertEventDeliveries(child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg1, childID, runtimereplayclaim.CommittedReplayScopeSubscribed)
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
	findRecoveryAftermathLog(t, publisher.logs, childID, "skipped", "replay_claim_not_acquired")
}

func TestRecoveryManager_ExplicitlySkipsReplayWithoutPersistedRecipients(t *testing.T) {
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
	childID := uuid.NewString()

	parent := events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC())

	child := events.NewProjectionEvent(childID,
		events.EventType("system.recover.no_recipients"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-1*time.Minute).UTC())

	if err := pg.AppendEvent(ctx, parent); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, childID, runtimereplayclaim.CommittedReplayScopeDirect)
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 0 {
		t.Fatalf("direct replayed events = %#v, want none", capture.direct)
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
	findRecoveryAftermathLog(t, capture.logs, childID, "skipped", "no_persisted_recipients")
}

func TestRecoveryManager_DoesNotEmitAftermathLogForRuntimeLogReplayCandidate(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	capture := &recoveryCapturePublisher{inner: bus}

	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, 'platform.runtime_log', 'global', '{"message":"prior diagnostics"}'::jsonb, 'runtime', 'platform', now()
		)
	`, eventID); err != nil {
		t.Fatalf("seed runtime log event: %v", err)
	}

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.logs) != 0 {
		t.Fatalf("runtime log aftermath entries = %#v, want none", capture.logs)
	}
	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&receiptCount); err != nil {
		t.Fatalf("count runtime log receipt: %v", err)
	}
	if receiptCount != 0 {
		t.Fatalf("runtime log pipeline receipts = %d, want 0", receiptCount)
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

	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"entity_id":"`+entityID+`"}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()).
		WithEntityID(entityID)); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}
	child := (events.NewProjectionEvent(eventID,
		events.EventType("system.recover.explicit"),
		"runtime", "", []byte(`{"entity_id":"`+entityID+`"}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC())).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, child); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, eventID, []string{"agent-a"}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, eventID, runtimereplayclaim.CommittedReplayScopeDirect)

	agentA := bus.Subscribe("agent-a")
	agentB := bus.Subscribe("agent-b", events.EventType("system.recover.explicit"))

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 || capture.direct[0].ID() != eventID {
		t.Fatalf("direct replayed events = %#v, want [%s]", capture.direct, eventID)
	}
	select {
	case evt := <-agentA:
		if evt.ID() != eventID {
			t.Fatalf("agent-a replayed event id = %q, want %q", evt.ID(), eventID)
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

func TestRecoveryManager_ReplaysSubscribedInternalOnlyUsingCommittedReplayScope(t *testing.T) {
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

	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(eventID,
		events.EventType("system.recover.internal"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, eventID, runtimereplayclaim.CommittedReplayScopeSubscribed)

	internalCh := bus.SubscribeInternal("workflow-runtime", events.EventType("system.recover.internal"))

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 1 || capture.direct[0].ID() != eventID {
		t.Fatalf("direct replayed events = %#v, want [%s]", capture.direct, eventID)
	}
	select {
	case evt := <-internalCh:
		if evt.ID() != eventID {
			t.Fatalf("internal replayed event id = %q, want %q", evt.ID(), eventID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected internal subscriber to receive replayed subscribed event")
	}
}

func TestRecoveryManager_DirectEmptyManifestDoesNotBroadenToCurrentInternalSubscribers(t *testing.T) {
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

	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(parentID,
		events.EventType("system.parent"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().Add(-2*time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(parent): %v", err)
	}
	if err := pg.UpsertPipelineReceipt(ctx, parentID, "processed", ""); err != nil {
		t.Fatalf("UpsertPipelineReceipt(parent): %v", err)
	}
	if err := pg.AppendEvent(ctx, events.NewProjectionEvent(eventID,
		events.EventType("system.recover.direct_empty"),
		"runtime", "", []byte(`{"ok":true}`), 0, runID,
		parentID, events.EventEnvelope{}, time.Now().Add(-time.Minute).UTC()),
	); err != nil {
		t.Fatalf("AppendEvent(child): %v", err)
	}
	persistCommittedReplayScope(t, ctx, pg, eventID, runtimereplayclaim.CommittedReplayScopeDirect)

	internalCh := bus.SubscribeInternal("workflow-runtime", events.EventType("system.recover.direct_empty"))

	rm := runtimepipeline.NewRecoveryManagerWith(pg, capture)
	if err := rm.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(capture.direct) != 0 {
		t.Fatalf("direct replayed events = %#v, want none", capture.direct)
	}
	select {
	case evt := <-internalCh:
		t.Fatalf("direct replay should not broaden to internal subscriber: %#v", evt)
	case <-time.After(50 * time.Millisecond):
	}
	findRecoveryAftermathLog(t, capture.logs, eventID, "skipped", "no_persisted_recipients")
}

func TestRecoveryManager_FailsClosedWithoutReplayClaimOwner(t *testing.T) {
	store := &recoveryMissingClaimStore{
		events: []events.PersistedReplayEvent{
			{Event: events.NewProjectionEvent("evt-missing-claim",
				events.EventType("system.recover"), "", "", []byte(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
			},
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
