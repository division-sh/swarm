package pipeline

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestApplyEngineStateMutationMirrorsDataAccumulationIntoEntityProjection(t *testing.T) {
	instance := &WorkflowInstance{
		Metadata:     map[string]any{"research_context": map[string]any{"summary": "done"}},
		StateBuckets: map[string]any{},
	}
	mutation := runtimeengine.StateMutation{
		Metadata: map[string]any{
			"research_context":              map[string]any{"summary": "done"},
			"last_data_accumulation_event": "research.completed",
			"last_data_accumulation_source": "research.completed",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "research_context", SourceField: "payload.research_context"},
			},
		},
	}
	applyEngineStateMutation(instance, mutation, map[string]struct{}{"research_context": {}})

	entityProjection, _ := workflowStateBucketObject(*instance, workflowStateBucketEntityProjection)
	got, ok := entityProjection["research_context"].(map[string]any)
	if !ok || got["summary"] != "done" {
		t.Fatalf("entity_projection research_context = %#v", entityProjection["research_context"])
	}
	if got := instance.Metadata["last_data_accumulation_event"]; got != "research.completed" {
		t.Fatalf("last_data_accumulation_event = %#v", got)
	}
}

func TestPipelineEngineDispatcherCollectsEmitIntentsWithChainDepth(t *testing.T) {
	bus := &recordingPipelineBus{}
	dispatcher := pipelineEngineDispatcher{
		coordinator: &FactoryPipelineCoordinator{bus: bus},
	}
	eventCollector := make([]events.Event, 0, 1)
	intentCollector := make([]runtimeengine.EmitIntent, 0, 1)
	ctx := context.WithValue(context.Background(), pipelineEmitCollectorKey{}, &eventCollector)
	ctx = context.WithValue(ctx, pipelineEmitIntentCollectorKey{}, &intentCollector)

	intent := runtimeengine.EmitIntent{
		Event:      events.Event{Type: events.EventType("custom.emitted")}.WithEntityID("ent-1"),
		ChainDepth: 3,
	}
	if err := dispatcher.DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if got := len(intentCollector); got != 1 {
		t.Fatalf("intent collector count = %d, want 1", got)
	}
	if got := intentCollector[0].ChainDepth; got != 3 {
		t.Fatalf("intent chain depth = %d, want 3", got)
	}
	if got := len(eventCollector); got != 1 {
		t.Fatalf("event collector count = %d, want 1", got)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0", got)
	}
}

type recordingScheduleStore struct {
	upserts []Schedule
	cancels []Schedule
}

func (s *recordingScheduleStore) UpsertSchedule(_ context.Context, sc Schedule) error {
	s.upserts = append(s.upserts, sc)
	return nil
}
func (s *recordingScheduleStore) CancelSchedule(context.Context, string, string) error { return nil }
func (s *recordingScheduleStore) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (s *recordingScheduleStore) MarkScheduleFired(context.Context, Schedule) error { return nil }
func (s *recordingScheduleStore) CancelScheduleExact(_ context.Context, sc Schedule) error {
	s.cancels = append(s.cancels, sc)
	return nil
}
func (s *recordingScheduleStore) MarkScheduleFiredExact(context.Context, Schedule) error { return nil }

func TestPipelineEngineTimerApplierPersistsTimersAndDefersSchedulerToPostCommit(t *testing.T) {
	store := &recordingScheduleStore{}
	scheduler := NewScheduler()
	defer scheduler.Stop()
	pc := &FactoryPipelineCoordinator{
		timerScheduler:     scheduler,
		timerScheduleStore: store,
	}
	actions := make([]func(), 0, 2)
	ctx := withPipelinePostCommitActions(context.Background(), &actions)
	sc := Schedule{
		AgentID:    "owner",
		EventType:  "timer.review",
		Mode:       "once",
		At:         time.Now().Add(time.Hour),
		VerticalID: "ent-1",
		TaskID:     "timer-1",
	}

	pc.persistWorkflowTimerSchedule(ctx, sc)
	if got := len(store.upserts); got != 1 {
		t.Fatalf("persisted schedules = %d, want 1", got)
	}
	if got := len(actions); got != 1 {
		t.Fatalf("post-commit actions = %d, want 1", got)
	}
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks before flush = %d, want 0", got)
	}
	flushPipelinePostCommitActions(actions)
	if got := len(scheduler.tasks); got != 1 {
		t.Fatalf("scheduler tasks after flush = %d, want 1", got)
	}

	cancelActions := make([]func(), 0, 1)
	cancelCtx := withPipelinePostCommitActions(context.Background(), &cancelActions)
	pc.persistWorkflowTimerCancellation(cancelCtx, sc)
	if got := len(store.cancels); got != 1 {
		t.Fatalf("persisted cancels = %d, want 1", got)
	}
	if got := len(cancelActions); got != 1 {
		t.Fatalf("cancel post-commit actions = %d, want 1", got)
	}
	flushPipelinePostCommitActions(cancelActions)
	if got := len(scheduler.tasks); got != 0 {
		t.Fatalf("scheduler tasks after cancel flush = %d, want 0", got)
	}
}

func TestPipelineEngineOutboxPersistsEventsAndDeliveriesInTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").
		WithArgs("evt-1", "custom.emitted", "", "", "", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO event_deliveries").
		WithArgs("evt-1", "reviewer").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ctx := withSQLTxContext(context.Background(), tx)
	outbox := pipelineEngineOutbox{}
	intent := runtimeengine.EmitIntent{
		Event: events.Event{
			ID:        "evt-1",
			Type:      events.EventType("custom.emitted"),
			Payload:   mustJSON(map[string]any{"entity_id": "ent-1"}),
			CreatedAt: time.Now().UTC(),
		}.WithEntityID("ent-1"),
		Recipients: []string{"reviewer"},
	}
	if err := outbox.WriteOutbox(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
