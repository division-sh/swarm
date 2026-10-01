package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestActivityResultPublicationEvidenceBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, db, ctx := backend.open(t)
			ctx = testAuthorActivityContext()
			sibling := uuid.NewString()
			requireRunningRunForTest(t, ctx, selected, sibling, time.Now().UTC())
			journal := selected.(pipeline.ActivityAttemptJournal)
			fault := errors.New("post-acknowledgment cleanup")
			for _, invalid := range []any{nil, db, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
				if got, err := ObserveActivityResultPublicationStorageForTest(ctx, invalid); err == nil || got != (ActivityResultPublicationStorage{}) {
					t.Fatalf("evidence accepted raw/nil authority %T", invalid)
				}
				if p, count, err := ActivityJournalCleanupPersistenceFaultForTest(invalid, fault); err == nil || p.Configured() || count != nil {
					t.Fatalf("cleanup fault accepted raw/nil authority %T", invalid)
				}
			}
			if p, count, err := ActivityJournalCleanupPersistenceFaultForTest(selected, nil); err == nil || p.Configured() || count != nil {
				t.Fatal("absent cleanup fault accepted")
			}
			start := pipeline.ActivityAttemptRecord{
				RequestEventID: uuid.NewString(), RunID: sibling, ExecutionMode: executionmode.Live,
				ActivityID: "publication-evidence", Tool: "provider_write", EffectClass: "non_idempotent_write",
				Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "exact",
			}
			started, inserted, err := journal.StartActivityAttempt(ctx, start)
			if err != nil || !inserted {
				t.Fatalf("start selected journal: inserted=%t err=%v", inserted, err)
			}
			started.Status = pipeline.ActivityAttemptStatusSucceeded
			started.ResultEventID, started.ResultEventType = uuid.NewString(), start.SuccessEvent
			started.ResultPayload = map[string]any{"ok": true}
			journalWithFault, err := activityJournalCleanupFaultForTest(selected, fault)
			if err != nil {
				t.Fatal(err)
			}
			// The exact selected journal owns the actual commit verdict. The
			// wrapper can add an error only after that acknowledgment.
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, acknowledged, err := journalWithFault.CompleteActivityAttempt(canceled, started); acknowledged || !errors.Is(err, context.Canceled) || errors.Is(err, fault) || journalWithFault.acknowledgments.Load() != 0 {
				t.Fatalf("canceled commit invented acknowledgment: %t err=%v", acknowledged, err)
			}
			receipt, acknowledged, err := journalWithFault.CompleteActivityAttempt(ctx, started)
			if !acknowledged || !errors.Is(err, fault) || receipt.CompletedAt == nil || journalWithFault.acknowledgments.Load() != 1 {
				t.Fatalf("cleanup changed commit verdict: %t err=%v", acknowledged, err)
			}
			stored, found, err := journal.LoadActivityAttempt(ctx, started.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(receipt, stored) {
				t.Fatal("cleanup lost the real durable receipt")
			}
			connections := db.Stats().InUse
			got, err := ObserveActivityResultPublicationStorageForTest(ctx, selected)
			if err != nil || got.Runs != 2 || got.ActivityAttempts != 1 || got.SuccessfulActivityAttempts != 1 {
				t.Fatalf("evidence omitted sibling run or attempt: %+v err=%v", got, err)
			}
			var physical ActivityResultPublicationStorage
			for _, row := range []struct {
				table string
				count *int
			}{
				{"runs", &physical.Runs}, {"events", &physical.Events}, {"event_deliveries", &physical.Deliveries},
				{"entity_state", &physical.Entities}, {"api_idempotency", &physical.Receipts}, {"conversation_forks", &physical.ConversationForks},
				{"runtime_reset_operations", &physical.ResetOperations}, {"activity_attempts", &physical.ActivityAttempts},
			} {
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+row.table).Scan(row.count); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_attempts WHERE status='succeeded'").Scan(&physical.SuccessfulActivityAttempts); err != nil || physical != got {
				t.Fatalf("evidence differs from storage: %+v vs %+v err=%v", got, physical, err)
			}
			if partial, err := ObserveActivityResultPublicationStorageForTest(canceled, selected); !errors.Is(err, context.Canceled) || partial != (ActivityResultPublicationStorage{}) {
				t.Fatalf("canceled read returned evidence: %+v err=%v", partial, err)
			}
			got.Runs = 999
			if fresh, err := ObserveActivityResultPublicationStorageForTest(ctx, selected); err != nil || fresh != physical || db.Stats().InUse != connections {
				t.Fatal("readback mutated storage or retained a connection")
			}
		})
	}
}
