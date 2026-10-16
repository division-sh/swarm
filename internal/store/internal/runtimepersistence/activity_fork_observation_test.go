package runtimepersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestActivityForkStoragePreservesExactFactsAndSeparateCutsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			entity := uuid.NewString()
			if err := SeedSelectedSourceEntityForTest(ctx, selected, SelectedSourceEntityFixture{
				RunID: event.RunID(), EventID: event.ID(), EntityID: entity, EntityType: "fork_observation", CreatedAt: time.Unix(1700002210, 0).UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			source, err := ReadActivityForkSourceStorageForTest(ctx, selected, event.RunID(), entity, event.ID())
			if err != nil || source.Flow != "flow-a/1" || source.EntityType != "fork_observation" || source.Stage != "pending" || !bytes.Equal(source.Fields, []byte(`{}`)) || !bytes.Equal(source.Accumulator, []byte(`{}`)) || !bytes.Equal(source.Payload, event.Payload()) || source.AttemptStatus != "" {
				t.Fatalf("source physical fields/raw bytes/missing status changed: %+v/%v", source, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("source cuts merged or escaped their original selected reader: %+v", counts)
			}
			if got, err := ReadActivityForkSourceStorageForTest(ctx, selected, event.RunID(), entity, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, ActivityForkSourceStorage{}) {
				t.Fatalf("late missing event returned a provisional first-stage source: %+v/%v", got, err)
			}
			raw, err := ReadActivityForkRequestPayloadStorageForTest(ctx, selected, event.RunID(), event.ID())
			physical, physicalErr := ReadPublicationEventProjectionForTest(ctx, selected, event.ID())
			if err != nil || physicalErr != nil || !bytes.Equal(raw, physical.Payload) {
				t.Fatalf("request query substituted canonical bytes or changed physical scope: %s/%v/%v", raw, err, physicalErr)
			}
			result, err := ReadActivityForkResultStorageForTest(ctx, selected, event.RunID(), event.ID())
			if err != nil || result.EventName != string(event.Type()) || !bytes.Equal(result.Payload, raw) {
				t.Fatalf("result type/native payload changed: %+v/%v", result, err)
			}
			journal := captureActivityFixtureJournal(t, ctx, selected)
			inputHash := sha256.Sum256([]byte(`{}`))
			started, inserted, err := journal.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{
				RequestEventID: event.ID(), RunID: event.RunID(), EntityID: entity, FlowInstance: "flow-a/1", ExecutionMode: executionmode.Live,
				ActivityID: "fork-observation", Tool: "provider.fixture", Attempt: 1,
				SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: fmt.Sprintf("sha256:%x", inputHash),
			})
			if err != nil || !inserted {
				t.Fatalf("start real native journal: %t/%v", inserted, err)
			}
			started.Status, started.ResultEventID, started.ResultEventType = pipeline.ActivityAttemptStatusSucceeded, uuid.NewString(), started.SuccessEvent
			started.ResultPayload = map[string]any{"observed": true}
			completed, acknowledged, err := journal.CompleteActivityAttempt(ctx, started)
			if err != nil || !acknowledged {
				t.Fatalf("complete real native journal: %t/%v", acknowledged, err)
			}
			before := probe.Snapshot()
			attempt, err := ReadActivityForkAttemptStorageForTest(ctx, selected, event.ID())
			var generation attemptgeneration.Generation
			decodeErr := json.Unmarshal(attempt.Generation, &generation)
			if err != nil || decodeErr != nil || !reflect.DeepEqual(generation, completed.Generation) || attempt.RunID != event.RunID() || attempt.Status != completed.Status || attempt.ResultID != completed.ResultEventID || !bytes.Equal(attempt.Failure, []byte(`null`)) {
				t.Fatalf("attempt physical identity/status/generation/null failure changed: %+v/%v/%v", attempt, err, decodeErr)
			}
			if count, err := CountActivityForkAttemptStorageForTest(ctx, selected, event.ID()); err != nil || count != 1 {
				t.Fatalf("request-only journal count=%d/%v, want1", count, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != before.Total.ReadCommits+2 || counts.Total.WriteCommits != before.Total.WriteCommits || counts.Active != 0 {
				t.Fatalf("attempt/count cuts merged or changed journal authority: %+v", counts)
			}
			after, err := ReadActivityForkSourceStorageForTest(ctx, selected, event.RunID(), entity, event.ID())
			source.AttemptStatus = completed.Status
			if err != nil || !reflect.DeepEqual(after, source) {
				t.Fatalf("real journal status was hidden or other source facts changed: %+v/%v", after, err)
			}
			assertActivityForkLateJournalFailure(t, ctx, selected, event.RunID(), entity, event.ID(), source, probe)
			if raw, err := ReadActivityForkRequestPayloadStorageForTest(ctx, selected, uuid.NewString(), event.ID()); !errors.Is(err, sql.ErrNoRows) || raw != nil {
				t.Fatalf("foreign run got request bytes: %s/%v", raw, err)
			}
			if result, err := ReadActivityForkResultStorageForTest(ctx, selected, uuid.NewString(), event.ID()); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(result, ActivityForkResultStorage{}) {
				t.Fatalf("foreign run got result bytes/type: %+v/%v", result, err)
			}
			if count, err := CountActivityForkAttemptStorageForTest(ctx, selected, uuid.NewString()); err != nil || count != 0 {
				t.Fatalf("absent request count=%d/%v", count, err)
			}
		})
	}
}

func assertActivityForkLateJournalFailure(t testing.TB, ctx context.Context, selected any, run, entity, event string, expected ActivityForkSourceStorage, probe *transactiontest.Collector) {
	t.Helper()
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `ALTER TABLE activity_attempts RENAME TO unavailable_activity_attempts`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		if err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), selected, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_activity_attempts RENAME TO activity_attempts`)
			return err
		}); err != nil {
			t.Error(err)
			return
		}
		restored = true
	}
	defer restore()
	before := probe.Snapshot()
	if got, err := ReadActivityForkSourceStorageForTest(ctx, selected, run, entity, event); err == nil || !reflect.DeepEqual(got, ActivityForkSourceStorage{}) {
		t.Fatalf("failed third read returned provisional entity/event facts: %+v/%v", got, err)
	}
	if counts := probe.Snapshot(); counts.Total.ReadCommits != before.Total.ReadCommits+2 || counts.Total.WriteCommits != before.Total.WriteCommits || counts.Active != 0 {
		t.Fatalf("late failed source read leaked or collapsed original cuts: %+v", counts)
	}
	restore()
	if !restored {
		t.Fatal("could not restore journal read cut")
	}
	if got, err := ReadActivityForkSourceStorageForTest(ctx, selected, run, entity, event); err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("restored exact journal/source facts changed: %+v/%v", got, err)
	}
}

func TestActivityForkStorageRejectsUnavailableAndForeignOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			assertActivityForkReadRefusals(t, cancelled, selected, event.RunID(), event.ID(), context.Canceled)
			for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}} {
				assertActivityForkReadRefusals(t, ctx, owner, event.RunID(), event.ID(), nil)
			}
			assertActivityForkReadRefusals(t, ctx, selected, "invalid", "invalid", nil)
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertActivityForkReadRefusals(t, ctx, selected, event.RunID(), event.ID(), nil)
		})
	}
}

func assertActivityForkReadRefusals(t testing.TB, ctx context.Context, selected any, run, event string, cause error) {
	t.Helper()
	validError := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	if row, err := ReadActivityForkSourceStorageForTest(ctx, selected, run, event, event); !validError(err) || !reflect.DeepEqual(row, ActivityForkSourceStorage{}) {
		t.Fatalf("refused source read returned provisional evidence: %+v/%v", row, err)
	}
	if raw, err := ReadActivityForkRequestPayloadStorageForTest(ctx, selected, run, event); !validError(err) || raw != nil {
		t.Fatalf("refused request read returned bytes: %s/%v", raw, err)
	}
	if row, err := ReadActivityForkResultStorageForTest(ctx, selected, run, event); !validError(err) || !reflect.DeepEqual(row, ActivityForkResultStorage{}) {
		t.Fatalf("refused result read returned provisional evidence: %+v/%v", row, err)
	}
	if row, err := ReadActivityForkAttemptStorageForTest(ctx, selected, event); !validError(err) || !reflect.DeepEqual(row, ActivityForkAttemptStorage{}) {
		t.Fatalf("refused journal read returned provisional evidence: %+v/%v", row, err)
	}
	if count, err := CountActivityForkAttemptStorageForTest(ctx, selected, event); !validError(err) || count != 0 {
		t.Fatalf("refused count returned a provisional scalar: %d/%v", count, err)
	}
}
