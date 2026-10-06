package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func seedConnectorCredentialStorageWitness(t *testing.T, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	for index := 0; index < 2; index++ {
		event := eventtest.ExistingRunRootIngress(uuid.NewString(), "activity.evidence", "fixture", "",
			[]byte(`{"token":"prefix-fixture-secret-suffix"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
		if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
			t.Fatal(err)
		}
	}
	journal := fixture.store.(pipeline.ActivityAttemptJournal)
	for _, columns := range [][2]string{
		{"fixture-secret", "safe"},
		{"safe", "fixture-secret"},
		{"fixture-secret", "fixture-secret"},
		{"safe", "safe"},
	} {
		record, inserted, err := journal.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{
			RequestEventID: uuid.NewString(), RunID: runID, ExecutionMode: executionmode.Live,
			ActivityID: uuid.NewString(), Tool: "fixture.credential_storage", EffectClass: "non_idempotent_write",
			Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "exact-storage-input",
		})
		if err != nil || !inserted {
			t.Fatalf("start actual journal: inserted=%t err=%v", inserted, err)
		}
		record.Status, record.ResultEventID, record.ResultEventType = pipeline.ActivityAttemptStatusFailed, uuid.NewString(), record.FailureEvent
		record.ResultPayload = map[string]any{"token": columns[0]}
		failure := failures.FromError(failures.New(failures.ClassInternalFailure, "credential_storage_fixture", "fixture", "storage_witness",
			map[string]any{"token": columns[1]}), "fixture", "storage_witness").Failure
		record.Failure = &failure
		if _, acknowledged, err := journal.CompleteActivityAttempt(ctx, record); err != nil || !acknowledged {
			t.Fatalf("complete actual failed journal receipt: acknowledged=%t err=%v", acknowledged, err)
		}
	}
}

func TestConnectorCredentialLeakStoragePreservesOriginalSnapshotAndCardinalityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, runID, otherRun := testAuthorActivityContext(), uuid.NewString(), uuid.NewString()
			seedConnectorCredentialStorageWitness(t, fixture, runID)
			seedConnectorCredentialStorageWitness(t, fixture, otherRun)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			want := ConnectorCredentialLeakStorageEvidence{EventPayloads: 2, ActivityResultsOrFailures: 3}
			if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, runID, "fixture-secret"); err != nil || got != want {
				t.Fatalf("stored event/attempt scope, duplicate cardinality or OR count changed: %+v %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("credential witness bypassed original read snapshot: %+v", counts)
			}
			for _, identity := range [][2]string{{uuid.NewString(), "fixture-secret"}, {runID, "absent-fixture-secret"}} {
				if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, identity[0], identity[1]); err != nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
					t.Fatalf("absent token/run returned leak evidence: %+v %v", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadConnectorCredentialLeakStorageForTest(cancelled, fixture.store, runID, "fixture-secret"); !errors.Is(err, context.Canceled) || got != (ConnectorCredentialLeakStorageEvidence{}) {
				t.Fatalf("cancelled credential witness returned evidence: %+v %v", got, err)
			}
			if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, runID, "fixture-secret"); err != nil || got != want || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("credential observation changed persisted rows: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, runID, "fixture-secret"); err == nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
				t.Fatalf("closed selected owner returned leak evidence: %+v %v", got, err)
			}
		})
	}
}

func TestConnectorCredentialLeakStorageRejectsForeignOwnersAndPartialEvidence(t *testing.T) {
	ctx, runID := testAuthorActivityContext(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, owner, runID, "fixture-secret"); err == nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
			t.Fatalf("foreign owner %T returned leak evidence: %+v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			seedConnectorCredentialStorageWitness(t, fixture, runID)
			for _, identity := range [][2]string{{"", "fixture-secret"}, {"invalid", "fixture-secret"}, {uuid.Nil.String(), "fixture-secret"}, {"550E8400-E29B-41D4-A716-446655440000", "fixture-secret"}, {runID, ""}} {
				if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, identity[0], identity[1]); err == nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
					t.Fatalf("invalid credential witness identity returned evidence: %+v %v", got, err)
				}
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE activity_attempts RENAME TO credential_unavailable_attempts`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE credential_unavailable_attempts RENAME TO activity_attempts`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, runID, "fixture-secret"); err == nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
				t.Fatalf("late query failure exposed partial positive event evidence: %+v %v", got, err)
			}
		})
	}
}
