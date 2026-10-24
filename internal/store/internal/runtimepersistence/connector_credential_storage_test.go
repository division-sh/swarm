package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func seedConnectorCredentialStorageWitness(t *testing.T, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	for index := 0; index < 2; index++ {
		event := eventtest.ExistingRunRootIngress(uuid.NewString(), "activity.evidence", "fixture", "",
			[]byte(`{"token":"prefix-fixture-secret-suffix"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
		if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
			t.Fatal(err)
		}
	}
	journal := activityNativeJournalForFixture(t, fixture)
	for _, columns := range [][2]string{
		{"fixture-secret", "safe"},
		{"safe", "fixture-secret"},
		{"fixture-secret", "fixture-secret"},
		{"safe", "safe"},
	} {
		record, inserted, err := journal.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{
			RequestEventID: uuid.NewString(), RunID: runID, ExecutionMode: executionmode.Live,
			ActivityID: uuid.NewString(), Tool: "fixture.credential_storage",
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
			want := ConnectorCredentialLeakStorageEvidence{EventPayloads: 2, ActivityResultsOrFailures: 3}
			if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, runID, "fixture-secret"); err != nil || got != want {
				t.Fatalf("stored event/attempt scope, duplicate cardinality or OR count changed: %+v %v", got, err)
			}
			for _, identity := range [][2]string{{uuid.NewString(), "fixture-secret"}, {runID, "absent-fixture-secret"}} {
				if got, err := ReadConnectorCredentialLeakStorageForTest(ctx, fixture.store, identity[0], identity[1]); err != nil || got != (ConnectorCredentialLeakStorageEvidence{}) {
					t.Fatalf("absent token/run returned leak evidence: %+v %v", got, err)
				}
			}
		})
	}
}
