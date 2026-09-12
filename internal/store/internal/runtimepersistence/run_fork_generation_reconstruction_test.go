package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

// Reconstruct the store owners over the same durable database, not an activity
// runtime. This proves pre-execution preparation survives loss of owner-local
// memory; process execution/recovery has separate served proofs.
func TestForkGenerationPreparationReconstructedOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, status := range []string{"succeeded", "failed", "uncertain"} {
			t.Run(backend.name+"/"+status, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				child, event, source := seedActivityEvidenceReuse(t, fixture, backend.name == "postgres", false, status, 2)
				owner := fixture.store.(selectedActivityProjectionStore)
				first, err := owner.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, owner, event.RunID()))
				if err != nil || len(first) != 1 {
					t.Fatalf("initial loop-bearing preparation: count=%d err=%v", len(first), err)
				}
				var payload runForkActivityRequestPayload
				if err := json.Unmarshal(first[0].Payload, &payload); err != nil {
					t.Fatal(err)
				}
				want, err := loopruntime.ForkGeneration(source.Generation, child.ForkRunID, payload.EntityID)
				if err != nil || payload.Generation != want || want.Attempt != 2 || payload.Attempt != 1 {
					t.Fatalf("loop attempt confused with activity attempt: payload=%#v want=%#v err=%v", payload, want, err)
				}
				node, err := activityidentity.ParseOwnerKey(payload.NodeID)
				if err != nil {
					t.Fatal(err)
				}
				fact := activityidentity.Fact{RunID: child.ForkRunID, SourceEventID: payload.SourceEventID, ParentEventID: payload.ParentEventID,
					EntityID: payload.EntityID, Owner: node, ExecutionFlowID: payload.FlowID, HandlerEventKey: payload.HandlerEventKey,
					ActivityID: payload.ActivityID, Tool: payload.Tool, Attempt: 1, RevisionID: want.RevisionID}
				requestID := activityidentity.RequestEventID(fact)
				copied, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, requestID)
				if err != nil || !found || copied.Generation != want || copied.Status != status || copied.ResultPayload[want.RevisionField] != want.RevisionID || copied.ResultEventID != activityidentity.ResultEventID(fact, copied.ResultEventType) {
					t.Fatalf("initial exact generation result: %#v found=%v err=%v", copied, found, err)
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				var reconstructed interface {
					selectedActivityProjectionStore
					activityTimestampJournal
				}
				switch store := fixture.store.(type) {
				case *PostgresStore:
					var fresh *PostgresStore
					fresh, err = newPostgresStoreComposition(store.backend)
					if err == nil {
						bootstrapTestPostgresStore(t, fresh)
						reconstructed = fresh
					}
				case *SQLiteRuntimeStore:
					reconstructed, err = newSQLiteStoreComposition(store.schema, store.backend, nil)
				default:
					t.Fatalf("unexpected backend %T", fixture.store)
				}
				if err != nil {
					t.Fatal(err)
				}
				for range 2 {
					again, err := reconstructed.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, reconstructed, event.RunID()))
					if err != nil || !reflect.DeepEqual(first, again) {
						t.Fatalf("reconstructed preparation re-elected generation: err=%v", err)
					}
					reloaded, found, err := reconstructed.LoadActivityAttempt(ctx, requestID)
					if err != nil || !found || !reflect.DeepEqual(copied, reloaded) {
						t.Fatalf("reconstructed copy changed terminal evidence: %#v found=%v err=%v", reloaded, found, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("reconstructed generation preparation changed durable state")
					}
				}
			})
		}
	}
}
