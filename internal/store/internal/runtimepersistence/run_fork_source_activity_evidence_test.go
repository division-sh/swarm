package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func TestForkRecordedActivityGenerationEvidenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, field := range []string{"unchanged", "run_id", "entity_id", "source_event_id", "parent_event_id", "flow_instance", "node_id", "handler_event_key", "activity_id", "tool", "loop_flow", "loop_activation", "loop_revision", "loop_attempt", "loop_field", "loop_unknown", "result_event_id"} {
				t.Run(field, func(t *testing.T) {
					ctx := context.Background()
					child, event, record := seedActivityEvidenceReuse(t, fixture, backend.name == "postgres", false, "succeeded", 2)
					store := fixture.store.(selectedActivityProjectionStore)
					original := originalCarriageForRun(t, fixture.store, event.RunID())
					column, value := field, any("")
					switch field {
					case "unchanged":
					case "run_id", "entity_id", "source_event_id", "parent_event_id", "result_event_id":
						// Existing identities keep this probe beyond UUID/FK syntax checks.
						value = child.ForkRunID
						if field == "source_event_id" || field == "parent_event_id" || field == "result_event_id" {
							value = event.ID()
						}
					case "flow_instance":
						value = "foreign/flow"
					case "node_id":
						value = activityidentity.MustNodeOwner(mustPersistenceNode("flow-a", "reader")).Key()
					case "handler_event_key":
						value = "review.inspect"
					case "activity_id":
						value = "inspect"
					case "tool":
						value = "provider.read"
					default:
						column = "loop_generation"
						raw, err := json.Marshal(record.Generation)
						if err != nil {
							t.Fatal(err)
						}
						var generation map[string]any
						if err := json.Unmarshal(raw, &generation); err != nil {
							t.Fatal(err)
						}
						switch field {
						case "loop_flow":
							generation["flow_id"] = "another-flow"
						case "loop_activation":
							generation["activation_id"] = "another-activation"
						case "loop_revision":
							generation["revision_id"] = "another-revision"
						case "loop_attempt":
							generation["attempt"] = 1
						case "loop_field":
							generation["revision_field"] = "business_value"
						case "loop_unknown":
							generation["foreign_authority"] = "cannot disappear during decoding"
						default:
							t.Fatalf("unclassified source-journal probe %s", field)
						}
						value = forkTestJSON(t, generation)
					}
					if field != "unchanged" {
						if _, err := fixture.db.ExecContext(ctx, "UPDATE activity_attempts SET "+column+"=$2 WHERE request_event_id=$1", event.ID(), value); err != nil {
							t.Fatalf("probe must reach source journal agreement, not stop at SQL: %v", err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					loaded, err := store.LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), event.RunID(), child.ForkRunID, []string{event.ID()}, original)
					if field != "unchanged" {
						if err == nil {
							t.Fatalf("source %s corruption authorized copied activity evidence", field)
						}
						if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("source journal rejection mutated durable state")
						}
						return
					}
					if err != nil || len(loaded) != 1 {
						t.Fatalf("exact source journal rejected: count=%d err=%v", len(loaded), err)
					}
					var request runForkActivityRequestPayload
					if err := json.Unmarshal(loaded[0].Payload, &request); err != nil {
						t.Fatal(err)
					}
					want, err := loopruntime.ForkGeneration(record.Generation, child.ForkRunID, record.EntityID)
					if err != nil || request.Generation != want {
						t.Fatalf("attempt two was not preserved: got=%#v want=%#v err=%v", request.Generation, want, err)
					}
					owner, err := activityidentity.ParseOwnerKey(request.NodeID)
					if err != nil {
						t.Fatal(err)
					}
					fact := activityidentity.Fact{RunID: child.ForkRunID, SourceEventID: request.SourceEventID, ParentEventID: request.ParentEventID,
						EntityID: request.EntityID, Owner: owner, ExecutionFlowID: request.FlowID, HandlerEventKey: request.HandlerEventKey,
						ActivityID: request.ActivityID, Tool: request.Tool, Attempt: 1, RevisionID: want.RevisionID}
					copied, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, activityidentity.RequestEventID(fact))
					if err != nil || !found || copied.Generation != want || copied.ResultPayload[want.RevisionField] != want.RevisionID || copied.InputHash != record.InputHash {
						t.Fatalf("copied result did not consume exact correspondence: %#v found=%v err=%v", copied, found, err)
					}
					before = snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					again, err := store.LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), event.RunID(), child.ForkRunID, []string{event.ID()}, original)
					if err != nil || !reflect.DeepEqual(loaded, again) || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatalf("repeated generation copy was not stable: %v", err)
					}
				})
			}
		})
	}
}
