package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/google/uuid"
)

// These are real journal/copy/readback proofs. Loop-bearing source writers and
// progressed-child ownership are covered separately by the generation matrix.
func TestForkGenerationRepeatedActivityEvidenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, status := range []string{"succeeded", "failed", "uncertain"} {
				for _, field := range []string{
					"unchanged", "equivalent_json", "result_payload", "failure", "input_hash", "loop_generation", "generation_unknown_field", "loop_stage",
					"entity_id", "flow_instance", "source_event_id", "parent_event_id", "attempt", "node_id", "handler_event_key",
					"activity_id", "tool", "success_event", "failure_event", "result_event_type", "result_event_id",
					"started_at", "completed_at", "updated_at",
				} {
					t.Run(status+"/"+field, func(t *testing.T) {
						ctx := testAuthorActivityContext()
						child, event, source := seedActivityTimestampReuse(t, fixture, backend.name == "postgres", false, status)
						store := fixture.store.(selectedActivityProjectionStore)
						first, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID()))
						if err != nil || len(first) != 1 {
							t.Fatalf("initial copy: events=%d err=%v", len(first), err)
						}
						var payload runForkActivityRequestPayload
						if err := json.Unmarshal(first[0].Payload, &payload); err != nil {
							t.Fatal(err)
						}
						owner, err := activityidentity.ParseOwnerKey(payload.NodeID)
						if err != nil {
							t.Fatal(err)
						}
						requestID := activityidentity.RequestEventID(activityidentity.Fact{
							RunID: child.ForkRunID, SourceEventID: payload.SourceEventID, ParentEventID: payload.ParentEventID,
							EntityID: payload.EntityID, Owner: owner, ExecutionFlowID: payload.FlowID,
							HandlerEventKey: payload.HandlerEventKey, ActivityID: payload.ActivityID, Tool: payload.Tool,
							Attempt: 1, RevisionID: payload.Generation.RevisionID,
						})
						if field != "unchanged" {
							column := field
							var value any = "conflicting-value"
							switch field {
							case "equivalent_json":
								column = "result_payload"
								encoded, err := json.MarshalIndent(source.ResultPayload, "", "  ")
								if err != nil {
									t.Fatal(err)
								}
								value = string(encoded)
							case "result_payload":
								value = `{"result":{"ok":false},"n":9007199254740993}`
							case "failure":
								if status == "succeeded" {
									value = `{"code":"unexpected-failure"}`
								} else {
									failure := *source.Failure
									failure.Operation = "conflicting-operation"
									encoded, err := json.Marshal(failure)
									if err != nil {
										t.Fatal(err)
									}
									value = string(encoded)
								}
							case "loop_generation":
								value = `{"loop_id":"unproven-loop"}`
							case "generation_unknown_field":
								column = "loop_generation"
								value = `{"loop_id":"","activation_id":"","revision_field":"","revision_id":"","attempt":0,"unexpected_authority":"foreign"}`
							case "entity_id", "source_event_id", "parent_event_id", "result_event_id":
								value = uuid.NewString()
							case "attempt":
								value = 2
							case "started_at", "completed_at", "updated_at":
								value = source.StartedAt.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
							}
							beforeCorruption := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
							_, err := fixture.db.ExecContext(ctx, "UPDATE activity_attempts SET "+column+" = $1 WHERE request_event_id = $2", value, requestID)
							if field == "attempt" || (field == "failure" && status == "succeeded") {
								if err == nil || !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
									t.Fatalf("schema must reject impossible terminal evidence: %v", err)
								}
								if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(beforeCorruption, after) {
									t.Fatal("schema rejection changed persisted tables")
								}
								return
							}
							if err != nil {
								t.Fatal(err)
							}
						}
						before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						again, err := store.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, event.RunID()))
						if field == "unchanged" || field == "equivalent_json" {
							if err != nil || !reflect.DeepEqual(first, again) {
								t.Fatalf("exact repeated evidence: err=%v", err)
							}
							got, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, requestID)
							if err != nil || !found || got.InputHash != source.InputHash || !reflect.DeepEqual(got.ResultPayload, source.ResultPayload) || !reflect.DeepEqual(got.Failure, source.Failure) {
								t.Fatalf("repeated public journal readback: found=%v err=%v got=%#v", found, err, got)
							}
						} else if err == nil || !strings.Contains(err.Error(), "conflicts with canonical fork") {
							t.Fatalf("conflicting %s accepted: %v", field, err)
						}
						if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("repeated evidence admission changed persisted tables")
						}
					})
				}
			}
		})
	}
}
