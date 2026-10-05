package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// The loop buckets are controlled decoder inputs on an actually constructed
// header. This proves native activity admission/result persistence, not a served
// loop journey or the compiler's production of those inputs.
func TestActivityAdmissionConsumesConstructedHeaderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"fieldless", "fields"} {
			for _, cell := range []string{"current", "stale", "wrong_entity", "wrong_path", "wrong_header_flow", "missing_header", "malformed_bucket"} {
				t.Run(backend+"/"+shape+"/"+cell, func(t *testing.T) {
					files := map[string]string{"schema.yaml": "name: activity-header\nstages:\n  review: {initial: true}\n"}
					if shape == "fields" {
						files["entities.yaml"] = "default:\n  title: text\n"
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
					run := correlation.RunIDFromContext(f.ctx)
					req := sqliteFlowActivationRequest(f.bundle, ".", run, "", run)
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !committed.Created || !committed.Acknowledged {
						t.Fatalf("canonical construction: %+v %v", committed, err)
					}
					at := time.Now().UTC()
					loop, err := loopruntime.New(run, req.Instance.EntityID, ".", "revision", "revision_id", uuid.NewString(), "review", 3, at)
					if err != nil {
						t.Fatal(err)
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, loop); err != nil {
						t.Fatal(err)
					}
					raw, err := json.Marshal(buckets)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.Exec(`UPDATE flow_instances SET accumulator=$3 WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID, string(raw)); err != nil {
						t.Fatal(err)
					}
					if shape == "fields" {
						// Corrupt the obsolete lifecycle shadow while keeping declared
						// fields valid. No executing consumer may use that shadow.
						if _, err := f.db.Exec(`UPDATE entity_state SET accumulator='{}' WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					}
					record := pipeline.ActivityAttemptRecord{
						RequestEventID: uuid.NewString(), RunID: run, EntityID: req.Instance.EntityID, FlowInstance: req.Instance.InstancePath,
						NodeID: activityidentity.MustNodeOwner(mustPersistenceNode(".", "writer")).Key(), HandlerEventKey: "request", ActivityID: "write", Tool: "provider.write",
						EffectClass: "non_idempotent_write", Attempt: 1, ExecutionMode: executionmode.Live,
						SuccessEvent: "write.succeeded", FailureEvent: "write.failed", InputHash: "exact-input", Generation: loop.Generation(), LoopStage: "review",
					}
					switch cell {
					case "stale":
						record.Generation.RevisionID = uuid.NewString()
					case "wrong_entity":
						record.EntityID = uuid.NewString()
					case "wrong_path":
						record.FlowInstance = "foreign/one"
					case "wrong_header_flow":
						if _, err := f.db.Exec(`UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					case "missing_header":
						if _, err := f.db.Exec(`DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					case "malformed_bucket":
						if _, err := f.db.Exec(`UPDATE flow_instances SET accumulator='[]' WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID); err != nil {
							t.Fatal(err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					journal := f.store.(activityStoryJournal)
					actual, inserted, err := journal.ClaimActivityAttemptForLoopGeneration(f.ctx, record)
					if cell != "current" {
						if err == nil || inserted {
							t.Fatalf("invalid header/generation admitted: %+v %t %v", actual, inserted, err)
						}
						if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("refused activity mutated durable state")
						}
						return
					}
					if err != nil || !inserted || actual.Status != pipeline.ActivityAttemptStatusStarted {
						t.Fatalf("canonical activity admission: %+v %t %v", actual, inserted, err)
					}
					completed, _, err := journal.CompleteActivityAttempt(f.ctx, activityStoryTerminal(t, actual, "succeeded"))
					if err != nil || completed.Status != pipeline.ActivityAttemptStatusSucceeded || !completed.Generation.Equal(loop.Generation()) {
						t.Fatalf("result persistence: %+v %v", completed, err)
					}
					loaded, found, err := journal.LoadActivityAttempt(f.ctx, record.RequestEventID)
					if err != nil || !found || !reflect.DeepEqual(loaded, completed) {
						t.Fatalf("persisted result: %+v %t %v", loaded, found, err)
					}
					var fields int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, req.Instance.EntityID).Scan(&fields); err != nil || (shape == "fieldless" && fields != 0) {
						t.Fatalf("fieldless activity synthesized fields: count=%d error=%v", fields, err)
					}
				})
			}
		}
	}
}
