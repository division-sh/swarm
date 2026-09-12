package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// #642 owns future completed-history fork admission. Its original success oracle
// is preserved verbatim in testdata/future_capabilities; these tests prove current
// source completion and the exact fail-closed policy without mutation.
func TestServedJoinWriterCompletedHistoryRefusalBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, false)
}

func TestServedJoinWriterCompletedHistoryRefusalSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, true)
}

func testServedJoinWriterCompletedHistoryRefusal(t *testing.T, separateCheckpoint bool) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkLoopRetainedJoin(t)
			if separateCheckpoint {
				root = canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint(t)
			}
			bundle := loadWorkflowValidationBundleAt(t, root)
			carriage, err := semanticview.CompileOriginalLoopCarriage(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.bootstrap", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "member-one"}, "idempotency_key": "retained-join-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			rows, err := rt.DB.Query(`SELECT CAST(d.failure AS TEXT) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1`, started.RunID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var failure string
				if err := rows.Scan(&failure); err != nil {
					t.Fatal(err)
				}
				t.Errorf("ordinary join writer dead-lettered before fork: %s", failure)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if t.Failed() {
				t.FailNow()
			}
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			first, joins := readRetainedRootJoins(t, rt, started.RunID)
			if len(joins) != 1 || first.Attempt != 1 {
				t.Fatalf("first ordinary join: loop=%+v joins=%+v", first, joins)
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
				"payload": map[string]any{"revision_id": first.RevisionID, "token": "member-one"}, "idempotency_key": "retained-join-repeat",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			current, sourceJoins := readRetainedRootJoins(t, rt, started.RunID)
			if current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {
				t.Fatalf("ordinary repeat lost retained joins: loop=%+v joins=%+v", current, sourceJoins)
			}
			for _, join := range sourceJoins {
				if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete || len(join.Outputs) != 1 || !join.TimerCancelled || join.OutcomePending || !join.OutcomeFired {
					t.Fatalf("ordinary join did not complete truthfully: %+v", join)
				}
			}
			if !separateCheckpoint {
				rows, err := rt.DB.Query(`SELECT payload FROM events WHERE run_id=$1 AND event_name='join.observed'`, started.RunID)
				if err != nil {
					t.Fatal(err)
				}
				observed := map[string]int{}
				for rows.Next() {
					var raw []byte
					if err := rows.Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var payload struct {
						RevisionID string `json:"revision_id"`
						Completed  int    `json:"completed"`
					}
					if err := json.Unmarshal(raw, &payload); err != nil || payload.Completed != 1 {
						t.Fatalf("join outcome lost typed completion: %s %v", raw, err)
					}
					observed[payload.RevisionID]++
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(observed, map[string]int{first.RevisionID: 1, current.RevisionID: 1}) {
					t.Fatalf("join outcomes substituted captured generation: %#v", observed)
				}
			}
			if separateCheckpoint {
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "checkpoint.requested", "run_id": started.RunID, "source_event_id": started.EventID,
					"payload": map[string]any{"revision_id": current.RevisionID}, "idempotency_key": "retained-join-checkpoint",
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				waitForkReceiverSourceCompletion(t, rt, started.RunID)
			}
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='join.observed' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			var frontierRevision int64
			if err := rt.DB.QueryRow(`SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND present`, started.RunID, frontier).Scan(&frontierRevision); err != nil || frontierRevision <= 0 {
				t.Fatalf("missing fixed-revision frontier: revision=%d err=%v", frontierRevision, err)
			}
			// Delivery settlement does not join the lifecycle candidate executor. Stop
			// the real source writers before measuring the store-only refusal.
			if err := rt.Runtime.Shutdown(); err != nil {
				t.Fatalf("join source runtime before refusal snapshot: %v", err)
			}
			before := snapshotForkReceiverApplication(t, rt)
			owner, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork owner")
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: started.RunID, At: frontier, OriginalLoopCarriage: carriage}
			const refusal = "fork materialization requires execution-ready plan; blockers: timer_history_unproven, flow_route_history_unproven"
			for attempt := 0; attempt < 2; attempt++ {
				child, err := owner.Materialize(servedControlProofAuthorActivityContext(t, rt), request)
				if err == nil || err.Error() != refusal {
					t.Fatalf("completed ordinary join history must retain both exact policy blockers: result=%+v err=%v", child, err)
				}
				if child.ForkRunID != "" || child.MaterializedEntityCount != 0 {
					t.Fatalf("refused materialization returned child work: %+v", child)
				}
				if child.SourceRunID != started.RunID || child.ForkPoint.EventID != frontier || child.ForkPoint.Revision != frontierRevision || child.ExecutionReady || !child.DeliveryResumeBlocked {
					t.Fatalf("refusal lost exact selected-revision admission: %+v", child)
				}
				var codes []string
				for _, blocker := range child.UnsupportedBlockers {
					codes = append(codes, blocker.Code)
				}
				if !reflect.DeepEqual(codes, []string{runfork.RunForkBlockerTimerHistoryUnproven, runfork.RunForkBlockerFlowRouteHistoryUnproven}) {
					t.Fatalf("refusal lost canonical blocker evidence: %+v", child.UnsupportedBlockers)
				}
				if after := snapshotForkReceiverApplication(t, rt); !reflect.DeepEqual(before, after) {
					for table, rows := range after {
						if !reflect.DeepEqual(before[table], rows) {
							t.Errorf("refused join-history materialization changed table %s", table)
						}
					}
					t.Fatal("refused join-history materialization changed application state")
				}
			}
		})
	}
}

func readRetainedRootJoins(t *testing.T, rt servedControlProofRuntime, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	var raw []byte
	if err := rt.DB.QueryRow(`SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, persisted)
	if err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
	if err != nil || !found {
		t.Fatalf("root loop missing: found=%v err=%v", found, err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return loop, joins
}
