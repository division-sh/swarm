package serveapp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedForkAccumulatorRetainedGenerationBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, clearOnAdmit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%t", backend, clearOnAdmit), func(t *testing.T) {
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					selected = owner
					return previous(owner)
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				root := canonicalrouting.CopyForkLoopAccumulator(t, clearOnAdmit)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.requested", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{"token": "loop-notice-proof"}, "idempotency_key": "accumulator-start",
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				waitForkReceiverSourceCompletion(t, rt, started.RunID)
				first := readForkLoopNoticeActivation(t, rt, started.RunID)
				initial := readForkHandlerAccumulators(t, rt, started.RunID)
				wantInitial := 1
				if clearOnAdmit {
					wantInitial = 0
				}
				if len(initial) != wantInitial {
					t.Fatalf("first ordinary admission: %#v", initial)
				}
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "review/review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
					"payload": map[string]any{"revision_id": first.RevisionID, "token": "loop-notice-proof"}, "idempotency_key": "accumulator-repeat",
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				waitForkReceiverSourceCompletion(t, rt, started.RunID)
				current := readForkLoopNoticeActivation(t, rt, started.RunID)
				if current.Attempt != 2 || current.ActivationID != first.ActivationID || current.RevisionID == first.RevisionID {
					t.Fatalf("ordinary repeat: first=%+v current=%+v", first, current)
				}
				source := readForkHandlerAccumulators(t, rt, started.RunID)
				wantCount := 2
				if clearOnAdmit {
					wantCount = 0
				}
				if len(source) != wantCount {
					t.Fatalf("ordinary repeat retained %d buckets, want %d: %#v", len(source), wantCount, source)
				}
				for key, value := range initial {
					if clearOnAdmit {
						if _, exists := source[key]; exists {
							t.Fatal("explicit clear retained the old bucket")
						}
					} else if !reflect.DeepEqual(value, source[key]) {
						t.Fatal("repeat altered the retained historical bucket")
					}
				}
				var frontier string
				if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				before := readServedForkRecipientSourceDomain(t, rt, started.RunID)
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("missing selected fork owner")
				}
				plan, err := family.Plan(servedControlProofAuthorActivityContext(t, rt), runfork.RunForkPlanRequest{SourceRunID: started.RunID, At: frontier})
				if err != nil {
					t.Fatal(err)
				}
				for _, entity := range plan.Entities {
					if entity.EntityID == flowidentity.EntityID("review") {
						t.Logf("fixed-R accumulator before fork: %#v", entity.Accumulator)
					}
				}
				params := map[string]any{"source_run_id": started.RunID, "fork_event_id": frontier, "confirm_source_freeze": true, "idempotency_key": "accumulator-fork"}
				var fork apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
				child := readForkHandlerAccumulators(t, rt, fork.ForkRunID)
				if len(child) != len(source) {
					t.Fatalf("child bucket count=%d source=%d; source=%#v child=%#v", len(child), len(source), source, child)
				}
				for key, value := range source {
					ref, ok := timeridentity.ParseAccumulatorBucketKey(key)
					if !ok || ref.Generation.Attempt < 1 || ref.Generation.Attempt > 2 {
						t.Fatalf("ordinary writer produced invalid generation key %q", key)
					}
					generation := first.Generation()
					if ref.Generation.Attempt == 2 {
						generation = current.Generation()
					}
					want, err := loopruntime.ForkGeneration(generation, fork.ForkRunID, flowidentity.EntityID("review"))
					if err != nil {
						t.Fatal(err)
					}
					ref.Generation = want
					if generation.Attempt == 1 {
						if !reflect.DeepEqual(value, child[ref.Key()]) {
							t.Fatalf("historical bucket contents or identity changed: source=%#v child=%#v", value, child[ref.Key()])
						}
					} else {
						// R precedes this delivery's admission. This bucket is newly
						// written by child execution, not copied from post-R state.
						bucket, ok := child[ref.Key()].(map[string]any)
						if !ok {
							t.Fatalf("missing child current bucket %q", ref.Key())
						}
						items, ok := bucket["items"].([]any)
						if !ok || len(items) != 1 {
							t.Fatalf("current child items: %#v", bucket)
						}
						item := items[0].(map[string]any)
						if item["event_id"] != activityidentity.ForkLineageEventID(fork.ForkRunID, frontier) || item["revision_id"] != want.RevisionID || item["token"] != "loop-notice-proof" {
							t.Fatalf("current child execution evidence: %#v", item)
						}
						if !reflect.DeepEqual(bucket["received"], map[string]any{"loop-notice-proof": true}) {
							t.Fatalf("current child dedup evidence: %#v", bucket)
						}
					}
					if _, exists := child[key]; exists {
						t.Fatal("child retained source generation key")
					}
				}
				requireForkLoopNotice(t, rt, fork.ForkRunID, activityidentity.ForkLineageEventID(fork.ForkRunID, frontier), readForkLoopNoticeActivation(t, rt, fork.ForkRunID).RevisionID)
				var replay apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
				if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(child, readForkHandlerAccumulators(t, rt, fork.ForkRunID)) || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
					t.Fatal("repeated fork changed child accumulators or source history")
				}
			})
		}
	}
}

func readForkHandlerAccumulators(t *testing.T, rt servedControlProofRuntime, runID string) map[string]any {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, flowidentity.EntityID("review")).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var state map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{}
	for _, node := range state {
		if buckets, ok := node["handler_accumulators"].(map[string]any); ok {
			for key, value := range buckets {
				if _, duplicate := result[key]; duplicate {
					t.Fatalf("duplicate full bucket identity %q", key)
				}
				result[key] = value
			}
		}
	}
	return result
}
