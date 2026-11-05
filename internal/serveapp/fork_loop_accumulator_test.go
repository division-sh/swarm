package serveapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestServedForkAccumulatorRetainedGenerationBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkLoopAccumulator(t)
			publication := newExactOriginPublicationProbe(t)
			rt := startServedLiveFlowProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "loop-notice-proof"}, "idempotency_key": "accumulator-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			first := readForkLoopActivation(t, rt, started.RunID)
			initial := readForkCollectedReviews(t, rt.Endpoint, started.RunID)
			if !reflect.DeepEqual(initial, []any{map[string]any{"revision_id": first.RevisionID, "token": "loop-notice-proof"}}) {
				t.Fatalf("first ordinary admission: %#v", initial)
			}
			repeated := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
				"payload": map[string]any{"revision_id": first.RevisionID, "token": "loop-notice-proof"}, "idempotency_key": "accumulator-repeat",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			current := readForkLoopActivation(t, rt, started.RunID)
			if current.Attempt != 2 || current.ActivationID != first.ActivationID || current.RevisionID == first.RevisionID {
				t.Fatalf("ordinary repeat: first=%+v current=%+v", first, current)
			}
			source := readForkCollectedReviews(t, rt.Endpoint, started.RunID)
			if !reflect.DeepEqual(source, []any{map[string]any{"revision_id": current.RevisionID, "token": "loop-notice-proof"}}) {
				t.Fatalf("repeat did not start a new collected list: %#v", source)
			}
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			if err := waitExactOriginPublications(t.Context(), publication, started.EventID, repeated.EventID); err != nil {
				t.Fatal(err)
			}
			before := readServedForkRecipientSourceDomain(t, rt, started.RunID)
			family, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork family")
			}
			plan, err := family.Plan(servedControlProofAuthorActivityContext(t, rt), runfork.RunForkPlanRequest{SourceRunID: started.RunID, At: frontier})
			if err != nil {
				t.Fatal(err)
			}
			for _, entity := range plan.Entities {
				if entity.EntityID == flowidentity.EntityID("review") {
					t.Logf("fixed-R collected state before fork: %#v", entity.Fields)
				}
			}
			params := map[string]any{"source_run_id": started.RunID, "fork_event_id": frontier, "idempotency_key": "accumulator-fork"}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, fork.ForkRunID, frontier)
			want, err := loopruntime.ForkGeneration(current.Generation(), fork.ForkRunID, flowidentity.EntityID("review"))
			if err != nil {
				t.Fatal(err)
			}
			child := readForkCollectedReviews(t, rt.Endpoint, fork.ForkRunID)
			if !reflect.DeepEqual(child, []any{map[string]any{"revision_id": want.RevisionID, "token": "loop-notice-proof"}}) {
				t.Fatalf("fork did not collect the remapped current revision: %#v", child)
			}
			requireForkLoopStateEffect(t, rt, fork.ForkRunID, activityidentity.ForkLineageEventID(fork.ForkRunID, frontier), want.RevisionID)
			var replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(child, readForkCollectedReviews(t, rt.Endpoint, fork.ForkRunID)) || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
				t.Fatal("repeated fork changed collected items or source history")
			}
		})
	}
}

func readForkCollectedReviews(t *testing.T, endpoint, runID string) []any {
	t.Helper()
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": flowidentity.EntityID("review")}, &entity)
	items, ok := entity.Fields["reviews"].([]any)
	if !ok {
		t.Fatalf("collected reviews are not an ordinary state list: %#v", entity.Fields)
	}
	return items
}
