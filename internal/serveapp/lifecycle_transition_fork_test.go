package serveapp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"

	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestRetainedRootForkOriginalArtifactPreserved(t *testing.T) {
	raw, err := os.ReadFile("testdata/future_capabilities/lifecycle_transition_fork_original_test.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	const want = "4204b142a6927d8d45babede97d77ba42fe2c986be16b82fc2aeb9e3c78831de"
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
		t.Fatalf("original root fork proof at edae2a094 changed: %s", got)
	}
}

func TestServedCompiledRootLoopForkProjectsRevisionOnBothStores(t *testing.T) {
	runLifecycleRootForkPolicy(t)
}

func runLifecycleRootForkPolicy(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleForkSource(t, false)
			publication := newExactOriginPublicationProbe(t)
			rt := startServedLiveFlowProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
			var rootEventOwner any = rt.SQLite
			if rt.Postgres != nil {
				rootEventOwner = rt.Postgres
			}
			requireServedLiveRootMode(t, rt.Runtime.ExecutionPosture, storetest.LoadCanonicalEventRecord(t, t.Context(), rootEventOwner, seed.EventID), seed.RunID)
			origins := []string{seed.EventID}
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, seed.RunID, "waiting")
			loopStart := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "loop.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-loop-start"})
			origins = append(origins, loopStart.EventID)
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "fork-pause"})
			frontierEvent := "loop.admit"
			frontierPayload := map[string]any{"revision_id": readLifecycleLoop(t, rt.ReceiverStateReader, seed.RunID, entityID).RevisionID}
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": frontierEvent, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": frontierPayload, "idempotency_key": "fork-frontier"})
			waitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)
			origins = append(origins, frontier.EventID)
			if err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {
				t.Fatal(err)
			}
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "lifecycle-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)

			if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 || fork.ForkRunStatus != "live" || fork.SourceRunStatus != "paused" || fork.SourceFrozen || !reflect.DeepEqual(fork, duplicate) {
				t.Fatalf("fork=%#v duplicate=%#v", fork, duplicate)
			}
			waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, fork.ForkRunID, frontier.EventID)
			requireLifecycleRootLoopForkRevision(t, rt, seed.RunID, entityID, frontier.EventID, fork.ForkRunID)
			childBefore := lifecycleStoredSnapshot(t, rt, fork.ForkRunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
			if !reflect.DeepEqual(fork, duplicate) || lifecycleStoredSnapshot(t, rt, fork.ForkRunID) != childBefore {
				t.Fatal("fork retry changed response or child state/history")
			}
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("fork mutated parent business state/history")
			}
		})
	}
}

func advanceLifecycleForkSourceHistory(t *testing.T, ctx context.Context, selected interface {
	PipelineObligations() pipelineobligation.Store
}, runID string) {
	t.Helper()
	owner := selected.PipelineObligations()
	eventID := uuid.NewString()
	// Hold publication before insertion so live recovery cannot consume the fixture.
	claim, err := owner.ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	settled := false
	defer func() {
		if !settled {
			if err := owner.Release(context.WithoutCancel(ctx), claim); err != nil {
				t.Error(err)
			}
		}
	}()
	storetest.InsertRevisionedExistingRunRootEventRecord(t, ctx, selected,
		eventID, runID, "source.after", eventtest.Producer(events.EventProducerExternal, "fork-source"), []byte(`{"seed":true}`),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), time.Now().UTC())
	outcome, err := owner.Settle(ctx, claim, pipelineobligation.Acknowledged("source_advanced_fixture"))
	settled = outcome.Committed()
	if err != nil || !settled {
		t.Fatalf("settle post-cut source history: committed=%v error=%v", settled, err)
	}
}

func requireLifecycleRootLoopForkRevision(t *testing.T, rt servedControlProofRuntime, parentRun, parentEntity, frontierID, runID string) {
	t.Helper()
	entity := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, runID, "", "review")
	parent := readLifecycleStoredLoop(t, rt, parentRun, parentEntity)
	child := readLifecycleStoredLoop(t, rt, runID, entity)
	if child.ActivationID == parent.ActivationID || child.RevisionID == parent.RevisionID ||
		child.Attempt != parent.Attempt || child.MaxAttempts != parent.MaxAttempts ||
		parent.CurrentStage != "drafting" || child.CurrentStage != "review" || child.Status != parent.Status ||
		child.CloseReason != parent.CloseReason || child.LoopID != parent.LoopID || child.RevisionField != parent.RevisionField {
		t.Fatalf("fork loop identity/business facts: parent=%#v child=%#v", parent, child)
	}
	public := readLifecycleLoop(t, rt.ReceiverStateReader, runID, entity)
	if public.RevisionID != child.RevisionID || public.Status != loopruntime.StatusOpen {
		t.Fatalf("child public loop=%#v", public)
	}
	var childEvent, original, copied string
	if err := rt.DB.QueryRow(`SELECT event_id,CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='loop.admit'`, runID).Scan(&childEvent, &copied); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_id=$2`, parentRun, frontierID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var sourcePayload, childPayload map[string]any
	if err := json.Unmarshal([]byte(original), &sourcePayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(copied), &childPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sourcePayload, map[string]any{"revision_id": parent.RevisionID}) ||
		!reflect.DeepEqual(childPayload, map[string]any{"revision_id": child.RevisionID}) {
		t.Fatalf("root-declared revision did not project: source=%#v child=%#v", sourcePayload, childPayload)
	}
	for run, event := range map[string]string{parentRun: frontierID, runID: childEvent} {
		source := readForkReceiverProducerEvidence(t, rt, run, event).Source
		if source.Kind() != events.RoutingSourceRoot {
			t.Fatalf("root input source changed: %#v", source)
		}
	}
	var status string
	if err := rt.DB.QueryRow(`SELECT status FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, runID, childEvent).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" {
		t.Fatalf("projected root input delivery = %s, want delivered", status)
	}
	for _, event := range []string{"loop.close", "work.completed"} {
		requireLifecycleEventCount(t, rt, runID, event, 0)
	}
}

func readLifecycleStoredLoop(t *testing.T, rt servedControlProofRuntime, runID, entityID string) loopruntime.Activation {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var bucket map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &bucket); err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(bucket, ".", "revision")
	if err != nil || !found {
		t.Fatalf("loop missing: %v %s", err, raw)
	}
	return loop
}
