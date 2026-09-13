package serveapp

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
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

func TestServedCompiledFrozenGateForkControlRefusalOnBothStores(t *testing.T) {
	runLifecycleRootForkPolicy(t, true)
}

func TestServedCompiledAbsentSourceLoopForkUnexpectedRevisionOnBothStores(t *testing.T) {
	runLifecycleRootForkPolicy(t, false)
}

func runLifecycleRootForkPolicy(t *testing.T, gate bool) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleForkSource(t, gate))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
			stage := "waiting"
			if gate {
				stage = "review"
			}
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", stage)
			var parentCard map[string]any
			if gate {
				parentCard = lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			} else {
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "loop.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-loop-start"})
				stage = "drafting"
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, stage)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "fork-pause"})
			frontierEvent := "work.observed"
			frontierPayload := map[string]any{"seed": true}
			if !gate {
				frontierEvent = "loop.admit"
				frontierPayload = map[string]any{"revision_id": readLifecycleLoop(t, rt, seed.RunID, entityID).RevisionID}
			}
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": frontierEvent, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": frontierPayload, "idempotency_key": "fork-frontier"})
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "lifecycle-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)

			if fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 || !reflect.DeepEqual(fork, duplicate) {
				t.Fatalf("fork=%#v duplicate=%#v", fork, duplicate)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			if gate {
				requireLifecycleFrozenGateControlRefusal(t, rt, seed.RunID, parentCard, fork.ForkRunID)
			} else {
				requireLifecycleAbsentSourceUnexpectedRevision(t, rt, seed.RunID, entityID, frontier.EventID, fork.ForkRunID)
			}
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

func requireLifecycleFrozenGateControlRefusal(t *testing.T, rt servedControlProofRuntime, parentRun string, parentParams map[string]any, runID string) {
	t.Helper()
	entity := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, runID, "", "review")
	childParams := lifecycleGateDecisionParams(t, rt, runID, "approve")
	readCard := func(id any) map[string]any {
		var detail struct {
			Card map[string]any `json:"decision_card"`
		}
		requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": id}, &detail)
		if detail.Card == nil {
			t.Fatal("missing public card")
		}
		return detail.Card
	}
	parent, child := readCard(parentParams["card_id"]), readCard(childParams["card_id"])
	if child["card_id"] == parent["card_id"] || child["run_id"] != runID || child["status"] != "pending" {
		t.Fatalf("fork card identity/status: parent=%#v child=%#v", parent, child)
	}
	for _, key := range []string{"snapshot", "card_content_hash", "decision_schema_hash", "bundle_hash", "workflow_version", "effective_cadence", "decision"} {
		if !reflect.DeepEqual(parent[key], child[key]) {
			t.Fatalf("frozen %s changed: parent=%#v child=%#v", key, parent, child)
		}
	}
	pa, ok := parent["anchor"].(map[string]any)
	ca, childOK := child["anchor"].(map[string]any)
	if !ok || !childOK {
		t.Fatalf("missing anchors: parent=%#v child=%#v", parent, child)
	}
	parentEntity, ok := pa["entity_id"].(string)
	if !ok || parentEntity == "" {
		t.Fatal("parent gate has no exact entity anchor")
	}
	parentGate := readLifecycleTemplateGate(t, rt, parentRun, parentEntity, ".")
	childGate := readLifecycleTemplateGate(t, rt, runID, entity, ".")
	if childGate.ActivationID != ca["stage_activation_id"] || childGate.CardID != child["card_id"] ||
		childGate.RoutesJSON != parentGate.RoutesJSON || childGate.BundleHash != parentGate.BundleHash ||
		childGate.DecisionID != parentGate.DecisionID || childGate.Stage != parentGate.Stage ||
		childGate.Status != parentGate.Status || !childGate.OpenedAt.Equal(parentGate.OpenedAt) {
		t.Fatalf("fork changed frozen gate authority: parent=%#v child=%#v", parentGate, childGate)
	}
	for _, key := range []string{"flow_id", "stage"} {
		if ca[key] != pa[key] {
			t.Fatalf("fork changed declaration %s: %#v", key, ca)
		}
	}
	if ca["entity_id"] != entity || ca["stage_activation_id"] == "" || ca["stage_activation_id"] == pa["stage_activation_id"] ||
		ca["flow_instance"] != runID || ca["flow_instance_id"] != runID {
		t.Fatalf("fork-local anchor not exact: parent=%#v child=%#v", pa, ca)
	}
	raw, err := json.Marshal(eventtest.RootRoutingSource(entity))
	if err != nil {
		t.Fatal(err)
	}
	var expected any
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ca["routing_source"], expected) {
		t.Fatalf("fork source=%#v want=%#v", ca["routing_source"], expected)
	}
	var bindingID string
	if err := rt.DB.QueryRow(`SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, runID).Scan(&bindingID); err != nil {
		t.Fatal(err)
	}
	// Await legitimate completion maintenance before attributing a snapshot delta
	// to refusal. No application table or completion-candidate column is excluded.
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		var settled bool
		if err := rt.DB.QueryRow(`SELECT completion_due_at IS NULL FROM runs WHERE run_id=$1`, runID).Scan(&settled); err != nil {
			t.Fatal(err)
		}
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fork completion candidate did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before := snapshotForkReceiverApplication(t, rt)
	for attempt := 0; attempt < 2; attempt++ {
		refusal := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", childParams)
		details, ok := refusal.Data["details"].(map[string]any)
		if refusal.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" || refusal.Data["retryable"] != false || !ok ||
			details["operation"] != "mailbox.decide" || details["run_id"] != runID || details["binding_id"] != bindingID ||
			details["required_capability"] != "selected_fork_deferred_execution" {
			t.Fatalf("wrong selected refusal: %+v", refusal)
		}
		after := snapshotForkReceiverApplication(t, rt)
		if !reflect.DeepEqual(before, after) {
			for table, rows := range before {
				if !reflect.DeepEqual(rows, after[table]) {
					t.Errorf("refusal changed %s: before=%v after=%v", table, rows, after[table])
				}
			}
			t.Fatal("refused control changed application database, including completion records")
		}
	}
	if !reflect.DeepEqual(parent, readCard(parentParams["card_id"])) || !reflect.DeepEqual(child, readCard(childParams["card_id"])) {
		t.Fatal("refusal changed either frozen card")
	}
	requireLifecycleEventCount(t, rt, runID, "work.completed", 0)
	requireLifecycleEventCount(t, rt, parentRun, "work.completed", 0)
}

func requireLifecycleAbsentSourceUnexpectedRevision(t *testing.T, rt servedControlProofRuntime, parentRun, parentEntity, frontierID, runID string) {
	t.Helper()
	entity := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, runID, "", "drafting")
	parent := readLifecycleStoredLoop(t, rt, parentRun, parentEntity)
	child := readLifecycleStoredLoop(t, rt, runID, entity)
	if child.ActivationID == parent.ActivationID || child.RevisionID == parent.RevisionID ||
		child.Attempt != parent.Attempt || child.MaxAttempts != parent.MaxAttempts ||
		child.CurrentStage != parent.CurrentStage || child.Status != parent.Status ||
		child.CloseReason != parent.CloseReason || child.LoopID != parent.LoopID || child.RevisionField != parent.RevisionField {
		t.Fatalf("fork loop identity/business facts: parent=%#v child=%#v", parent, child)
	}
	public := readLifecycleLoop(t, rt, runID, entity)
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
	if copied != original {
		t.Fatalf("opaque payload rewritten: original=%s child=%s", original, copied)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(copied), &payload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload, map[string]any{"revision_id": parent.RevisionID}) {
		t.Fatalf("unexpected opaque payload: %#v", payload)
	}
	for run, event := range map[string]string{parentRun: frontierID, runID: childEvent} {
		source := readForkReceiverProducerEvidence(t, rt, run, event).Source
		if source.Kind() != events.RoutingSourceAbsent {
			t.Fatalf("invented source: %#v", source)
		}
	}
	var status, reason, rawFailure string
	if err := rt.DB.QueryRow(`SELECT status,reason_code,CAST(failure AS TEXT) FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, runID, childEvent).Scan(&status, &reason, &rawFailure); err != nil {
		t.Fatal(err)
	}
	var failure failures.Envelope
	if err := json.Unmarshal([]byte(rawFailure), &failure); err != nil {
		t.Fatal(err)
	}
	if status != "dead_letter" || reason != "handler_terminal_failure" || failure.Class != failures.ClassUnexpectedArrival ||
		failure.Detail.Code != "loop_revision_unexpected" || failure.Retryable || !failure.Deterministic ||
		failure.Detail.Attributes["supplied_revision_id"] != parent.RevisionID || failure.Detail.Attributes["current_revision_id"] != child.RevisionID {
		t.Fatalf("wrong terminal delivery: %s/%s %s", status, reason, rawFailure)
	}
	for _, record := range readLifecycleTransitionHistory(t, rt, runID, entity) {
		if record.To == "review" || record.To == "done" {
			t.Fatalf("unexpected revision advanced child: %#v", record)
		}
	}
	for _, event := range []string{"loop.close", "work.completed"} {
		requireLifecycleEventCount(t, rt, runID, event, 0)
	}
}

func readLifecycleStoredLoop(t *testing.T, rt servedControlProofRuntime, runID, entityID string) loopruntime.Activation {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
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
