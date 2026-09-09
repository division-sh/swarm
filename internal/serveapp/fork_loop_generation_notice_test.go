package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedForkLoopGenerationNoticeBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyForkLoopGenerationNotice(t)
			bundle := loadWorkflowValidationBundleAt(t, root)
			carriage, err := semanticview.CompileOriginalLoopCarriage(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			if err := carriage.RequireSource(bundle.SourceArtifact.BundleHash()); err != nil {
				t.Fatal(err)
			}
			if err := carriage.RequireSource("different-selected-source"); err == nil {
				t.Fatal("selected artifact supplied original meaning")
			}
			role, found, err := carriage.Resolve(semanticview.LoopEventScope{FlowID: "review", EventType: "review/review.requested"})
			if err != nil || !found || role.LoopID() != "revision" || role.RevisionField() != "revision_id" {
				t.Fatalf("original input declaration: %+v %v %v", role, found, err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "loop-notice-proof"}, "idempotency_key": "loop-notice-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested'`, started.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			source := readForkLoopNoticeActivation(t, rt, started.RunID)
			if source.Attempt != 1 || source.MaxAttempts != 3 || source.FlowID != "review" || source.CurrentStage != "reviewing" {
				t.Fatalf("ordinary source loop: %+v", source)
			}
			requireForkLoopNotice(t, rt, started.RunID, frontier, source.RevisionID)
			sourceDomain := readServedForkRecipientSourceDomain(t, rt, started.RunID)
			sourceNotices := readForkReceiverNoticeDomain(t, rt, started.RunID)
			params := map[string]any{"source_run_id": started.RunID, "fork_event_id": frontier, "allow_source_freeze": true, "idempotency_key": "loop-notice-fork"}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.SourceRunID != started.RunID || fork.ForkEventID != frontier || fork.ForkRunID == "" || fork.ForkRunID == started.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("fork: %+v", fork)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			child := readForkLoopNoticeActivation(t, rt, fork.ForkRunID)
			expected, err := loopruntime.Fork(source, fork.ForkRunID, flowidentity.EntityID("review"))
			if err != nil {
				t.Fatal(err)
			}
			if child.Generation() != expected.Generation() || child.MaxAttempts != source.MaxAttempts || child.CurrentStage != "reviewing" {
				t.Fatalf("child generation or executed stage: %+v, expected %+v", child, expected)
			}
			childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, frontier)
			requireForkLoopNotice(t, rt, fork.ForkRunID, childEvent, expected.RevisionID)
			beforeRepeat := readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID)
			noticesBeforeRepeat := readForkReceiverNoticeDomain(t, rt, fork.ForkRunID)
			var replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(beforeRepeat, readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID)) || !reflect.DeepEqual(noticesBeforeRepeat, readForkReceiverNoticeDomain(t, rt, fork.ForkRunID)) {
				t.Fatal("replayed fork changed child state or repeated the notice")
			}
			if !reflect.DeepEqual(sourceDomain, readServedForkRecipientSourceDomain(t, rt, started.RunID)) || !reflect.DeepEqual(sourceNotices, readForkReceiverNoticeDomain(t, rt, started.RunID)) {
				t.Fatal("fork changed source loop or business history")
			}
		})
	}
}

func readForkLoopNoticeActivation(t *testing.T, rt servedControlProofRuntime, runID string) loopruntime.Activation {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, flowidentity.EntityID("review")).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil || len(activations) != 1 {
		t.Fatalf("loop inventory: %+v %v", activations, err)
	}
	return activations[0]
}

func requireForkLoopNotice(t *testing.T, rt servedControlProofRuntime, runID, eventID, revision string) {
	t.Helper()
	var public operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
	if public.RunID != runID || public.Payload["revision_id"] != revision || public.Payload["token"] != "loop-notice-proof" || len(public.Deliveries) != 1 || public.Deliveries[0].Status != "delivered" || len(public.DeadLetters) != 0 {
		t.Fatalf("executed revision public readback: %+v", public)
	}
	var count int
	var raw string
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1 AND m.source_event_id=$2 AND m.item_type='fork_loop_review'`, runID, eventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("notice count=%d, want one", count)
	}
	var itemID string
	if err := rt.DB.QueryRow(`SELECT m.item_id,CAST(m.payload AS TEXT) FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1 AND m.source_event_id=$2 AND m.item_type='fork_loop_review'`, runID, eventID).Scan(&itemID, &raw); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["revision_id"] != revision || payload["token"] != "loop-notice-proof" {
		t.Fatalf("real mailbox effect used wrong generation: %v", payload)
	}
	generationJSON, err := json.Marshal(payload["loop_generation"])
	if err != nil {
		t.Fatal(err)
	}
	var generation attemptgeneration.Generation
	if err := json.Unmarshal(generationJSON, &generation); err != nil {
		t.Fatal(err)
	}
	if !generation.Valid() || generation != readForkLoopNoticeActivation(t, rt, runID).Generation() || generation.RevisionID != revision {
		t.Fatalf("durable notice generation: %+v", generation)
	}
	var detail struct {
		Kind   string               `json:"kind"`
		Notice mailbox.V1ItemDetail `json:"notice"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": itemID}, &detail)
	// The public notice projection deliberately excludes the private typed
	// generation. The declared revision field remains public business data.
	wantPublic := map[string]any{"revision_id": revision, "token": "loop-notice-proof"}
	if detail.Kind != "notice" || detail.Notice.Item.SourceEventID != eventID || detail.Notice.Item.SourceFlow != "review" || !reflect.DeepEqual(detail.Notice.Payload, wantPublic) {
		t.Fatalf("public notice lost executed loop identity: %+v; want event=%q payload=%#v", detail, eventID, wantPublic)
	}
	var emitted, deliveries int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, runID, eventID, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&emitted); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`, runID, eventID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if emitted != 0 || deliveries != 0 {
		t.Fatalf("notice produced post-R work: events=%d deliveries=%d", emitted, deliveries)
	}
}
