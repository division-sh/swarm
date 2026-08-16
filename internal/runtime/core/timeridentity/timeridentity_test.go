package timeridentity

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestTimerHandleRoundTripPreservesLoopGeneration(t *testing.T) {
	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	handle := WorkflowTimerHandleForGeneration("review.expiry", generation)
	parsed, ok := ParseTimerHandle(handle.PayloadMetadata())
	if !ok || !parsed.Generation().Equal(generation) || parsed.TaskID() != handle.TaskID() {
		t.Fatalf("parsed handle = %#v ok=%v", parsed, ok)
	}
}

func TestTimerHandleParserRejectsRetiredAccumulationTimeoutKind(t *testing.T) {
	payload := map[string]any{
		"timer_handle": map[string]any{
			"kind":   "accumulation_timeout",
			"bucket": map[string]any{"node_id": "collector", "event_type": "item.arrived"},
		},
	}
	if handle, ok := ParseTimerHandle(payload); ok {
		t.Fatalf("retired accumulation timeout parsed as %#v", handle)
	}
}

func TestParseStartTrigger(t *testing.T) {
	trigger, err := ParseStartTrigger("state:active")
	if err != nil {
		t.Fatalf("ParseStartTrigger: %v", err)
	}
	if trigger.Kind != TriggerKindState || trigger.Name != "active" {
		t.Fatalf("trigger = %#v", trigger)
	}
	if !trigger.MatchesStage("active") {
		t.Fatal("expected state trigger to match stage")
	}
}

func TestParseTriggerRejectsUnprefixedValues(t *testing.T) {
	if _, err := ParseStartTrigger("ticket.opened"); err == nil {
		t.Fatal("expected unprefixed trigger to be rejected")
	}
}

func TestParseCancelTriggerRejectsBoot(t *testing.T) {
	if _, err := ParseCancelTrigger("boot"); err == nil {
		t.Fatal("expected cancel_on boot to be rejected")
	}
}

func TestParseDelayDurationSupportsGoDurationAndDayUnit(t *testing.T) {
	tests := []struct {
		raw  string
		want time.Duration
	}{
		{raw: "30m", want: 30 * time.Minute},
		{raw: "1h30m", want: 90 * time.Minute},
		{raw: "7d", want: 7 * 24 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, ok := ParseDelayDuration(tc.raw)
			if !ok {
				t.Fatalf("ParseDelayDuration(%q) did not parse", tc.raw)
			}
			if got != tc.want {
				t.Fatalf("ParseDelayDuration(%q) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseDelayDurationRejectsInvalidOrNonPositiveValues(t *testing.T) {
	for _, raw := range []string{"", "0s", "-1s", "1.5d", "soon"} {
		t.Run(raw, func(t *testing.T) {
			if got, ok := ParseDelayDuration(raw); ok {
				t.Fatalf("ParseDelayDuration(%q) = %s, want rejection", raw, got)
			}
		})
	}
}

func TestTimerHandlePayloadRoundTrip(t *testing.T) {
	handle := WorkflowTimerHandle("check_timer")
	parsed, ok := ParseTimerHandle(handle.PayloadMetadata())
	if !ok {
		t.Fatal("expected workflow timer handle payload to round trip")
	}
	if parsed.Kind() != TimerHandleWorkflowTimer || parsed.TimerID() != "check_timer" {
		t.Fatalf("parsed = %#v", parsed)
	}
	if parsed.TaskID() != "check_timer" {
		t.Fatalf("TaskID() = %q", parsed.TaskID())
	}
}

func TestAccumulatorBucketRefRetainsStreamWindowAndGenerationIdentity(t *testing.T) {
	bucket := NewAccumulatorWindowBucketRef(identitytest.RootNode(t, "collector"), "item.arrived", "2026-Q1:closed")
	parsedBucket, ok := ParseAccumulatorBucketKey(bucket.Key())
	if !ok {
		t.Fatalf("ParseAccumulatorBucketKey(%q) failed", bucket.Key())
	}
	if parsedBucket != bucket {
		t.Fatalf("parsed bucket key = %#v, want %#v", parsedBucket, bucket)
	}
}

func TestParseAccumulatorBucketKey(t *testing.T) {
	if _, ok := ParseAccumulatorBucketKey("collector:item.arrived"); ok {
		t.Fatal("legacy local-node accumulator bucket key was accepted")
	}
	bucket := NewAccumulatorWindowBucketRef(identitytest.RootNode(t, "collector"), "item.arrived", "")
	parsed, ok := ParseAccumulatorBucketKey(bucket.Key())
	if !ok || !parsed.Node.Equal(bucket.Node) || parsed.EventType != "item.arrived" {
		t.Fatalf("parsed bucket = %#v ok=%v", parsed, ok)
	}
}

func TestJoinHandleRoundTripIncludesOwningFlowAndStageIdentity(t *testing.T) {
	awaiting := mustJoinHandle(t, TimerHandleJoinTimeout, "orders", "join-node", "item.completed", "awaiting", "shared", "window-1", attemptgeneration.Generation{})
	reviewing := mustJoinHandle(t, TimerHandleJoinTimeout, "orders", "join-node", "item.completed", "reviewing", "shared", "window-1", attemptgeneration.Generation{})
	foreignFlow := mustJoinHandle(t, TimerHandleJoinTimeout, "returns", "join-node", "item.completed", "awaiting", "shared", "window-1", attemptgeneration.Generation{})
	foreignPackage := mustJoinHandleForNode(t, TimerHandleJoinTimeout, identitytest.ExecutableNode(t, "dependency", "orders", "join-node"), "item.completed", "awaiting", "shared", "window-1", attemptgeneration.Generation{})
	if awaiting.TaskID() == reviewing.TaskID() {
		t.Fatalf("join task ids collide across stages: %q", awaiting.TaskID())
	}
	if awaiting.TaskID() == foreignFlow.TaskID() {
		t.Fatalf("join task ids collide across owning flows: %q", awaiting.TaskID())
	}
	if awaiting.TaskID() == foreignPackage.TaskID() {
		t.Fatalf("join task ids collide across owning packages: %q", awaiting.TaskID())
	}
	parsed, ok := ParseTimerHandle(awaiting.PayloadMetadata())
	ref, refOK := parsed.JoinRef()
	if !ok || !refOK || !ref.Node().Equal(identitytest.FlowNode(t, "orders", "join-node")) || ref.Stage() != "awaiting" || ref.JoinID() != "shared" || ref.Window() != "window-1" {
		t.Fatalf("parsed join handle = %#v, %v", parsed, ok)
	}
}

func TestJoinHandleGenerationHasOneCanonicalOwner(t *testing.T) {
	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	handle := mustJoinHandle(t, TimerHandleJoinTimeout, "", "join-node", "item.completed", "awaiting", "shared", "window-1", generation)
	payload := handle.PayloadMetadata()
	inner := payload["timer_handle"].(map[string]any)
	if _, duplicated := inner[attemptgeneration.PayloadKey]; duplicated {
		t.Fatal("join generation was duplicated outside JoinRef")
	}
	inner[attemptgeneration.PayloadKey] = generation.PayloadValue()
	if parsed, ok := ParseTimerHandle(payload); ok {
		t.Fatalf("join handle with duplicate outer generation parsed as %#v", parsed)
	}
}

func TestJoinHandleCodecRejectsMissingOrContradictoryDeclarationIdentity(t *testing.T) {
	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	root := mustJoinHandle(t, TimerHandleJoinTimeout, "", "join-node", "item.completed", "awaiting", "shared", "window-1", generation)
	if _, ref, ok := ParseJoinHandle(root.PayloadMetadata()); !ok || ref.FlowID() != "" || !ref.Generation().Equal(generation) {
		t.Fatalf("explicit root handle failed strict round trip: ref=%#v ok=%v", ref, ok)
	}

	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing package coordinate", mutate: func(payload map[string]any) {
			delete(payload["timer_handle"].(map[string]any)["join"].(map[string]any)["node"].(map[string]any), "package_key")
		}},
		{name: "missing flow coordinate", mutate: func(payload map[string]any) {
			delete(payload["timer_handle"].(map[string]any)["join"].(map[string]any)["node"].(map[string]any), "flow_id")
		}},
		{name: "missing local coordinate", mutate: func(payload map[string]any) {
			delete(payload["timer_handle"].(map[string]any)["join"].(map[string]any)["node"].(map[string]any), "node_id")
		}},
		{name: "missing derived revision", mutate: func(payload map[string]any) { delete(payload, "revision_id") }},
		{name: "contradictory derived revision", mutate: func(payload map[string]any) { payload["revision_id"] = "rev-hostile" }},
		{name: "duplicate outer generation", mutate: func(payload map[string]any) {
			payload["timer_handle"].(map[string]any)[attemptgeneration.PayloadKey] = generation.PayloadValue()
		}},
		{name: "unknown join field", mutate: func(payload map[string]any) {
			payload["timer_handle"].(map[string]any)["join"].(map[string]any)["flow_name"] = "root"
		}},
		{name: "unknown payload field", mutate: func(payload map[string]any) { payload["workflow_name"] = "root" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cloneTimerPayload(t, root.PayloadMetadata())
			tc.mutate(payload)
			if handle, ref, ok := ParseJoinHandle(payload); ok {
				t.Fatalf("hostile payload parsed as handle=%#v ref=%#v", handle, ref)
			}
		})
	}
}

func cloneTimerPayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func mustJoinHandle(t *testing.T, kind TimerHandleKind, flowID, nodeID, handlerEvent, stage, joinID, window string, generation attemptgeneration.Generation) TimerHandle {
	t.Helper()
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", flowID, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return mustJoinHandleForNode(t, kind, node, handlerEvent, stage, joinID, window, generation)
}

func mustJoinHandleForNode(t *testing.T, kind TimerHandleKind, node runtimeidentity.ExecutableNode, handlerEvent, stage, joinID, window string, generation attemptgeneration.Generation) TimerHandle {
	t.Helper()
	ref, err := NewJoinRefForGeneration(node, handlerEvent, stage, joinID, window, generation)
	if err != nil {
		t.Fatal(err)
	}
	var handle TimerHandle
	if kind == TimerHandleJoinComplete {
		handle, err = JoinCompleteHandle(ref)
	} else {
		handle, err = JoinTimeoutHandle(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	return handle
}
