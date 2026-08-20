package joinruntime

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

func TestActivationKeyIsolatesLoopGenerations(t *testing.T) {
	first := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-1", Attempt: 1}
	second := first
	second.RevisionID, second.Attempt = "rev-2", 2
	if left, right := ActivationKeyForGeneration("review", "items", "window", first), ActivationKeyForGeneration("review", "items", "window", second); left == right || left == "" || right == "" {
		t.Fatalf("generation keys collide: %q %q", left, right)
	}
}

func TestActivationOrdersResultsByMembershipAndClassifiesDuplicates(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinHandle(t, "", "line_items", "awaiting", "node", "item.done", "dispatch-1", attemptgeneration.Generation{}), []string{"a", "b"}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := activation.Add("b", map[string]any{"score": 2}); err != nil || got != AddAccepted {
		t.Fatalf("add b = %q, %v", got, err)
	}
	if got, err := activation.Add("a", map[string]any{"score": 1}); err != nil || got != AddAccepted {
		t.Fatalf("add a = %q, %v", got, err)
	}
	if got, want := activation.Results(), []any{map[string]any{"score": float64(1)}, map[string]any{"score": float64(2)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %#v, want membership order %#v", got, want)
	}
	if got, err := activation.Add("a", map[string]any{"score": 1}); err != nil || got != AddExactDuplicate {
		t.Fatalf("exact duplicate = %q, %v", got, err)
	}
	if got, err := activation.Add("a", map[string]any{"score": 9}); err != nil || got != AddConflictingDuplicate {
		t.Fatalf("conflicting duplicate = %q, %v", got, err)
	}
	if got, err := activation.Add("c", map[string]any{"score": 3}); err != nil || got != AddUnexpected {
		t.Fatalf("unexpected member = %q, %v", got, err)
	}
}

func TestActivationPersistsThroughTypedStateBuckets(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinHandle(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), []string{}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	activation.Close(CloseReasonComplete, true, false)
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := Load(buckets, identitytest.RootNode(t, "node"), activation.Key())
	if err != nil || !ok {
		t.Fatalf("load = %#v, %v, %v", loaded, ok, err)
	}
	if !reflect.DeepEqual(loaded, activation) {
		t.Fatalf("round trip = %#v, want %#v", loaded, activation)
	}
}

func TestJoinActivationPersistsTypedDeclarationHandle(t *testing.T) {
	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinHandle(t, "", "shared", "awaiting", "join-node", "item.completed", "window-1", generation), []string{"a"}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	raw := buckets[joinNodeBucketKey(identitytest.RootNode(t, "join-node"))][bucketKey].(map[string]any)[activation.Key()].(map[string]any)
	for _, retired := range []string{"flow_id", "node_id", "handler_event", "stage", "join_id", "window", "loop_generation", "timer_task_id", "timer_event_type"} {
		if _, exists := raw[retired]; exists {
			t.Fatalf("activation persisted retired authority %q: %#v", retired, raw)
		}
	}
	handle, ok := raw["timer_handle"].(map[string]any)
	join, joinOK := handle["join"].(map[string]any)
	persistedGeneration, generationOK := join[attemptgeneration.PayloadKey].(map[string]any)
	persistedNode, nodeOK := join["node"].(map[string]any)
	if !ok || !joinOK || !nodeOK || !generationOK || persistedNode["package_key"] != "." || persistedNode["flow_id"] != "" || persistedNode["node_id"] != "join-node" || persistedGeneration["revision_id"] != "rev-2" {
		t.Fatalf("persisted typed handle = %#v", raw["timer_handle"])
	}
	loaded, found, err := Load(buckets, identitytest.RootNode(t, "join-node"), activation.Key())
	if err != nil || !found || !loaded.JoinRef().Equal(activation.JoinRef()) || loaded.TimerTaskID() != activation.TimerTaskID() {
		t.Fatalf("typed activation readback = found:%v activation:%#v err:%v", found, loaded, err)
	}
}

func TestJoinActivationRejectsRetiredFlatIdentityRows(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, retired := range []string{"flow_id", "join_id", "timer_task_id", "loop_generation"} {
		t.Run(retired, func(t *testing.T) {
			activation, err := NewActivation(testJoinHandle(t, "", "shared", "awaiting", "join-node", "item.completed", "", attemptgeneration.Generation{}), []string{"a"}, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(activation)
			if err != nil {
				t.Fatal(err)
			}
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			row[retired] = "retired-authority"
			buckets := map[string]map[string]any{joinNodeBucketKey(identitytest.RootNode(t, "join-node")): {bucketKey: map[string]any{activation.Key(): row}}}
			if loaded, found, err := Load(buckets, identitytest.RootNode(t, "join-node"), activation.Key()); err == nil || found {
				t.Fatalf("retired row loaded as %#v found=%v err=%v", loaded, found, err)
			}
		})
	}
}

func TestNewActivationRejectsInvalidMembership(t *testing.T) {
	now := time.Now().UTC()
	for _, members := range [][]string{{""}, {"a", "a"}} {
		if _, err := NewActivation(testJoinHandle(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), members, now, now.Add(time.Hour)); err == nil {
			t.Fatalf("members %#v accepted", members)
		}
	}
}

func TestActivationKeyIncludesStageIdentity(t *testing.T) {
	awaiting := ActivationKey("awaiting", "shared", "window-1")
	reviewing := ActivationKey("reviewing", "shared", "window-1")
	if awaiting == "" || reviewing == "" || awaiting == reviewing {
		t.Fatalf("activation keys = awaiting:%q reviewing:%q, want distinct stage-scoped identities", awaiting, reviewing)
	}
}

func TestCompletionSatisfiedUsesOneDefaultAndCustomOwner(t *testing.T) {
	now := time.Now().UTC()
	activation, err := NewActivation(testJoinHandle(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), []string{}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := CompletionSatisfied(activation, "", nil); err != nil || !complete {
		t.Fatalf("default zero-member completion = %v, %v, want true", complete, err)
	}
	called := false
	complete, err := CompletionSatisfied(activation, "join.completed >= 1", func(expression string, joinContext map[string]any) (bool, error) {
		called = true
		if expression != "join.completed >= 1" || joinContext["completed"] != 0 {
			t.Fatalf("custom completion input = %q %#v", expression, joinContext)
		}
		return false, nil
	})
	if err != nil || complete || !called {
		t.Fatalf("custom zero-member completion = complete:%v called:%v err:%v, want false/true/nil", complete, called, err)
	}
}

func testJoinHandle(t *testing.T, flowID, joinID, stage, nodeID, handlerEvent, window string, generation attemptgeneration.Generation) timeridentity.TimerHandle {
	t.Helper()
	node := identitytest.RootNode(t, nodeID)
	if flowID != "" {
		node = identitytest.FlowNode(t, flowID, nodeID)
	}
	ref, err := timeridentity.NewJoinRefForGeneration(node, handlerEvent, stage, joinID, window, generation)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}
