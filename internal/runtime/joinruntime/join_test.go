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
	if left, right := ActivationKey(testJoinRef(t, "", "items", "review", "node", "item.done", "entry", first)), ActivationKey(testJoinRef(t, "", "items", "review", "node", "item.done", "entry", second)); left == right || left == "" || right == "" {
		t.Fatalf("generation keys collide: %q %q", left, right)
	}
}

func TestActivationOrdersResultsByMembershipAndClassifiesDuplicates(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinRef(t, "", "line_items", "awaiting", "node", "item.done", "dispatch-1", attemptgeneration.Generation{}), []string{"a", "b"}, nil, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := activation.Add("b", map[string]any{"score": 2}); err != nil || got != AddAccepted {
		t.Fatalf("add b = %q, %v", got, err)
	}
	if got, err := activation.Add("a", map[string]any{"score": 1}); err != nil || got != AddAccepted {
		t.Fatalf("add a = %q, %v", got, err)
	}
	got, err := activation.Results()
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{map[string]any{"score": int64(1)}, map[string]any{"score": int64(2)}}; !reflect.DeepEqual(got, want) {
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

func TestA2ArrivalContextFieldsMatchRuntimeProjection(t *testing.T) {
	at := time.Now().UTC()
	activation, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "entry", attemptgeneration.Generation{}), []string{"a"}, nil, at, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	context, err := activation.Context()
	if err != nil {
		t.Fatal(err)
	}
	fields := SupportedContextFields()
	if len(fields) != len(context) {
		t.Fatalf("declared context fields=%v differ from runtime projection=%#v", fields, context)
	}
	for _, field := range fields {
		if _, found := context[field]; !found {
			t.Fatalf("declared field %q has no runtime projection", field)
		}
	}
}

func TestActivationPersistsThroughTypedStateBuckets(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), []string{}, nil, now, now.Add(time.Hour))
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

func TestA2JoinOutputRoundTripPreservesAdmittedNumberKinds(t *testing.T) {
	now := time.Now().UTC()
	activation, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "entry", attemptgeneration.Generation{}), []string{"a"}, nil, now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	output := map[string]any{"integer": int64(1), "double": float64(1), "nested": []any{float64(2), int64(2)}}
	if disposition, err := activation.Add("a", output); err != nil || disposition != AddAccepted {
		t.Fatalf("add: %s %v", disposition, err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := Load(buckets, activation.JoinRef().Node(), activation.Key())
	if err != nil || !found {
		t.Fatalf("load: found=%v %v", found, err)
	}
	results, err := loaded.Results()
	if err != nil || !reflect.DeepEqual(results, []any{output}) {
		t.Fatalf("numeric results changed: %#v %v", results, err)
	}
	if disposition, err := loaded.Add("a", output); err != nil || disposition != AddExactDuplicate {
		t.Fatalf("readback changed duplicate identity: %s %v", disposition, err)
	}
	if listed, err := List(buckets); err != nil || len(listed) != 1 || !listed[0].JoinRef().Equal(activation.JoinRef()) {
		t.Fatalf("list: %#v %v", listed, err)
	}
}

func TestJoinActivationPersistsTypedDeclarationHandle(t *testing.T) {
	generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	activation, err := NewActivation(testJoinRef(t, "", "shared", "awaiting", "join-node", "item.completed", "entry-1", generation), []string{"a"}, nil, now, now.Add(time.Hour))
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
	_, hasRetiredFlowID := persistedNode["flow_id"]
	if !ok || !joinOK || !nodeOK || !generationOK || persistedNode["flow_path"] != "." || hasRetiredFlowID || persistedNode["node_id"] != "join-node" || persistedGeneration["revision_id"] != "rev-2" {
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
			activation, err := NewActivation(testJoinRef(t, "", "shared", "awaiting", "join-node", "item.completed", "", attemptgeneration.Generation{}), []string{"a"}, nil, now, now.Add(time.Hour))
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
		if _, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), members, nil, now, now.Add(time.Hour)); err == nil {
			t.Fatalf("members %#v accepted", members)
		}
	}
}

func TestA2JoinLoadAndListRejectContradictoryBucketOwnership(t *testing.T) {
	for _, corruption := range []string{"node", "key", "bucket_shape"} {
		t.Run(corruption, func(t *testing.T) {
			at := time.Now().UTC()
			activation, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "entry", attemptgeneration.Generation{}), []string{"a"}, nil, at, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			buckets := map[string]map[string]any{}
			if err := Store(buckets, activation); err != nil {
				t.Fatal(err)
			}
			node, key := activation.JoinRef().Node(), activation.Key()
			switch corruption {
			case "node":
				original := node
				node = identitytest.RootNode(t, "other")
				buckets[joinNodeBucketKey(node)] = buckets[joinNodeBucketKey(original)]
				delete(buckets, joinNodeBucketKey(original))
			case "key":
				joins := buckets[joinNodeBucketKey(node)][bucketKey].(map[string]any)
				wrongKey := key + "-foreign"
				joins[wrongKey] = joins[key]
				delete(joins, key)
				key = wrongKey
			case "bucket_shape":
				buckets[joinNodeBucketKey(node)][bucketKey] = []any{"corrupt"}
			}
			before, err := json.Marshal(buckets)
			if err != nil {
				t.Fatal(err)
			}
			if loaded, found, err := Load(buckets, node, key); err == nil || found {
				t.Fatalf("single-arm reader accepted %s corruption: found=%v arm=%#v err=%v", corruption, found, loaded, err)
			}
			if listed, err := List(buckets); err == nil || len(listed) != 0 {
				t.Fatalf("catalog reader accepted %s corruption: arms=%#v err=%v", corruption, listed, err)
			}
			if corruption == "bucket_shape" {
				if err := Store(buckets, activation); err == nil {
					t.Fatal("writer replaced corrupt retained state with a new activation map")
				}
			}
			after, err := json.Marshal(buckets)
			if err != nil || string(after) != string(before) {
				t.Fatalf("corruption refusal changed persisted evidence: %v", err)
			}
		})
	}
}

func TestActivationKeyIncludesStageIdentity(t *testing.T) {
	awaiting := ActivationKey(testJoinRef(t, "", "shared", "awaiting", "node", "item.done", "entry-1", attemptgeneration.Generation{}))
	reviewing := ActivationKey(testJoinRef(t, "", "shared", "reviewing", "node", "item.done", "entry-1", attemptgeneration.Generation{}))
	if awaiting == "" || reviewing == "" || awaiting == reviewing {
		t.Fatalf("activation keys = awaiting:%q reviewing:%q, want distinct stage-scoped identities", awaiting, reviewing)
	}
}

func TestA2CountUsesExactContributorIdentityAndLexicalOrder(t *testing.T) {
	now := time.Now().UTC()
	count := 2
	activation, err := NewActivation(testJoinRef(t, "", "join", "awaiting", "node", "item.done", "", attemptgeneration.Generation{}), nil, &count, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"z", " a "} {
		if disposition, err := activation.Add(member, member); err != nil || disposition != AddAccepted {
			t.Fatalf("count arrival %q: %s %v", member, disposition, err)
		}
	}
	results, err := activation.Results()
	if err != nil || !reflect.DeepEqual(results, []any{" a ", "z"}) || activation.Completed() != activation.Expected() || len(activation.Missing()) != 0 {
		t.Fatalf("count result: %#v %v", activation, err)
	}
	if disposition, err := activation.Add(" a ", " a "); err != nil || disposition != AddExactDuplicate {
		t.Fatalf("exact duplicate at count cap: %s %v", disposition, err)
	}
	if disposition, err := activation.Add(" a ", "changed"); err != nil || disposition != AddConflictingDuplicate {
		t.Fatalf("conflicting duplicate at count cap: %s %v", disposition, err)
	}
	if disposition, err := activation.Add("a", "new"); err != nil || disposition != AddUnexpected {
		t.Fatalf("whitespace was used as an identity alias: %s %v", disposition, err)
	}
}

func testJoinRef(t *testing.T, flowID, joinID, stage, nodeID, handlerEvent, occurrence string, generation attemptgeneration.Generation) timeridentity.JoinRef {
	t.Helper()
	node := identitytest.RootNode(t, nodeID)
	if flowID != "" {
		node = identitytest.FlowNode(t, flowID, nodeID)
	}
	ref, err := timeridentity.NewJoinRef(node, handlerEvent, stage, joinID)
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{RunID: "run", FlowScope: "flow", InstanceID: "one", InstancePath: "flow/one", EntityID: "entity", Stage: stage, Cause: "construction"}
	if occurrence != "" {
		entry.Cause, entry.EventID, entry.OccurrenceID, entry.TransitionID = "delivery", "event", occurrence, "transition"
	}
	ref, err = ref.BindStageEntry(entry, generation)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
