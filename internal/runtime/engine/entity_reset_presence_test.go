package engine

import (
	"reflect"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityPrivateBucketResetPreservesFields(t *testing.T) {
	current, peer := testRootExecutableNode(t, "collector"), testRootExecutableNode(t, "peer")
	for _, fields := range []map[string]any{
		{},
		{"dedup_key": "business", "accumulated_count": int64(9), "accumulated_total": int64(12), "received_event_ids": []any{"business"}},
	} {
		before := cloneStringAnyMap(fields)
		frame := &executionFrame{req: ExecutionRequest{Node: current, Handler: c.SystemNodeEventHandler{Clear: &c.ClearSpec{Targets: []string{"accumulator_state"}}}}}
		frame.state.State = testStateSnapshot("ready", fields, nil, nil)
		storeAccumulator(&frame.state.State, current, "work.received", &Accumulator{Items: []map[string]any{{"id": "one"}}})
		storeAccumulator(&frame.state.State, peer, "work.received", &Accumulator{Items: []map[string]any{{"id": "peer"}}})
		peerBefore, _ := frame.state.State.StateBucket(peer.Key())
		peerFields := cloneStringAnyMap(peerBefore.Raw())
		executor := &Executor{}
		if err := executor.stepClear(frame); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(frame.state.State.StateCarrier.Fields, before) {
			t.Fatalf("private reset changed business fields: %#v", frame.state.State.StateCarrier.Fields)
		}
		bucket, _ := frame.state.State.StateBucket(current.Key())
		if _, present := bucket.Raw()[handlerAccumulatorBucketKey]; present {
			t.Fatal("current private accumulator survived reset")
		}
		peerAfter, _ := frame.state.State.StateBucket(peer.Key())
		if !reflect.DeepEqual(peerAfter.Raw(), peerFields) {
			t.Fatal("reset changed a sibling node bucket")
		}
	}
}
