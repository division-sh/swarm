package engine

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestEntityResetProjectionUsesWholeNodeOwnership(t *testing.T) {
	for _, variant := range []string{"normal", "immutable", "nonempty refinement"} {
		t.Run(variant, func(t *testing.T) {
			root := t.TempDir()
			writeEngineProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: node-reset\n")
			writeEngineProjectionFixtureFile(t, filepath.Join(root, "types.yaml"), "types:\n  Item:\n    id: text\n")
			entities := `work:
  first:
    type: '[Item]'
    materialize_from: collector.first_items
  second:
    type: '[Item]?'
    materialize_from: collector.second_items
  peer:
    type: '[Item]'
    materialize_from: peer.peer_items
`
			if variant == "immutable" {
				entities = strings.Replace(entities, "  first:\n", "  first:\n    immutable: true\n", 1)
			}
			if variant == "nonempty refinement" {
				entities = strings.Replace(entities, "  first:\n", "  first:\n    length: {min: 1}\n", 1)
			}
			writeEngineProjectionFixtureFile(t, filepath.Join(root, "entities.yaml"), entities)
			writeEngineProjectionFixtureFile(t, filepath.Join(root, "events.yaml"), "work.first:\n  id: text\nwork.second:\n  id: text\nwork.peer:\n  id: text\n")
			writeEngineProjectionFixtureFile(t, filepath.Join(root, "nodes.yaml"), `collector:
  execution_type: system_node
  state_schema:
    fields:
      first_items: '[Item]'
      second_items: '[Item]'
  event_handlers:
    work.first:
      accumulate: {into: first_items, dedup_by: payload.id}
    work.second:
      accumulate: {into: second_items, dedup_by: payload.id}
peer:
  execution_type: system_node
  state_schema:
    fields:
      peer_items: '[Item]'
  event_handlers:
    work.peer:
      accumulate: {into: peer_items, dedup_by: payload.id}
`)
			repo := repoRootForEngineProjectionTest(t)
			bundle, err := c.LoadWorkflowContractBundleWithOverrides(repo, root, c.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			executor := &Executor{deps: RuntimeDependencies{Source: semanticview.Wrap(bundle)}}
			frame := &executionFrame{req: ExecutionRequest{Node: testRootExecutableNode(t, "collector"), HandlerEventKey: "work.first"}}
			frame.state.State = testStateSnapshot("ready", map[string]any{
				"first":  []any{map[string]any{"id": "first"}},
				"second": []any{map[string]any{"id": "second"}},
				"peer":   []any{map[string]any{"id": "peer"}},
			}, nil, nil)
			err = executor.resetNodeEntityProjections(frame)
			if err == nil {
				err = executor.validateEntityMutationList(frame)
			}
			if variant != "normal" {
				if err == nil {
					t.Fatal("reset bypassed immutable/refinement validation")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range []string{"first", "second"} {
				if got, present := frame.state.State.StateCarrier.Fields[target]; !present || !reflect.DeepEqual(got, []any{}) {
					t.Fatalf("%s reset = %#v, present=%t; want present empty list", target, got, present)
				}
			}
			if !reflect.DeepEqual(frame.state.State.StateCarrier.Fields["peer"], []any{map[string]any{"id": "peer"}}) {
				t.Fatal("sibling node projection changed")
			}
		})
	}
}
