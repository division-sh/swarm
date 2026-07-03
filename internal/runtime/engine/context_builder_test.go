package engine

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
)

func TestBuildBaseContext_CopiesPayloadMetadataAndPolicy(t *testing.T) {
	input := ContextBuilderInput{
		FlowID: "flow-1",
		State: StateSnapshot{
			EntityID:        "entity-1",
			WorkflowName:    "wf",
			WorkflowVersion: "v1",
			CurrentState:    "researching",
			StateCarrier:    NewStateCarrier(map[string]any{"k": "v"}, map[string]bool{"review": true}, nil),
		},
		Payload: map[string]any{"p": "x"},
	}

	base := BuildBaseContext(input)

	if got := base.PlatformEntity.Raw()["id"]; got != "entity-1" {
		t.Fatalf("_entity.id = %#v", got)
	}
	if got := base.Entity.Raw()["k"]; got != "v" {
		t.Fatalf("entity metadata not reflected into entity context: %#v", got)
	}
	if got := base.PlatformEntity.Raw()["current_state"]; got != "researching" {
		t.Fatalf("_entity.current_state = %#v", got)
	}
	if _, ok := base.Entity.Raw()["current_state"]; ok {
		t.Fatalf("platform current_state leaked into entity context: %#v", base.Entity.Raw())
	}
	if got := base.Gates.Bool("review"); !got {
		t.Fatalf("gates bucket missing review: %#v", base.Gates.Raw())
	}
	input.Payload["p"] = "changed"
	input.State.StateCarrier.Metadata["k"] = "changed"
	if got := base.Payload.Raw()["p"]; got != "x" {
		t.Fatalf("payload clone lost isolation: %#v", got)
	}
	if got := base.Metadata.Raw()["k"]; got != "v" {
		t.Fatalf("metadata clone lost isolation: %#v", got)
	}
}

func TestBuildBaseContext_UsesConcreteFlowInstanceForPlatformEntity(t *testing.T) {
	input := ContextBuilderInput{
		FlowID: "child",
		State: StateSnapshot{
			EntityID:     "entity-1",
			WorkflowName: "child",
			CurrentState: "waiting",
			StateCarrier: NewStateCarrier(map[string]any{
				"flow_path": "child/inst-1",
			}, nil, nil),
		},
	}

	base := BuildBaseContext(input)

	if got := base.PlatformEntity.Raw()["flow_instance"]; got != "child/inst-1" {
		t.Fatalf("_entity.flow_instance = %#v, want concrete flow path", got)
	}
	if got := base.FlowID; got != "child" {
		t.Fatalf("base FlowID = %q, want executing flow id", got)
	}
}

func TestContextOverlayHelpers_DoNotMutateOriginal(t *testing.T) {
	base := BaseContext{
		Payload:     values.Wrap(map[string]any{"a": 1}),
		Accumulated: values.Wrap(map[string]any{"b": 2}),
		FanOut:      values.Wrap(map[string]any{"c": 3}),
	}

	withPayload := WithPayload(base, map[string]any{"a": 9})
	withAccumulated := WithAccumulated(base, map[string]any{"b": 8})
	withFanOut := WithFanOutItem(base, map[string]any{"c": 7})

	if got := base.Payload.Raw()["a"]; got != 1 {
		t.Fatalf("base payload mutated: %#v", got)
	}
	if got := base.Accumulated.Raw()["b"]; got != 2 {
		t.Fatalf("base accumulated mutated: %#v", got)
	}
	if got := base.FanOut.Raw()["c"]; got != 3 {
		t.Fatalf("base fanout mutated: %#v", got)
	}
	if got := withPayload.Payload.Raw()["a"]; got != 9 {
		t.Fatalf("withPayload wrong value: %#v", got)
	}
	if got := withAccumulated.Accumulated.Raw()["b"]; got != 8 {
		t.Fatalf("withAccumulated wrong value: %#v", got)
	}
	if got := withFanOut.FanOut.Raw()["c"]; got != 7 {
		t.Fatalf("withFanOut wrong value: %#v", got)
	}
}

func TestExecutionStateBucketHelpers(t *testing.T) {
	state := ExecutionState{}
	state.SetAccumulated("node-1", map[string]any{"count": 2})
	state.SetFanOut("target", "review")
	state.SetComputed("score", 9)

	if got, ok := state.AccumulatedBucket().Lookup(paths.Parse("node-1.count")); !ok || got != 2 {
		t.Fatalf("accumulated lookup = %#v, %v", got, ok)
	}
	if got, ok := state.FanOutBucket().Lookup(paths.Parse("target")); !ok || got != "review" {
		t.Fatalf("fanout lookup = %#v, %v", got, ok)
	}
	if got, ok := state.ComputedBucket().Lookup(paths.Parse("score")); !ok || got != 9 {
		t.Fatalf("computed lookup = %#v, %v", got, ok)
	}
}

func TestStateSnapshotBucketHelpers(t *testing.T) {
	var snapshot StateSnapshot
	snapshot.SetMetadata("status", "ready")
	snapshot.SetGate("review", true)

	if got := snapshot.MetadataBucket().String("status"); got != "ready" {
		t.Fatalf("metadata string = %q", got)
	}
	if !snapshot.StateCarrier.Gates["review"] {
		t.Fatalf("snapshot gates missing: %#v", snapshot.StateCarrier.Gates)
	}
	if !snapshot.GatesBucket().Bool("review") {
		t.Fatalf("typed gates missing: %#v", snapshot.GatesBucket().Raw())
	}
}

func TestStateMutationAndResultBucketHelpers(t *testing.T) {
	var mutation StateMutation
	mutation.SetMetadata("status", "ready")
	mutation.SetGateValue("review", false)
	mutation.SetStateBuckets(map[string]map[string]any{"node-1": {"count": 2}})

	if got := mutation.MetadataBucket().String("status"); got != "ready" {
		t.Fatalf("mutation metadata string = %q", got)
	}
	if mutation.StateCarrier.Gates["review"] {
		t.Fatalf("mutation gates wrong: %#v", mutation.StateCarrier.Gates)
	}
	if mutation.GatesBucket().Bool("review") {
		t.Fatalf("mutation gates wrong: %#v", mutation.GatesBucket().Raw())
	}
	if bucket, ok := mutation.StateBucketsBucket().Map("node-1"); !ok || bucket.Int("count") != 2 {
		t.Fatalf("state buckets wrong: %#v, %v", bucket.Raw(), ok)
	}

	var result ExecutionResult
	result.SetComputed("score", 9)
	if got := result.ComputedBucket().Int("score"); got != 9 {
		t.Fatalf("computed bucket score = %d", got)
	}
}
