package engine

import (
	"testing"

	"empireai/internal/runtime/core/paths"
	"empireai/internal/runtime/core/values"
)

func TestBuildBaseContext_CopiesPayloadMetadataAndPolicy(t *testing.T) {
	input := ContextBuilderInput{
		FlowID: "flow-1",
		State: StateSnapshot{
			EntityID:        "entity-1",
			WorkflowName:    "wf",
			WorkflowVersion: "v1",
			CurrentState:    "researching",
			Metadata:        map[string]any{"k": "v"},
			Gates:           map[string]bool{"review": true},
		},
		Payload: map[string]any{"p": "x"},
	}

	base := BuildBaseContext(input)

	if got := base.Entity.Raw()["entity_id"]; got != "entity-1" {
		t.Fatalf("entity_id = %#v", got)
	}
	if got := base.Entity.Raw()["k"]; got != "v" {
		t.Fatalf("entity metadata not reflected into entity context: %#v", got)
	}
	if got := base.Entity.Raw()["current_state"]; got != "researching" {
		t.Fatalf("current_state = %#v", got)
	}
	if got := base.Gates.Bool("review"); !got {
		t.Fatalf("gates bucket missing review: %#v", base.Gates.Raw())
	}
	input.Payload["p"] = "changed"
	input.State.Metadata["k"] = "changed"
	if got := base.Payload.Raw()["p"]; got != "x" {
		t.Fatalf("payload clone lost isolation: %#v", got)
	}
	if got := base.Metadata.Raw()["k"]; got != "v" {
		t.Fatalf("metadata clone lost isolation: %#v", got)
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
	if !snapshot.Gates["review"] {
		t.Fatalf("snapshot gates missing: %#v", snapshot.Gates)
	}
	if gates, ok := snapshot.MetadataBucket().Map("gates"); !ok || !gates.Bool("review") {
		t.Fatalf("metadata gates missing: %#v, %v", gates.Raw(), ok)
	}
}

func TestStateMutationAndResultBucketHelpers(t *testing.T) {
	var mutation StateMutation
	mutation.SetMetadata("status", "ready")
	mutation.SetGateValue("review", false)
	mutation.SetStateBuckets(map[string]any{"node-1": map[string]any{"count": 2}})

	if got := mutation.MetadataBucket().String("status"); got != "ready" {
		t.Fatalf("mutation metadata string = %q", got)
	}
	if mutation.Gates["review"] {
		t.Fatalf("mutation gates wrong: %#v", mutation.Gates)
	}
	if gates, ok := mutation.MetadataBucket().Map("gates"); !ok || gates.Bool("review") {
		t.Fatalf("mutation gates wrong: %#v, %v", gates.Raw(), ok)
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
