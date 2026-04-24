package engine

import (
	"strings"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/core/values"

	"gopkg.in/yaml.v3"
)

func TestArrivalIdentifier_PriorityOrder(t *testing.T) {
	evt := events.Event{ID: "evt-1", SourceAgent: "agent-source"}
	payload := map[string]any{
		"id":       "payload-id",
		"event_id": "payload-event",
		"item_id":  "payload-item",
		"source":   "payload-source",
		"from":     "payload-from",
		"agent_id": "payload-agent",
		"node_id":  "payload-node",
	}
	if got := arrivalIdentifier(evt, payload); got != "evt-1" {
		t.Fatalf("arrivalIdentifier = %q", got)
	}

	if got := arrivalIdentifier(events.Event{}, payload); got != "payload-event" {
		t.Fatalf("arrivalIdentifier payload event fallback = %q", got)
	}
	delete(payload, "event_id")
	if got := arrivalIdentifier(events.Event{}, payload); got != "payload-id" {
		t.Fatalf("arrivalIdentifier payload id fallback = %q", got)
	}
	delete(payload, "id")
	if got := arrivalIdentifier(events.Event{}, payload); got != "payload-item" {
		t.Fatalf("arrivalIdentifier item fallback = %q", got)
	}

	if got := arrivalIdentifier(events.Event{ID: "evt-2"}, map[string]any{"dimension": "not-identity"}); got != "evt-2" {
		t.Fatalf("arrivalIdentifier should ignore dimension payloads, got %q", got)
	}
}

func TestDedupIdentifier_UsesContractConfiguredKey(t *testing.T) {
	base := BaseContext{Payload: values.Wrap(map[string]any{
		"dimension": "retention_architecture",
		"from":      "legacy-sender",
	})}
	got := dedupIdentifier(base, ExecutionState{}, events.Event{ID: "evt-1"}, &runtimecontracts.AccumulateSpec{
		DedupBy:   "payload.dimension",
		DedupPath: paths.Parse("payload.dimension"),
	})
	if got != "retention_architecture" {
		t.Fatalf("dedupIdentifier = %q", got)
	}
}

func TestDedupIdentifier_DefaultsToEventIdentityBeforeSource(t *testing.T) {
	base := BaseContext{Payload: values.Wrap(map[string]any{
		"item_id": "payload-item",
		"source":  "legacy-source",
	})}
	got := dedupIdentifier(base, ExecutionState{}, events.Event{ID: "evt-1", SourceAgent: "agent-source"}, nil)
	if got != "evt-1" {
		t.Fatalf("dedupIdentifier default = %q", got)
	}
}

func TestResolveRefRequiresExplicitScope(t *testing.T) {
	base := BaseContext{
		Entity:   values.Wrap(map[string]any{"status": "ready"}),
		Metadata: values.Wrap(map[string]any{"status": "ready"}),
		Payload:  values.Wrap(map[string]any{"score": 7, "nested": map[string]any{"value": "x"}}),
		Policy:   values.Wrap(map[string]any{"mode": "strict"}),
	}
	state := ExecutionState{
		Accumulated: map[string]any{"count": 2},
		FanOut:      map[string]any{"item": "fan"},
		Computed:    map[string]any{"grade": "A"},
	}

	cases := map[string]any{
		"payload.score":        7,
		"payload.nested.value": "x",
		"metadata.status":      "ready",
		"entity.status":        "ready",
		"policy.mode":          "strict",
		"accumulated.count":    2,
		"fan_out.item":         "fan",
		"computed.grade":       "A",
	}
	for ref, want := range cases {
		if got := resolveRef(base, state, ref); got != want {
			t.Fatalf("resolveRef(%q) = %#v, want %#v", ref, got, want)
		}
	}
	if got := resolveRef(base, state, "score"); got != nil {
		t.Fatalf("resolveRef(score) = %#v, want nil for unscoped ref", got)
	}
}

func TestSetValuePathAndEmitFieldsPayload(t *testing.T) {
	base := BaseContext{
		Entity:  values.Wrap(map[string]any{"current_state": "researching"}),
		Payload: values.Wrap(map[string]any{"score": 9}),
	}
	state := ExecutionState{}
	transformed, err := emitFieldsPayload(base, state, runtimecontracts.EmitSpec{
		Fields: map[string]runtimecontracts.ExpressionValue{
			"nested.score":   runtimecontracts.CELExpression("payload.score"),
			"nested.state":   runtimecontracts.CELExpression("entity.current_state"),
			"literal.string": runtimecontracts.CELExpression(`"hello"`),
			"explicit.ref":   runtimecontracts.RefExpression("payload.score"),
			"explicit.value": runtimecontracts.LiteralExpression("ready"),
		},
	})
	if err != nil {
		t.Fatalf("emitFieldsPayload(...) error = %v", err)
	}
	nested, ok := transformed["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested transform missing: %#v", transformed)
	}
	if got := nested["score"]; got != 9 {
		t.Fatalf("nested.score = %#v", got)
	}
	if got := nested["state"]; got != "researching" {
		t.Fatalf("nested.state = %#v", got)
	}
	literal, ok := transformed["literal"].(map[string]any)
	if !ok || literal["string"] != "hello" {
		t.Fatalf("literal transform wrong: %#v", transformed)
	}
	explicit, ok := transformed["explicit"].(map[string]any)
	if !ok {
		t.Fatalf("explicit transform missing: %#v", transformed)
	}
	if got := explicit["ref"]; got != 9 {
		t.Fatalf("explicit.ref = %#v, want 9", got)
	}
	if got := explicit["value"]; got != "ready" {
		t.Fatalf("explicit.value = %#v, want ready", got)
	}
}

func TestEmitFieldsPayload_NormalizesWholeNumberJSONInputs(t *testing.T) {
	base := BaseContext{
		Payload: values.Wrap(map[string]any{"raw_score": 25.0}),
	}
	transformed, err := emitFieldsPayload(base, ExecutionState{}, runtimecontracts.EmitSpec{
		Fields: map[string]runtimecontracts.ExpressionValue{
			"score": runtimecontracts.CELExpression("payload.raw_score * 2"),
		},
	})
	if err != nil {
		t.Fatalf("emitFieldsPayload(...) error = %v", err)
	}
	if got := transformed["score"]; got != 50 {
		t.Fatalf("score = %#v, want 50", got)
	}
}

func TestEmitFieldsPayload_EvaluatesYAMLLoadedScalarEmitFieldsAsCEL(t *testing.T) {
	var spec runtimecontracts.EmitSpec
	if err := yaml.Unmarshal([]byte(`
event: signals.category_ready
fields:
  mode: payload.mode
  batch: "{'scan_id': payload.scan_id, 'geography': payload.geography}"
  count: 0
  quoted_literal: "'ready'"
  explicit_literal:
    literal: ready
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	base := BaseContext{
		Payload: values.Wrap(map[string]any{
			"mode":      "corpus",
			"scan_id":   "scan-1",
			"geography": "us",
		}),
	}
	transformed, err := emitFieldsPayload(base, ExecutionState{}, spec)
	if err != nil {
		t.Fatalf("emitFieldsPayload(...) error = %v", err)
	}
	if got := transformed["mode"]; got != "corpus" {
		t.Fatalf("mode = %#v, want corpus", got)
	}
	batch, ok := transformed["batch"].(map[string]any)
	if !ok {
		t.Fatalf("batch = %#v, want object", transformed["batch"])
	}
	if batch["scan_id"] != "scan-1" || batch["geography"] != "us" {
		t.Fatalf("batch = %#v, want scan/geography from payload", batch)
	}
	if got := transformed["count"]; got != 0 {
		t.Fatalf("count = %#v, want 0", got)
	}
	if got := transformed["quoted_literal"]; got != "ready" {
		t.Fatalf("quoted_literal = %#v, want ready", got)
	}
	if got := transformed["explicit_literal"]; got != "ready" {
		t.Fatalf("explicit_literal = %#v, want ready", got)
	}
}

func TestApplyDataAccumulationToState_NormalizesTargets(t *testing.T) {
	state := &StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)}
	payload := map[string]any{
		"score":  4,
		"nested": map[string]any{"value": "ok"},
	}
	base := BaseContext{Payload: values.Wrap(payload)}
	spec := runtimecontracts.WorkflowDataAccumulation{
		SourceEvent: "task.completed",
		Writes: []runtimecontracts.WorkflowDataWrite{
			{TargetField: "entity.score", SourceField: "score"},
			{TargetField: "status", SourceField: "nested.value"},
			{TargetField: "literal", Value: runtimecontracts.LiteralExpression("fixed")},
			{TargetField: "dispatch_count", Value: runtimecontracts.CELExpression("fan_out.count")},
		},
	}

	if err := applyDataAccumulationToState(base, ExecutionState{FanOut: map[string]any{"count": 3}}, state, spec); err != nil {
		t.Fatalf("applyDataAccumulationToState(...) error = %v", err)
	}

	if got := state.StateCarrier.Metadata["score"]; got != 4 {
		t.Fatalf("score = %#v", got)
	}
	if got := state.StateCarrier.Metadata["status"]; got != "ok" {
		t.Fatalf("status = %#v", got)
	}
	if got := state.StateCarrier.Metadata["literal"]; got != "fixed" {
		t.Fatalf("literal = %#v", got)
	}
	if got := state.StateCarrier.Metadata["dispatch_count"]; got != 3 {
		t.Fatalf("dispatch_count = %#v", got)
	}
	if got := state.StateCarrier.Metadata["last_data_accumulation_source"]; got != "task.completed" {
		t.Fatalf("source event = %#v", got)
	}
}

func TestAccumulatorStoreLoad_PreservesHandlerAccumulatorBucketPath(t *testing.T) {
	state := &StateSnapshot{}
	acc := &Accumulator{
		Expected:      []string{"a", "b"},
		ExpectedCount: 2,
		Received:      map[string]bool{"a": true},
		Items:         []map[string]any{{"payload": map[string]any{"score": 8}}},
		LastEventID:   "evt-1",
	}

	storeAccumulator(state, "node-1", events.EventType("task.completed"), acc)

	nodeBucket, ok := state.StateCarrier.StateBuckets["node-1"]
	if !ok {
		t.Fatalf("node bucket missing: %#v", state.StateCarrier.StateBuckets)
	}
	accBuckets, ok := nodeBucket[handlerAccumulatorBucketKey].(map[string]any)
	if !ok {
		t.Fatalf("handler accumulator bucket missing: %#v", nodeBucket)
	}
	if _, ok := accBuckets["node-1:task.completed"]; !ok {
		t.Fatalf("handler key missing: %#v", accBuckets)
	}

	loaded, ok := loadAccumulator(*state, "node-1", events.EventType("task.completed"))
	if !ok {
		t.Fatal("expected accumulator to load")
	}
	if loaded.ExpectedCount != 2 || len(loaded.Items) != 1 || !loaded.Received["a"] {
		t.Fatalf("loaded accumulator mismatch: %#v", loaded)
	}
}

func TestApplyDataAccumulationToState_AppliesExpressionOnlyWrites(t *testing.T) {
	state := &StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)}
	base := BaseContext{
		Entity: values.Wrap(map[string]any{
			"mode": "corpus",
		}),
		Policy: values.Wrap(map[string]any{
			"scoring_dimensions": []any{
				"build_complexity",
				"automation_completeness",
			},
		}),
	}
	spec := runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{
				TargetField: "entity.dimensions_requested",
				Value:       runtimecontracts.CELExpression("policy.scoring_dimensions"),
			},
			{
				TargetField: "entity.scoring_rubric",
				Value:       runtimecontracts.CELExpression("'corpus_rubric'"),
			},
		},
	}

	if err := applyDataAccumulationToState(base, ExecutionState{}, state, spec); err != nil {
		t.Fatalf("applyDataAccumulationToState(...) error = %v", err)
	}

	dimensions, ok := state.StateCarrier.Metadata["dimensions_requested"].([]any)
	if !ok || len(dimensions) != 2 || dimensions[0] != "build_complexity" || dimensions[1] != "automation_completeness" {
		t.Fatalf("dimensions_requested = %#v", state.StateCarrier.Metadata["dimensions_requested"])
	}
	if got := state.StateCarrier.Metadata["scoring_rubric"]; got != "corpus_rubric" {
		t.Fatalf("scoring_rubric = %#v", got)
	}
}

func TestApplyDataAccumulationToState_EvaluatesArithmeticCELExpressions(t *testing.T) {
	state := &StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)}
	base := BaseContext{
		Entity: values.Wrap(map[string]any{
			"revision_count": 0,
		}),
	}
	spec := runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{
				TargetField: "entity.revision_count",
				Value:       runtimecontracts.CELExpression("entity.revision_count + 1"),
			},
		},
	}

	if err := applyDataAccumulationToState(base, ExecutionState{}, state, spec); err != nil {
		t.Fatalf("applyDataAccumulationToState(...) error = %v", err)
	}

	if got := state.StateCarrier.Metadata["revision_count"]; got != 1.0 && got != 1 {
		t.Fatalf("revision_count = %#v, want 1", got)
	}
}

func TestApplyDataAccumulationToState_WritesNestedTargetPath(t *testing.T) {
	state := &StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{
		"analysis": map[string]any{
			"summary":      "stale",
			"report_count": 2,
		},
	}, nil, nil)}
	base := BaseContext{
		Payload: values.Wrap(map[string]any{
			"summary": "ready",
		}),
	}
	spec := runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{{
			SourceField:   "summary",
			TargetPathRef: "entity.analysis.summary",
		}},
	}

	if err := applyDataAccumulationToState(base, ExecutionState{}, state, spec); err != nil {
		t.Fatalf("applyDataAccumulationToState(...) error = %v", err)
	}

	analysis, ok := state.StateCarrier.Metadata["analysis"].(map[string]any)
	if !ok {
		t.Fatalf("analysis = %#v", state.StateCarrier.Metadata["analysis"])
	}
	if got := analysis["summary"]; got != "ready" {
		t.Fatalf("analysis.summary = %#v, want ready", got)
	}
	if got := analysis["report_count"]; got != 2 {
		t.Fatalf("analysis.report_count = %#v, want 2", got)
	}
}

func TestApplyDataAccumulationToState_FailsClosedOnCELRuntimeError(t *testing.T) {
	state := &StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)}
	base := BaseContext{
		Entity: values.Wrap(map[string]any{}),
	}
	spec := runtimecontracts.WorkflowDataAccumulation{
		Writes: []runtimecontracts.WorkflowDataWrite{
			{
				TargetField: "entity.revision_count",
				Value:       runtimecontracts.CELExpression("entity.revision_count + 1"),
			},
		},
	}

	err := applyDataAccumulationToState(base, ExecutionState{}, state, spec)
	if err == nil {
		t.Fatal("expected data accumulation CEL runtime error")
	}
	if !strings.Contains(err.Error(), "data_accumulation target entity.revision_count") {
		t.Fatalf("error = %v, want data_accumulation target context", err)
	}
	if _, exists := state.StateCarrier.Metadata["revision_count"]; exists {
		t.Fatalf("revision_count unexpectedly persisted after CEL runtime error: %#v", state.StateCarrier.Metadata["revision_count"])
	}
}

func TestComputeWeightedAverageReadsFlattenedAccumulatorItems(t *testing.T) {
	acc := &Accumulator{
		Items: []map[string]any{
			{"dimension": "build_complexity", "score": 80},
			{"dimension": "automation_completeness", "score": 70},
		},
	}
	got := computeWeightedAverage(acc, &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpWeightedAverage,
		Keys: runtimecontracts.ComputeKeyConfig{
			DimensionKey: "dimension",
			ScoreKeys:    []string{"score"},
		},
		Tiers: []runtimecontracts.ComputeTier{
			{Dimensions: []string{"build_complexity", "automation_completeness"}, Weight: 1},
		},
	})
	if got != 75 {
		t.Fatalf("computeWeightedAverage(flattened) = %v, want 75", got)
	}
}

func TestComputeWeightedAverageStillSupportsLegacyNestedPayloadItems(t *testing.T) {
	acc := &Accumulator{
		Items: []map[string]any{
			{"payload": map[string]any{"dimension": "build_complexity", "score": 80}},
			{"payload": map[string]any{"dimension": "automation_completeness", "score": 70}},
		},
	}
	got := computeWeightedAverage(acc, &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpWeightedAverage,
		Keys: runtimecontracts.ComputeKeyConfig{
			DimensionKey: "dimension",
			ScoreKeys:    []string{"score"},
		},
		Tiers: []runtimecontracts.ComputeTier{
			{Dimensions: []string{"build_complexity", "automation_completeness"}, Weight: 1},
		},
	})
	if got != 75 {
		t.Fatalf("computeWeightedAverage(legacy nested) = %v, want 75", got)
	}
}

func TestNextChainDepth_EnforcesLimit(t *testing.T) {
	if next, err := nextChainDepth(1, 3); err != nil || next != 2 {
		t.Fatalf("nextChainDepth ok = %d, %v", next, err)
	}
	if next, err := nextChainDepth(3, 3); err != ErrChainDepthExceeded || next != 4 {
		t.Fatalf("nextChainDepth overflow = %d, %v", next, err)
	}
}

func TestExpectedAccumulatorTargets(t *testing.T) {
	base := BaseContext{Payload: values.Wrap(map[string]any{
		"sources": []any{"b", "a", "a"},
		"count":   "3",
	})}
	if ids, count := expectedAccumulatorTargets(base, ExecutionState{}, paths.Parse("payload.sources"), "payload.sources"); len(ids) != 2 || count != 3 {
		t.Fatalf("expectedAccumulatorTargets sources = %v, %d", ids, count)
	}
	if ids, count := expectedAccumulatorTargets(base, ExecutionState{}, paths.Parse("payload.count"), "payload.count"); ids != nil || count != 3 {
		t.Fatalf("expectedAccumulatorTargets count = %v, %d", ids, count)
	}
}

func TestAccumulatorComplete(t *testing.T) {
	acc := &Accumulator{
		Expected:      []string{"a", "b"},
		ExpectedCount: 2,
		Received:      map[string]bool{"a": true},
	}
	complete, err := accumulatorComplete(acc, &runtimecontracts.AccumulateSpec{Completion: runtimecontracts.ParseAccumulateCompletion("all")}, nil)
	if err != nil || complete {
		t.Fatalf("accumulatorComplete all = %v, %v", complete, err)
	}
	acc.Received["b"] = true
	complete, err = accumulatorComplete(acc, &runtimecontracts.AccumulateSpec{
		Completion: runtimecontracts.ParseAccumulateCompletion("threshold"),
		Threshold:  2,
	}, nil)
	if err != nil || !complete {
		t.Fatalf("accumulatorComplete threshold = %v, %v", complete, err)
	}
	complete, err = accumulatorComplete(acc, &runtimecontracts.AccumulateSpec{Completion: runtimecontracts.ParseAccumulateCompletion("received_count >= 2")}, func(expression string, extra map[string]any) (bool, error) {
		if expression != "received_count >= 2" {
			t.Fatalf("unexpected expression: %q", expression)
		}
		accumulation, _ := extra["accumulation"].(map[string]any)
		return accumulation["received_count"] == 2, nil
	})
	if err != nil || !complete {
		t.Fatalf("accumulatorComplete expression = %v, %v", complete, err)
	}
	acc = &Accumulator{
		StartedAt:     "2026-03-14T00:00:00Z",
		LastEventType: "accumulate.timeout",
	}
	complete, err = accumulatorComplete(acc, &runtimecontracts.AccumulateSpec{Completion: runtimecontracts.ParseAccumulateCompletion("timeout")}, nil)
	if err != nil || !complete {
		t.Fatalf("accumulatorComplete timeout = %v, %v", complete, err)
	}
}

func TestComputeValue(t *testing.T) {
	acc := &Accumulator{
		Items: []map[string]any{
			{"payload": map[string]any{"axis": "quality", "score_value": 10}},
			{"payload": map[string]any{"axis": "speed", "score_value": 4}},
		},
	}
	value, err := computeValue(acc, nil, &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpWeightedAverage,
		Keys: runtimecontracts.ComputeKeyConfig{
			DimensionKey: "axis",
			ScoreKeys:    []string{"score_value", "fallback_score"},
		},
		Tiers: []runtimecontracts.ComputeTier{{
			Dimensions: []string{"quality", "speed"},
			Weight:     2,
		}},
	})
	if err != nil || value.(float64) != 7 {
		t.Fatalf("computeValue weighted_average = %#v, %v", value, err)
	}

	acc.Items = append(acc.Items, map[string]any{"payload": map[string]any{"count_value": 9}})
	value, err = computeValue(acc, nil, &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		Keys: runtimecontracts.ComputeKeyConfig{
			NumericKeys: []string{"count_value"},
		},
	})
	if err != nil || value.(int) != 3 {
		t.Fatalf("computeValue count = %#v, %v", value, err)
	}

	acc = &Accumulator{
		Items: []map[string]any{
			{"payload": map[string]any{"score": 55.0}},
			{"payload": map[string]any{"score": 81.0}},
			{"payload": map[string]any{"score": 74.0}},
		},
	}
	value, err = computeValue(acc, nil, &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpPickOrAverage,
		Keys: runtimecontracts.ComputeKeyConfig{
			NumericKeys: []string{"score"},
		},
	})
	if err != nil || value.(float64) != 81 {
		t.Fatalf("computeValue pick_or_average = %#v, %v", value, err)
	}

	acc = &Accumulator{
		Items: []map[string]any{
			{"payload": map[string]any{"score": 80.0, "weight": 0.5}},
			{"payload": map[string]any{"score": 90.0, "weight": 0.3}},
			{"payload": map[string]any{"score": 70.0, "weight": 0.2}},
		},
	}
	value, err = computeValue(acc, nil, &runtimecontracts.ComputeSpec{
		Operation:   runtimecontracts.ComputeOpWeightedAverage,
		StoreAs:     "entity.composite",
		ValueField:  "score",
		WeightField: "weight",
	})
	if err != nil {
		t.Fatalf("computeValue legacy weighted_average error = %v", err)
	}
	if got := value.(float64); got != 81 {
		t.Fatalf("computeValue legacy weighted_average = %v, want 81", got)
	}
}
