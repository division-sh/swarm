package pipeline

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestWorkflowEntityFieldsAvailableBeforeCondition_ExcludesLaterTopLevelDataWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "score", SourceField: "score"},
			},
		},
	}

	available := WorkflowEntityFieldsAvailableBeforeCondition(handler, WorkflowConditionContextRule)
	if _, ok := available["score"]; ok {
		t.Fatalf("score unexpectedly available before rule selection: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforeCondition_IncludesEarlierComputeWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpCount,
			StoreAs:   "entity.composite_score",
		},
	}

	available := WorkflowEntityFieldsAvailableBeforeCondition(handler, WorkflowConditionContextOnComplete)
	if _, ok := available["composite_score"]; !ok {
		t.Fatalf("composite_score missing from on_complete availability: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforeDataAccumulation_IncludesEarlierComputeButNotSiblingWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpCount,
			StoreAs:   "entity.composite_score",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "base_score", SourceField: "score"},
			},
		},
	}

	available := WorkflowEntityFieldsAvailableBeforeDataAccumulation(handler)
	if _, ok := available["composite_score"]; !ok {
		t.Fatalf("composite_score missing from data_accumulation availability: %#v", available)
	}
	if _, ok := available["base_score"]; ok {
		t.Fatalf("base_score unexpectedly available before sibling data_accumulation write: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforePayloadTransform_IncludesCreateEntityTopLevelWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.LiteralExpression(0)},
			},
		},
	}

	available := WorkflowEntityFieldsAvailableBeforePayloadTransform(handler)
	if _, ok := available["revision_count"]; !ok {
		t.Fatalf("revision_count missing from payload_transform availability: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforePayloadTransform_ExcludesRuleOnlyWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			Condition: "entity.entity_id != null",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "revision_count", Value: runtimecontracts.LiteralExpression(0)},
				},
			},
		}},
	}

	available := WorkflowEntityFieldsAvailableBeforePayloadTransform(handler)
	if _, ok := available["revision_count"]; ok {
		t.Fatalf("revision_count unexpectedly available before payload_transform: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforePayloadTransform_ExcludesRuleOnlyComputeOutputs(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Rules: []runtimecontracts.HandlerRuleEntry{{
			Condition: "entity.entity_id != null",
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpCount,
				StoreAs:   "entity.revision_count",
			},
		}},
	}

	available := WorkflowEntityFieldsAvailableBeforePayloadTransform(handler)
	if _, ok := available["revision_count"]; ok {
		t.Fatalf("revision_count unexpectedly available before payload_transform: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforePayloadTransform_PreservesUnconditionalWriterWhenRuleAlsoWritesField(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpCount,
			StoreAs:   "entity.revision_count",
		},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			Condition: "entity.entity_id != null",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "revision_count", Value: runtimecontracts.LiteralExpression(0)},
				},
			},
		}},
	}

	available := WorkflowEntityFieldsAvailableBeforePayloadTransform(handler)
	if _, ok := available["revision_count"]; !ok {
		t.Fatalf("revision_count missing from payload_transform availability: %#v", available)
	}
}

func TestWorkflowEntityFieldsAvailableBeforeCondition_StillExcludesCreateEntityTopLevelWrites(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.LiteralExpression(0)},
			},
		},
	}

	available := WorkflowEntityFieldsAvailableBeforeCondition(handler, WorkflowConditionContextRule)
	if _, ok := available["revision_count"]; ok {
		t.Fatalf("revision_count unexpectedly available before rule selection: %#v", available)
	}
}

func TestWorkflowEntityReadsPersistedStateBeforeHandlerWrites(t *testing.T) {
	tests := []struct {
		name  string
		phase WorkflowEntityFieldLifecyclePhase
		want  bool
	}{
		{name: "guard", phase: WorkflowEntityFieldLifecycleGuard, want: true},
		{name: "filter", phase: WorkflowEntityFieldLifecycleFilter, want: true},
		{name: "count", phase: WorkflowEntityFieldLifecycleCount, want: true},
		{name: "rule", phase: WorkflowEntityFieldLifecycleRule, want: true},
		{name: "on_complete", phase: WorkflowEntityFieldLifecycleOnComplete, want: true},
		{name: "data_accumulation", phase: WorkflowEntityFieldLifecycleDataAccumulation, want: false},
		{name: "reduce", phase: WorkflowEntityFieldLifecycleReduce, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WorkflowEntityReadsPersistedStateBeforeHandlerWrites(tt.phase); got != tt.want {
				t.Fatalf("WorkflowEntityReadsPersistedStateBeforeHandlerWrites(%q) = %v, want %v", tt.phase, got, tt.want)
			}
		})
	}
}
