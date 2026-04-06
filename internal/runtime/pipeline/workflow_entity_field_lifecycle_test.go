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
