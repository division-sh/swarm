package bootverify

import (
	"reflect"
	"sort"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestExecutableReaderCensusClassifiesEveryHandlerAndRuleField(t *testing.T) {
	assertExecutableReaderCensusFields(t, reflect.TypeOf(runtimecontracts.SystemNodeEventHandler{}), systemNodeEventHandlerExecutableReaderCensus)
	assertExecutableReaderCensusFields(t, reflect.TypeOf(runtimecontracts.HandlerRuleEntry{}), handlerRuleEntryExecutableReaderCensus)
}

func TestExecutableReaderCensusCoversEveryReaderFamily(t *testing.T) {
	entityRef := runtimecontracts.RefExpression("entity.verticals")
	entityCEL := runtimecontracts.CELExpression("entity.verticals")
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{name: "action input", handler: runtimecontracts.SystemNodeEventHandler{Action: runtimecontracts.ActionSpec{InstanceIDFrom: "entity.verticals"}}},
		{name: "activity input", handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}},
		{name: "select binding", handler: runtimecontracts.SystemNodeEventHandler{SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "id", Ref: "entity.verticals"}}}}},
		{name: "select or create binding", handler: runtimecontracts.SystemNodeEventHandler{SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "id", Ref: "entity.verticals"}}}}},
		{name: "emit field", handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{"value": entityCEL}}}},
		{name: "guard", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: "size(entity.verticals) > 0"}}},
		{name: "direct write source", handler: runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{SourceField: "entity.verticals", TargetRef: "metadata.copy"}}}}},
		{name: "direct write ref value", handler: runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetRef: "metadata.copy", Value: entityRef}}}}},
		{name: "condition", handler: runtimecontracts.SystemNodeEventHandler{Condition: "size(entity.verticals) > 0"}},
		{name: "logic", handler: runtimecontracts.SystemNodeEventHandler{Logic: "entity.verticals"}},
		{name: "loop source", handler: runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{From: "entity.verticals"}}},
		{name: "rule action", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{Action: runtimecontracts.ActionSpec{InstanceIDFrom: "entity.verticals"}}}}},
		{name: "on complete activity", handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}}}},
		{name: "accumulate", handler: runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{From: "entity.verticals"}}},
		{name: "join members by", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Members: runtimecontracts.JoinMembersSpec{By: "entity.verticals"}}}},
		{name: "join window", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Window: &runtimecontracts.JoinWindowSpec{From: "entity.verticals"}}}},
		{name: "join completion", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{CompleteWhen: "size(entity.verticals) > 0"}}},
		{name: "compute lookup", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Lookup: &runtimecontracts.ComputeLookupSpec{On: []string{"entity.verticals"}}}}},
		{name: "compute validation", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Validation: &runtimecontracts.ComputeValidationSpec{Input: map[string]string{"value": "entity.verticals"}}}}},
		{name: "compute module", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Module: &runtimecontracts.ComputeModuleSpec{Input: map[string]string{"value": "entity.verticals"}}}}},
		{name: "query select", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Select: []string{"entity.verticals"}}}},
		{name: "fan out", handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "entity.verticals"}}},
		{name: "group by key", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{Key: "entity.verticals"}}},
		{name: "filter", handler: runtimecontracts.SystemNodeEventHandler{Filter: &runtimecontracts.FilterSpec{Source: "entity.verticals"}}},
		{name: "reduce param", handler: runtimecontracts.SystemNodeEventHandler{Reduce: &runtimecontracts.ReduceSpec{Params: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}},
		{name: "count", handler: runtimecontracts.SystemNodeEventHandler{Count: &runtimecontracts.CountSpec{ItemsFrom: "entity.verticals"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readers := handlerExecutableReaderExpressionsForSource(nil, "coordinator", "node", "event", tc.handler)
			for _, reader := range readers {
				if reader.Expression == "entity.verticals" || reader.Expression == "size(entity.verticals) > 0" {
					return
				}
			}
			t.Fatalf("reader census omitted entity.verticals: %#v", readers)
		})
	}
}

func assertExecutableReaderCensusFields[T any](t *testing.T, typ reflect.Type, census map[string]T) {
	t.Helper()
	want := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		want = append(want, typ.Field(i).Name)
	}
	got := make([]string, 0, len(census))
	for field := range census {
		got = append(got, field)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reader census fields = %#v, want %#v", got, want)
	}
}
