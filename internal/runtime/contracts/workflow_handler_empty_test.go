package contracts

import (
	"reflect"
	"testing"
)

func TestEmptyHandlerClassifiesEveryDeclarationField(t *testing.T) {
	examples := map[string]SystemNodeEventHandler{
		"Activity":         {Activity: ActivitySpec{Tool: "inspect"}},
		"CreateEntity":     {CreateEntity: true},
		"Description":      {Description: "annotation only"},
		"Emit":             {Emit: EmitSpec{Event: "observed"}},
		"OnSuccess":        {OnSuccess: HandlerOnSuccessSpec{Emit: EmitSpec{Event: "observed"}}},
		"Guard":            {Guard: &GuardSpec{}},
		"AdvancesTo":       {AdvancesTo: "ready"},
		"SetsGate":         {SetsGate: &GateSpec{}},
		"ClearGates":       {ClearGates: []string{"ready"}},
		"DataAccumulation": {DataAccumulation: WorkflowDataAccumulation{SourceEvent: "observed"}},
		"Condition":        {Condition: "true"},
		"Logic":            {Logic: "true"},
		"Loop":             {Loop: &LoopOperationSpec{}},
		"OnComplete":       {OnComplete: []HandlerRuleEntry{{}}},
		"Rules":            {Rules: []HandlerRuleEntry{{}}},
		"Accumulate":       {Accumulate: &AccumulateSpec{}},
		"Join":             {Join: &JoinSpec{}},
		"JoinUntilPlans":   {JoinUntilPlans: []WorkflowJoinPlan{{}}},
		"Compute":          {Compute: &ComputeSpec{}},
		"Query":            {Query: &QuerySpec{}},
		"FanOut":           {FanOut: &FanOutSpec{}},
		"GroupBy":          {GroupBy: &GroupBySpec{}},
		"Filter":           {Filter: &FilterSpec{}},
		"Reduce":           {Reduce: &ReduceSpec{}},
		"Count":            {Count: &CountSpec{}},
		"Clear":            {Clear: &ClearSpec{}},
	}
	declaration := reflect.TypeOf(SystemNodeEventHandler{})
	if declaration.NumField() != len(examples) {
		t.Fatal("new handler field requires explicit empty-execution classification")
	}
	if !(SystemNodeEventHandler{}).EmptyExecution() {
		t.Fatal("explicit empty declaration was not empty")
	}
	for i := 0; i < declaration.NumField(); i++ {
		name := declaration.Field(i).Name
		example, found := examples[name]
		if !found {
			t.Fatalf("handler field %s has no classification proof", name)
		}
		t.Run(name, func(t *testing.T) {
			if example.EmptyExecution() != (name == "Description") {
				t.Fatalf("incorrect empty-execution classification for %s", name)
			}
		})
	}
	if (SystemNodeEventHandler{DataAccumulation: WorkflowDataAccumulation{Writes: []WorkflowDataWrite{{}}}}).EmptyExecution() {
		t.Fatal("data write acquired empty-execution permission")
	}
}
