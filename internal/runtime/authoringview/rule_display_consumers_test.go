package authoringview

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestContainedOperationPresentationKeepsRuleContextAndOperands(t *testing.T) {
	writes := []runtimecontracts.WorkflowDataWrite{{
		Operation: runtimecontracts.WorkflowDataOperationAppend, TargetRef: "entity.audit_log",
		Value: runtimecontracts.RefExpression("payload.audit"),
	}}
	for _, tc := range []struct {
		context string
		labels  [2]string
	}{
		{"rules", [2]string{"handler.rules[0]: when payload.ready &&\npayload.allowed.data_accumulation", "handler.rules[1]: else.data_accumulation"}},
		{"on_complete", [2]string{"handler.on_complete[0]: condition payload.ready &&\npayload.allowed.data_accumulation", "handler.on_complete[1]: else.data_accumulation"}},
	} {
		t.Run(tc.context, func(t *testing.T) {
			rows := []runtimecontracts.HandlerRuleEntry{
				{Condition: "payload.ready &&\npayload.allowed", DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: writes}},
				{Condition: "else", DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: writes}},
			}
			handler := runtimecontracts.SystemNodeEventHandler{}
			if tc.context == "rules" {
				handler.Rules = rows
			} else {
				handler.OnComplete = rows
			}
			node := runtimecontracts.SystemNodeContract{EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"request": handler}}
			refs := containedOperationRefs("child", "worker", node)
			if len(refs) != 2 {
				t.Fatalf("contained operations = %#v, want both positional rows", refs)
			}
			for i, ref := range refs {
				if ref.FlowID != "child" || ref.NodeID != "worker" || ref.EventType != "request" || ref.Scope != tc.labels[i] || !reflect.DeepEqual(ref.Write, writes[0]) {
					t.Fatalf("authoring operation[%d] lost context, full text or operands: %#v", i, ref)
				}
			}
		})
	}
}
