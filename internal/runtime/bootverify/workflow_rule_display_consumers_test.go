package bootverify

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRuleDisplayWriteAndPayloadConsumersPreserveExactSites(t *testing.T) {
	writes := []runtimecontracts.WorkflowDataWrite{
		{Operation: runtimecontracts.WorkflowDataOperationSet, TargetRef: "entity.score", SourceField: "payload.score"},
		{Operation: runtimecontracts.WorkflowDataOperationAppend, TargetRef: "entity.verticals.active_jobs", Key: runtimecontracts.LiteralExpression("north"), Value: runtimecontracts.RefExpression("payload.job")},
	}
	for _, tc := range []struct {
		context string
		labels  [2]string
	}{
		{"rules", [2]string{"handler.rules[0]: when payload.ready.data_accumulation", "handler.rules[1]: else.data_accumulation"}},
		{"on_complete", [2]string{"handler.on_complete[0]: condition payload.ready.data_accumulation", "handler.on_complete[1]: else.data_accumulation"}},
	} {
		t.Run(tc.context, func(t *testing.T) {
			rows := []runtimecontracts.HandlerRuleEntry{
				{Condition: "payload.ready", DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: writes}},
				{Condition: "else", DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: writes}},
			}
			handler := runtimecontracts.SystemNodeEventHandler{}
			if tc.context == "rules" {
				handler.Rules = rows
			} else {
				handler.OnComplete = rows
			}
			bundle := containedStateOperationBundle(writes[1])
			node := bundle.Nodes["node-1"]
			node.EventHandlers["job.received"] = handler
			bundle.Nodes["node-1"] = node
			source := semanticview.Wrap(bundle)
			owner := identitytest.RootNode(t, "node-1")
			contained := wave1ContainedStateOperations(source)
			targets := wave1HandlerWriteTargets(owner, "job.received", handler)
			payload := payloadFieldCoverageSites(source)
			if len(contained) != 2 || len(targets) != 2 || len(payload) != 2 {
				t.Fatalf("site census changed: contained=%#v targets=%#v payload=%#v", contained, targets, payload)
			}
			for i, label := range tc.labels {
				if ref := contained[i]; ref.Kind != label || !ref.Node.Equal(owner) || ref.EventType != "job.received" || ref.WriteIndex != 1 || !reflect.DeepEqual(ref.Write, writes[1]) {
					t.Fatalf("contained site[%d] lost display or execution coordinates: %#v", i, ref)
				}
				if ref := targets[i]; ref.Kind != label || !ref.Node.Equal(owner) || ref.EventType != "job.received" || !ref.HasWriteIndex || ref.WriteIndex != 0 || ref.Target != "entity.score" || ref.Operation != runtimecontracts.WorkflowDataOperationSet {
					t.Fatalf("direct write site[%d] lost display or execution coordinates: %#v", i, ref)
				}
				if site := payload[i]; site.Scope != label || !site.Node.Equal(owner) || site.EventType != "job.received" || !reflect.DeepEqual(site.Accumulation.Writes, writes) {
					t.Fatalf("payload site[%d] lost display or operands: %#v", i, site)
				}
				finding := containedStateOperationFinding(contained[i], "exact target refusal")
				if !strings.Contains(finding.Message, label+" write[1]") || !strings.HasSuffix(finding.Message, "exact target refusal") {
					t.Fatalf("finding lost presentation or refusal: %#v", finding)
				}
			}
		})
	}
}

func TestRuleDisplayJoinWriteConsumersKeepSingularContexts(t *testing.T) {
	writes := []runtimecontracts.WorkflowDataWrite{
		{Operation: runtimecontracts.WorkflowDataOperationSet, TargetRef: "entity.score", Value: runtimecontracts.LiteralExpression(1)},
		{Operation: runtimecontracts.WorkflowDataOperationAppend, TargetRef: "entity.verticals.active_jobs", Value: runtimecontracts.RefExpression("payload.job")},
	}
	row := runtimecontracts.HandlerRuleEntry{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: writes}}
	handler := runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{OnComplete: row, OnDeadline: row}}
	bundle := containedStateOperationBundle(writes[1])
	node := bundle.Nodes["node-1"]
	node.EventHandlers["job.received"] = handler
	bundle.Nodes["node-1"] = node
	contained := wave1ContainedStateOperations(semanticview.Wrap(bundle))
	targets := wave1HandlerWriteTargets(identitytest.RootNode(t, "node-1"), "job.received", handler)
	if len(contained) != 2 || len(targets) != 2 {
		t.Fatalf("join site census changed: contained=%#v targets=%#v", contained, targets)
	}
	for i, label := range []string{"handler.join.on_complete.data_accumulation", "handler.join.on_deadline.data_accumulation"} {
		if contained[i].Kind != label || targets[i].Kind != label || contained[i].WriteIndex != 1 || targets[i].WriteIndex != 0 {
			t.Fatalf("singular join context lost: contained=%#v target=%#v", contained[i], targets[i])
		}
	}
}
