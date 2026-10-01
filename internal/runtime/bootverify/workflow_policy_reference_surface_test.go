package bootverify

import (
	"context"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestR3PolicyReferenceSurfaceMatrix(t *testing.T) {
	ref := c.RefExpression("policy.missing")
	cel := c.CELExpression("policy.missing")
	for _, tc := range []struct {
		name    string
		handler c.SystemNodeEventHandler
	}{
		{"activity", c.SystemNodeEventHandler{Activity: c.ActivitySpec{Input: map[string]c.ExpressionValue{"value": ref}}}},
		{"emit", c.SystemNodeEventHandler{Emit: c.EmitSpec{Event: "event", Fields: map[string]c.ExpressionValue{"value": cel}}}},
		{"guard", c.SystemNodeEventHandler{Guard: &c.GuardSpec{Check: "policy.missing > 0"}}},
		{"write source", c.SystemNodeEventHandler{DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{SourceField: "policy.missing", TargetRef: "metadata.copy"}}}}},
		{"write value", c.SystemNodeEventHandler{DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetRef: "metadata.copy", Value: ref}}}}},
		{"condition", c.SystemNodeEventHandler{Condition: "policy.missing > 0"}},
		{"logic", c.SystemNodeEventHandler{Logic: "policy.missing"}},
		{"rule when", c.SystemNodeEventHandler{Rules: []c.HandlerRuleEntry{{Condition: "policy.missing > 0"}}}},
		{"rule activity", c.SystemNodeEventHandler{Rules: []c.HandlerRuleEntry{{Activity: c.ActivitySpec{Input: map[string]c.ExpressionValue{"value": ref}}}}}},
		{"completion", c.SystemNodeEventHandler{OnComplete: []c.HandlerRuleEntry{{Condition: "policy.missing > 0"}}}},
		{"accumulate from", c.SystemNodeEventHandler{Accumulate: &c.AccumulateSpec{From: "policy.missing"}}},
		{"accumulate window", c.SystemNodeEventHandler{Accumulate: &c.AccumulateSpec{Window: "policy.missing"}}},
		{"accumulate dedup", c.SystemNodeEventHandler{Accumulate: &c.AccumulateSpec{DedupBy: "policy.missing"}}},
		{"join members", c.SystemNodeEventHandler{Join: &c.JoinSpec{Members: c.JoinMembersSpec{By: "policy.missing"}}}},
		{"join window", c.SystemNodeEventHandler{Join: &c.JoinSpec{Window: &c.JoinWindowSpec{From: "policy.missing"}}}},
		{"lookup", c.SystemNodeEventHandler{Compute: &c.ComputeSpec{Lookup: &c.ComputeLookupSpec{On: []string{"policy.missing"}}}}},
		{"validation", c.SystemNodeEventHandler{Compute: &c.ComputeSpec{Validation: &c.ComputeValidationSpec{Input: map[string]string{"value": "policy.missing"}}}}},
		{"module", c.SystemNodeEventHandler{Compute: &c.ComputeSpec{Module: &c.ComputeModuleSpec{Input: map[string]string{"value": "policy.missing"}}}}},
		{"query", c.SystemNodeEventHandler{Query: &c.QuerySpec{Source: "policy.missing"}}},
		{"group", c.SystemNodeEventHandler{GroupBy: &c.GroupBySpec{ItemsFrom: "policy.missing", Key: "id"}}},
		{"filter", c.SystemNodeEventHandler{Filter: &c.FilterSpec{Source: "policy.missing"}}},
		{"reduce", c.SystemNodeEventHandler{Reduce: &c.ReduceSpec{Source: "policy.missing"}}},
		{"count", c.SystemNodeEventHandler{Count: &c.CountSpec{ItemsFrom: "policy.missing"}}},
		{"query count operand", c.SystemNodeEventHandler{Guard: &c.GuardSpec{Check: "query_entities(id == policy.missing).count > 0"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadTier8FixtureBundle(t, "test-boot-success")
			node, event, _, ok := firstBundleHandler(bundle)
			if !ok {
				t.Fatal("missing handler")
			}
			writeBundleHandler(t, bundle, node, event, tc.handler)
			findings := newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}).conditionPolicyAlignment()
			if len(findings) == 0 || !strings.Contains(findings[0].Message, `policy["missing"]`) {
				t.Fatalf("reader escaped policy checker: %#v", findings)
			}
		})
	}
}

func TestR3PolicyReferenceMutationGuard(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-condition-policy")
	assertRefused := func(report Report) bool {
		return reportContains(report.Errors(), "condition_policy_alignment", `policy["nonexistent_key"]`)
	}
	if !assertRefused(Run(context.Background(), source, Options{})) {
		t.Fatal("real verification did not reject typo")
	}
	original := bootCheckRegistry
	bootCheckRegistry = append([]Check(nil), original...)
	t.Cleanup(func() { bootCheckRegistry = original })
	for i := range bootCheckRegistry {
		if bootCheckRegistry[i].ID == "condition_policy_alignment" {
			bootCheckRegistry[i].Run = func(*checkerContext) []Finding { return nil }
		}
	}
	if assertRefused(Run(context.Background(), source, Options{})) {
		t.Fatal("proof still passes when the policy checker is disabled")
	}
}

func TestR3PolicyCompiledFanOutEmitReference(t *testing.T) {
	handler := c.SystemNodeEventHandler{FanOut: &c.FanOutSpec{ItemsFrom: "payload.line_items", As: "line_item", Identity: "line_item.id", Emit: c.EmitSpec{Event: "line_item.requested", Fields: map[string]c.ExpressionValue{"line_item_id": c.CELExpression("policy.missing")}}}}
	bundle := fanOutValidationBundle(*handler.FanOut)
	node := bundle.Nodes["dispatcher"]
	node.EventHandlers["order.accepted"] = handler
	bundle.Nodes["dispatcher"] = node
	completeBootverifyFanOutFixture(t, bundle, "dispatcher", "order.accepted")
	if failures := bundle.FanOutPlanFailures(); len(failures) != 0 {
		t.Fatalf("fan-out fixture: %v", failures)
	}
	findings := newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}).conditionPolicyAlignment()
	if !findingContainsAll(findings, "condition_policy_alignment", `policy["missing"]`, "fan_out") {
		t.Fatalf("compiled emit escaped checker: %#v", findings)
	}
}

func TestR3PolicySelectorsAndGateContext(t *testing.T) {
	for _, kind := range []string{"active builtin", "inert guard annotation", "timer", "gate"} {
		t.Run(kind, func(t *testing.T) {
			bundle := loadTier8FixtureBundle(t, "test-boot-success")
			node, event, handler, ok := firstBundleHandler(bundle)
			if !ok {
				t.Fatal("missing handler")
			}
			want := 1
			switch kind {
			case "active builtin", "inert guard annotation":
				handler.Guard = &c.GuardSpec{PolicyRef: "missing"}
				if kind == "active builtin" {
					handler.Guard.ID = "state_in_phase"
					handler.Guard.Check = "state_in_phase"
				} else {
					want = 0
				}
				writeBundleHandler(t, bundle, node, event, handler)
			case "timer":
				bundle.Semantics.Timers = []c.WorkflowTimerContract{{ID: "timer", Node: identitytest.RootNode(t, "timer-owner"), Delay: "{{missing}}s"}}
			case "gate":
				bundle.Semantics.Gates = []c.WorkflowGatePlan{{Stage: "review", Decision: "approval", Context: map[string]c.ExpressionValue{"value": c.CELExpression("policy.missing")}}}
			}
			findings := newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}).conditionPolicyAlignment()
			if len(findings) != want {
				t.Fatalf("selector %s: %#v", kind, findings)
			}
		})
	}
}
