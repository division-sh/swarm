package bootverify

import (
	"context"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestR3PolicyGuardVerificationExecutionParity(t *testing.T) {
	policy := map[string]any{"config": map[string]any{}, "null_value": nil, "wrong": true, "list": []any{"yes"}}
	for _, tc := range []struct {
		expression string
		want       bool
		invalid    bool
	}{
		{`policy.?missing.child.orValue(false)`, false, false},
		{`policy[?"missing"]["child"].orValue(false)`, false, false},
		{`policy.?config.child.orValue(false)`, false, false},
		{`policy.?null_value.child.orValue(false)`, false, false},
		{`policy.?wrong.child.orValue(false)`, false, false},
		{`has(policy.missing) && policy.missing.child == true`, false, false},
		{`!has(policy.missing) || policy.missing.child == true`, true, false},
		{`has(policy.missing) ? policy.missing.child == true : false`, false, false},
		{`has(policy.config) && policy.config.child == true`, false, true},
		{`has(policy.null_value) && policy.null_value.child == true`, false, true},
		{`policy.missing.?child.orValue(false)`, false, true},
		{`policy.missing.child == true`, false, true},
		{`policy.list[0u] == "yes"`, true, false},
		{`policy.list[0.0] == "yes"`, true, false},
		{`policy.list[9u] == "yes"`, false, true},
		{`policy.list[9] == "yes"`, false, true},
		{`policy.list[18446744073709551615u] == "yes"`, false, true},
		{`policy[true] == "yes"`, false, true},
		{`policy.list[0.5] == "yes"`, false, true},
		{`policy.list[?9u].orValue("no") == "no"`, true, false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			value, runtimeErr := workflowexpr.EvalValueExpression(tc.expression, workflowexpr.ValueContext{Policy: policy})
			if tc.invalid {
				if runtimeErr == nil {
					t.Fatalf("negative runtime control succeeded: %#v", value)
				}
			} else if runtimeErr != nil || value != tc.want {
				t.Fatalf("runtime = %#v, %v; want %t", value, runtimeErr, tc.want)
			}
			bundle := semanticview.CloneBundleForPreview(loadTier8FixtureBundle(t, "test-boot-success"), policy)
			node, event, handler, ok := firstBundleHandler(bundle)
			if !ok {
				t.Fatal("missing handler")
			}
			handler.Guard = &c.GuardSpec{Check: tc.expression}
			writeBundleHandler(t, bundle, node, event, handler)
			failures := Run(context.Background(), semanticview.Wrap(bundle), Options{}).Errors()
			if !tc.invalid && len(failures) != 0 {
				t.Fatalf("safe supported guard rejected: %#v", failures)
			}
			if tc.invalid && len(failures) == 0 {
				t.Fatal("invalid supported guard passed verification")
			}
			for _, failure := range failures {
				if failure.CheckID != "condition_policy_alignment" || !strings.Contains(failure.Location, "guard") || !strings.Contains(failure.Location, event) {
					t.Fatalf("unexpected failing gate or missing field location: %#v", failure)
				}
			}
		})
	}
}

func TestR3PolicyGuardNearestKeyTeachingError(t *testing.T) {
	bundle := semanticview.CloneBundleForPreview(loadTier8FixtureBundle(t, "test-boot-success"), map[string]any{"investigate": true})
	node, event, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("missing handler")
	}
	handler.Guard = &c.GuardSpec{Check: "policy.investigat == true"}
	writeBundleHandler(t, bundle, node, event, handler)
	failures := Run(context.Background(), semanticview.Wrap(bundle), Options{}).Errors()
	if len(failures) != 1 || failures[0].CheckID != "condition_policy_alignment" || !strings.Contains(failures[0].Location, "guard") || !strings.Contains(failures[0].Location, event) || !strings.Contains(failures[0].Message, `did you mean policy["investigate"]?`) || !strings.Contains(failures[0].Message, "declared root keys:") || !strings.Contains(failures[0].Message, `"investigate"`) {
		t.Fatalf("incomplete supported teaching error: %#v", failures)
	}
}

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
		{"accumulate key", c.SystemNodeEventHandler{Accumulate: &c.AccumulateSpec{Key: "policy.missing"}}},
		{"join members", c.SystemNodeEventHandler{Join: &c.JoinSpec{Members: c.JoinMembersSpec{By: "policy.missing"}}}},
		{"join source", c.SystemNodeEventHandler{Join: &c.JoinSpec{Members: c.JoinMembersSpec{From: "policy.missing"}}}},
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

func TestR3PolicyClosedJoinOutcomeScope(t *testing.T) {
	for _, outcome := range []string{"on_complete", "on_deadline"} {
		for _, key := range []string{"supported", "known", "missing"} {
			t.Run(outcome+"/"+key, func(t *testing.T) {
				bundle := semanticview.CloneBundleForPreview(joinValidationBundle(), map[string]any{"known": []any{"ok"}})
				node := bundle.Nodes["join-node"]
				handler := node.EventHandlers["item.completed"]
				rule := &handler.Join.OnComplete
				if outcome == "on_deadline" {
					rule = &handler.Join.OnDeadline
				}
				if key != "supported" {
					for field := range rule.Emit.Fields {
						rule.Emit.Fields[field] = c.CELExpression("policy." + key)
					}
				}
				node.EventHandlers["item.completed"] = handler
				bundle.Nodes["join-node"] = node
				semanticviewtest.WrapRootAgents(bundle)
				err := c.CompileWorkflowSemantics(bundle)
				if key == "supported" {
					if err != nil {
						t.Fatalf("supported closed outcome rejected: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "join."+outcome) || !strings.Contains(err.Error(), "closed join outcome may not reference policy") {
					t.Fatalf("policy escaped closed-outcome scope: %v", err)
				}
			})
		}
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
