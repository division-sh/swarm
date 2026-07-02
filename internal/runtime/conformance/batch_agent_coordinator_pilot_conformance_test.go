package conformance

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/batchagentcoordinatorpilot"
)

func TestBatchAgentCoordinatorPilotConformance_CoversBatchAgentConnectScatter(t *testing.T) {
	source := batchagentcoordinatorpilot.LoadSource(t)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("batch-agent coordinator pilot hard invalidities = %#v, want none", got)
	}

	handler, ok := source.NodeEventHandler(batchagentcoordinatorpilot.NodeID, batchagentcoordinatorpilot.InputEvent)
	if !ok {
		t.Fatalf("handler %s/%s missing", batchagentcoordinatorpilot.NodeID, batchagentcoordinatorpilot.InputEvent)
	}
	if handler.Accumulate == nil || len(handler.Accumulate.OnComplete) != 1 {
		t.Fatalf("accumulate.on_complete = %#v, want one batch rule", handler.Accumulate)
	}
	rule := handler.Accumulate.OnComplete[0]
	if rule.BatchAgent == nil {
		t.Fatalf("accumulate.on_complete rule = %#v, want batch_agent", rule)
	}
	assertBatchAgentCoordinatorPilotSpec(t, rule.BatchAgent)
	if rule.BatchAgent.Emit.Target.Kind != "" || rule.BatchAgent.Emit.Broadcast {
		t.Fatalf("batch_agent emit target/broadcast = %#v/%v, want parent connect authority only", rule.BatchAgent.Emit.Target, rule.BatchAgent.Emit.Broadcast)
	}

	emits := runtimecontracts.HandlerEmitEvents(handler)
	wantResolvedOutput := batchagentcoordinatorpilot.CoordinatorFlowID + "/" + batchagentcoordinatorpilot.OutputEvent
	if len(emits) != 1 || emits[0] != wantResolvedOutput {
		t.Fatalf("HandlerEmitEvents = %#v, want %s from batch_agent emit", emits, wantResolvedOutput)
	}
	if !batchAgentCoordinatorPilotAuthoredSite(source) {
		t.Fatalf("AuthoredEmitSites missing accumulate.on_complete.batch_agent emit for %s; sites=%s", batchagentcoordinatorpilot.OutputEvent, batchAgentCoordinatorPilotSiteSummary(source))
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one parent connect route plan", plans)
	}
	plan := plans[0]
	if plan.Source.FlowID != batchagentcoordinatorpilot.CoordinatorFlowID || plan.Source.Pin != batchagentcoordinatorpilot.OutputPin || plan.Source.Key != "account_id" {
		t.Fatalf("route plan source = %#v, want coordinator.account_classified keyed by account_id", plan.Source)
	}
	if plan.Receiver.FlowID != batchagentcoordinatorpilot.AccountFlowID || plan.Receiver.Pin != batchagentcoordinatorpilot.OutputPin || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want accounts.account_classified template input", plan.Receiver)
	}
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want instance-key runtime resolution", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.InstanceKey == nil || strings.Join(plan.InstanceKey.Fields, ",") != "account_id" {
		t.Fatalf("route plan instance key = %#v, want account_id", plan.InstanceKey)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan instance key mappings = %#v, want implicit account_id -> account_id", plan.InstanceKey.Mappings)
	}
}

func assertBatchAgentCoordinatorPilotSpec(t *testing.T, spec *runtimecontracts.BatchAgentSpec) {
	t.Helper()
	if spec.Agent != batchagentcoordinatorpilot.AgentID {
		t.Fatalf("batch_agent.agent = %q, want %s", spec.Agent, batchagentcoordinatorpilot.AgentID)
	}
	if spec.ItemsFrom != "accumulated.items" {
		t.Fatalf("batch_agent.items_from = %q, want accumulated.items", spec.ItemsFrom)
	}
	if spec.Result.ItemsFrom != "results" || spec.Result.CorrelationKey != "account_id" {
		t.Fatalf("batch_agent.result = %#v, want results/account_id", spec.Result)
	}
	if strings.Join(spec.Result.RequiredFields, ",") != "account_id,bucket,score" {
		t.Fatalf("batch_agent.required_fields = %#v, want account_id/bucket/score", spec.Result.RequiredFields)
	}
	wantFields := map[string]string{
		"account_id": "batch_agent.result.account_id",
		"bucket":     "batch_agent.result.bucket",
		"score":      "batch_agent.result.score",
		"handle":     "batch_agent.source_item.handle",
	}
	for field, ref := range wantFields {
		got, ok := spec.Emit.Fields[field]
		if !ok || batchAgentCoordinatorPilotExpression(got) != ref {
			t.Fatalf("batch_agent.emit.fields[%s] = %#v, want %s", field, got, ref)
		}
	}
}

func batchAgentCoordinatorPilotExpression(value runtimecontracts.ExpressionValue) string {
	if value.Ref != "" {
		return value.Ref
	}
	return value.CEL
}

func batchAgentCoordinatorPilotAuthoredSite(source semanticview.Source) bool {
	for _, site := range semanticview.AuthoredEmitSites(source) {
		if site.FlowID == batchagentcoordinatorpilot.CoordinatorFlowID &&
			site.NodeID == batchagentcoordinatorpilot.NodeID &&
			site.HandlerEvent == batchagentcoordinatorpilot.InputEvent &&
			site.Site == "handler.accumulate.on_complete.batch_agent.emit" &&
			site.Spec.EventType() == batchagentcoordinatorpilot.OutputEvent {
			return true
		}
	}
	return false
}

func batchAgentCoordinatorPilotSiteSummary(source semanticview.Source) string {
	var out []string
	for _, site := range semanticview.AuthoredEmitSites(source) {
		out = append(out, strings.Join([]string{site.FlowID, site.NodeID, site.HandlerEvent, site.Site, site.Spec.EventType()}, "|"))
	}
	return strings.Join(out, ";")
}
