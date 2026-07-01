package conformance

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestTemplateFlowPilotConformance_CoversInstanceCenteredAuthoringOwners(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-flow pilot hard invalidities = %#v, want none", got)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("template-flow pilot source did not expose bundle")
	}
	primary, err := bundle.ResolveFlowPrimaryEntity("scoring")
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(scoring): %v", err)
	}
	if primary.EntityType != "validation" {
		t.Fatalf("scoring primary entity = %q, want validation", primary.EntityType)
	}
	instance, err := bundle.ResolveFlowTemplateInstance("scoring")
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance(scoring): %v", err)
	}
	if got := strings.Join(instance.By, ","); got != "account_id" {
		t.Fatalf("scoring instance fields = %q, want account_id", got)
	}
	if instance.OnMissing != "create" || instance.OnConflict != "reuse" {
		t.Fatalf("scoring lifecycle policy = %s/%s, want create/reuse", instance.OnMissing, instance.OnConflict)
	}
	output, ok := bundle.FlowOutputEventPin("producer", "validation_requested")
	if !ok {
		t.Fatal("producer validation_requested output pin missing")
	}
	if output.Key != "account_id" || strings.Join(output.Carries, ",") != "account_id,score,decision" {
		t.Fatalf("producer output pin key/carries = %q/%#v, want account_id/[account_id score decision]", output.Key, output.Carries)
	}

	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template route plan", plans)
	}
	plan := plans[0]
	if plan.ResolutionKind != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want instance_key runtime resolution", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.Source.FlowID != "producer" || plan.Source.Pin != "validation_requested" || plan.Source.Key != "account_id" {
		t.Fatalf("route plan source = %#v, want producer.validation_requested keyed by account_id", plan.Source)
	}
	if plan.Receiver.FlowID != "scoring" || plan.Receiver.Pin != "validation_requested" || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want template scoring.validation_requested", plan.Receiver)
	}
	if plan.InstanceKey == nil || strings.Join(plan.InstanceKey.Fields, ",") != "account_id" {
		t.Fatalf("route plan instance key = %#v, want account_id", plan.InstanceKey)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan instance key mappings = %#v, want implicit account_id -> account_id", plan.InstanceKey.Mappings)
	}
}

func TestTemplateFlowPilotConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        templateflowpilot.Options
		checkID     string
		wantMessage string
	}{
		{
			name:        "unsupported receiver select_entity on connected normal path",
			opts:        templateflowpilot.Options{UnsupportedReceiverSelection: true},
			checkID:     "redundant_in_topology_select_entity",
			wantMessage: "instance.by plus parent connect",
		},
		{
			name:        "bad connect instance key mapping",
			opts:        templateflowpilot.Options{BadConnectMapping: true},
			checkID:     "composition_connect_validation",
			wantMessage: "connect_key_adapter_source_missing",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        templateflowpilot.Options{ProducerTarget: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_target_common_path_forbidden",
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        templateflowpilot.Options{ProducerBroadcast: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_broadcast_common_path_forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templateflowpilot.LoadSource(t, tc.opts)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if !templateFlowPilotConformanceFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected hard invalidity %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func templateFlowPilotConformanceFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != checkID {
			continue
		}
		if substr == "" || strings.Contains(finding.Message, substr) {
			return true
		}
	}
	return false
}
