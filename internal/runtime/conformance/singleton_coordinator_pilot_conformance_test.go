package conformance

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
)

func TestSingletonCoordinatorPilotConformance_CoversSingletonMapCoordinatorOwners(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:file-scope"))
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:firstMapWriteYAML"), canonicalrouting.SourceID("internal/runtime/testfixtures/singletoncoordinatorpilot/fixture.go:writeCoordinator"))
	source := singletoncoordinatorpilot.LoadSource(t, singletoncoordinatorpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("singleton coordinator pilot hard invalidities = %#v, want none", got)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("singleton coordinator pilot source did not expose bundle")
	}

	primary, err := bundle.ResolveFlowPrimaryEntity(singletoncoordinatorpilot.FlowID)
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(%s): %v", singletoncoordinatorpilot.FlowID, err)
	}
	if primary.EntityType != singletoncoordinatorpilot.EntityType {
		t.Fatalf("primary entity = %q, want %s", primary.EntityType, singletoncoordinatorpilot.EntityType)
	}
	if got := primary.Contract.Fields["lead_index"].Type; got != "map[text]LeadScore" {
		t.Fatalf("lead_index type = %q, want map[text]LeadScore", got)
	}
	if got := primary.Contract.Fields["audit_log"].Type; got != "[AuditEntry]" {
		t.Fatalf("audit_log type = %q, want [AuditEntry]", got)
	}

	coordinator, err := bundle.ResolveFlowSingletonCoordinator(singletoncoordinatorpilot.FlowID)
	if err != nil {
		t.Fatalf("ResolveFlowSingletonCoordinator(%s): %v", singletoncoordinatorpilot.FlowID, err)
	}
	if coordinator.PrimaryEntity.EntityType != singletoncoordinatorpilot.EntityType {
		t.Fatalf("singleton primary entity = %q, want %s", coordinator.PrimaryEntity.EntityType, singletoncoordinatorpilot.EntityType)
	}
	if !singletonCoordinatorContains(coordinator.ContainedState, "lead_index", "map") {
		t.Fatalf("singleton contained fields = %#v, want lead_index map", coordinator.ContainedState)
	}
	if !singletonCoordinatorContains(coordinator.ContainedState, "audit_log", "list") {
		t.Fatalf("singleton contained fields = %#v, want audit_log list", coordinator.ContainedState)
	}

	handler, ok := source.NodeEventHandler(singletoncoordinatorpilot.NodeID, singletoncoordinatorpilot.InputEvent)
	if !ok {
		t.Fatalf("handler %s/%s missing", singletoncoordinatorpilot.NodeID, singletoncoordinatorpilot.InputEvent)
	}
	containedWrites := make([]runtimecontracts.WorkflowDataWrite, 0, len(handler.DataAccumulation.Writes))
	for _, write := range handler.DataAccumulation.Writes {
		if write.IsContainedOperation() {
			containedWrites = append(containedWrites, write)
		}
	}
	if got := len(containedWrites); got != 6 {
		t.Fatalf("contained handler writes = %d, want 6 in %#v", got, handler.DataAccumulation.Writes)
	}
	wantOps := []runtimecontracts.WorkflowDataOperation{
		runtimecontracts.WorkflowDataOperationSet,
		runtimecontracts.WorkflowDataOperationMerge,
		runtimecontracts.WorkflowDataOperationAppend,
		runtimecontracts.WorkflowDataOperationAppend,
		runtimecontracts.WorkflowDataOperationAppend,
		runtimecontracts.WorkflowDataOperationUpdate,
	}
	contract, ok := entityruntime.ResolveForFlow(source, singletoncoordinatorpilot.FlowID)
	if !ok {
		t.Fatalf("entityruntime.ResolveForFlow(%s) missing", singletoncoordinatorpilot.FlowID)
	}
	for idx, write := range containedWrites {
		if write.Operation != wantOps[idx] {
			t.Fatalf("write[%d] op = %q, want %q", idx, write.Operation, wantOps[idx])
		}
		if !write.IsContainedOperation() {
			t.Fatalf("write[%d] is not a contained operation: %#v", idx, write)
		}
		if _, err := entityruntime.ResolveContainedOperationTarget(contract, write.Target(), string(write.Operation), !write.Key.IsZero(), !write.Index.IsZero()); err != nil {
			t.Fatalf("ResolveContainedOperationTarget(write[%d]): %v", idx, err)
		}
	}
	if containedWrites[0].Key.Ref != "payload.lead_id" {
		t.Fatalf("map set key = %#v, want explicit payload.lead_id ref", containedWrites[0].Key)
	}
	if containedWrites[2].Key.Ref != "payload.lead_id" {
		t.Fatalf("map append key = %#v, want explicit payload.lead_id ref", containedWrites[2].Key)
	}

	plans, issues := pinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 0 {
		t.Fatalf("singleton coordinator contained state produced route plans = %#v, want none", plans)
	}
}

func TestSingletonCoordinatorPilotConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        singletoncoordinatorpilot.Options
		checkID     string
		wantMessage string
	}{
		{
			name:        "dynamic bracket target path",
			opts:        singletoncoordinatorpilot.Options{DynamicBracketTarget: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "dynamic bracket path syntax",
		},
		{
			name:        "missing map key",
			opts:        singletoncoordinatorpilot.Options{MissingMapKey: true},
			checkID:     "load",
			wantMessage: "requires key",
		},
		{
			name:        "wrong value shape",
			opts:        singletoncoordinatorpilot.Options{WrongValueShape: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "undeclared",
		},
		{
			name:        "undeclared target",
			opts:        singletoncoordinatorpilot.Options{UndeclaredTarget: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "missing_index",
		},
		{
			name:        "unsupported operation",
			opts:        singletoncoordinatorpilot.Options{UnsupportedOperation: true},
			checkID:     "load",
			wantMessage: "unsupported workflow data write op",
		},
		{
			name:        "bad list index",
			opts:        singletoncoordinatorpilot.Options{BadListIndex: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "list index cannot be negative",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := singletoncoordinatorpilot.LoadBundleResult(t, tc.opts)
			if tc.checkID == "load" {
				if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want %q", err, tc.wantMessage)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			source := semanticview.Wrap(bundle)
			report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
			if !singletonCoordinatorPilotFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected hard invalidity %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func singletonCoordinatorContains(fields []runtimecontracts.SingletonCoordinatorContainedField, name, kind string) bool {
	for _, field := range fields {
		if strings.TrimSpace(field.Name) == name && strings.TrimSpace(field.Kind) == kind {
			return true
		}
	}
	return false
}

func singletonCoordinatorPilotFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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
