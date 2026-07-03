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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
)

func TestFinalFlowInstanceAuthoringFixture_CoversSealedContractOwners(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("final sealed fixture hard invalidities = %#v, want none", got)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("final sealed fixture source did not expose bundle")
	}

	accountPrimary, err := bundle.ResolveFlowPrimaryEntity(finalflowinstanceauthoring.TemplateFlowID)
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(%s): %v", finalflowinstanceauthoring.TemplateFlowID, err)
	}
	if accountPrimary.EntityType != finalflowinstanceauthoring.TemplateEntityType {
		t.Fatalf("template primary entity = %q, want %s", accountPrimary.EntityType, finalflowinstanceauthoring.TemplateEntityType)
	}
	if got := accountPrimary.Contract.Fields[finalflowinstanceauthoring.TemplateInstanceBy].Type; got != "string" {
		t.Fatalf("template primary entity key field type = %q, want string", got)
	}

	instance, err := bundle.ResolveFlowTemplateInstance(finalflowinstanceauthoring.TemplateFlowID)
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance(%s): %v", finalflowinstanceauthoring.TemplateFlowID, err)
	}
	if got := strings.Join(instance.By, ","); got != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("template instance.by = %q, want %s", got, finalflowinstanceauthoring.TemplateInstanceBy)
	}
	if instance.OnMissing != "create" || instance.OnConflict != "reuse" {
		t.Fatalf("template lifecycle policy = %s/%s, want create/reuse", instance.OnMissing, instance.OnConflict)
	}

	output, ok := bundle.FlowOutputEventPin(finalflowinstanceauthoring.ProducerFlowID, finalflowinstanceauthoring.ProducerOutputPin)
	if !ok {
		t.Fatalf("producer output pin %s missing", finalflowinstanceauthoring.ProducerOutputPin)
	}
	if output.Key != finalflowinstanceauthoring.TemplatePayloadKey || !finalFixtureContainsString(output.Carries, finalflowinstanceauthoring.TemplatePayloadKey) {
		t.Fatalf("producer output key/carries = %q/%#v, want %s carried", output.Key, output.Carries, finalflowinstanceauthoring.TemplatePayloadKey)
	}

	plans, issues := pinrouting.LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template instance-key route plan", plans)
	}
	plan := plans[0]
	if plan.ResolutionKind != pinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution {
		t.Fatalf("route plan resolution = %s runtime=%v, want instance_key runtime resolution", plan.ResolutionKind, plan.RequiresRuntimeResolution)
	}
	if plan.Source.FlowID != finalflowinstanceauthoring.ProducerFlowID || plan.Source.Pin != finalflowinstanceauthoring.ProducerOutputPin || plan.Source.Key != finalflowinstanceauthoring.TemplatePayloadKey {
		t.Fatalf("route plan source = %#v, want %s.%s keyed by %s", plan.Source, finalflowinstanceauthoring.ProducerFlowID, finalflowinstanceauthoring.ProducerOutputPin, finalflowinstanceauthoring.TemplatePayloadKey)
	}
	if plan.Receiver.FlowID != finalflowinstanceauthoring.TemplateFlowID || plan.Receiver.Pin != finalflowinstanceauthoring.TemplateInputPin || plan.Receiver.Mode != "template" {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", plan.Receiver, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateInputPin)
	}
	if plan.InstanceKey == nil || strings.Join(plan.InstanceKey.Fields, ",") != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want %s", plan.InstanceKey, finalflowinstanceauthoring.TemplateInstanceBy)
	}
	if len(plan.InstanceKey.Mappings) != 1 ||
		plan.InstanceKey.Mappings[0].Source != finalflowinstanceauthoring.TemplatePayloadKey ||
		plan.InstanceKey.Mappings[0].Target != finalflowinstanceauthoring.TemplateInstanceBy ||
		!plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route plan mappings = %#v, want explicit %s -> %s", plan.InstanceKey.Mappings, finalflowinstanceauthoring.TemplatePayloadKey, finalflowinstanceauthoring.TemplateInstanceBy)
	}

	coordinatorPrimary, err := bundle.ResolveFlowPrimaryEntity(finalflowinstanceauthoring.CoordinatorFlowID)
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(%s): %v", finalflowinstanceauthoring.CoordinatorFlowID, err)
	}
	if coordinatorPrimary.EntityType != finalflowinstanceauthoring.CoordinatorEntityType {
		t.Fatalf("coordinator primary entity = %q, want %s", coordinatorPrimary.EntityType, finalflowinstanceauthoring.CoordinatorEntityType)
	}
	if got := coordinatorPrimary.Contract.Fields["lead_index"].Type; got != "map[text]LeadScore" {
		t.Fatalf("lead_index type = %q, want map[text]LeadScore", got)
	}
	if got := coordinatorPrimary.Contract.Fields["audit_log"].Type; got != "[AuditEntry]" {
		t.Fatalf("audit_log type = %q, want [AuditEntry]", got)
	}
	coordinator, err := bundle.ResolveFlowSingletonCoordinator(finalflowinstanceauthoring.CoordinatorFlowID)
	if err != nil {
		t.Fatalf("ResolveFlowSingletonCoordinator(%s): %v", finalflowinstanceauthoring.CoordinatorFlowID, err)
	}
	if coordinator.PrimaryEntity.EntityType != finalflowinstanceauthoring.CoordinatorEntityType {
		t.Fatalf("singleton primary entity = %q, want %s", coordinator.PrimaryEntity.EntityType, finalflowinstanceauthoring.CoordinatorEntityType)
	}
	if !finalFixtureContainedField(coordinator.ContainedState, "lead_index", "map") || !finalFixtureContainedField(coordinator.ContainedState, "audit_log", "list") {
		t.Fatalf("singleton contained state = %#v, want lead_index map and audit_log list", coordinator.ContainedState)
	}

	handler, ok := source.NodeEventHandler(finalflowinstanceauthoring.CoordinatorNodeID, finalflowinstanceauthoring.CoordinatorInput)
	if !ok {
		t.Fatalf("handler %s/%s missing", finalflowinstanceauthoring.CoordinatorNodeID, finalflowinstanceauthoring.CoordinatorInput)
	}
	containedWrites := make([]runtimecontracts.WorkflowDataWrite, 0, len(handler.DataAccumulation.Writes))
	for _, write := range handler.DataAccumulation.Writes {
		if write.IsContainedOperation() {
			containedWrites = append(containedWrites, write)
		}
	}
	if got := len(containedWrites); got != 6 {
		t.Fatalf("contained writes = %d, want 6: %#v", got, handler.DataAccumulation.Writes)
	}
	contract, ok := entityruntime.ResolveForFlow(source, finalflowinstanceauthoring.CoordinatorFlowID)
	if !ok {
		t.Fatalf("entityruntime.ResolveForFlow(%s) missing", finalflowinstanceauthoring.CoordinatorFlowID)
	}
	for idx, write := range containedWrites {
		if _, err := entityruntime.ResolveContainedOperationTarget(contract, write.Target(), string(write.Operation), !write.Key.IsZero(), !write.Index.IsZero()); err != nil {
			t.Fatalf("ResolveContainedOperationTarget(write[%d]): %v", idx, err)
		}
	}
}

func TestFinalFlowInstanceAuthoringFixture_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        finalflowinstanceauthoring.Options
		checkID     string
		wantMessage string
		loadError   bool
	}{
		{
			name:        "missing output key evidence",
			opts:        finalflowinstanceauthoring.Options{MissingOutputKey: true},
			checkID:     "composition_connect_validation",
			wantMessage: "output_key_missing",
		},
		{
			name:        "missing output carries evidence",
			opts:        finalflowinstanceauthoring.Options{MissingOutputCarries: true},
			checkID:     "output_pin_key_carries_validation",
			wantMessage: "carries",
		},
		{
			name:        "renamed key adapter references missing producer evidence",
			opts:        finalflowinstanceauthoring.Options{BadConnectMapping: true},
			checkID:     "composition_connect_validation",
			wantMessage: "connect_key_adapter_source_missing",
		},
		{
			name:        "duplicate adapter mapping is ambiguous",
			opts:        finalflowinstanceauthoring.Options{DuplicateConnectMapping: true},
			checkID:     "composition_connect_validation",
			wantMessage: "connect_key_adapter_duplicate_source",
		},
		{
			name:        "normal connected receiver select_entity is illegal",
			opts:        finalflowinstanceauthoring.Options{UnsupportedReceiverSelector: true},
			checkID:     "redundant_in_topology_select_entity",
			wantMessage: "instance.by plus parent connect",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        finalflowinstanceauthoring.Options{ProducerTarget: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_target_common_path_forbidden",
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        finalflowinstanceauthoring.Options{ProducerBroadcast: true},
			checkID:     "pin_target_resolution",
			wantMessage: "producer_broadcast_common_path_forbidden",
		},
		{
			name:        "bad map write dynamic bracket target",
			opts:        finalflowinstanceauthoring.Options{DynamicBracketTarget: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "dynamic bracket path syntax",
		},
		{
			name:        "bad map write missing key",
			opts:        finalflowinstanceauthoring.Options{MissingMapKey: true},
			wantMessage: "requires key",
			loadError:   true,
		},
		{
			name:        "bad map write wrong value shape",
			opts:        finalflowinstanceauthoring.Options{WrongMapValueShape: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "undeclared",
		},
		{
			name:        "bad map write undeclared target",
			opts:        finalflowinstanceauthoring.Options{UndeclaredMapTarget: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "missing_index",
		},
		{
			name:        "bad map write unsupported operation",
			opts:        finalflowinstanceauthoring.Options{UnsupportedMapOp: true},
			wantMessage: "unsupported workflow data write op",
			loadError:   true,
		},
		{
			name:        "bad list write negative index",
			opts:        finalflowinstanceauthoring.Options{BadListIndex: true},
			checkID:     "contained_state_operation_compliance",
			wantMessage: "list index cannot be negative",
		},
		{
			name:        "retired static create_entity",
			opts:        finalflowinstanceauthoring.Options{StaticCreateEntity: true},
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "retired static select_entity",
			opts:        finalflowinstanceauthoring.Options{StaticSelectEntity: true},
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "retired static select_or_create_entity",
			opts:        finalflowinstanceauthoring.Options{StaticSelectOrCreate: true},
			checkID:     "select_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "retired static missing acquisition",
			opts:        finalflowinstanceauthoring.Options{StaticMissingAcquisition: true},
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "static multi-row entity ownership is retired",
		},
		{
			name:        "retired root default caller-selected entity id",
			opts:        finalflowinstanceauthoring.Options{RootDefaultEntityIDSource: true},
			checkID:     "flow_boundary_create_entity_validation",
			wantMessage: "caller-selected entity_id",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := finalflowinstanceauthoring.LoadBundleResult(t, tc.opts)
			if tc.loadError {
				if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want containing %q", err, tc.wantMessage)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
			if !finalFixtureFindingContains(report.Errors(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected bootverify error %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.Errors())
			}
		})
	}
}

func TestFinalFlowInstanceAuthoringFixture_RouteAuthorityBypassInventoryStaysClassified(t *testing.T) {
	root := conformanceRepoRoot(t)
	inventory := loadRouteAuthorityDriftInventory(t, root)
	for _, id := range []string{
		"direct_delivery_path",
		"event_delivery_plan_compatibility",
		"route_table_resolve",
		"receiver_carrier",
	} {
		dimension := routeAuthorityDriftSearchDimensionByID(t, &inventory, id)
		if !dimension.ClassifiedPathsRequired {
			t.Fatalf("search_dimension %s classified_paths_required = false, want true for final bypass guard", id)
		}
	}
	for _, id := range []string{
		"direct_delivery_path_classification",
		"event_delivery_plan_compatibility_adapter",
		"route_table_resolve_role_separation",
		"receiver_carrier_evidence",
		"live_carriers_internal_subscriptions",
	} {
		family := routeAuthorityDriftSeamFamilyByID(t, &inventory, id)
		if len(family.InvalidAuthority) == 0 && strings.Contains(family.Layer, "non_authority") {
			t.Fatalf("seam_family %s invalid_authority missing", id)
		}
	}
	if problems := validateRouteAuthorityDriftInventory(root, inventory); len(problems) != 0 {
		t.Fatalf("route authority inventory validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func finalFixtureContainedField(fields []runtimecontracts.SingletonCoordinatorContainedField, name, kind string) bool {
	for _, field := range fields {
		if strings.TrimSpace(field.Name) == name && strings.TrimSpace(field.Kind) == kind {
			return true
		}
	}
	return false
}

func finalFixtureContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func finalFixtureFindingContains(findings []runtimebootverify.Finding, checkID, substr string) bool {
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
