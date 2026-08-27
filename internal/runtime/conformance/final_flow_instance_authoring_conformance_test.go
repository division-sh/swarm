package conformance

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
)

func TestFinalFlowInstanceAuthoringFixture_CoversSealedContractOwners(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
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
	if got := accountPrimary.Contract.Fields[finalflowinstanceauthoring.TemplateInstanceBy].Type; got != "text" {
		t.Fatalf("template primary entity key field type = %q, want text", got)
	}

	instance, err := bundle.ResolveFlowTemplateInstance(finalflowinstanceauthoring.TemplateFlowID)
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance(%s): %v", finalflowinstanceauthoring.TemplateFlowID, err)
	}
	if got := instance.Field.Path(); got != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("template instance = %q, want %s", got, finalflowinstanceauthoring.TemplateInstanceBy)
	}
	output, ok := bundle.FlowOutputEventPin(finalflowinstanceauthoring.ProducerFlowID, finalflowinstanceauthoring.ProducerOutputPin)
	if !ok {
		t.Fatalf("producer output pin %s missing", finalflowinstanceauthoring.ProducerOutputPin)
	}
	if output.Event != finalflowinstanceauthoring.ProducerOutput || output.Key != "" || len(output.Carries) != 0 {
		t.Fatalf("producer output = %#v, want canonical event without duplicate key ownership", output)
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template instance-key route plan", plans)
	}
	plan := plans[0]
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if plan.ResolutionKind() != pinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want instance_key runtime resolution", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if sourceEndpoint.FlowID != finalflowinstanceauthoring.ProducerFlowID || sourceEndpoint.Pin != finalflowinstanceauthoring.ProducerOutputPin || sourceEndpoint.Key != "" {
		t.Fatalf("route plan source = %#v, want %s.%s without producer key authority", sourceEndpoint, finalflowinstanceauthoring.ProducerFlowID, finalflowinstanceauthoring.ProducerOutputPin)
	}
	if receiverEndpoint.FlowID != finalflowinstanceauthoring.TemplateFlowID || receiverEndpoint.Pin != finalflowinstanceauthoring.TemplateInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", receiverEndpoint, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateInputPin)
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate || plan.InstanceKey().Field().Path() != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select-or-create/%s", plan.InstanceKey(), finalflowinstanceauthoring.TemplateInstanceBy)
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload."+finalflowinstanceauthoring.TemplatePayloadKey {
		t.Fatalf("route plan source = %#v, want payload source for %s", key, finalflowinstanceauthoring.TemplatePayloadKey)
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
			name:        "retired receiver instance key",
			opts:        finalflowinstanceauthoring.Options{RetiredInstanceKey: true},
			wantMessage: "resolution.instance_key is retired",
			loadError:   true,
		},
		{
			name:        "missing receiver carry evidence",
			opts:        finalflowinstanceauthoring.Options{MissingOutputCarries: true},
			checkID:     "composition_connect_validation",
			wantMessage: "must declare a carry named account_id",
		},
		{
			name:        "normal connected receiver select_entity is illegal",
			opts:        finalflowinstanceauthoring.Options{UnsupportedReceiverSelector: true},
			checkID:     "redundant_in_topology_select_entity",
			wantMessage: "scalar receiver instance",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        finalflowinstanceauthoring.Options{ProducerTarget: true},
			wantMessage: "RETIRED-EMIT-ROUTING: emit.target",
			loadError:   true,
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        finalflowinstanceauthoring.Options{ProducerBroadcast: true},
			wantMessage: "RETIRED-EMIT-ROUTING: emit.broadcast",
			loadError:   true,
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
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), runtimebootverify.Options{})
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
		"typed_connect_recipient_evaluation",
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
		"compiled_connect_recipient_evaluation",
		"live_carriers_internal_subscriptions",
	} {
		family := routeAuthorityDriftSeamFamilyByID(t, &inventory, id)
		if len(family.InvalidAuthority) == 0 && strings.Contains(family.Layer, "non_authority") {
			t.Fatalf("seam_family %s invalid_authority missing", id)
		}
	}
	if problems := validateRouteAuthorityDriftInventory(t, root, inventory); len(problems) != 0 {
		t.Fatalf("route authority inventory validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
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
