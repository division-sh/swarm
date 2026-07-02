package authoringview

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestBuildShowsTemplateInstanceRouteKeysAndCarries(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	scoring := flowByID(t, view, "scoring")
	if scoring.PrimaryEntity == nil || scoring.PrimaryEntity.Type != "validation" {
		t.Fatalf("scoring primary entity = %#v, want validation", scoring.PrimaryEntity)
	}
	if scoring.TemplateInstance == nil {
		t.Fatalf("scoring template instance missing")
	}
	if got := strings.Join(scoring.TemplateInstance.By, ","); got != "account_id" {
		t.Fatalf("scoring instance.by = %q, want account_id", got)
	}
	if scoring.TemplateInstance.OnMissing != "create" || scoring.TemplateInstance.OnConflict != "reuse" {
		t.Fatalf("scoring instance policy = %#v, want create/reuse", scoring.TemplateInstance)
	}

	producer := flowByID(t, view, "producer")
	output := outputPinByName(t, producer, "validation_requested")
	if output.Key != "account_id" || !containsString(output.Carries, "account_id") {
		t.Fatalf("producer output key/carries = %#v, want account_id carried", output)
	}

	if len(view.ConnectRoutePlans) != 1 {
		t.Fatalf("connect route plan count = %d, want 1: %#v", len(view.ConnectRoutePlans), view.ConnectRoutePlans)
	}
	plan := view.ConnectRoutePlans[0]
	if plan.Source.FlowID != "producer" || plan.Source.Pin != "validation_requested" || plan.Source.Key != "account_id" {
		t.Fatalf("route source = %#v, want producer.validation_requested keyed by account_id", plan.Source)
	}
	if plan.Receiver.FlowID != "scoring" || plan.Receiver.Pin != "validation_requested" {
		t.Fatalf("route receiver = %#v, want scoring.validation_requested", plan.Receiver)
	}
	if plan.ResolutionKind != "instance_key" {
		t.Fatalf("route resolution = %q, want instance_key", plan.ResolutionKind)
	}
	if plan.InstanceKey == nil {
		t.Fatalf("route instance key missing")
	}
	if got := strings.Join(plan.InstanceKey.Fields, ","); got != "account_id" {
		t.Fatalf("route instance key fields = %q, want account_id", got)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route implicit mapping = %#v, want account_id -> account_id explicit=false", plan.InstanceKey.Mappings)
	}
}

func TestBuildShowsRouteIssueAndAuthoredDiagnosticLocation(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{BadConnectMapping: true})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	if len(view.RoutePlanIssues) != 1 {
		t.Fatalf("route plan issue count = %d, want 1: %#v", len(view.RoutePlanIssues), view.RoutePlanIssues)
	}
	issue := view.RoutePlanIssues[0]
	if issue.Failure != "route_plan_instance_key_adapter_invalid" {
		t.Fatalf("route issue failure = %q, want route_plan_instance_key_adapter_invalid", issue.Failure)
	}
	if issue.AuthoredLocation == "" || !strings.HasSuffix(issue.AuthoredLocation, "package.yaml") {
		t.Fatalf("route issue authored location = %q, want package.yaml", issue.AuthoredLocation)
	}

	diag := diagnosticByCheckID(t, view, "composition_connect_validation")
	if diag.AuthoredLocation == "" {
		t.Fatalf("diagnostic authored location empty: %#v", diag)
	}
	if !strings.Contains(diag.Message, "connect producer.validation_requested -> scoring.validation_requested") {
		t.Fatalf("diagnostic message = %q, want connect context", diag.Message)
	}
}

func TestBuildShowsSingletonContainedOperations(t *testing.T) {
	source := singletoncoordinatorpilot.LoadSource(t, singletoncoordinatorpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	flow := flowByID(t, view, singletoncoordinatorpilot.FlowID)
	if flow.SingletonCoordinator == nil {
		t.Fatalf("singleton coordinator view missing")
	}
	if flow.SingletonCoordinator.PrimaryEntity != singletoncoordinatorpilot.EntityType {
		t.Fatalf("singleton primary entity = %q, want %s", flow.SingletonCoordinator.PrimaryEntity, singletoncoordinatorpilot.EntityType)
	}
	if !containsContainedField(flow.SingletonCoordinator.ContainedState, "lead_index", "map") {
		t.Fatalf("singleton contained state = %#v, want lead_index map", flow.SingletonCoordinator.ContainedState)
	}
	if !containsContainedField(flow.SingletonCoordinator.ContainedState, "audit_log", "list") {
		t.Fatalf("singleton contained state = %#v, want audit_log list", flow.SingletonCoordinator.ContainedState)
	}
	if len(flow.ContainedOperations) < 5 {
		t.Fatalf("contained operation count = %d, want at least 5: %#v", len(flow.ContainedOperations), flow.ContainedOperations)
	}
	mapSet := containedOperationByTargetAndOp(t, flow, "entity.lead_index", "set")
	if mapSet.MapKeyType != "text" || mapSet.MapValueType != "LeadScore" || mapSet.SourceFile == "" {
		t.Fatalf("lead_index set view = %#v, want typed map target and source file", mapSet)
	}
	listAppend := containedOperationByTargetAndOp(t, flow, "entity.audit_log", "append")
	if listAppend.ListItemType != "AuditEntry" || listAppend.SourceFile == "" {
		t.Fatalf("audit_log append view = %#v, want typed list target and source file", listAppend)
	}
}

func mustBuild(t testing.TB, source semanticview.Source, report *runtimebootverify.Report) View {
	t.Helper()
	view, err := Build(context.Background(), source, BuildOptions{BootReport: report})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return view
}

func flowByID(t testing.TB, view View, id string) FlowView {
	t.Helper()
	for _, flow := range view.Flows {
		if flow.ID == id {
			return flow
		}
	}
	t.Fatalf("flow %q not found in %#v", id, view.Flows)
	return FlowView{}
}

func outputPinByName(t testing.TB, flow FlowView, name string) OutputPinView {
	t.Helper()
	for _, pin := range flow.OutputPins {
		if pin.Name == name {
			return pin
		}
	}
	t.Fatalf("output pin %q not found in %#v", name, flow.OutputPins)
	return OutputPinView{}
}

func diagnosticByCheckID(t testing.TB, view View, checkID string) DiagnosticView {
	t.Helper()
	for _, diagnostic := range view.Diagnostics {
		if diagnostic.CheckID == checkID {
			return diagnostic
		}
	}
	t.Fatalf("diagnostic %q not found in %#v", checkID, view.Diagnostics)
	return DiagnosticView{}
}

func containedOperationByTargetAndOp(t testing.TB, flow FlowView, target, op string) ContainedOperationView {
	t.Helper()
	for _, operation := range flow.ContainedOperations {
		if operation.Target == target && operation.Operation == op {
			return operation
		}
	}
	t.Fatalf("contained operation %s %s not found in %#v", op, target, flow.ContainedOperations)
	return ContainedOperationView{}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsContainedField(fields []SingletonContainedFieldView, name, kind string) bool {
	for _, field := range fields {
		if field.Name == name && field.Kind == kind {
			return true
		}
	}
	return false
}
