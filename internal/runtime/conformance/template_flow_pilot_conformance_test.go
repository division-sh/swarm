package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectexisting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectorcreate"
)

func TestTemplateFlowPilotConformance_CoversInstanceCenteredAuthoringOwners(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-flow pilot hard invalidities = %#v, want none", got)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("template-flow pilot source did not expose bundle")
	}
	primary, err := bundle.ResolveFlowPrimaryEntity("account")
	if err != nil {
		t.Fatalf("ResolveFlowPrimaryEntity(account): %v", err)
	}
	if primary.EntityType != "state" {
		t.Fatalf("account primary entity = %q, want state", primary.EntityType)
	}
	instance, err := bundle.ResolveFlowTemplateInstance("account")
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance(account): %v", err)
	}
	if got := instance.Field.Path(); got != "account_id" {
		t.Fatalf("account instance fields = %q, want account_id", got)
	}
	output, ok := bundle.FlowOutputEventPin("producer", "account.ready")
	if !ok {
		t.Fatal("producer account_ready output pin missing")
	}
	if output.EventType() != "account.ready" {
		t.Fatalf("producer output event = %q, want account.ready", output.EventType())
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	plans = requireRootInputConnections(t, plans, "producer", "account.requested")
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one template route plan", plans)
	}
	plan := plans[0]
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want select-or-create runtime resolution", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if sourceEndpoint.FlowID != "producer" || sourceEndpoint.Pin != "account.ready" {
		t.Fatalf("route plan source = %#v, want producer.account_ready", sourceEndpoint)
	}
	if receiverEndpoint.FlowID != "account" || receiverEndpoint.Pin != "account.ready" || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template account.account_ready", receiverEndpoint)
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate || plan.InstanceKey().Field().Path() != "account_id" {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload.account_id" {
		t.Fatalf("route plan instance source = %#v, want payload.account_id", key)
	}
}

func TestTemplateFlowPilotConformance_FailClosedMatrix(t *testing.T) {
	tests := []struct {
		name        string
		opts        templateflowpilot.Options
		checkID     string
		wantMessage string
		loadError   bool
	}{
		{
			name:        "unsupported receiver select_entity on connected normal path",
			opts:        templateflowpilot.Options{UnsupportedReceiverSelection: true},
			loadError:   true,
			wantMessage: "handler field",
		},
		{
			name:        "producer target cannot rescue common composition",
			opts:        templateflowpilot.Options{ProducerTarget: true},
			wantMessage: "emit field \"target\" is not supported",
			loadError:   true,
		},
		{
			name:        "producer broadcast cannot replace parent connect authority",
			opts:        templateflowpilot.Options{ProducerBroadcast: true},
			wantMessage: "emit field \"broadcast\" is not supported",
			loadError:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := templateflowpilot.LoadBundleResult(t, tc.opts)
			if tc.loadError {
				if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want containing %q", err, tc.wantMessage)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			source := semanticview.Wrap(bundle)
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
			if !templateFlowPilotConformanceFindingContains(report.HardInvalidities(), tc.checkID, tc.wantMessage) {
				t.Fatalf("expected hard invalidity %s containing %q, got %#v", tc.checkID, tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func TestTemplateSelectExistingConformance_CoversResolutionSelectOwner(t *testing.T) {
	source := templateselectexisting.LoadSource(t)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-select-existing hard invalidities = %#v, want none", got)
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	var plan runtimepinrouting.ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.InstanceKey() != nil && candidate.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeSelect {
			plan = candidate
		}
	}
	if plan.InstanceKey() == nil {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want select route plan", plans)
	}
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if sourceEndpoint.FlowID != templateselectexisting.ProducerFlowID || sourceEndpoint.Pin != templateselectexisting.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", sourceEndpoint, templateselectexisting.ProducerFlowID, templateselectexisting.ProducerOutputPin)
	}
	if receiverEndpoint.FlowID != templateselectexisting.TemplateFlowID || receiverEndpoint.Pin != templateselectexisting.TemplateInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", receiverEndpoint, templateselectexisting.TemplateFlowID, templateselectexisting.TemplateInputPin)
	}
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelect || plan.InstanceKey().Field().Path() != templateselectexisting.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload."+templateselectexisting.TemplateInstanceBy {
		t.Fatalf("route plan source = %#v, want payload.%s", key, templateselectexisting.TemplateInstanceBy)
	}

	material, failure := runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, runtimepinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}))
	if !failure.Empty() || len(material.Keys) != 1 || material.Keys[0].Field.Path() != "account_id" || material.Keys[0].Value != "acct-1" {
		t.Fatalf("compiled material=%#v failure=%s, want account_id acct-1", material, failure.Code())
	}
	if projected := runtimepinrouting.MaterializeConnectRoutePlan(plan); projected.Failure != runtimepinrouting.ConnectFailureReceiverResolutionMissing || !projected.Target.Empty() || len(projected.TargetSet) != 0 {
		t.Fatalf("static projection elected a runtime receiver: %#v", projected)
	}
}

func TestTemplateSelectOrCreateConformance_CoversResolutionSelectOrCreateOwner(t *testing.T) {
	source := templateselectorcreate.LoadSource(t)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-select-or-create hard invalidities = %#v, want none", got)
	}

	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	plans = requireRootInputConnections(t, plans, "producer", "account.requested")
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one select-or-create route plan", plans)
	}
	plan := plans[0]
	sourceEndpoint := plan.SourceEndpoint().Readback()
	receiverEndpoint := plan.ReceiverEndpoint().Readback()
	if sourceEndpoint.FlowID != templateselectorcreate.ProducerFlowID || sourceEndpoint.Pin != templateselectorcreate.ProducerOutputPin {
		t.Fatalf("route plan source = %#v, want %s.%s", sourceEndpoint, templateselectorcreate.ProducerFlowID, templateselectorcreate.ProducerOutputPin)
	}
	if receiverEndpoint.FlowID != templateselectorcreate.TemplateFlowID || receiverEndpoint.Pin != templateselectorcreate.TemplateInputPin || !plan.ReceiverEndpoint().IsTemplate() {
		t.Fatalf("route plan receiver = %#v, want template %s.%s", receiverEndpoint, templateselectorcreate.TemplateFlowID, templateselectorcreate.TemplateInputPin)
	}
	if plan.ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || !plan.RequiresRuntimeResolution() {
		t.Fatalf("route plan resolution = %s runtime=%v, want runtime instance-key select-or-create", plan.ResolutionKind().Code(), plan.RequiresRuntimeResolution())
	}
	if plan.InstanceKey() == nil || plan.InstanceKey().Mode() != runtimecontracts.FlowInputResolutionModeSelectOrCreate || plan.InstanceKey().Field().Path() != templateselectorcreate.TemplateInstanceBy {
		t.Fatalf("route plan instance key = %#v, want select-or-create/account_id", plan.InstanceKey())
	}
	if key := plan.InstanceKey().Readback(); key.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || key.SourcePath != "payload."+templateselectorcreate.TemplateInstanceBy {
		t.Fatalf("route plan source = %#v, want payload.%s", key, templateselectorcreate.TemplateInstanceBy)
	}

	material, failure := runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, runtimepinrouting.AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}))
	if !failure.Empty() || len(material.Keys) != 1 || material.Keys[0].Field.Path() != "account_id" || material.Keys[0].Value != "acct-1" {
		t.Fatalf("compiled material=%#v failure=%s, want account_id acct-1", material, failure.Code())
	}
	if projected := runtimepinrouting.MaterializeConnectRoutePlan(plan); projected.Failure != runtimepinrouting.ConnectFailureReceiverResolutionMissing || !projected.Target.Empty() || len(projected.TargetSet) != 0 {
		t.Fatalf("static projection elected a runtime receiver: %#v", projected)
	}
}

func TestNotifyAllChildrenConformance_FailsClosedForRouteKeyGaps(t *testing.T) {
	portfolioEntityID := runtimeflowidentity.EntityID("portfolio")
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	runID := eventtest.UUID("run-notify-all-children")
	root := runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
	portfolio, err := runtimeflowidentity.KeylessChild(source, root, "portfolio")
	if err != nil {
		t.Fatal(err)
	}
	receipts := conformanceConstructionReceipts{}
	receipts.add(t, source, runID, root, "")
	receipts.add(t, source, runID, portfolio, "")
	for index, key := range []string{"acct-a-one", "acct-a-two"} {
		instance, err := runtimeflowidentity.KeyedChild(source, root, "account", key)
		if err != nil {
			t.Fatal(err)
		}
		instance.EntityID = runtimeflowidentity.EntityID(fmt.Sprintf("ent-a%d", index+1))
		receipts.add(t, source, runID, instance, "acct-a")
	}
	tests := []struct {
		name               string
		payload            json.RawMessage
		wantAdmissionError string
	}{
		{
			name:               "missing account key",
			payload:            json.RawMessage(`{"command":"refresh"}`),
			wantAdmissionError: "$.account_id is required",
		},
		{
			name:    "ambiguous account key",
			payload: json.RawMessage(`{"account_id":"acct-a","command":"refresh"}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sourceFact := conformanceSourceArtifactFact(t, source)
			store := &fanOutPinRouteMemoryStore{}
			eb, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
				ContractBundle:     source,
				SourceArtifactFact: sourceFact,
				Durable: runtimebus.DurableDependencies{
					Instances:                receipts,
					ConstructionPublications: receipts,
					ActiveAgents:             store,
				},
				TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(context.Context, runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
					t.Fatal("fail-closed fan-out route should not activate an account instance")
					return runtimepipeline.FlowInstanceActivationPlan{}, nil
				}),
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.RunCreatingRootIngressWithRoutingSource(
				eventtest.UUID("evt-notify-all-children-negative-"+tc.name),
				events.EventType("portfolio/account.notify.requested"),
				"",
				"",
				tc.payload,
				0,
				eventtest.UUID("run-notify-all-children"),
				"",
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID:       "portfolio",
					FlowInstance: "portfolio",
					EntityID:     portfolioEntityID,
				}),
				eventtest.StaticFlowRoutingSource("portfolio", "portfolio", portfolioEntityID),
				time.Now().UTC(),
			)
			preflight, err := eb.CheckPublishRecipientPlan(testAuthorActivityContextForBundle(context.Background(), sourceFact), evt)
			if tc.wantAdmissionError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantAdmissionError) {
					t.Fatalf("CheckPublishRecipientPlan error = %v, want %q", err, tc.wantAdmissionError)
				}
				return
			}
			var corruption *runtimepipeline.FlowInstanceConstructionCorruption
			if !errors.As(err, &corruption) {
				t.Fatalf("duplicate native selector was not construction corruption: %v", err)
			}
			if len(preflight.DeliveryRoutes) != 0 || len(preflight.Recipients) != 0 ||
				len(preflight.RoutedRecipients) != 0 || len(preflight.SubscriptionRecipients) != 0 {
				t.Fatalf("fail-closed fan-out route exposed executable recipients: routes=%#v recipients=%#v routed=%#v subscriptions=%#v",
					preflight.DeliveryRoutes, preflight.Recipients, preflight.RoutedRecipients, preflight.SubscriptionRecipients)
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

type fanOutPinRouteMemoryStore struct {
	runtimebus.InMemoryEventStore
	activeAgents   []runtimebus.ActiveAgentDescriptor
	deliveryRoutes map[string][]events.DeliveryRoute
}

func (s *fanOutPinRouteMemoryStore) ListActiveAgentDescriptors(context.Context, string) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.activeAgents...), nil
}

func (s *fanOutPinRouteMemoryStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		if s.deliveryRoutes == nil {
			s.deliveryRoutes = map[string][]events.DeliveryRoute{}
		}
		s.deliveryRoutes[req.Event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
		return nil
	})
}
