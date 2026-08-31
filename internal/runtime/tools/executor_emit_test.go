package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type publishBusCapture struct {
	event events.Event
	count int
}

func TestHandleEmitToolPreservesImportedAgentSemanticSource(t *testing.T) {
	const (
		flowID       = "telegram-chat"
		flowPath     = "telegram-ingress/telegram-chat"
		instancePath = flowPath + "/chat-1"
		agentID      = "phrase-bot"
	)
	eventType := "telegram.reply.requested"
	eventEntry := runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Type: "object",
		Properties: map[string]runtimecontracts.EventFieldSpec{
			"chat_id": {Type: "string"},
			"text":    {Type: "string"},
		},
		Required: []string{"chat_id", "text"},
	}}
	entry := runtimecontracts.EffectiveAgentRegistryEntry(agentID, runtimecontracts.AgentRegistryEntry{
		ID: agentID, Role: agentID, EmitEvents: []string{eventType},
	})
	flow := runtimecontracts.FlowContractView{
		Path: flowPath,
		Paths: runtimecontracts.FlowContractPaths{
			ID: flowID, Flow: flowID, PackageKey: "bot", AgentsFile: "/contracts/bot/flows/telegram-chat/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventType: eventEntry,
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{agentID: entry},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventType: eventEntry,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, agentID, flowID)
	declaration := semanticview.AgentDeclarations(source)
	if len(declaration) != 1 {
		t.Fatalf("agent declarations = %#v, want one imported declaration", declaration)
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration[0])
	if err != nil {
		t.Fatalf("agent declaration name: %v", err)
	}
	actor := models.AgentConfig{
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ID:            agentID,
		Identity:      agentidentitytest.Declared(t, agentID, plan.OwnerURI, flowPath, "chat-1", instancePath),
		Role:          agentID,
		FlowID:        flowID,
		FlowPath:      instancePath,
		EntityID:      eventtest.UUID("telegram-chat-entity"),
		EmitEvents:    []string{eventType},
	}
	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   NewEmitRegistry(source, nil),
	})
	if _, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_telegram_reply_requested", map[string]any{
		"chat_id": "42", "text": "hello",
	}); err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want one", bus.count)
	}
	wantRoute := events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: actor.EntityID}
	if got := bus.event.RoutingSource().Route().Normalized(); got != wantRoute {
		t.Fatalf("routing source route = %#v, want declaration flow plus concrete runtime instance %#v", got, wantRoute)
	}
	if got := string(bus.event.Type()); got != instancePath+"/"+eventType {
		t.Fatalf("emitted event type = %q, want declaration-owned concrete path %q", got, instancePath+"/"+eventType)
	}
}

type emitPreflightCaptureBus struct {
	*runtimebus.EventBus
	preflight events.Event
}

func (b *emitPreflightCaptureBus) CheckPublishRecipientPlan(ctx context.Context, evt events.Event) (runtimebus.PublishRecipientPlan, error) {
	b.preflight = evt
	return b.EventBus.CheckPublishRecipientPlan(ctx, evt)
}

const emitRoutePlanTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func (b *publishBusCapture) Publish(_ context.Context, evt events.Event) error {
	b.event = evt
	b.count++
	return nil
}

func (b *publishBusCapture) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.event = evt
	b.count++
	return nil
}

func (b *publishBusCapture) PublishDirectRoutes(_ context.Context, evt events.Event, _ []events.DeliveryRoute) error {
	b.event = evt
	b.count++
	return nil
}

type emitWorkflowInstanceLoader struct {
	rows map[string]runtimepipeline.WorkflowInstance
	err  error
}

func (l emitWorkflowInstanceLoader) Enabled() bool { return true }

func (l emitWorkflowInstanceLoader) Load(_ context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimepipeline.WorkflowInstance, bool, error) {
	if l.err != nil {
		return runtimepipeline.WorkflowInstance{}, false, l.err
	}
	instance, ok := l.rows[strings.TrimSpace(identity.Route.InstancePath)]
	return instance, ok, nil
}

type emitRoutePlanStore struct {
	events       map[string]events.Event
	routes       map[string][]events.DeliveryRoute
	scopes       map[string]runtimepipelineobligation.CommittedScope
	active       []string
	targetOwners []runtimebus.ActiveTargetDescriptor
}

func newEmitRoutePlanStore() *emitRoutePlanStore {
	return &emitRoutePlanStore{
		events: map[string]events.Event{},
		routes: map[string][]events.DeliveryRoute{},
		scopes: map[string]runtimepipelineobligation.CommittedScope{},
	}
}

func newEmitRoutePlanEventBus(t *testing.T, store *emitRoutePlanStore, source semanticview.Source) *runtimebus.EventBus {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "emit-route-plan-test",
		BundleHash:        emitRoutePlanTestBundleHash,
	})
	if err != nil {
		t.Fatalf("create emit route-plan runtime occurrence: %v", err)
	}
	sourceFact, err := runtimecorrelation.NewEphemeralBundleSourceFact(emitRoutePlanTestBundleHash)
	if err != nil {
		t.Fatalf("construct emit route-plan source fact: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire emit route-plan runtime occurrence: %v", err)
			return
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join emit route-plan process owner: %v", err)
		}
	})
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(store, runtimebus.EventBusOptions{
		ExecutionPosture: executionposture.Live,
		BundleSourceFact: sourceFact,
		ContractBundle:   source,
		Durable:          runtimebus.DurableDependencies{TargetOwners: store},
		WorkOwner:        owner, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return bus
}

func (s *emitRoutePlanStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, s.beginPublish, s.finalizePublish)
}

func (s *emitRoutePlanStore) beginPublish(_ context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	event := admitted.Event()
	if existing, ok := s.events[event.ID()]; ok {
		if !reflect.DeepEqual(existing, event) {
			return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event %s conflicts with its admitted fixture", event.ID())
		}
		return runtimebus.EventAppendExactDuplicate, nil
	}
	s.events[event.ID()] = event
	s.active = append(s.active, event.ID())
	return runtimebus.EventAppendInserted, nil
}

func (s *emitRoutePlanStore) finalizePublish(_ context.Context, req runtimebus.CommitPublishRequest) error {
	event := req.Event.Event()
	if len(s.active) == 0 || s.active[len(s.active)-1] != event.ID() {
		return errors.New("prepared event finalization does not match active emit event")
	}
	s.routes[event.ID()] = events.NormalizeDeliveryRoutes(req.DeliveryRoutes)
	s.scopes[event.ID()] = req.ReplayScope
	s.active = s.active[:len(s.active)-1]
	return nil
}

func (s *emitRoutePlanStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	var out []string
	for _, route := range s.routes[eventID] {
		if route.Recipient.IsAgent() {
			out = append(out, route.Recipient.ID())
		}
	}
	return out, nil
}

func (s *emitRoutePlanStore) ListSelectedRunTargetOwners(context.Context, string) ([]runtimebus.ActiveTargetDescriptor, error) {
	return append([]runtimebus.ActiveTargetDescriptor(nil), s.targetOwners...), nil
}

func TestHandleEmitTool_PreservesPayloadForFlowScopedEmit(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category":  {Type: "string"},
						"signal_id": {Type: "string"},
					},
					Required: []string{"category", "signal_id"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "discovery",
						Flow: "discovery",
					},
					Schema: runtimecontracts.FlowSchemaDocument{
						Mode: runtimecontracts.FlowModeStatic,
						Pins: runtimecontracts.FlowPins{},
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"category.assessed": {},
					},
					Path: "discovery",
				},
			},
		},
	}
	bundle.FlowTree.ByID["discovery"].Events["category.assessed"] = bundle.Events["category.assessed"]
	source := toolTestSourceWithDeclaredAgent(t, bundle, "market-research-agent", "discovery")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Identity:      toolTestAgentIdentity(t, "market-research-agent", "discovery", "discovery"),
		EntityID:      eventtest.UUID("market-research-discovery-source"),
		Role:          "market_research",
		FlowID:        "discovery",
		FlowPath:      "discovery",
		EmitEvents:    []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_category_assessed", map[string]any{
		"category":  "AP automation",
		"signal_id": "sig-1",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type()), "discovery/category.assessed"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
	}

	var payload map[string]any
	if err := json.Unmarshal(bus.event.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got, want := payload["category"], "AP automation"; got != want {
		t.Fatalf("payload category = %#v, want %q", got, want)
	}
	if got, want := payload["signal_id"], "sig-1"; got != want {
		t.Fatalf("payload signal_id = %#v, want %q", got, want)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_ValidatesCriteriaCitationsBeforePublish(t *testing.T) {
	tests := []struct {
		name       string
		payload    map[string]any
		wantReason string
	}{
		{
			name: "valid single citation",
			payload: map[string]any{
				"cite": "FX-HARD-01",
			},
		},
		{
			name: "valid list citation",
			payload: map[string]any{
				"cites": []any{"FX-HARD-01"},
			},
		},
		{
			name: "empty list citation",
			payload: map[string]any{
				"cites": []any{},
			},
			wantReason: "criteria_citation_shape_invalid",
		},
		{
			name: "unknown id",
			payload: map[string]any{
				"cite": "FX-MISSING",
			},
			wantReason: "criteria_id_unknown",
		},
		{
			name: "class mismatch",
			payload: map[string]any{
				"cite": "FX-SOFT-04",
			},
			wantReason: "criteria_class_not_allowed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec, bus, actor := criteriaCitationEmitTestExecutor(t)
			_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_cto_spec_vetoed", tc.payload)
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("handleEmitTool: %v", err)
				}
				if bus.count != 1 {
					t.Fatalf("publish count = %d, want 1", bus.count)
				}
				return
			}
			if err == nil {
				t.Fatal("handleEmitTool error = nil, want criteria citation rejection")
			}
			runtimeErr, ok := failures.As(err)
			if !ok || runtimeErr == nil {
				t.Fatalf("error = %#v, want canonical failure", err)
			}
			if runtimeErr.Failure.Class != failures.ClassSchemaInvalid || runtimeErr.Failure.Detail.Code != "criteria_citation_validation_failed" {
				t.Fatalf("failure = %#v", runtimeErr.Failure)
			}
			if runtimeErr.Failure.Retryable {
				t.Fatalf("runtime error retryable = true, want false")
			}
			if got := runtimeErr.Failure.Detail.Attributes["reason"]; got != tc.wantReason {
				t.Fatalf("failure reason = %#v, want %q", got, tc.wantReason)
			}
			if bus.count != 0 {
				t.Fatalf("publish count = %d, want 0", bus.count)
			}
		})
	}
}

func TestHandleEmitTool_CriteriaCitationsSelectActorScopeForSharedEffectiveName(t *testing.T) {
	exec, bus, actor := criteriaCitationEmitTestExecutor(t)
	if _, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_cto_spec_vetoed", map[string]any{
		"cite": "FX-HARD-01",
	}); err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func criteriaCitationEmitTestExecutor(t testing.TB) (*Executor, *publishBusCapture, models.AgentConfig) {
	return criteriaCitationEmitTestExecutorWithAgent(t, runtimecontracts.AgentRegistryEntry{
		ID:         "cto-agent",
		Role:       "cto",
		EmitEvents: []string{"cto.spec_vetoed"},
		Criteria:   []string{"feasibility_exclusions"},
	})
}

func criteriaCitationEmitTestExecutorWithAgent(t testing.TB, agent runtimecontracts.AgentRegistryEntry) (*Executor, *publishBusCapture, models.AgentConfig) {
	flow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Path:   "validation",
		Policy: runtimecontracts.PolicyDocument{
			Criteria: map[string]runtimecontracts.PolicyCriteriaSet{
				"feasibility_exclusions": {
					Classes: map[string]runtimecontracts.PolicyCriteriaClass{
						"hard": {Disposition: "cto.spec_vetoed"},
						"soft": {Disposition: "cto.spec_revision_needed"},
					},
					Rules: []runtimecontracts.PolicyCriteriaRule{{
						ID:    "FX-HARD-01",
						Class: "hard",
						Text:  "Requires regulated real-time integration.",
					}, {
						ID:    "FX-SOFT-04",
						Class: "soft",
						Text:  "Missing MVP spec.",
					}},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"cto-agent": agent,
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"cto.spec_vetoed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"cite": {
							Type: "text",
							Citation: runtimecontracts.CriteriaCitation{
								Criteria:       "feasibility_exclusions",
								AllowedClasses: []string{"hard"},
							},
						},
						"cites": {
							Type: "[text]",
							Citation: runtimecontracts.CriteriaCitation{
								Criteria:       "feasibility_exclusions",
								AllowedClasses: []string{"hard"},
							},
						},
					},
				},
			},
		},
	}
	otherFlow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "other-validation", Flow: "other-validation"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Path:   "other-validation",
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"cto-agent": {Role: "other-cto", Criteria: []string{"other_criteria"}},
		},
	}
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow, otherFlow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation":       &root.Children[0],
				"other-validation": &root.Children[1],
			},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"validation/cto-agent":       {Kind: "agent", FlowID: "validation", LocalID: "cto-agent", Path: "validation", Full: "swarm-test://validation/cto-agent"},
				"other-validation/cto-agent": {Kind: "agent", FlowID: "other-validation", LocalID: "cto-agent", Path: "other-validation", Full: "swarm-test://other-validation/cto-agent"},
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{
				"swarm-test://validation/cto-agent":       {Kind: "agent", FlowID: "validation", LocalID: "cto-agent", Path: "validation", Full: "swarm-test://validation/cto-agent"},
				"swarm-test://other-validation/cto-agent": {Kind: "agent", FlowID: "other-validation", LocalID: "cto-agent", Path: "other-validation", Full: "swarm-test://other-validation/cto-agent"},
			},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "cto-agent", "validation")
	emitRegistry := NewEmitRegistry(source, nil)
	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "cto-agent",
		Identity:      toolTestAgentIdentity(t, "cto-agent", "validation", "validation"),
		EntityID:      eventtest.UUID("cto-validation-source"),
		Role:          "cto",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"cto.spec_vetoed"},
		Criteria:      []string{"feasibility_exclusions"},
	}
	return exec, bus, actor
}

func TestHandleEmitTool_RejectsMutableActorCriteriaGrant(t *testing.T) {
	exec, bus, actor := criteriaCitationEmitTestExecutorWithAgent(t, runtimecontracts.AgentRegistryEntry{
		ID:         "cto-agent",
		Role:       "cto",
		EmitEvents: []string{"cto.spec_vetoed"},
	})
	actor.Criteria = []string{"feasibility_exclusions"}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_cto_spec_vetoed", map[string]any{
		"cite": "FX-HARD-01",
	})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want mutable criteria grant rejection")
	}
	runtimeErr, ok := failures.As(err)
	if !ok || runtimeErr == nil {
		t.Fatalf("error = %#v, want runtime error", err)
	}
	if runtimeErr.Failure.Detail.Code != "criteria_citation_validation_failed" {
		t.Fatalf("failure detail = %q, want criteria_citation_validation_failed", runtimeErr.Failure.Detail.Code)
	}
	if got := runtimeErr.Failure.Detail.Attributes["reason"]; got != "criteria_set_not_allowed" {
		t.Fatalf("failure reason = %#v, want criteria_set_not_allowed", got)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_PreservesInboundChildFlowOwnerAndExecutionMode(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"research.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"summary": {Type: "string"},
					},
					Required: []string{"summary"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "validation",
						Flow: "validation",
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"research.completed": {},
					},
					Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
					Path:   "validation",
				},
			},
		},
	}
	bundle.FlowTree.ByID["validation"].Events["research.completed"] = bundle.Events["research.completed"]
	source := toolTestSourceWithDeclaredAgent(t, bundle, "business-research-agent", "validation")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "mock",
		ID:            "business-research-agent",
		Identity:      toolTestAgentIdentity(t, "business-research-agent", "validation", "validation/inst-1"),
		Role:          "business_research",
		FlowID:        "validation",
		FlowPath:      "validation/inst-1",
		EmitEvents:    []string{"research.completed"},
	}
	inbound := toolTestInboundEvent(
		events.EventType("validation/validation.started"),
		nil,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "validation/inst-1"),
		executionmode.Mock,
	)
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_research_completed", map[string]any{
		"summary": "research done",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got, want := bus.event.EntityID(), "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"; got != want {
		t.Fatalf("published event entity_id = %q, want %q", got, want)
	}
	if got, want := bus.event.FlowInstance(), "validation/inst-1"; got != want {
		t.Fatalf("published event flow_instance = %q, want %q", got, want)
	}
	if got := bus.event.ExecutionMode(); got != executionmode.Mock {
		t.Fatalf("published event execution mode = %q, want mock", got)
	}
	conflictCtx := runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	if _, err := exec.handleEmitTool(conflictCtx, actor, "emit_research_completed", map[string]any{"summary": "must not publish"}); err == nil || !strings.Contains(err.Error(), "conflicts with agent") {
		t.Fatalf("conflicting execution mode error = %v", err)
	}
	if bus.count != 1 {
		t.Fatalf("publish count after execution mode conflict = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_DoesNotAdoptForeignInboundFlowOwner(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"research.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"summary": {Type: "string"},
					},
					Required: []string{"summary"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "validation",
						Flow: "validation",
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"research.completed": {},
					},
					Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
					Path:   "validation",
				},
			},
		},
	}
	bundle.FlowTree.ByID["validation"].Events["research.completed"] = bundle.Events["research.completed"]
	source := toolTestSourceWithDeclaredAgent(t, bundle, "business-research-agent", "validation")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "business-research-agent",
		Identity:      toolTestAgentIdentity(t, "business-research-agent", "validation", "validation"),
		Role:          "business_research",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"research.completed"},
	}
	inbound := toolTestInboundEvent(
		events.EventType("scoring/vertical.shortlisted"),
		nil,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "scoring/inst-1"),
		executionmode.Live,
	)
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_research_completed", map[string]any{
		"summary": "research done",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got, want := bus.event.FlowInstance(), "validation"; got != want {
		t.Fatalf("published event flow_instance = %q, want %q", got, want)
	}
}

func TestHandleEmitTool_KeepsFlowOutputPinAtParentScope(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"vertical.discovered": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"name": {Type: "string"},
					},
					Required: []string{"name"},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": {
					Paths: runtimecontracts.FlowContractPaths{
						ID:   "discovery",
						Flow: "discovery",
					},
					Schema: runtimecontracts.FlowSchemaDocument{
						Mode: runtimecontracts.FlowModeStatic,
						Pins: runtimecontracts.FlowPins{
							Outputs: runtimecontracts.FlowOutputPins{
								EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "vertical.discovered"}},
							},
						},
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"vertical.discovered": {},
					},
					Path: "root/discovery",
				},
			},
		},
	}
	bundle.FlowTree.ByID["discovery"].Events["vertical.discovered"] = bundle.Events["vertical.discovered"]
	source := toolTestSourceWithDeclaredAgent(t, bundle, "discovery-coordinator", "discovery")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "discovery-coordinator",
		Identity:      toolTestAgentIdentity(t, "discovery-coordinator", "discovery", "root/discovery"),
		EntityID:      eventtest.UUID("discovery-coordinator-source"),
		Role:          "discovery_coordinator",
		FlowID:        "discovery",
		FlowPath:      "root/discovery",
		EmitEvents:    []string{"vertical.discovered"},
	}

	parentOwner := events.RouteIdentity{
		FlowID: "root", FlowInstance: "root/run-1", EntityID: "44444444-4444-4444-4444-444444444444",
	}
	ctx := runtimedelivery.WithRoute(toolEventTestContext(actor), events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient(actor.ID), AgentIdentity: actor.Identity,
		Target: events.MustExistingEntityTarget(parentOwner),
	})
	_, err := exec.handleEmitTool(ctx, actor, "emit_vertical_discovered", map[string]any{
		"name": "Law firm AP automation",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type()), "root/discovery/vertical.discovered"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
	}
	if got := bus.event.TargetRoute(); got != parentOwner {
		t.Fatalf("published target route = %#v, want parent owner %#v", got, parentOwner)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_TargetsParentRouteForChildPinOutput(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeTemplate,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "analysis.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "analyzer", "analyzer-flow")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root",
		EntityID:     "11111111-1111-1111-1111-111111111111",
	}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				ParentFlowID:       parentRoute.FlowID,
				ParentFlowInstance: parentRoute.FlowInstance,
				ParentEntityID:     parentRoute.EntityID,
			},
		}},
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Identity:      toolTestAgentIdentity(t, "analyzer", "analyzer-flow", "analyzer-flow/inst-1"),
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "analyzer-flow/inst-1",
		EntityID:      "22222222-2222-2222-2222-222222222222",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	childRoute := events.RouteIdentity{
		FlowID:       "analyzer-flow",
		FlowInstance: "analyzer-flow/inst-1",
		EntityID:     "22222222-2222-2222-2222-222222222222",
	}
	wrongInboundParent := events.RouteIdentity{
		FlowID:       "wrong-root",
		FlowInstance: "wrong-root",
		EntityID:     "33333333-3333-3333-3333-333333333333",
	}
	inbound := toolTestInboundEvent(
		events.EventType("analyzer-flow/analysis.requested"),
		nil,
		events.EnvelopeForTargetRoute(events.EnvelopeForSourceRoute(events.EventEnvelope{}, wrongInboundParent), childRoute),
		executionmode.Live,
	)
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got := bus.event.TargetRoute(); got != parentRoute {
		t.Fatalf("target route = %#v, want parent route %#v", got, parentRoute)
	}
	if got := bus.event.SourceRoute(); got.Empty() || got.FlowID != "analyzer-flow" || got.FlowInstance != "analyzer-flow/inst-1" {
		t.Fatalf("source route = %#v, want analyzer-flow/inst-1 source", got)
	}
}

func TestHandleEmitTool_FailsClosedOnIncompleteStoredParentRoute(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeTemplate,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "analysis.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "analyzer", "analyzer-flow")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				ParentFlowID:   "root",
				ParentEntityID: "11111111-1111-1111-1111-111111111111",
			},
		}},
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Identity:      toolTestAgentIdentity(t, "analyzer", "analyzer-flow", "analyzer-flow/inst-1"),
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "analyzer-flow/inst-1",
		EntityID:      "22222222-2222-2222-2222-222222222222",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), toolTestInboundEvent("analyzer-flow/analysis.requested", nil, events.EventEnvelope{}, executionmode.Live))

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want parent_route_incomplete")
	}
	if !strings.Contains(err.Error(), "parent_route_incomplete") {
		t.Fatalf("handleEmitTool error = %v, want parent_route_incomplete", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_StaticChildPinOutputTargetsDeliveryEntity(t *testing.T) {
	source := staticChildPinOutputTestSource(t)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Identity:      toolTestAgentIdentity(t, "analyzer", "analyzer-flow", "root/analyzer-flow"),
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "root/analyzer-flow",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	inboundEntityID := "11111111-1111-1111-1111-111111111111"
	sourceEntityID := "33333333-3333-3333-3333-333333333333"
	currentOwner := events.RouteIdentity{
		FlowID: "root", FlowInstance: "root/run-1", EntityID: "44444444-4444-4444-4444-444444444444",
	}
	inbound := toolTestInboundEvent(
		events.EventType("analyzer-flow/analysis.requested"),
		nil,
		events.EnvelopeForTargetRoute(events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       "wrong-root",
			FlowInstance: "wrong-root",
			EntityID:     sourceEntityID,
		}), events.RouteIdentity{FlowID: "inbound", FlowInstance: "inbound/one", EntityID: inboundEntityID}),
		executionmode.Live,
	)

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)
	ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(actor.ID),
		AgentIdentity: actor.Identity,
		Target:        events.MustExistingEntityTarget(currentOwner),
	})

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if got := bus.event.TargetRoute(); got != currentOwner {
		t.Fatalf("target route = %#v, want exact current delivery route %#v", got, currentOwner)
	}
	if got := bus.event.TargetRoute().EntityID; got == inboundEntityID || got == sourceEntityID {
		t.Fatalf("target entity = %q, must not use inbound=%q or source=%q", got, inboundEntityID, sourceEntityID)
	}
}

func TestHandleEmitTool_StaticChildPinOutputRejectsMissingOrEntitylessDeliveryOwner(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withRoute func(context.Context, models.AgentConfig) context.Context
	}{
		{name: "missing", withRoute: func(ctx context.Context, _ models.AgentConfig) context.Context { return ctx }},
		{name: "entityless", withRoute: func(ctx context.Context, actor models.AgentConfig) context.Context {
			return runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
				Recipient: events.MustAgentDeliveryRecipient(actor.ID), AgentIdentity: actor.Identity,
				Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root/run-1"}),
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := staticChildPinOutputTestSource(t)
			bus := &publishBusCapture{}
			actor := models.AgentConfig{
				ExecutionMode: "live", ID: "analyzer",
				Identity: toolTestAgentIdentity(t, "analyzer", "analyzer-flow", "root/analyzer-flow"),
				Role:     "analyzer", FlowID: "analyzer-flow", FlowPath: "root/analyzer-flow",
				EmitEvents: []string{"analyzer-flow/analysis.done"},
			}
			exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})
			inbound := toolTestInboundEvent(events.EventType("analyzer-flow/analysis.requested"), nil,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "inbound", FlowInstance: "inbound/one", EntityID: "inbound-owner"}), executionmode.Live)
			ctx := tc.withRoute(runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound), actor)
			_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
			if err == nil || !strings.Contains(err.Error(), "target_required_missing") {
				t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
			}
			if bus.count != 0 {
				t.Fatalf("publish count = %d, want zero", bus.count)
			}
		})
	}
}

func staticChildPinOutputTestSource(t testing.TB) semanticview.Source {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {Payload: runtimecontracts.EventPayloadSpec{Type: "object"}},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "analyzer-flow", Flow: "analyzer-flow"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeStatic,
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "analysis.done"}}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"analysis.done": {}},
		Path:   "root/analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{analyzerFlow}},
		ByID: map[string]*runtimecontracts.FlowContractView{"analyzer-flow": &analyzerFlow},
	}
	return toolTestSourceWithDeclaredAgent(t, bundle, "analyzer", "analyzer-flow")
}

func TestHandleEmitTool_RootStaticPinOutputStillRequiresTarget(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	analyzerFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analyzer-flow",
			Flow: "analyzer-flow",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeStatic,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "analysis.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "analyzer", "analyzer-flow")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Identity:      toolTestAgentIdentity(t, "analyzer", "analyzer-flow", "analyzer-flow"),
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "analyzer-flow",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	inbound := toolTestInboundEvent(
		events.EventType("analyzer-flow/analysis.requested"),
		nil,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
		executionmode.Live,
	)
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want target_required_missing")
	}
	if !strings.Contains(err.Error(), "target_required_missing") {
		t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_RootSchemaPinOutputStillRequiresTarget(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "root.ready"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"root.ready": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "root-agent", "")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "root-agent",
		Identity:      toolTestRootAgentIdentity(t, "root-agent"),
		EntityID:      eventtest.UUID("root-agent-source"),
		Role:          "root-agent",
		EmitEvents:    []string{"root.ready"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_ready", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want target_required_missing")
	}
	if !strings.Contains(err.Error(), "target_required_missing") {
		t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
	}
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_RoutesTypedSameFlowOutputToNodeConsumer(t *testing.T) {
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests/tier8-boot-verification/test-boot-event-cycle"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load cycle fixture: %v", err)
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "root-agent", "")
	store := newEmitRoutePlanStore()
	eventBus := newEmitRoutePlanEventBus(t, store, source)
	actor := models.AgentConfig{ExecutionMode: "live", ID: "root-agent", Identity: toolTestRootAgentIdentity(t, "root-agent"), Role: "root-agent", EntityID: eventtest.UUID("root-agent-cycle-source"), EmitEvents: []string{"cycle.ping"}}
	exec := NewExecutorWithOptions(eventBus, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})

	out, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_cycle_ping", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	eventID := emitToolResultString(t, out, "event_id")
	persisted := store.events[eventID]
	wantRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "test-node")),
		Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
			FlowID: source.WorkflowName(), FlowInstance: persisted.RunID(), EntityID: runtimeflowidentity.EntityID(persisted.RunID()),
		}),
	}
	if !emitDeliveryRoutesContain(store.routes[eventID], wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want typed same-flow node consumer", store.routes[eventID])
	}
}

func TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNode(t *testing.T) {
	bundle := emitRoutePlanTestBundle([]emitRoutePlanTestFlow{{
		id:   "review",
		mode: runtimecontracts.FlowModeTemplate,
		inputs: []runtimecontracts.FlowInputEventPin{{
			Event: "assessment.reported",
		}},
		outputs: []runtimecontracts.FlowOutputEventPin{{
			Event: "assessment.reported",
		}},
		nodes: map[string]runtimecontracts.SystemNodeContract{
			"review-finalize": {
				ID: "review-finalize", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"assessment.reported": {}},
			},
		},
	}}, nil)
	source := toolTestSourceWithDeclaredAgent(t, bundle, "reviewer", "review")
	store := newEmitRoutePlanStore()
	eventBus := newEmitRoutePlanEventBus(t, store, source)
	route := runtimeflowidentity.DeriveRoute("review", "instance-1")
	if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RunScopedFlowInstance{
		RunID: toolTestRunID,
		Route: route,
	}}); err != nil {
		t.Fatalf("PublishPersistedFlowInstanceRoute: %v", err)
	}
	entityID := runtimeflowidentity.EntityID(route.InstancePath)
	actor := models.AgentConfig{
		ExecutionMode: "live", ID: "reviewer", Identity: toolTestAgentIdentity(t, "reviewer", "review", route.InstancePath),
		Role: "reviewer", FlowID: "review", FlowPath: route.InstancePath, EntityID: entityID,
		EmitEvents: []string{"assessment.reported"},
	}
	exec := NewExecutorWithOptions(eventBus, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})
	inbound := toolTestInboundEvent(
		events.EventType(route.InstancePath+"/assessment.requested"), nil,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), route.InstancePath),
		executionmode.Live,
	)
	out, err := exec.handleEmitTool(runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound), actor, "emit_assessment_reported", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	eventID := emitToolResultString(t, out, "event_id")
	routes := store.routes[eventID]
	want := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "review-finalize")),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "review", FlowInstance: route.InstancePath}),
	}
	if !emitDeliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want same-instance template node %#v", routes, want)
	}
}

func TestHandleEmitTool_RoutesConnectedOutputPinThroughCanonicalRouteAuthority(t *testing.T) {
	source := emitRoutePlanStaticSource(t, runtimecontracts.FlowPackageConnect{
		Event:  "deploy.done",
		From:   "producer",
		To:     "consumer",
		Rename: "deploy.completed",
	})
	store := newEmitRoutePlanStore()
	eb := newEmitRoutePlanEventBus(t, store, source)
	emitRegistry := NewEmitRegistry(source, nil)
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "producer-agent",
		Identity:      toolTestAgentIdentity(t, "producer-agent", "producer", "producer"),
		Role:          "producer",
		FlowID:        "producer",
		FlowPath:      "producer",
		EntityID:      runtimeflowidentity.EntityID("producer-entity"),
		EmitEvents:    []string{"deploy.done"},
	}
	if tools := emitRegistry.GenerateEmitToolsForActor(actor, nil); !emitToolDefinitionsContain(tools, "emit_deploy_done") {
		t.Fatalf("generated emit tools = %#v, want emit_deploy_done", tools)
	}
	exec := NewExecutorWithOptions(eb, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RunCreatingRootIngress(
		eventtest.UUID("emit-connected-output-parent"),
		events.EventType("producer/deploy.requested"),
		"runtime",
		"",
		nil,
		0,
		eventtest.UUID("emit-connected-output-run"),
		"",
		events.EventEnvelope{},
		time.Now().UTC()))
	out, err := exec.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v (%s)", err, failures.Format(err))
	}
	eventID := emitToolResultString(t, out, "event_id")
	persisted := store.events[eventID]
	if got, want := string(persisted.Type()), "producer/deploy.done"; got != want {
		t.Fatalf("persisted event type = %q, want %q", got, want)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "consumer", "consumer-node")), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
	}),
	}
	wantEventTarget := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer"}
	if got := persisted.TargetRoute().Normalized(); got != wantEventTarget {
		t.Fatalf("persisted event target route = %#v, want connect address %#v", got, wantEventTarget)
	}
	if !emitDeliveryRoutesContain(store.routes[eventID], wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", store.routes[eventID], wantRoute)
	}
	if store.routes[eventID][0].ConnectClaim.Empty() {
		t.Fatal("persisted delivery route connect claim is empty")
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestHandleEmitTool_RootReceiverConnectRemainsTargetlessBeforePreflight(t *testing.T) {
	source := emitRoutePlanRootReceiverSource(t)
	store := newEmitRoutePlanStore()
	eb := newEmitRoutePlanEventBus(t, store, source)
	emitRegistry := NewEmitRegistry(source, nil)
	runID := eventtest.UUID("emit-root-receiver-run")
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: runID,
		EntityID:     runtimeflowidentity.EntityID("root-entity"),
	}
	store.targetOwners = []runtimebus.ActiveTargetDescriptor{{
		ID: parentRoute.FlowInstance, EntityID: parentRoute.EntityID, FlowInstance: parentRoute.FlowInstance,
	}}
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "producer-agent",
		Identity:      toolTestAgentIdentity(t, "producer-agent", "producer", "producer/inst-1"),
		Role:          "producer",
		FlowID:        "producer",
		FlowPath:      "producer/inst-1",
		EntityID:      runtimeflowidentity.EntityID("producer-entity"),
		EmitEvents:    []string{"producer/deploy.done"},
	}
	probe := &emitPreflightCaptureBus{EventBus: eb}
	exec := NewExecutorWithOptions(probe, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"producer/inst-1": {
				ParentFlowID: parentRoute.FlowID, ParentFlowInstance: parentRoute.FlowInstance, ParentEntityID: parentRoute.EntityID,
			},
		}},
	})
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RunCreatingRootIngress(
		eventtest.UUID("emit-root-receiver-parent"),
		events.EventType("producer/deploy.requested"),
		"runtime",
		"",
		nil,
		0,
		runID,
		"",
		events.EventEnvelope{},
		time.Now().UTC()))

	out, err := exec.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	eventID := emitToolResultString(t, out, "event_id")
	persisted := store.events[eventID]
	if got, want := string(persisted.Type()), "producer/inst-1/deploy.done"; got != want {
		t.Fatalf("persisted event type = %q, want concrete template event %q", got, want)
	}
	if !probe.preflight.TargetRoute().Empty() || len(probe.preflight.TargetRoutes()) != 0 {
		t.Fatalf("preflight producer target = %#v/%#v, want targetless lowered-connect event", probe.preflight.TargetRoute(), probe.preflight.TargetRoutes())
	}
	if got := persisted.TargetRoute(); got != parentRoute {
		t.Fatalf("persisted target route = %#v, want EventBus-selected root owner %#v", got, parentRoute)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "root-receiver")), Target: events.MustExistingEntityTarget(parentRoute)}
	if !emitDeliveryRoutesContain(store.routes[eventID], wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", store.routes[eventID], wantRoute)
	}
}

func TestHandleEmitTool_RootReceiverConnectRequiresSelectedOwnerNotParentMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows map[string]runtimepipeline.WorkflowInstance
	}{
		{name: "missing", rows: map[string]runtimepipeline.WorkflowInstance{}},
		{name: "incomplete", rows: map[string]runtimepipeline.WorkflowInstance{
			"producer/inst-1": {ParentFlowID: "root", ParentFlowInstance: "root/inst-1"},
		}},
		{name: "complete_but_unselected", rows: map[string]runtimepipeline.WorkflowInstance{
			"producer/inst-1": {
				ParentFlowID: "root", ParentFlowInstance: "root/inst-1", ParentEntityID: eventtest.UUID("unselected-root"),
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := emitRoutePlanRootReceiverSource(t)
			store := newEmitRoutePlanStore()
			eb := newEmitRoutePlanEventBus(t, store, source)
			actor := models.AgentConfig{
				ExecutionMode: "live",
				ID:            "producer-agent",
				Identity:      toolTestAgentIdentity(t, "producer-agent", "producer", "producer/inst-1"),
				Role:          "producer",
				FlowID:        "producer",
				FlowPath:      "producer/inst-1",
				EntityID:      runtimeflowidentity.EntityID("producer-entity"),
				EmitEvents:    []string{"producer/deploy.done"},
			}
			exec := NewExecutorWithOptions(eb, ExecutorOptions{
				WorkflowSource:    source,
				EmitRegistry:      NewEmitRegistry(source, nil),
				WorkflowInstances: emitWorkflowInstanceLoader{rows: tc.rows},
			})

			_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_deploy_done", map[string]any{})
			if err == nil || !strings.Contains(err.Error(), "route_plan_preflight_failed") {
				t.Fatalf("handleEmitTool error = %v, want exact selected-owner preflight failure", err)
			}
			if len(store.events) != 0 || len(store.routes) != 0 {
				t.Fatalf("persisted events/routes = %d/%d, want 0/0", len(store.events), len(store.routes))
			}
		})
	}
}

func TestHandleEmitTool_FailsClosedForConnectedOutputWithoutCanonicalRouteAuthority(t *testing.T) {
	source := emitRoutePlanSource(t, nil)
	store := newEmitRoutePlanStore()
	eb := newEmitRoutePlanEventBus(t, store, source)
	rawSubscription, err := eb.SubscribeInternal(context.Background(), "raw-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
	if err != nil {
		t.Fatalf("SubscribeInternal: %v", err)
	}
	rawSubscription.MarkReady()
	t.Cleanup(func() { _ = rawSubscription.Complete(false) })
	raw := rawSubscription.Deliveries()
	emitRegistry := NewEmitRegistry(source, nil)
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "producer-agent",
		Identity:      toolTestAgentIdentity(t, "producer-agent", "producer", "producer"),
		Role:          "producer",
		FlowID:        "producer",
		FlowPath:      "producer",
		EntityID:      runtimeflowidentity.EntityID("producer-entity"),
		EmitEvents:    []string{"deploy.done"},
	}
	exec := NewExecutorWithOptions(eb, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RunCreatingRootIngress(
		eventtest.UUID("emit-missing-route-parent"),
		events.EventType("producer/deploy.requested"),
		"runtime",
		"",
		nil,
		0,
		eventtest.UUID("emit-missing-route-run"),
		"",
		events.EventEnvelope{},
		time.Now().UTC()))
	_, err = exec.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
	if err == nil {
		t.Fatal("handleEmitTool error = nil, want target_required_missing")
	}
	if !strings.Contains(err.Error(), "target_required_missing") {
		t.Fatalf("handleEmitTool error = %v, want target_required_missing", err)
	}
	if len(store.events) != 0 {
		t.Fatalf("persisted events = %#v, want none", store.events)
	}
	if len(store.routes) != 0 {
		t.Fatalf("persisted routes = %#v, want none", store.routes)
	}
	select {
	case evt := <-raw:
		t.Fatalf("raw subscriber received %#v, want no lower-precedence rescue", evt)
	default:
	}
}

func TestHandleEmitTool_FailsClosedOnUndeclaredPayloadField(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"category": {Type: "string"},
					},
					Required: []string{"category"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		EmitEvents:    []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_category_assessed", map[string]any{
		"category":   "AP automation",
		"unexpected": true,
	})
	requireToolFailure(t, err, failures.ClassSchemaInvalid, "schema_validation_failed")
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_AllowsDeclaredTemplateIDBusinessPayload(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo.template.selected": {
				Payload: runtimecontracts.EventPayloadSpec{
					Type: "object",
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"template_id": {Type: "string"},
					},
					Required: []string{"template_id"},
				},
			},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "repo-agent", "")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "repo-agent",
		Identity:      toolTestRootAgentIdentity(t, "repo-agent"),
		EntityID:      eventtest.UUID("repo-agent-source"),
		Role:          "repo_agent",
		EmitEvents:    []string{"repo.template.selected"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_repo_template_selected", map[string]any{
		"template_id": "application-basic-v1",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.event.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("payload template_id = %#v, want business value", got)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_AllowsValidWave1EventPayloadTypes(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Scalars: map[string]runtimecontracts.ScalarTypeDecl{
				"TraceID": {Base: "uuid"},
				"Label":   {Base: "text"},
			},
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"Mode": {Values: []string{"fast", "deep"}, Default: "fast"},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ScanDetails": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"source": {Type: "text"},
						"count":  {Type: "integer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"mode":     {Type: "Mode"},
						"details":  {Type: "ScanDetails"},
						"labels":   {Type: "[Label]"},
						"trace_id": {Type: "TraceID"},
					},
					Required: []string{"mode", "details", "labels", "trace_id"},
				},
			},
		},
	}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "market-research-agent", "")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Identity:      toolTestRootAgentIdentity(t, "market-research-agent"),
		EntityID:      eventtest.UUID("market-research-source"),
		Role:          "market_research",
		EmitEvents:    []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_scan_completed", map[string]any{
		"mode": "fast",
		"details": map[string]any{
			"source": "scanner-a",
			"count":  2,
		},
		"labels":   []any{"a", "b"},
		"trace_id": "11111111-1111-1111-1111-111111111111",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	if bus.count != 1 {
		t.Fatalf("publish count = %d, want 1", bus.count)
	}
}

func TestHandleEmitTool_ResolvesDuplicateLeafScopedSchemasThroughActor(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:   "review",
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ReviewRequest"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	validationFlow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:   "validation",
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ValidationRequest"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Children: []runtimecontracts.FlowContractView{reviewFlow, validationFlow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"ReviewPriority":     {Values: []string{"urgent"}, Default: "urgent"},
				"ValidationPriority": {Values: []string{"low"}, Default: "low"},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ReviewRequest": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"priority": {Type: "ReviewPriority"},
					},
				},
				"ValidationRequest": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"priority": {Type: "ValidationPriority"},
					},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}
	toolTestDeclareAgent(t, bundle, "review-agent", "review")
	source := toolTestSourceWithDeclaredAgent(t, bundle, "validation-agent", "validation")
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	reviewActor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "review-agent",
		Identity:      toolTestAgentIdentity(t, "review-agent", "review", "review"),
		EntityID:      eventtest.UUID("review-agent-source"),
		Role:          "reviewer",
		FlowID:        "review",
		FlowPath:      "review",
		EmitEvents:    []string{"review/task.requested"},
	}
	_, err := exec.handleEmitTool(toolEventTestContext(reviewActor), reviewActor, "emit_task_requested", map[string]any{
		"details": map[string]any{
			"priority": "urgent",
		},
	})
	if err != nil {
		t.Fatalf("review handleEmitTool: %v", err)
	}
	if got, want := string(bus.event.Type()), "review/task.requested"; got != want {
		t.Fatalf("review published event type = %q, want %q", got, want)
	}

	validationActor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "validation-agent",
		Identity:      toolTestAgentIdentity(t, "validation-agent", "validation", "validation"),
		EntityID:      eventtest.UUID("validation-agent-source"),
		Role:          "validator",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"validation/task.requested"},
	}
	_, err = exec.handleEmitTool(toolEventTestContext(validationActor), validationActor, "emit_task_requested", map[string]any{
		"details": map[string]any{
			"priority": "low",
		},
	})
	if err != nil {
		t.Fatalf("validation handleEmitTool: %v", err)
	}
	if got, want := string(bus.event.Type()), "validation/task.requested"; got != want {
		t.Fatalf("validation published event type = %q, want %q", got, want)
	}
	if bus.count != 2 {
		t.Fatalf("publish count = %d, want 2", bus.count)
	}
}

func TestHandleEmitTool_FailsClosedOnSameActorDuplicateLeafScopedSchemas(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
					Required: []string{"priority"},
				},
			},
		},
	}
	validationFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
					Required: []string{"priority"},
				},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Children: []runtimecontracts.FlowContractView{reviewFlow, validationFlow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "dual-scope-agent",
		Role:          "reviewer",
		FlowID:        "review",
		FlowPath:      "review",
		EmitEvents:    []string{"review/task.requested", "validation/task.requested"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_task_requested", map[string]any{
		"priority": "urgent",
	})
	requireToolFailure(t, err, failures.ClassSchemaInvalid, "invalid_emit_tool_name")
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

func TestHandleEmitTool_FailsClosedOnNamedTypeViolation(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"ScanDetails": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"source": {Type: "text"},
						"count":  {Type: "integer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"details": {Type: "ScanDetails"},
					},
					Required: []string{"details"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		EmitEvents:    []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(toolEventTestContext(actor), actor, "emit_scan_completed", map[string]any{
		"details": "not-an-object",
	})
	requireToolFailure(t, err, failures.ClassSchemaInvalid, "schema_validation_failed")
	if bus.count != 0 {
		t.Fatalf("publish count = %d, want 0", bus.count)
	}
}

type emitRoutePlanTestFlow struct {
	id      string
	mode    string
	inputs  []runtimecontracts.FlowInputEventPin
	outputs []runtimecontracts.FlowOutputEventPin
	nodes   map[string]runtimecontracts.SystemNodeContract
}

func emitRoutePlanStaticSource(t testing.TB, connect runtimecontracts.FlowPackageConnect) semanticview.Source {
	t.Helper()
	return emitRoutePlanSource(t, []runtimecontracts.FlowPackageConnect{connect})
}

func emitRoutePlanRootReceiverSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	fixtureRoot := canonicalrouting.CopyTemplateOutputRootConnect(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load template-output root-connect fixture: %v", err)
	}
	return toolTestSourceWithDeclaredAgent(t, bundle, "producer-agent", "producer")
}

func emitRoutePlanSource(t testing.TB, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	t.Helper()
	bundle := emitRoutePlanTestBundle([]emitRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Event: "deploy.completed",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.completed": {}},
				},
			},
		},
	}, connects)
	return toolTestSourceWithDeclaredAgent(t, bundle, "producer-agent", "producer")
}

func emitRoutePlanTestBundle(flows []emitRoutePlanTestFlow, connects []runtimecontracts.FlowPackageConnect) *runtimecontracts.WorkflowContractBundle {
	connects = append([]runtimecontracts.FlowPackageConnect(nil), connects...)
	for i := range connects {
		connects[i].SourceFile = "package.yaml"
		connects[i].SourceLine = i + 1
	}
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	eventCatalog := map[string]runtimecontracts.EventCatalogEntry{}
	for _, flow := range flows {
		schema := runtimecontracts.FlowSchemaDocument{
			Mode: flow.mode,
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: flow.inputs},
				Outputs: runtimecontracts.FlowOutputPins{EventPins: flow.outputs},
			},
		}
		flowEvents := map[string]runtimecontracts.EventCatalogEntry{}
		for _, eventType := range append(emitRoutePlanInputEvents(flow.inputs), emitRoutePlanOutputEvents(flow.outputs)...) {
			eventCatalog[eventType] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Type: "object"}}
			flowEvents[eventType] = runtimecontracts.EventCatalogEntry{}
		}
		view := runtimecontracts.FlowContractView{
			Paths:  runtimecontracts.FlowContractPaths{ID: flow.id, Flow: flow.id, PackageKey: "."},
			Schema: schema,
			Events: flowEvents,
			Path:   flow.id,
			Nodes:  flow.nodes,
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		flowSchemas[flow.id] = schema
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{PackageKey: "."}, Children: children}
	for index := range root.Children {
		byID[root.Children[index].Paths.ID] = &root.Children[index]
	}
	return &runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Name: "root", Version: "1.0.0", Connect: connects},
		PackageTree: []runtimecontracts.LoadedProjectPackage{{
			Key: ".", Paths: runtimecontracts.ProjectPackagePaths{PackageFile: "package.yaml"},
			Manifest: runtimecontracts.ProjectPackageDocument{Connect: connects},
		}},
		Events: eventCatalog,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
		FlowSchemas: flowSchemas,
	}
}

func emitRoutePlanInputEvents(pins []runtimecontracts.FlowInputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func emitRoutePlanOutputEvents(pins []runtimecontracts.FlowOutputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func emitDeliveryRoutesContain(in []events.DeliveryRoute, want events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(in) {
		if events.SameDeliveryRecipientIdentity(route, want) &&
			(want.ConnectClaim.Empty() || route.ConnectClaim.Equal(want.ConnectClaim)) {
			return true
		}
	}
	return false
}

func emitToolDefinitionsContain(in []llm.ToolDefinition, name string) bool {
	for _, tool := range in {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func emitToolResultString(t testing.TB, out any, key string) string {
	t.Helper()
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("emit tool result = %#v, want map", out)
	}
	value, ok := result[key].(string)
	if !ok || value == "" {
		t.Fatalf("emit tool result[%q] = %#v, want non-empty string", key, result[key])
	}
	return value
}
