package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"time"
)

type publishBusCapture struct {
	event events.Event
	count int
}

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

type emitWorkflowInstanceLoader struct {
	rows map[string]runtimepipeline.WorkflowInstance
	err  error
}

func (l emitWorkflowInstanceLoader) Enabled() bool { return true }

func (l emitWorkflowInstanceLoader) Load(_ context.Context, ref string) (runtimepipeline.WorkflowInstance, bool, error) {
	if l.err != nil {
		return runtimepipeline.WorkflowInstance{}, false, l.err
	}
	instance, ok := l.rows[strings.TrimSpace(ref)]
	return instance, ok, nil
}

type emitRoutePlanStore struct {
	events      map[string]events.Event
	routes      map[string][]events.DeliveryRoute
	scopes      map[string]runtimereplayclaim.CommittedReplayScope
	receipts    map[string]string
	receiptErrs map[string]*failures.Envelope
}

func newEmitRoutePlanStore() *emitRoutePlanStore {
	return &emitRoutePlanStore{
		events:      map[string]events.Event{},
		routes:      map[string][]events.DeliveryRoute{},
		scopes:      map[string]runtimereplayclaim.CommittedReplayScope{},
		receipts:    map[string]string{},
		receiptErrs: map[string]*failures.Envelope{},
	}
}

func (s *emitRoutePlanStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events[evt.ID()] = evt
	return nil
}

func (s *emitRoutePlanStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *emitRoutePlanStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	var out []string
	for _, route := range s.routes[eventID] {
		if route.SubscriberType == "agent" {
			out = append(out, route.SubscriberID)
		}
	}
	return out, nil
}

func (s *emitRoutePlanStore) SupportsPersistedReplay() bool { return true }

func (s *emitRoutePlanStore) PersistEventWithDeliveryRouteSetAndScope(_ context.Context, evt events.Event, routes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error {
	s.events[evt.ID()] = evt
	s.routes[evt.ID()] = events.NormalizeDeliveryRoutes(routes)
	s.scopes[evt.ID()] = scope
	return nil
}

func (s *emitRoutePlanStore) UpsertPipelineReceipt(_ context.Context, eventID, status string, failure *failures.Envelope) error {
	s.receipts[eventID] = status
	s.receiptErrs[eventID] = failures.CloneEnvelope(failure)
	return nil
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		FlowID:        "discovery",
		FlowPath:      "discovery",
		EmitEvents:    []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_category_assessed", map[string]any{
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
			exec, bus, actor := criteriaCitationEmitTestExecutor()
			_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_cto_spec_vetoed", tc.payload)
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

func criteriaCitationEmitTestExecutor() (*Executor, *publishBusCapture, models.AgentConfig) {
	return criteriaCitationEmitTestExecutorWithAgent(runtimecontracts.AgentRegistryEntry{
		ID:         "cto-agent",
		Role:       "cto",
		EmitEvents: []string{"cto.spec_vetoed"},
		Criteria:   []string{"feasibility_exclusions"},
	})
}

func criteriaCitationEmitTestExecutorWithAgent(agent runtimecontracts.AgentRegistryEntry) (*Executor, *publishBusCapture, models.AgentConfig) {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
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
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": &root.Children[0],
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)
	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "cto-agent",
		Role:          "cto",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"cto.spec_vetoed"},
		Criteria:      []string{"feasibility_exclusions"},
	}
	return exec, bus, actor
}

func TestHandleEmitTool_RejectsMutableActorCriteriaGrant(t *testing.T) {
	exec, bus, actor := criteriaCitationEmitTestExecutorWithAgent(runtimecontracts.AgentRegistryEntry{
		ID:         "cto-agent",
		Role:       "cto",
		EmitEvents: []string{"cto.spec_vetoed"},
	})
	actor.Criteria = []string{"feasibility_exclusions"}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_cto_spec_vetoed", map[string]any{
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
					Path: "validation",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "mock",
		ID:            "business-research-agent",
		Role:          "business_research",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"research.completed"},
	}
	inbound := eventtest.InExecutionMode(eventtest.RootIngress(
		"",
		events.EventType("validation/validation.started"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "validation/inst-1"),
		time.Time{},
	), executionmode.Mock)
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
					Path: "validation",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "business-research-agent",
		Role:          "business_research",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"research.completed"},
	}
	inbound := eventtest.RootIngress(
		"",
		events.EventType("scoring/vertical.shortlisted"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "scoring/inst-1"),
		time.Time{},
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
						Pins: runtimecontracts.FlowPins{
							Outputs: runtimecontracts.FlowOutputPins{
								Events: []string{"vertical.discovered"},
							},
						},
					},
					Events: map[string]runtimecontracts.EventCatalogEntry{
						"vertical.discovered": {},
					},
					Path: "discovery",
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "discovery-coordinator",
		Role:          "discovery_coordinator",
		FlowID:        "discovery",
		FlowPath:      "discovery",
		EmitEvents:    []string{"vertical.discovered"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_vertical_discovered", map[string]any{
		"name": "Law firm AP automation",
	})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}

	if got, want := string(bus.event.Type()), "discovery/vertical.discovered"; got != want {
		t.Fatalf("published event type = %q, want %q", got, want)
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
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root",
		EntityID:     "11111111-1111-1111-1111-111111111111",
	}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				Metadata: map[string]any{
					"parent_flow_id":       parentRoute.FlowID,
					"parent_flow_instance": parentRoute.FlowInstance,
					"parent_entity_id":     parentRoute.EntityID,
				},
			},
		}},
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
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
	inbound := eventtest.RootIngress(
		"",
		events.EventType("analyzer-flow/analysis.requested"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EnvelopeForSourceRoute(events.EventEnvelope{}, wrongInboundParent), childRoute),
		time.Time{},
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
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{
		WorkflowSource: source,
		EmitRegistry:   emitRegistry,
		WorkflowInstances: emitWorkflowInstanceLoader{rows: map[string]runtimepipeline.WorkflowInstance{
			"analyzer-flow/inst-1": {
				Metadata: map[string]any{
					"parent_flow_id":   "root",
					"parent_entity_id": "11111111-1111-1111-1111-111111111111",
				},
			},
		}},
	})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "analyzer-flow/inst-1",
		EntityID:      "22222222-2222-2222-2222-222222222222",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RootIngress("", events.EventType("analyzer-flow/analysis.requested"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

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
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"analysis.done": {},
		},
		Path: "root/analyzer-flow",
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &runtimecontracts.FlowContractView{
			Children: []runtimecontracts.FlowContractView{analyzerFlow},
		},
		ByID: map[string]*runtimecontracts.FlowContractView{
			"analyzer-flow": &analyzerFlow,
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "root/analyzer-flow",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	inbound := eventtest.RootIngress(
		"",
		events.EventType("analyzer-flow/analysis.requested"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForSourceRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"), events.RouteIdentity{
			FlowID:       "wrong-root",
			FlowInstance: "wrong-root",
			EntityID:     "33333333-3333-3333-3333-333333333333",
		}),
		time.Time{},
	)

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), inbound)

	_, err := exec.handleEmitTool(ctx, actor, "emit_analysis_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	want := events.RouteIdentity{EntityID: "11111111-1111-1111-1111-111111111111"}
	if got := bus.event.TargetRoute(); got != want {
		t.Fatalf("target route = %#v, want delivery entity route %#v", got, want)
	}
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
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"analysis.done"},
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		Role:          "analyzer",
		FlowID:        "analyzer-flow",
		FlowPath:      "analyzer-flow",
		EmitEvents:    []string{"analyzer-flow/analysis.done"},
	}
	inbound := eventtest.RootIngress(
		"",
		events.EventType("analyzer-flow/analysis.requested"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
		time.Time{},
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
					Events: []string{"root.ready"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"root.ready": {
				Payload: runtimecontracts.EventPayloadSpec{Type: "object"},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "root-agent",
		Role:          "root-agent",
		EmitEvents:    []string{"root.ready"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_root_ready", map[string]any{})
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

func TestHandleEmitTool_RoutesConnectedOutputPinThroughCanonicalRouteAuthority(t *testing.T) {
	source := emitRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := newEmitRoutePlanStore()
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	emitRegistry := NewEmitRegistry(source, nil)
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "producer-agent",
		Role:          "producer",
		FlowID:        "producer",
		FlowPath:      "producer",
		EntityID:      "producer-entity",
		EmitEvents:    []string{"deploy.done"},
	}
	if tools := emitRegistry.GenerateEmitToolsForActor(actor, nil); !emitToolDefinitionsContain(tools, "emit_deploy_done") {
		t.Fatalf("generated emit tools = %#v, want emit_deploy_done", tools)
	}
	exec := NewExecutorWithOptions(eb, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RootIngress(
		"evt-parent",
		events.EventType("producer/deploy.requested"),
		"runtime",
		"",
		nil,
		0,
		"run-1474",
		"",
		events.EventEnvelope{},
		time.Now().UTC()))
	out, err := exec.handleEmitTool(ctx, actor, "emit_deploy_done", map[string]any{})
	if err != nil {
		t.Fatalf("handleEmitTool: %v", err)
	}
	eventID := emitToolResultString(t, out, "event_id")
	persisted := store.events[eventID]
	if got, want := string(persisted.Type()), "producer/deploy.done"; got != want {
		t.Fatalf("persisted event type = %q, want %q", got, want)
	}
	if persisted.HasTargetRoute() {
		t.Fatalf("persisted event target route = %#v, want no producer-authored target", persisted.TargetRoute())
	}
	wantRoute := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowID:       "consumer",
			FlowInstance: "consumer",
			EntityID:     runtimeflowidentity.EntityID("consumer"),
		},
	}
	if !emitDeliveryRoutesContain(store.routes[eventID], wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", store.routes[eventID], wantRoute)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
	if got := store.scopes[eventID]; got != runtimereplayclaim.CommittedReplayScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestHandleEmitTool_FailsClosedForConnectedOutputWithoutCanonicalRouteAuthority(t *testing.T) {
	source := emitRoutePlanSource(nil)
	store := newEmitRoutePlanStore()
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	raw := eb.SubscribeInternal("raw-listener", events.EventType("producer/deploy.done"), events.EventType("deploy.done"))
	emitRegistry := NewEmitRegistry(source, nil)
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "producer-agent",
		Role:          "producer",
		FlowID:        "producer",
		FlowPath:      "producer",
		EntityID:      "producer-entity",
		EmitEvents:    []string{"deploy.done"},
	}
	exec := NewExecutorWithOptions(eb, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	ctx := runtimebus.WithInboundEvent(unmanagedToolTestContext(), eventtest.RootIngress(
		"evt-parent",
		events.EventType("producer/deploy.requested"),
		"runtime",
		"",
		nil,
		0,
		"run-1474",
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
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		EmitEvents:    []string{"category.assessed"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_category_assessed", map[string]any{
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "repo-agent",
		Role:          "repo_agent",
		EmitEvents:    []string{"repo.template.selected"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_repo_template_selected", map[string]any{
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
				"Mode": {Values: []string{"fast", "deep"}},
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		EmitEvents:    []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_scan_completed", map[string]any{
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
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
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
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
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
				"ReviewPriority":     {Values: []string{"urgent"}},
				"ValidationPriority": {Values: []string{"low"}},
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
	source := semanticview.Wrap(bundle)
	emitRegistry := NewEmitRegistry(source, nil)

	bus := &publishBusCapture{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})

	reviewActor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "review-agent",
		Role:          "reviewer",
		FlowID:        "review",
		FlowPath:      "review",
		EmitEvents:    []string{"review/task.requested"},
	}
	_, err := exec.handleEmitTool(unmanagedToolTestContext(), reviewActor, "emit_task_requested", map[string]any{
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
		Role:          "validator",
		FlowID:        "validation",
		FlowPath:      "validation",
		EmitEvents:    []string{"validation/task.requested"},
	}
	_, err = exec.handleEmitTool(unmanagedToolTestContext(), validationActor, "emit_task_requested", map[string]any{
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
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "dual-scope-agent",
		Role:          "reviewer",
		FlowID:        "review",
		FlowPath:      "review",
		EmitEvents:    []string{"review/task.requested", "validation/task.requested"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_task_requested", map[string]any{
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
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source, EmitRegistry: emitRegistry})
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "market_research",
		EmitEvents:    []string{"scan.completed"},
	}

	_, err := exec.handleEmitTool(unmanagedToolTestContext(), actor, "emit_scan_completed", map[string]any{
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

func emitRoutePlanStaticSource(connect runtimecontracts.FlowPackageConnect) semanticview.Source {
	return emitRoutePlanSource([]runtimecontracts.FlowPackageConnect{connect})
}

func emitRoutePlanSource(connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return semanticview.Wrap(emitRoutePlanTestBundle([]emitRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.completed": {}},
				},
			},
		},
	}, connects))
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
	flowInputs := make(map[string][]string, len(flows))
	flowOutputs := make(map[string][]string, len(flows))
	flowInputPins := make(map[string][]runtimecontracts.FlowInputEventPin, len(flows))
	flowOutputPins := make(map[string][]runtimecontracts.FlowOutputEventPin, len(flows))
	nodeHandlers := map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	eventCatalog := map[string]runtimecontracts.EventCatalogEntry{}
	for _, flow := range flows {
		schema := runtimecontracts.FlowSchemaDocument{
			Mode: flow.mode,
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events:    emitRoutePlanInputEvents(flow.inputs),
					EventPins: flow.inputs,
				},
				Outputs: runtimecontracts.FlowOutputPins{
					Events:    emitRoutePlanOutputEvents(flow.outputs),
					EventPins: flow.outputs,
				},
			},
		}
		flowEvents := map[string]runtimecontracts.EventCatalogEntry{}
		for _, eventType := range append(schema.Pins.Inputs.Events, schema.Pins.Outputs.Events...) {
			eventCatalog[eventType] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Type: "object"}}
			flowEvents[eventType] = runtimecontracts.EventCatalogEntry{}
		}
		view := runtimecontracts.FlowContractView{
			Paths:  runtimecontracts.FlowContractPaths{ID: flow.id, Flow: flow.id},
			Schema: schema,
			Events: flowEvents,
			Path:   flow.id,
			Nodes:  flow.nodes,
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		flowSchemas[flow.id] = schema
		flowInputs[flow.id] = append([]string(nil), schema.Pins.Inputs.Events...)
		flowOutputs[flow.id] = append([]string(nil), schema.Pins.Outputs.Events...)
		flowInputPins[flow.id] = append([]runtimecontracts.FlowInputEventPin(nil), flow.inputs...)
		flowOutputPins[flow.id] = append([]runtimecontracts.FlowOutputEventPin(nil), flow.outputs...)
		for _, node := range flow.nodes {
			if len(node.EventHandlers) > 0 {
				nodeHandlers[node.ID] = node.EventHandlers
			}
		}
	}
	root := runtimecontracts.FlowContractView{Children: children}
	return &runtimecontracts.WorkflowContractBundle{
		Events: eventCatalog,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
		FlowSchemas: flowSchemas,
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs:          flowInputs,
			FlowOutputs:         flowOutputs,
			FlowInputEventPins:  flowInputPins,
			FlowOutputEventPins: flowOutputPins,
			CompositionConnects: connects,
			NodeHandlers:        nodeHandlers,
		},
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
		if route == want {
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
