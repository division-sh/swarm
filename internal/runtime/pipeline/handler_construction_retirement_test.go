package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func TestHandlerEffectsCannotAdmitMissingConstruction(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("missing-construction"), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{})
	for _, name := range []string{"existing", "entity-reader", "transitioner", "entityless"} {
		t.Run(name, func(t *testing.T) {
			node := pipelineNode(t, "review", name)
			_, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   MustDeliveryTargetHandler(node).ForEvent("work.ready"),
			})
			if err == nil || !strings.Contains(err.Error(), "owner is missing") {
				t.Fatalf("missing construction admitted by %s: %v", name, err)
			}
		})
	}
}

func TestHandlerBridgeCannotConstructThroughEffectsOrPreview(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	for _, test := range []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{"payload_only", runtimecontracts.SystemNodeEventHandler{}},
		{"stage", runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}},
		{"gate", runtimecontracts.SystemNodeEventHandler{SetsGate: &runtimecontracts.GateSpec{Name: "approved"}}},
		{"field_write", runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetField: "marker", Value: runtimecontracts.LiteralExpression("new")}}}}},
		{"retired_creation", runtimecontracts.SystemNodeEventHandler{CreateEntity: true}},
	} {
		for _, preview := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/preview=%t", test.name, preview), func(t *testing.T) {
				bus := &recordingPipelineBus{}
				pc := newPreviewPipelineCoordinatorForTest(bus, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}})
				evt := eventtest.RunCreatingRootIngress(eventtest.UUID("missing-bridge-target"), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Time{})
				_, err := pc.executeNodeContractHandler(context.Background(), pipelineNode(t, "review", "existing"), test.handler, workflowTriggerContext{Event: evt}, preview)
				if !errors.Is(err, runtimeengine.ErrUnconstructedWorkflowTarget) {
					t.Fatalf("handler constructed a missing target: %v", err)
				}
				if bus.publishedCount() != 0 || bus.outboxCount() != 0 {
					t.Fatal("refused construction emitted work")
				}
			})
		}
	}
}

func TestPreviewRequiresExactConstructedSnapshot(t *testing.T) {
	route := flowidentity.RouteForInstancePath("review/one")
	owner, err := flowidentity.NewRunScopedFlowInstance(testPipelineRunID, route)
	if err != nil {
		t.Fatal(err)
	}
	entityID := identity.NormalizeEntityID(FlowInstanceEntityID(route.InstancePath))
	address := runtimeengine.StateAddress{FlowID: identity.NormalizeFlowID("review"), FlowInstance: owner, EntityID: entityID}
	for _, test := range []struct {
		name string
		edit func(*runtimeengine.StateSnapshot)
	}{
		{"constructed", func(*runtimeengine.StateSnapshot) {}},
		{"fieldless constructed", func(s *runtimeengine.StateSnapshot) { s.Control.EntityType = ""; s.Fields = nil }},
		{"missing entity", func(s *runtimeengine.StateSnapshot) { s.EntityID = "" }},
		{"foreign entity", func(s *runtimeengine.StateSnapshot) {
			s.EntityID = identity.NormalizeEntityID(eventtest.UUID("foreign-preview"))
		}},
		{"missing workflow", func(s *runtimeengine.StateSnapshot) { s.WorkflowName = "" }},
		{"foreign workflow", func(s *runtimeengine.StateSnapshot) { s.WorkflowName = "sibling" }},
		{"missing version", func(s *runtimeengine.StateSnapshot) { s.WorkflowVersion = "" }},
		{"missing stage", func(s *runtimeengine.StateSnapshot) { s.CurrentState = "" }},
		{"missing entry", func(s *runtimeengine.StateSnapshot) { s.EnteredStateAt = time.Time{} }},
		{"missing path", func(s *runtimeengine.StateSnapshot) { s.Control.FlowPath = "" }},
		{"missing storage", func(s *runtimeengine.StateSnapshot) { s.Control.StorageRef = "" }},
		{"missing instance", func(s *runtimeengine.StateSnapshot) { s.Control.InstanceID = "" }},
		{"foreign path", func(s *runtimeengine.StateSnapshot) { s.Control.FlowPath = "review/two" }},
		{"fieldless with fields", func(s *runtimeengine.StateSnapshot) { s.Control.EntityType = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := runtimeengine.StateSnapshot{
				EntityID: entityID, WorkflowName: "review", WorkflowVersion: "v1", CurrentState: "pending", EnteredStateAt: time.Now().UTC(),
				StateCarrier: runtimeengine.NewStateCarrierWithOwners(map[string]any{"marker": "original"}, nil,
					runtimeStateControlForDeliveryTarget(route, "review_item"), nil, nil),
			}
			test.edit(&snapshot)
			projected, found, err := loadPreviewEngineState(snapshot, address)
			valid := strings.HasSuffix(test.name, "constructed")
			if valid {
				if err != nil || !found || projected.EntityID != entityID || projected.Control != snapshot.Control {
					t.Fatalf("exact snapshot lost: snapshot=%+v found=%t err=%v", projected, found, err)
				}
				return
			}
			if err == nil || found || !projected.EntityID.IsZero() {
				t.Fatalf("incomplete snapshot acquired construction: snapshot=%+v found=%t err=%v", projected, found, err)
			}
		})
	}
}
