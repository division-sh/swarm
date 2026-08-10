package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestClassifyDeliveryTargetOwnershipClosedUnion(t *testing.T) {
	source := deliveryTargetOwnershipSource()
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-classification"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	entityID := eventtest.UUID("existing-delivery-target")
	tests := []struct {
		name       string
		nodeID     string
		candidates []DeliveryTargetOwnerCandidate
		assert     func(*testing.T, events.DeliveryTargetOwnership)
	}{
		{
			name: "existing entity", nodeID: "existing",
			candidates: []DeliveryTargetOwnerCandidate{{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: entityID}}},
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.ExistingEntity() || owner.Route().EntityID != entityID {
					t.Fatalf("owner = %#v, want existing entity %s", owner, entityID)
				}
			},
		},
		{
			name: "first delivery materializes exact future entity", nodeID: "materializer",
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.MaterializingEntity() || owner.Route().EntityID != FlowInstanceEntityID("review/one") {
					t.Fatalf("owner = %#v, want canonical materializing entity", owner)
				}
			},
		},
		{
			name: "entityless receiver", nodeID: "entityless",
			assert: func(t *testing.T, owner events.DeliveryTargetOwnership) {
				if !owner.EntitylessReceiver() || owner.Route().EntityID != "" {
					t.Fatalf("owner = %#v, want explicit entityless receiver", owner)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := AdmitDeliveryTargetHandler(source, "review", test.nodeID)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			owner, err := ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(test.nodeID),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   handler.ForEvent("work.ready"), Candidates: test.candidates,
			})
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			test.assert(t, owner)
		})
	}
}

func TestClassifyDeliveryTargetOwnershipFailsClosedOnMissingOrContradictoryEvidence(t *testing.T) {
	source := deliveryTargetOwnershipSource()
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("delivery-target-hostile"), events.EventType("review/one/work.ready"),
		"", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{},
	)
	canonicalID := FlowInstanceEntityID("review/one")
	tests := []struct {
		name       string
		nodeID     string
		candidates []DeliveryTargetOwnerCandidate
		want       string
	}{
		{name: "entity scoped handler without owner", nodeID: "entity-reader", want: "owner is missing"},
		{
			name: "existing and materializing contradiction", nodeID: "materializer",
			candidates: []DeliveryTargetOwnerCandidate{
				{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: canonicalID}},
				{Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: canonicalID}, Materializing: true},
			},
			want: "contradictory existing and materializing",
		},
		{
			name: "wrong future identity", nodeID: "materializer",
			candidates: []DeliveryTargetOwnerCandidate{{
				Route: events.RouteIdentity{FlowInstance: "review/one", EntityID: eventtest.UUID("wrong-future-target")}, Materializing: true,
			}},
			want: "disagrees with canonical handler identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := AdmitDeliveryTargetHandler(source, "review", test.nodeID)
			if err != nil {
				t.Fatalf("admit handler: %v", err)
			}
			_, err = ClassifyDeliveryTargetOwnership(DeliveryTargetOwnershipRequest{
				Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(test.nodeID),
				Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"},
				Handler:   handler.ForEvent("work.ready"), Candidates: test.candidates,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("classification error = %v, want %q", err, test.want)
			}
		})
	}
}

func deliveryTargetOwnershipSource() semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Path: "review", Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"existing":     deliveryTargetOwnershipNode("existing", runtimecontracts.SystemNodeEventHandler{}),
			"materializer": deliveryTargetOwnershipNode("materializer", runtimecontracts.SystemNodeEventHandler{CreateEntity: true}),
			"entityless":   deliveryTargetOwnershipNode("entityless", runtimecontracts.SystemNodeEventHandler{}),
			"entity-reader": deliveryTargetOwnershipNode("entity-reader", runtimecontracts.SystemNodeEventHandler{
				Condition: "entity.status == 'ready'",
			}),
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"review": &root.Children[0]},
	}}
	return semanticview.Wrap(bundle)
}

func deliveryTargetOwnershipNode(id string, handler runtimecontracts.SystemNodeEventHandler) runtimecontracts.SystemNodeContract {
	return runtimecontracts.SystemNodeContract{
		ID: id, SubscribesTo: []string{"work.ready"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.ready": handler},
	}
}
