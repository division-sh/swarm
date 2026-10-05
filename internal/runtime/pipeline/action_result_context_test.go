package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPreviewProducerConsumesExecutionCoordinate(t *testing.T) {
	source := semanticview.Wrap(compiledAdapterSource(t))
	node := pipelineSourceNode(t, source, ".", "router")
	route := events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: testPipelineRunID}
	for _, tc := range []struct {
		name  string
		route events.RouteIdentity
		valid bool
	}{
		{name: "exact_preview", route: route, valid: true},
		{name: "missing_instance", route: events.RouteIdentity{FlowID: ".", EntityID: testPipelineRunID}},
		{name: "foreign_flow", route: events.RouteIdentity{FlowID: "child", FlowInstance: testPipelineRunID, EntityID: testPipelineRunID}},
		{name: "foreign_entity", route: events.RouteIdentity{FlowID: ".", FlowInstance: testPipelineRunID, EntityID: "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := workflowNodeProducerSource(context.Background(), source, node, ".", testPipelineRunID, tc.route)
			if tc.valid {
				if err != nil || got.Kind() != events.RoutingSourceStaticFlow || got.Route() != route {
					t.Fatalf("preview lost its exact execution owner: route=%+v err=%v", got.Route(), err)
				}
			} else if err == nil {
				t.Fatalf("disagreeing preview producer accepted: %+v", got)
			}
		})
	}
	ctx := withWorkflowNodeDeliveryRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(route),
	})
	if _, err := workflowNodeProducerSource(ctx, source, node, ".", testPipelineRunID, route); err == nil {
		t.Fatal("stamped durable delivery bypassed its target application")
	}
}
