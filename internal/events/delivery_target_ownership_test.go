package events

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestDeliveryTargetOwnershipRoundTripsClosedVariants(t *testing.T) {
	entityID := "11111111-1111-1111-1111-111111111111"
	tests := []struct {
		name  string
		owner DeliveryTargetOwnership
	}{
		{name: "existing entity", owner: MustExistingEntityTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: entityID})},
		{name: "materializing entity", owner: MustMaterializingEntityTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/two", EntityID: entityID})},
		{name: "entityless receiver", owner: MustEntitylessReceiverTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/three"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.owner)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var restored DeliveryTargetOwnership
			if err := json.Unmarshal(raw, &restored); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if restored != test.owner {
				t.Fatalf("restored ownership = %#v, want %#v", restored, test.owner)
			}
		})
	}
}

func TestDeliveryTargetOwnershipRejectsContradictoryVariants(t *testing.T) {
	entityID := "22222222-2222-2222-2222-222222222222"
	tests := []struct {
		name string
		make func() error
		want string
	}{
		{name: "existing without entity", make: func() error {
			_, err := NewExistingEntityTarget(RouteIdentity{FlowInstance: "review/one"})
			return err
		}, want: "requires exact entity identity"},
		{name: "materializing without entity", make: func() error {
			_, err := NewMaterializingEntityTarget(RouteIdentity{FlowInstance: "review/one"})
			return err
		}, want: "requires exact entity identity"},
		{name: "entityless with entity", make: func() error {
			_, err := NewEntitylessReceiverTarget(RouteIdentity{FlowInstance: "review/one", EntityID: entityID})
			return err
		}, want: "prohibits entity identity"},
		{name: "missing flow instance", make: func() error {
			_, err := NewEntitylessReceiverTarget(RouteIdentity{FlowID: "review"})
			return err
		}, want: "requires exact flow instance"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.make(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDeliveryTargetOwnershipDecodeFailsClosed(t *testing.T) {
	entityID := "33333333-3333-3333-3333-333333333333"
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown kind", raw: `{"kind":"stateless_receiver","route":{"flow_instance":"review/one"}}`},
		{name: "unknown field", raw: `{"kind":"entityless_receiver","route":{"flow_instance":"review/one"},"fallback":true}`},
		{name: "materializing without entity", raw: `{"kind":"materializing_entity","route":{"flow_instance":"review/one"}}`},
		{name: "entityless with entity", raw: `{"kind":"entityless_receiver","route":{"flow_instance":"review/one","entity_id":"` + entityID + `"}}`},
		{name: "trailing value", raw: `{"kind":"entityless_receiver","route":{"flow_instance":"review/one"}} {}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var owner DeliveryTargetOwnership
			if err := json.Unmarshal([]byte(test.raw), &owner); err == nil {
				t.Fatalf("decoded invalid ownership %#v", owner)
			}
		})
	}
}

func TestNodeDeliveryRouteRequiresTypedTargetOwnership(t *testing.T) {
	route := DeliveryRoute{Recipient: MustNodeDeliveryRecipient("receiver")}
	if _, err := route.Identity(); err == nil || !strings.Contains(err.Error(), "requires typed target ownership") {
		t.Fatalf("route identity error = %v, want typed target ownership failure", err)
	}
}

func TestTargetedAgentDeliveryAcceptsEntitylessReceiverOwnership(t *testing.T) {
	name, err := agentidentity.DeclaredName("reviewer", "test://delivery-target/reviewer")
	if err != nil {
		t.Fatalf("declare agent name: %v", err)
	}
	route, err := agentidentity.PresentRoute("review", "one", "review/one")
	if err != nil {
		t.Fatalf("declare agent route: %v", err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("declare agent identity: %v", err)
	}
	delivery := DeliveryRoute{
		Recipient: MustAgentDeliveryRecipient("reviewer"), AgentIdentity: identity,
		Target: MustEntitylessReceiverTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/one"}),
	}
	if _, err := delivery.Identity(); err != nil {
		t.Fatalf("targeted entityless agent delivery identity: %v", err)
	}
}
