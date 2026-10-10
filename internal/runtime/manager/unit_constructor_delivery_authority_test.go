package manager

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type explicitUnitDeliveryOwnerForTest struct{ deliverylifecycle.Store }

func TestUnitManagerConstructionDoesNotInventDeliveryPersistence(t *testing.T) {
	for _, options := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "options"}[options], func(t *testing.T) {
			var manager *AgentManager
			if options {
				manager = newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{})
			} else {
				manager = newTestAgentManager(t, nil, nil)
			}
			if manager.deliveryStore != nil || manager.roles.DeliveryRuntime != nil {
				t.Fatal("unit construction invented persistence or executable-delivery authority")
			}
		})
	}
	t.Run("projection", func(t *testing.T) {
		bus := newProjectionTestBus()
		manager := newProjectionTestManager(t, bus, (&projectionTestFactory{}).Build)
		if manager.deliveryStore != nil || bus.continuations != nil || bus.authority.Validate() == nil {
			t.Fatal("unit projection construction invented persistence or executable-delivery authority")
		}
		if provider := manager.roles.DeliveryRuntime; provider != nil {
			if _, err := provider.DeliveryAuthority(); err == nil {
				t.Fatal("unit projection provider returned live authority without an explicit native owner")
			}
		}
	})
}

func TestUnitManagerConstructionRetainsExplicitDeliveryOwner(t *testing.T) {
	owner := &explicitUnitDeliveryOwnerForTest{}
	manager := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{DeliveryStore: owner})
	if manager.deliveryStore != owner {
		t.Fatal("unit construction replaced explicit delivery ownership")
	}
	// This is an ownership/configuration unit, not a native durability witness.
	// The embedded nil role must never be used to claim or settle real work.
}
