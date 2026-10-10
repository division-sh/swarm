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
