package bus_test

import (
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
)

func testActiveAgentDescriptor(t testing.TB, agentID, entityID, flowInstance string) runtimebus.ActiveAgentDescriptor {
	t.Helper()
	return runtimebus.ActiveAgentDescriptor{
		Identity: runtimebustest.Identity(t, agentID, flowInstance),
		EntityID: entityID,
	}
}
