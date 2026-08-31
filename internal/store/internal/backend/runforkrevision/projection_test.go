package runforkrevision

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestEventDeliveryProjectionRetainsConcreteAgentRunIdentity(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyEventDeliveries)
	if !ok || spec.build == nil {
		t.Fatal("event-delivery revision projection is unavailable")
	}
	const runID = "11111111-1111-4111-8111-111111111111"
	values := map[string]any{
		"run_id": runID, "subscriber_type": "agent", "subscriber_id": "worker-agent",
		"agent_name_owner": "fixture://worker-agent", "agent_name_source": "declared",
		"agent_route_presence": "present", "agent_flow_scope_key": "worker-flow",
		"agent_flow_instance_id": "worker-001", "agent_flow_instance_path": "worker-flow/worker-001",
	}
	projected := spec.build(values)
	raw, ok := projected["agent_identity"].(map[string]any)
	if !ok {
		t.Fatalf("agent identity projection = %#v", projected["agent_identity"])
	}
	if got := normalizedText(raw["run_id"]); got != runID {
		t.Fatalf("agent identity run_id = %q, want %q", got, runID)
	}
	name := raw["name"].(map[string]any)
	route := raw["route"].(map[string]any)
	identity, err := agentidentity.New(
		normalizedText(raw["run_id"]),
		agentidentity.Name{
			AgentID: normalizedText(name["agent_id"]), Owner: normalizedText(name["owner"]),
			Source: agentidentity.NameSource(normalizedText(name["source"])),
		},
		agentidentity.Route{
			Presence: agentidentity.RoutePresence(normalizedText(route["presence"])),
			ScopeKey: normalizedText(route["scope_key"]), InstanceID: normalizedText(route["instance_id"]),
			InstancePath: normalizedText(route["instance_path"]),
		},
	)
	if err != nil {
		t.Fatalf("projected concrete agent identity: %v", err)
	}
	if identity.RunID != runID {
		t.Fatalf("projected identity = %#v", identity)
	}
}
