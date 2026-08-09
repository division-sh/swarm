package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func testAgentMemoryIdentity(t testing.TB, runID, agentID, flowInstance string) agentmemory.Identity {
	t.Helper()
	return mustTestAgentMemoryIdentity(runID, agentID, flowInstance)
}

func mustTestAgentMemoryIdentity(runID, agentID, flowInstance string) agentmemory.Identity {
	return agentmemory.Identity{
		RunID: runID,
		Agent: mustTestAgentIdentity(agentID, flowInstance),
	}
}

func testAgentIdentity(t testing.TB, agentID, flowInstance string) agentidentity.Identity {
	t.Helper()
	return mustTestAgentIdentity(agentID, flowInstance)
}

func testAgentIdentityStorageFields(t testing.TB, identity agentidentity.Identity) agentidentity.StorageFields {
	t.Helper()
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("test agent identity storage fields: %v", err)
	}
	return fields
}

func mustTestAgentIdentity(agentID, flowInstance string) agentidentity.Identity {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	name, err := agentidentity.RuntimeName(agentID, "store-test-fixture")
	if err != nil {
		panic(err)
	}
	route := agentidentity.RootRoute()
	if flowInstance != "" {
		scopeKey, instanceID, found := strings.Cut(flowInstance, "/")
		if !found {
			scopeKey = flowInstance
			instanceID = flowInstance
		}
		route, err = agentidentity.PresentRoute(scopeKey, instanceID, flowInstance)
		if err != nil {
			panic(err)
		}
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func testAgentDeliveryRoute(t testing.TB, agentID, flowInstance string) events.DeliveryRoute {
	t.Helper()
	identity := testAgentIdentity(t, agentID, flowInstance)
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
}

func seedTestAgentRow(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	postgres bool,
	identity agentidentity.Identity,
	status string,
) {
	t.Helper()
	fields := testAgentIdentityStorageFields(t, identity)
	status = strings.TrimSpace(status)
	if status == "" {
		status = "active"
	}
	now := time.Now().UTC()
	query := `
		INSERT INTO agents (
			agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance,
			role, model, llm_backend, memory_enabled, memory_source,
			runtime_descriptor, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'worker', 'regular', 'claude_cli', 1, 'authored', '{"type":"stub","execution_mode":"live"}', ?, ?)
	`
	args := []any{
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		status, now,
	}
	if postgres {
		query = `
			INSERT INTO agents (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				role, model, llm_backend, memory_enabled, memory_source,
				runtime_descriptor, status, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'worker', 'regular', 'claude_cli', TRUE, 'authored', '{"type":"stub","execution_mode":"live"}'::jsonb, $8, $9)
		`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed test agent row: %v", err)
	}
}
