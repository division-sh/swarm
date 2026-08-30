package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

const testAgentTopologyBundleHash = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func testAgentTopologyAdmission(t testing.TB) runtimeagenttopology.Admission {
	t.Helper()
	admission, err := runtimeagenttopology.StaticAdmission(
		"store-test-source-set-v1",
		testAgentTopologyBundleHash,
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct test agent topology admission: %v", err)
	}
	return admission
}

func testAgentTopologyJSON(t testing.TB) []byte {
	t.Helper()
	raw, err := json.Marshal(testAgentTopologyAdmission(t))
	if err != nil {
		t.Fatalf("marshal test agent topology admission: %v", err)
	}
	return raw
}

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
	memory := agentmemory.PlatformDefault()
	if strings.TrimSpace(fields.FlowInstancePath) != "" {
		memory = agentmemory.Authored(true)
	}
	cfg := withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            fields.AgentID,
		Identity:      identity,
		Role:          "worker",
		Type:          "stub",
		Model:         "regular",
		Memory:        memory,
		FlowPath:      fields.FlowInstancePath,
	})
	projection, err := projectPersistedAgentConfig(cfg, "")
	if err != nil {
		t.Fatalf("project test agent row: %v", err)
	}
	now := time.Now().UTC()
	processAuthorityID := "00000000-0000-0000-0000-00000000a001"
	processBootID := "00000000-0000-0000-0000-00000000b001"
	generationGrantID := "00000000-0000-0000-0000-00000000c001"
	runtimeInstanceID := "00000000-0000-0000-0000-00000000d001"
	query := `
		INSERT INTO agents (
			agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance,
			role, model, llm_backend, memory_enabled, memory_source,
			runtime_descriptor, status, created_at,
			lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_run_mode,
			lifecycle_process_authority_id, lifecycle_process_owner_id,
			lifecycle_process_boot_id, lifecycle_generation_grant_id,
			lifecycle_bundle_hash,
			lifecycle_runtime_instance_id, lifecycle_runtime_generation,
			topology_authority_kind, topology_admission, execution_lifetime
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'worker', 'regular', 'claude_cli', ?, ?, ?, ?, ?, 'running', 1, 1, 'standard', ?, ?, ?, ?, ?, ?, 1, 'static_declaration_plan', ?, 'durable_managed')
	`
	args := []any{
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		memory.Enabled, string(memory.Source), projection.RuntimeDescriptor, status, now,
		processAuthorityID, "store-test-seed", processBootID, generationGrantID,
		testAgentTopologyBundleHash, runtimeInstanceID, testAgentTopologyJSON(t),
	}
	if postgres {
		query = `
			INSERT INTO agents (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				role, model, llm_backend, memory_enabled, memory_source,
				runtime_descriptor, status, created_at,
				lifecycle_phase, lifecycle_generation, lifecycle_runtime_epoch, lifecycle_run_mode,
				lifecycle_process_authority_id, lifecycle_process_owner_id,
				lifecycle_process_boot_id, lifecycle_generation_grant_id,
				lifecycle_bundle_hash,
				lifecycle_runtime_instance_id, lifecycle_runtime_generation,
				topology_authority_kind, topology_admission, execution_lifetime
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'worker', 'regular', 'claude_cli', $8, $9, $10::jsonb, $11, $12, 'running', 1, 1, 'standard', $13::uuid, $14, $15::uuid, $16::uuid, $17, $18::uuid, 1, 'static_declaration_plan', $19::jsonb, 'durable_managed')
		`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed test agent row: %v", err)
	}
}
