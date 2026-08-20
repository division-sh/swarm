package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func managedConformanceExecutionContext(t testing.TB, ctx context.Context, authorityID string) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authorityID,
		1,
		"",
		"conformance-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build conformance managed execution admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedConformanceTurnRecord(t testing.TB, rec runtimellm.AgentTurnRecord) runtimellm.AgentTurnRecord {
	t.Helper()
	if rec.Identity.Agent.IsZero() {
		rec.Identity = conformanceAgentMemoryIdentity(t, rec.RunID, rec.AgentID)
		rec.FlowInstance = rec.Identity.FlowInstance()
	}
	runtimeMode := "task"
	if rec.Memory.Enabled {
		runtimeMode = "session"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: rec.Identity.Agent, RuntimeMode: runtimeMode, Provider: "conformance", Transport: "api", ProviderContract: "conformance-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: "conformance-persistence", RunID: rec.RunID, SessionID: rec.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build conformance managed capability surface: %v", err)
	}
	rec.CapabilitySurface = &surface
	return rec
}

// persistConformanceAgentTurnReadbackFixture seeds the immutable projection
// exercised by conformance readers. Runtime writes are owned by completion
// settlement; this test fixture is intentionally not exposed by the store.
func persistConformanceAgentTurnReadbackFixture(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	selected *store.PostgresStore,
	rec runtimellm.AgentTurnRecord,
) error {
	t.Helper()
	rec = managedConformanceTurnRecord(t, rec)
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return err
	}
	identity := rec.Identity.Normalize()
	fields, err := identity.Agent.Normalize().StorageFields()
	if err != nil {
		return err
	}
	if rec.CapabilitySurface == nil {
		return fmt.Errorf("conformance turn fixture requires managed capability surface")
	}
	if err := selected.SaveManagedCapabilitySurface(ctx, *rec.CapabilitySurface); err != nil {
		return err
	}
	rec = runtimellm.CanonicalizeTurnForPersistence(rec)
	if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
		return err
	}
	toolCalls, err := json.Marshal(rec.ToolCalls)
	if err != nil {
		return err
	}
	emittedEvents, err := json.Marshal(rec.EmittedEvents)
	if err != nil {
		return err
	}
	turnBlocks, err := json.Marshal(rec.TurnBlocks)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if plan.Enabled {
		result, err := tx.ExecContext(ctx, `
			UPDATE agent_sessions SET updated_at=now()
			WHERE session_id=$1::uuid AND run_id=$2::uuid AND agent_id=$3
			  AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6
			  AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9
			  AND memory_enabled=TRUE AND status='active'
		`, rec.SessionID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("conformance fixture has no exact active memory row")
		}
	} else if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,FALSE,$10,NULLIF($11,'')::uuid,'[]'::jsonb,1,'{}'::jsonb,'active',now(),now())
	`, rec.SessionID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, string(plan.Source), strings.TrimSpace(rec.EntityID)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls, emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, created_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9::uuid,$10,$11,$12,NULLIF($13,'')::uuid,
			NULLIF($14,'')::uuid,NULLIF($15,''),NULLIF($16,''),$17::uuid,$18::jsonb,$19::jsonb,
			CASE WHEN $20='' THEN NULL ELSE $20::jsonb END,CASE WHEN $21='' THEN NULL ELSE $21::jsonb END,
			$22::jsonb,$23,$24,$25,'live',now()
		)
	`, rec.CapabilitySurface.Authority.ID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, rec.SessionID, fields.FlowInstancePath, plan.Enabled, string(plan.Source),
		strings.TrimSpace(rec.EntityID), strings.TrimSpace(rec.TriggerEventID), strings.TrimSpace(rec.TriggerEventType), strings.TrimSpace(rec.TaskID),
		rec.CapabilitySurface.ID, string(toolCalls), string(emittedEvents), strings.TrimSpace(string(rec.RequestPayload)), strings.TrimSpace(string(rec.ResponseRaw)),
		string(turnBlocks), rec.ParseOK, int(rec.Latency/time.Millisecond), rec.RetryCount)
	if err != nil {
		return err
	}
	return tx.Commit()
}
