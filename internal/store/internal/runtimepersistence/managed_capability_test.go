package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storefailurecodec "github.com/division-sh/swarm/internal/store/internal/failurecodec"
	"github.com/google/uuid"
)

func managedCompletionTestSurface(t testing.TB, authority runtimeeffects.Authority, adapter string) managedcapabilities.Surface {
	t.Helper()
	executionKind := managedcapabilities.ExecutionNormalAgent
	executionAuthorityID := authority.ID
	runID := authority.Target.RunID
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		executionKind = managedcapabilities.ExecutionSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		if runID == "" {
			runID = authority.SelectedFork.ForkRunID
		}
	}
	transport := "api"
	switch adapter {
	case "claude_cli":
		transport = "cli"
	case "mock_python":
		transport = "in_process"
	}
	runtimeMode := "task"
	if authority.Target.Memory.Enabled {
		runtimeMode = "session"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Target.AgentIdentity, RuntimeMode: runtimeMode,
		Provider: adapter, Transport: transport, ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: executionKind, ExecutionAuthorityID: executionAuthorityID,
			RunID: runID, SessionID: authority.Target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed completion test surface: %v", err)
	}
	return surface
}

func managedExecutionStoreTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"store-test-authority",
		1,
		"",
		"store-test-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedNormalEffectStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	ctx = managedExecutionStoreTestContext(t, ctx)
	admission, _ := managedexecution.FromContext(ctx)
	principal, err := authority.Normal.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint normal managed-effect identity: %v", err)
	}
	turnID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-turn:"+principal)).String()
	sessionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-session:"+principal)).String()
	runID := managedNormalEffectStoreTestRunID(authority.Normal.AgentID)
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: authority.Normal.AgentID,
		AgentIdentity: authority.Normal.Identity, SessionID: sessionID, Memory: agentmemory.PlatformDefault(),
		FlowInstance: authority.Normal.Identity.FlowInstance(),
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Normal.Identity, RuntimeMode: "task", Provider: "store-test", Transport: "api",
		ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: admission.ExecutionAuthorityID,
			RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build normal managed-effect test surface: %v", err)
	}
	return managedcapabilities.WithContext(ctx, surface)
}

func managedNormalEffectStoreTestRunID(agentID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-run:"+agentID)).String()
}

func managedSelectedExecutionStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork,
		authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation,
		authority.SelectedFork.ForkRunID,
		"store-test-selected-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedAgentTurnRecordForTest(t testing.TB, rec runtimellm.AgentTurnRecord) runtimellm.AgentTurnRecord {
	t.Helper()
	if rec.Identity == (agentmemory.Identity{}) {
		rec.Identity = testAgentMemoryIdentity(t, rec.RunID, rec.AgentID, rec.FlowInstance)
	}
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{Identity: rec.Identity.Agent, RuntimeEpoch: 1, AgentID: rec.AgentID, Generation: 1},
		"store-test-owner",
		time.Unix(1, 0).UTC().Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: rec.RunID,
		AgentID: rec.AgentID, AgentIdentity: rec.Identity.Agent, SessionID: rec.SessionID,
		Memory: rec.Memory, FlowInstance: rec.FlowInstance, EntityID: rec.EntityID,
	}
	surface := managedCompletionTestSurface(t, authority, "anthropic_api")
	rec.CapabilitySurface = &surface
	return rec
}

type managedAgentTurnFixtureStore interface {
	SaveManagedCapabilitySurface(context.Context, managedcapabilities.Surface) error
}

// persistManagedAgentTurnReadbackFixture seeds append-only evidence for reader
// tests. Production writers must use the completion settlement owner.
func persistManagedAgentTurnReadbackFixture(t testing.TB, ctx context.Context, store managedAgentTurnFixtureStore, rec runtimellm.AgentTurnRecord) error {
	t.Helper()
	rec = managedAgentTurnRecordForTest(t, rec)
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return err
	}
	identity := rec.Identity.Normalize()
	if strings.TrimSpace(rec.SessionID) == "" {
		return fmt.Errorf("session_id is required")
	}
	if err := identity.ValidateOwner(); err != nil {
		return err
	}
	if plan.Enabled {
		if err := identity.Validate(); err != nil {
			return err
		}
	}
	if strings.TrimSpace(rec.RunID) != identity.RunID ||
		strings.TrimSpace(rec.AgentID) != identity.AgentID() ||
		strings.Trim(strings.TrimSpace(rec.FlowInstance), "/") != identity.FlowInstance() {
		return fmt.Errorf("agent turn display fields do not match concrete identity")
	}
	rec = runtimellm.CanonicalizeTurnForPersistence(rec)
	if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
		return fmt.Errorf("validate canonical runtime_log turn_blocks: %w", err)
	}
	if rec.CapabilitySurface == nil {
		return fmt.Errorf("managed turn fixture requires exact capability surface")
	}
	if err := store.SaveManagedCapabilitySurface(ctx, *rec.CapabilitySurface); err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	toolCalls, err := json.Marshal(rec.ToolCalls)
	if err != nil {
		return err
	}
	toolCalls = normalizeManagedTurnFixtureJSONArray(toolCalls)
	emittedEvents, err := json.Marshal(rec.EmittedEvents)
	if err != nil {
		return err
	}
	emittedEvents = normalizeManagedTurnFixtureJSONArray(emittedEvents)
	turnBlocks, err := json.Marshal(rec.TurnBlocks)
	if err != nil {
		return err
	}
	turnBlocks = normalizeManagedTurnFixtureJSONArray(turnBlocks)
	failurePayload := ""
	if encoded, err := storefailurecodec.Encode(rec.Failure); err != nil {
		return err
	} else if encoded != nil {
		failurePayload = encoded.(string)
	}
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}
	now := time.Now().UTC()

	switch selected := store.(type) {
	case *PostgresStore:
		return persistPostgresManagedAgentTurnReadbackFixture(ctx, selected.backend.ConstructionHandle(), rec, plan.Enabled, string(plan.Source), fields, string(toolCalls), string(emittedEvents), string(turnBlocks), failurePayload, latencyMS)
	case *SQLiteRuntimeStore:
		return persistSQLiteManagedAgentTurnReadbackFixture(ctx, selected.backend.ConstructionHandle(), rec, plan.Enabled, string(plan.Source), fields, string(toolCalls), string(emittedEvents), string(turnBlocks), failurePayload, latencyMS, now)
	default:
		return fmt.Errorf("unsupported managed turn fixture store %T", store)
	}
}

func persistPostgresManagedAgentTurnReadbackFixture(
	ctx context.Context,
	db *sql.DB,
	rec runtimellm.AgentTurnRecord,
	memoryEnabled bool,
	memorySource string,
	fields agentidentity.StorageFields,
	toolCalls, emittedEvents, turnBlocks, failurePayload string,
	latencyMS int,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if memoryEnabled {
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
			return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", rec.RunID, rec.AgentID, rec.FlowInstance, rec.SessionID)
		}
	} else if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,FALSE,$10,NULLIF($11,'')::uuid,'[]'::jsonb,1,'{}'::jsonb,'active',now(),now())
		ON CONFLICT (session_id) DO UPDATE SET
			run_id=EXCLUDED.run_id, agent_id=EXCLUDED.agent_id,
			agent_name_owner=EXCLUDED.agent_name_owner, agent_name_source=EXCLUDED.agent_name_source,
			agent_route_presence=EXCLUDED.agent_route_presence, flow_scope_key=EXCLUDED.flow_scope_key,
			flow_instance_id=EXCLUDED.flow_instance_id, flow_instance=EXCLUDED.flow_instance,
			memory_enabled=FALSE, memory_source=EXCLUDED.memory_source, entity_id=EXCLUDED.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=now()
	`, rec.SessionID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, memorySource, strings.TrimSpace(rec.EntityID)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls, emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, failure, created_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9::uuid,$10,$11,$12,NULLIF($13,'')::uuid,
			NULLIF($14,'')::uuid,NULLIF($15,''),NULLIF($16,''),$17::uuid,$18::jsonb,$19::jsonb,
			CASE WHEN $20='' THEN NULL ELSE $20::jsonb END,CASE WHEN $21='' THEN NULL ELSE $21::jsonb END,
			$22::jsonb,$23,$24,$25,'live',CASE WHEN $26='' THEN NULL ELSE $26::jsonb END,now()
		)
	`, rec.CapabilitySurface.Authority.ID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, rec.SessionID, fields.FlowInstancePath, memoryEnabled, memorySource,
		strings.TrimSpace(rec.EntityID), strings.TrimSpace(rec.TriggerEventID), strings.TrimSpace(rec.TriggerEventType), strings.TrimSpace(rec.TaskID),
		rec.CapabilitySurface.ID, toolCalls, emittedEvents, storeagent.NormalizeJSONPayload(rec.RequestPayload), storeagent.NormalizeJSONPayload(rec.ResponseRaw),
		turnBlocks, rec.ParseOK, latencyMS, rec.RetryCount, failurePayload)
	if err != nil {
		return fmt.Errorf("insert agent turn readback fixture: %w", err)
	}
	return tx.Commit()
}

func persistSQLiteManagedAgentTurnReadbackFixture(
	ctx context.Context,
	db *sql.DB,
	rec runtimellm.AgentTurnRecord,
	memoryEnabled bool,
	memorySource string,
	fields agentidentity.StorageFields,
	toolCalls, emittedEvents, turnBlocks, failurePayload string,
	latencyMS int,
	now time.Time,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if memoryEnabled {
		result, err := tx.ExecContext(ctx, `
			UPDATE agent_sessions SET updated_at=?
			WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=?
			  AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=?
			  AND flow_instance_id=? AND flow_instance=? AND memory_enabled=1 AND status='active'
		`, now, rec.SessionID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", rec.RunID, rec.AgentID, rec.FlowInstance, rec.SessionID)
		}
	} else if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,0,?,NULLIF(?,''),'[]',1,'{}','active',?,?)
		ON CONFLICT(session_id) DO UPDATE SET
			run_id=excluded.run_id, agent_id=excluded.agent_id,
			agent_name_owner=excluded.agent_name_owner, agent_name_source=excluded.agent_name_source,
			agent_route_presence=excluded.agent_route_presence, flow_scope_key=excluded.flow_scope_key,
			flow_instance_id=excluded.flow_instance_id, flow_instance=excluded.flow_instance,
			memory_enabled=0, memory_source=excluded.memory_source, entity_id=excluded.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=excluded.updated_at
	`, rec.SessionID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, memorySource, strings.TrimSpace(rec.EntityID), now, now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, tool_calls, emitted_events,
			request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, execution_mode, failure, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, rec.CapabilitySurface.Authority.ID, rec.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, rec.SessionID, fields.FlowInstancePath, memoryEnabled, memorySource,
		nullStringForManagedTurnFixture(rec.EntityID), nullStringForManagedTurnFixture(rec.TriggerEventID), nullStringForManagedTurnFixture(rec.TriggerEventType),
		nullStringForManagedTurnFixture(rec.TaskID), rec.CapabilitySurface.ID, toolCalls, emittedEvents,
		nullStringForManagedTurnFixture(storeagent.NormalizeJSONPayload(rec.RequestPayload)), nullStringForManagedTurnFixture(storeagent.NormalizeJSONPayload(rec.ResponseRaw)),
		turnBlocks, rec.ParseOK, latencyMS, rec.RetryCount, "live", nullStringForManagedTurnFixture(failurePayload), now)
	if err != nil {
		return fmt.Errorf("insert SQLite agent turn readback fixture: %w", err)
	}
	return tx.Commit()
}

func nullStringForManagedTurnFixture(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func normalizeManagedTurnFixtureJSONArray(raw []byte) []byte {
	normalized := storeagent.NormalizeJSONPayload(raw)
	if normalized == "" || normalized == "null" {
		return []byte("[]")
	}
	return []byte(normalized)
}

func withManagedCompletionTestSurface(t testing.TB, ctx context.Context, authority runtimeeffects.Authority, adapter string) context.Context {
	t.Helper()
	kind := managedexecution.KindNormalRuntime
	executionAuthorityID := authority.ID
	generation := authority.FenceGeneration
	runID := ""
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		kind = managedexecution.KindSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		generation = authority.SelectedFork.Generation
		runID = authority.SelectedFork.ForkRunID
	}
	admission, err := managedexecution.New(
		kind,
		executionAuthorityID,
		generation,
		runID,
		"store-test-completion-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed completion test admission: %v", err)
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	return managedcapabilities.WithContext(ctx, managedCompletionTestSurface(t, authority, adapter))
}

func applyManagedCompletionTestSurface(t testing.TB, turn *runtimeeffects.CompletionAgentTurn, authority runtimeeffects.Authority, adapter string) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface := managedCompletionTestSurface(t, authority, adapter)
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion test surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
}

func applyManagedCompletionContextSurface(t testing.TB, ctx context.Context, turn *runtimeeffects.CompletionAgentTurn) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("managed completion test surface is missing")
	}
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion context surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
}

type managedCapabilityTestStore interface {
	SaveManagedCapabilitySurface(context.Context, managedcapabilities.Surface) error
}

func seedManagedAgentTurnCapabilitySurface(
	t testing.TB,
	store managedCapabilityTestStore,
	runID string,
	identity agentidentity.Identity,
	sessionID, turnID, runtimeMode, scopeKey string,
) string {
	t.Helper()
	now := time.Unix(1, 0).UTC()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1,
			Identity:     identity,
			AgentID:      identity.AgentID(),
			Generation:   1,
		},
		"store-test-owner",
		now.Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID,
		AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: sessionID,
		Memory: agentmemory.PlatformDefault(), FlowInstance: identity.FlowInstance(),
		EntityID: scopeKey,
	}
	if runtimeMode != "task" {
		authority.Target.Memory = agentmemory.Authored(true)
	}
	surface := managedCompletionTestSurface(t, authority, "anthropic_api")
	if err := store.SaveManagedCapabilitySurface(context.Background(), surface); err != nil {
		t.Fatalf("seed managed agent-turn capability surface: %v", err)
	}
	return surface.ID
}

func TestCompletionRecoveryRejectsSameSlugSiblingCapabilityPrincipal(t *testing.T) {
	identityA := testAgentIdentity(t, "recovery-worker", "review/inst-a")
	identityB := testAgentIdentity(t, "recovery-worker", "review/inst-b")
	targetA := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(),
		AgentID: identityA.AgentID(), AgentIdentity: identityA, SessionID: uuid.NewString(),
		Memory: agentmemory.PlatformDefault(), FlowInstance: identityA.FlowInstance(),
	}
	authorityFor := func(identity agentidentity.Identity) runtimeeffects.Authority {
		target := targetA
		target.AgentIdentity = identity
		target.FlowInstance = identity.FlowInstance()
		token := runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1, Identity: identity, AgentID: identity.AgentID(), Generation: 1,
		}
		authority := runtimeeffects.NormalAgentAuthority(token, "recovery-test-owner", time.Now().UTC().Add(time.Minute))
		authority.Target = target
		return authority
	}
	surfaceA := managedCompletionTestSurface(t, authorityFor(identityA), "anthropic_api")
	surfaceB := managedCompletionTestSurface(t, authorityFor(identityB), "anthropic_api")
	evidence := completionRecoveryAuthorityEvidence{
		ActorTokenID: identityA.AgentID(), ExecutionMode: string(runtimeeffects.ExecutionModeLive),
	}
	evidence.UsageTarget.Kind = string(targetA.Kind)
	evidence.UsageTarget.ID = targetA.ID
	evidence.UsageTarget.RunID = targetA.RunID
	evidence.UsageTarget.AgentID = targetA.AgentID
	evidence.UsageTarget.AgentIdentity = targetA.AgentIdentity
	evidence.UsageTarget.SessionID = targetA.SessionID
	evidence.UsageTarget.MemoryEnabled = targetA.Memory.Enabled
	evidence.UsageTarget.MemorySource = string(targetA.Memory.Source)
	evidence.UsageTarget.FlowInstance = targetA.FlowInstance
	authorityEvidence, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal recovery authority evidence: %v", err)
	}
	recovered := completionRecoveryAttempt{
		OperationID: uuid.NewString(), AttemptID: uuid.NewString(),
		AuthorityKind: string(runtimeeffects.AuthorityNormalAgent), AuthorityID: identityA.AgentID(),
		AuthorityEvidence: string(authorityEvidence),
		OperationMode:     string(runtimeeffects.ExecutionModeLive), AttemptMode: string(runtimeeffects.ExecutionModeLive),
		Adapter: "anthropic_api", Transport: "api", State: string(runtimeeffects.StateAuthorized),
		TargetKind: string(targetA.Kind), TargetID: targetA.ID,
	}
	identityFields, err := identityA.StorageFields()
	if err != nil {
		t.Fatalf("encode recovery identity: %v", err)
	}
	recovered.AgentID = identityFields.AgentID
	recovered.AgentNameOwner = identityFields.NameOwner
	recovered.AgentNameSource = identityFields.NameSource
	recovered.AgentRoutePresence = identityFields.RoutePresence
	recovered.FlowScopeKey = identityFields.FlowScopeKey
	recovered.FlowInstanceID = identityFields.FlowInstanceID
	recovered.FlowInstance = identityFields.FlowInstancePath
	recovered.RuntimeEpoch = 1
	recovered.Generation = 1
	recovered.OriginDeliveryID = uuid.NewString()
	recovered.OriginRunID = targetA.RunID
	recovered.OriginRouteIdentity = "managed-capability-recovery-origin"
	recovered.OriginClaimToken = uuid.NewString()
	recovered.OriginClaimVersion = 1
	recovered.OriginSubscriber = identityA.AgentID()
	encodeSurface := func(surface managedcapabilities.Surface) string {
		raw, err := json.Marshal(surface)
		if err != nil {
			t.Fatalf("marshal recovery capability surface: %v", err)
		}
		return string(raw)
	}

	recovered.CapabilitySurfaceID = surfaceB.ID
	recovered.CapabilitySurface = encodeSurface(surfaceB)
	if _, _, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err == nil {
		t.Fatal("completion recovery accepted a same-slug sibling capability principal")
	}

	recovered.CapabilitySurfaceID = surfaceA.ID
	recovered.CapabilitySurface = encodeSurface(surfaceA)
	if _, settlement, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("completion recovery rejected exact capability principal: %v", err)
	} else if settlement.AgentTurn == nil || settlement.AgentTurn.Identity.Agent != identityA {
		t.Fatalf("recovered exact agent turn = %#v", settlement.AgentTurn)
	}
}
