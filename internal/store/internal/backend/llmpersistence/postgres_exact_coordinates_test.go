package llmpersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This isolates the real physical writers and collector with PostgreSQL UUID
// storage. It does not claim lifecycle admission or finalizer/ledger coverage.
func TestPostgresLLMExactEffectsUseStoredUUIDCoordinates(t *testing.T) {
	_, db, _ := testutil.StartEmptyPostgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE agent_conversation_audits (
		session_id UUID PRIMARY KEY, run_id UUID NOT NULL, agent_id TEXT,
		agent_name_owner TEXT, agent_name_source TEXT, agent_route_presence TEXT,
		flow_scope_key TEXT, flow_instance_id TEXT, flow_instance TEXT,
		memory_enabled BOOLEAN, memory_source TEXT, entity_id UUID,
		conversation JSONB, turn_count INTEGER, runtime_state JSONB,
		status TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE agent_sessions (LIKE agent_conversation_audits INCLUDING ALL)`); err != nil {
		t.Fatal(err)
	}
	name, err := agentidentity.DeclaredName("uuid-agent", "uuid-owner")
	if err != nil {
		t.Fatal(err)
	}
	for _, spelling := range []string{"canonical", "uppercase", "compact"} {
		t.Run(spelling, func(t *testing.T) {
			spell := func(id string) string {
				switch spelling {
				case "uppercase":
					return strings.ToUpper(id)
				case "compact":
					return strings.ReplaceAll(id, "-", "")
				default:
					return id
				}
			}
			sessionID, runID, nextRunID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			identity, err := agentidentity.New(spell(runID), name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			rec := runtimellm.AgentTurnRecord{
				SessionID: spell(sessionID), RunID: identity.RunID, Identity: identity,
				AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(),
				Memory: agentmemory.PlatformDefault(),
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			owner := &LLMPostgresOwner{}
			assertEffects := func(got *runforkrevision.Effects, family runforkrevision.Family, runs ...string) {
				t.Helper()
				want := runforkrevision.NewEffects()
				for _, run := range runs {
					if err := want.AddFact(run, family, sessionID); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("writer did not contribute stored UUID coordinates: got=%#v want=%#v", got, want)
				}
			}
			for _, step := range []string{"insert", "same-owner", "move-owner"} {
				if step == "move-owner" {
					rec.Identity.RunID, rec.RunID = spell(nextRunID), spell(nextRunID)
				}
				effects := runforkrevision.NewEffects()
				if err := owner.EnsureCompletionTurnMemoryTx(ctx, tx, effects, rec); err != nil {
					t.Fatalf("%s: %v", step, err)
				}
				if step == "move-owner" {
					assertEffects(effects, runforkrevision.FamilyAgentConversationAudits, runID, nextRunID)
				} else {
					assertEffects(effects, runforkrevision.FamilyAgentConversationAudits, runID)
				}
			}
			var storedSessionID, storedRunID string
			var turns int
			if err := tx.QueryRowContext(ctx, `SELECT session_id::text, run_id::text, turn_count FROM agent_conversation_audits WHERE session_id=$1::uuid`, rec.SessionID).Scan(&storedSessionID, &storedRunID, &turns); err != nil {
				t.Fatal(err)
			}
			if storedSessionID != sessionID || storedRunID != nextRunID || turns != 3 {
				t.Fatalf("audit readback: session=%s run=%s turns=%d", storedSessionID, storedRunID, turns)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO agent_sessions SELECT * FROM agent_conversation_audits WHERE session_id=$1::uuid`, rec.SessionID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET memory_enabled=TRUE WHERE session_id=$1::uuid`, rec.SessionID); err != nil {
				t.Fatal(err)
			}
			rec.Memory = agentmemory.Authored(true)
			effects := runforkrevision.NewEffects()
			if err := owner.EnsureCompletionTurnMemoryTx(ctx, tx, effects, rec); err != nil {
				t.Fatalf("live memory touch: %v", err)
			}
			assertEffects(effects, runforkrevision.FamilyAgentSessions, nextRunID)
		})
	}
}
